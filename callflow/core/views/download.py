# callflow/core/views/download_export.py
"""
On-demand data export: fetches live data for every requested system,
writes each one to its own Excel sheet as it completes, then zips
everything into one download.

Permission note:
  Access control is frontend-only (the Angular component reads the user's
  LDAP groups from localStorage and hides the button for users not in
  'callflow_download_files'). No server-side group check is applied here
  because Django receives all requests as AnonymousUser -- the app
  authenticates via LDAP and stores the result in localStorage, but never
  creates a Django session cookie, so any session-based check on this end
  would always fail.

Flow
----
  POST /callflow/api/export-data
    Body: { start_date, end_date, Complain_Type, msisdn_list,
            systems: ["smartcare","npp","splunk","viavi","remedy"] }
    -> 202 { job_id }

  GET  /callflow/api/export-data/<job_id>
    -> { status, step, progress_percent, result }

  GET  /callflow/api/export-data/file/<filename>
    -> streams the ZIP

Architecture note -- why Smartcare runs first:
  NPP   needs suspected_cells_raw  ]
  Viavi needs suspected_sites      ]  all from the Smartcare result
  Splunk needs suspected_sgw       ]
  Remedy needs suspected_sites     ]
  Once Smartcare finishes, the other four run in parallel.
"""

import zipfile
import logging
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.conf import settings
from django.http import FileResponse, Http404
from rest_framework.response import Response
from rest_framework import status as http_status

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..services.smartcare_service import fetch_smartcare_nodes_data
from ..services.npp_service import nppIntegration
from ..services.splunk_service import splunkIntegration
from ..services.viavi_service import viaviQueries
from ..services.remedy_service import remedyTicketsData
from ..utils.connections import viaviConnection
from ..utils.classes import Connection
from ..constants.general_constants import (
    SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
    DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD,
)

logger = logging.getLogger(__name__)

EXPORT_DIR_NAME = "exports"


def _export_dir() -> Path:
    d = Path(settings.OUTPUT_DIR) / EXPORT_DIR_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_excel(df: pd.DataFrame, path: Path, sheet_name: str = "Data"):
    """Writes DataFrame to Excel with better error handling."""
    try:
        if df is None or df.empty:
            df = pd.DataFrame({"message": ["No data returned for this system."]})
        df.to_excel(path, index=False, sheet_name=sheet_name[:31])
        logger.info(f"[EXPORT] Excel written: {path.name} ({len(df)} rows)")
        return True
    except Exception as e:
        logger.exception(f"[EXPORT] Failed to write Excel {path.name}: {e}")
        return False


# ---------------------------------------------------------------------------
# Per-system fetch helpers (each runs in its own thread)
# ---------------------------------------------------------------------------

def _fetch_npp(smartcare_result, window, log_path, ts, request_logger):
    suspected_cells_raw = smartcare_result.get("suspected_cells_raw") or []
    if not suspected_cells_raw:
        request_logger.warning("[EXPORT] NPP skipped — no suspected_cells_raw in Smartcare result")
        return pd.DataFrame()

    return nppIntegration(
        suspected_cells_raw=suspected_cells_raw,
        start_date=window["start_dt"], end_date=window["end_dt"],
        logs_file=log_path, timestamp=ts, resolution="daily",
    )


def _fetch_splunk(smartcare_result, window, log_path, ts, request_logger):
    processed_data = smartcare_result.get("processed_data", pd.DataFrame())
    if hasattr(processed_data, "empty") and not processed_data.empty and "SGW_PGW_NAME" in processed_data.columns:
        suspected_sgw = processed_data["SGW_PGW_NAME"].dropna().unique().tolist()
    else:
        suspected_sgw = []

    if not suspected_sgw:
        request_logger.warning("[EXPORT] Splunk skipped — no SGW/PGW names in Smartcare result")
        return pd.DataFrame()

    nodes = ["GGSN", "MME", "SGSN", "DNS", "DSC"]
    all_frames = []
    for node in nodes:
        try:
            df = splunkIntegration(
                settings.SPLUNK_USER, settings.SPLUNK_PASSWORD,
                window["query_start_1day_str"], window["query_end_1day_str"],
                log_path, node,request_logger
            )
            if not df.empty:
                df.insert(0, "node", node)
                all_frames.append(df)
        except Exception as e:
            request_logger.warning(f"[EXPORT] Splunk {node} failed: {e}")

    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


def _fetch_viavi(smartcare_result, window, log_path, ts, request_logger):
    processed_data = smartcare_result.get("processed_data", pd.DataFrame())
    if hasattr(processed_data, "empty") and not processed_data.empty and "modified_site_name" in processed_data.columns:
        suspected_sites = processed_data["modified_site_name"].dropna().unique().tolist()
    else:
        suspected_sites = []

    if not suspected_sites:
        request_logger.warning("[EXPORT] Viavi skipped — no site names in Smartcare result")
        return pd.DataFrame()

    viavi_conn = viaviConnection(log_path)
    return viaviQueries(
        viavi_conn,
        window["formatted_startDate"], window["formatted_endDate"],
        suspected_sites, log_path, ts,
    )


def _fetch_remedy(smartcare_result, window, log_path, ts, request_logger):
    processed_data = smartcare_result.get("processed_data", pd.DataFrame())
    if hasattr(processed_data, "empty") and not processed_data.empty and "modified_site_name" in processed_data.columns:
        suspected_sites = processed_data["modified_site_name"].dropna().unique().tolist()
    else:
        suspected_sites = []

    if not suspected_sites:
        request_logger.warning("[EXPORT] Remedy skipped — no site names in Smartcare result")
        return pd.DataFrame()

    conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
                      DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
    conn.connect()
    remedy_output, _ = remedyTicketsData(
        conn, window["start_dt"], window["end_dt"], suspected_sites, log_path, ts,
    )
    rows = []
    for site, data in (remedy_output or {}).items():
        for ticket in data.get("tickets", []):
            ticket["site"] = site
            rows.append(ticket)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


FETCHERS = {
    "npp":    _fetch_npp,
    "splunk": _fetch_splunk,
    "viavi":  _fetch_viavi,
    "remedy": _fetch_remedy,
}


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class DataExportAPIView(JobBackedAPIView):
    job_type = "data_export"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        msisdn_list = request.data.get("msisdn_list")
        if not msisdn_list:
            raise ValueError("msisdn_list is required")
        logger.info("shaiaaa")
        complain_type = request.data.get("Complain_Type", "Data")
        requested = request.data.get("systems", ["smartcare", "npp", "splunk", "viavi", "remedy"])

        log_path, ts = self.get_log_file("export", window["start_dt"], window["end_dt"])
        query_time_range = self.build_analysis_time_windows(window["start_dt"], window["end_dt"])

        return {
            "log_path": log_path, "ts": ts,
            "window": window, "query_time_range": query_time_range,
            "msisdn_list": msisdn_list, "complain_type": complain_type,
            "requested_systems": requested,
        }

    def run_job(self, job_id, request_logger, log_path, ts, window,
                query_time_range, msisdn_list, complain_type, requested_systems):

        clean_ts = ts.replace(':', '-').replace(' ', '_')
        export_dir = _export_dir()
        written_files = []
        failed_systems = []

        request_logger.info(f"[EXPORT] Starting export for msisdn_list: {msisdn_list[:100]}...")

        # 1. Smartcare (required)
        with log_step(request_logger, "fetch_smartcare", job_id, job_manager, 10):
            smartcare_result = fetch_smartcare_nodes_data(
                msisdn_list=msisdn_list,
                start_dt=window["start_dt"], end_dt=window["end_dt"],
                complain_type=complain_type,
                log_path=log_path, view="export", ts=ts,
            )

        if "smartcare" in requested_systems:
            sc_path = export_dir / f"smartcare_{clean_ts}.xlsx"
            with log_step(request_logger, "write_smartcare_excel", job_id, job_manager, 20):
                if _write_excel(smartcare_result.get("processed_data", pd.DataFrame()), sc_path, "Smartcare"):
                    written_files.append(("smartcare", sc_path))

        # 2. Parallel systems
        parallel_systems = [s for s in requested_systems if s in ["npp", "splunk", "viavi", "remedy"]]

        if parallel_systems:
            base_progress = 25
            with log_step(request_logger, "parallel_fetch", job_id, job_manager, base_progress):
                def run_one(sys_name):
                    try:
                        df = globals()[f"_fetch_{sys_name}"](smartcare_result, window, log_path, ts, request_logger)
                        path = export_dir / f"{sys_name}_{clean_ts}.xlsx"
                        if _write_excel(df, path, sys_name.capitalize()):
                            return sys_name, path, None
                        return sys_name, None, "Write failed"
                    except Exception as e:
                        request_logger.exception(f"{sys_name} failed")
                        return sys_name, None, str(e)

                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {executor.submit(run_one, s): s for s in parallel_systems}
                    for future in as_completed(futures):
                        sys_name, path, err = future.result()
                        if err:
                            failed_systems.append(sys_name)
                        elif path:
                            written_files.append((sys_name, path))

        # 3. ZIP
        with log_step(request_logger, "build_zip", job_id, job_manager, 90):
            zip_filename = f"callflow_export_{clean_ts}.zip"
            zip_path = export_dir / zip_filename

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for sys_name, file_path in written_files:
                    zf.write(file_path, arcname=f"{sys_name}/{file_path.name}")
                    request_logger.info(f"Added to ZIP: {sys_name}/{file_path.name}")

            size_kb = round(zip_path.stat().st_size / 1024, 1)
            request_logger.info(f"ZIP created: {zip_filename} ({size_kb} KB)")

        return {
            "zip_filename": zip_filename,
            "zip_url": f"/callflow/api/export-data/file/{zip_filename}",
            "systems_done": [s for s, _ in written_files],
            "failed": failed_systems,
            "size_kb": size_kb,
        }


class ExportFileDownloadAPIView(JobBackedAPIView):
    def get(self, request, filename=None):
        safe_name = Path(filename or "").name
        file_path = _export_dir() / safe_name

        if not file_path.exists():
            raise Http404(f"File not found: {safe_name}")

        return FileResponse(
            open(file_path, "rb"),
            content_type="application/zip",
            as_attachment=True,
            filename=safe_name,
        )
    """GET /export-data/file/<filename> — streams the pre-built ZIP."""

    def get(self, request, filename=None):
        safe_name = Path(filename or "").name
        if not safe_name.endswith(".zip"):
            return Response({"error": "Invalid filename."}, status=http_status.HTTP_400_BAD_REQUEST)

        file_path = _export_dir() / safe_name
        if not file_path.exists():
            raise Http404(f"File '{safe_name}' not found.")

        logger.info(f"[EXPORT SERVE] {safe_name}")
        return FileResponse(
            open(file_path, "rb"),
            content_type="application/zip",
            as_attachment=True,
            filename=safe_name,
        )