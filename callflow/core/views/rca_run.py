# callflow/core/views/rca_run.py
"""
POST /callflow/api/rca/run                        -> 202 {job_id, status: "PENDING"}  (job_id == request_id)
     {"msisdn": "010...,010...", "complain_type": "data", "start": "...", "end": "..."}
POST /callflow/api/rca/sites/run                  -> same, for sites (RCASiteRunAPIView; no Trino)
     {"sitelist": ["SITE_A", "SITE_B"], "start": "...", "end": "..."}
     GET routes below exist under both prefixes.
GET  /callflow/api/rca/run/{id}                   -> PENDING/RUNNING {request_id, status, progress_percent}
                                                     DONE: the RCA report (rca_runner/report.py)
                                                     FAILED: {request_id, status, error: {code, message}}
GET  /callflow/api/rca/run/{id}/findings.csv      -> one row per finding
GET  /callflow/api/rca/run/{id}/diagnostics       -> logs, file loading, browser and pipeline detail

One request runs every CallFlow system through the EXISTING orchestration
(AnalysisOrchestratorAPIView.run_job: same DAG, services, analyzers, RCA file
writers), then runs the EXISTING RCA HTML tool on that request's files in
headless Chromium (rca_runner.browser_runner) and returns the tool's own
results. No RCA logic is re-implemented in Python.

Files, logs and the final result for each request live under
    OUTPUT_DIR/rca_engine/rca/{request_id}/      (CSV inputs, manifest.json, logs/, rca_result.json,
                                          rca_tool_log.json, browser_console.log)
so concurrent requests never share a file.

Concurrency (shared by all Gunicorn workers, see rca_runner.limits):
    RCA_RUN_MAX_SYSTEM_JOBS  jobs querying Trino/Smartcare/NPP/Splunk/Viavi/Remedy at once (default 2)
    RCA_RUN_MAX_BROWSERS     headless browsers at once (default 1)
"""

import json
import logging
import time
from pathlib import Path

from django.conf import settings
from django.http import HttpResponse
from rest_framework import status as http_status
from rest_framework.response import Response

from ..jobs import job_manager
from ..orchestration import rca_files as rf
from ..orchestration.pipeline import FILE_SYSTEMS
from ..rca_runner import limits, site_mode
from ..rca_runner.browser_runner import RCARunError, run_rca_tool
from ..rca_runner.report import build_report, findings_csv
from .base import BaseCallFlowAPIView, parse_time_window
from .orchestrated_analysis import AnalysisOrchestratorAPIView

logger = logging.getLogger(__name__)

RESULT_FILE = "rca_result.json"          # the public report
ERROR_FILE = "rca_error.json"
DIAGNOSTICS_FILE = "rca_diagnostics.json"  # pipeline + tool detail, never in the report
CSV_FILE = "rca_findings.csv"
SITE_KPI_FILE = "smartcare_site_kpis.json"   # site run: the Smartcare site-KPI result (diagnostics)


def _setting(name, default):
    return getattr(settings, name, default)


def _tool_path():
    return Path(_setting("RCA_TOOL_PATH", Path(settings.BASE_DIR) / "rca_tool_V24.html"))


def _write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


class RCARunAPIView(AnalysisOrchestratorAPIView):
    job_type = "rca_run"
    output_subdir = "rca_engine"
    log_prefix = "rca_run"
    link_base = "/callflow/api/rca/run"

    def get_log_file(self, prefix, start_date, end_date):
        # Same contract as the base class, but the request log lives under rca_engine/logs.
        logs_dir = Path(settings.OUTPUT_DIR) / self.output_subdir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir / f"{prefix}.txt", f"{start_date}_{end_date}"

    def build_log_context(self, request):
        req = site_mode.parse_msisdn_request(request.data)
        window = parse_time_window(req["start"], req["end"])
        log_path, ts = self.get_log_file(self.log_prefix, window["start_dt"], window["end_dt"])
        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": req["msisdn_list"], "complain_type": req["complain_type"],
            "imsi": req["imsi"],
            "query_time_range": self.build_analysis_time_windows(window["start_dt"], window["end_dt"]),
        }

    def _report_systems(self, systems):
        return systems

    def _request_input(self, msisdn_list, complain_type, extra):
        return {"mode": "msisdn", "sites": None, "msisdn_count": len(msisdn_list),
                "complain_type": complain_type}

    def get(self, request, job_id=None):
        if not job_id:
            return Response({"error": "job_id is required"}, status=http_status.HTTP_400_BAD_REQUEST)
        try:
            rdir = rf.request_dir(self._base(), job_id)
        except ValueError:
            return Response({"error": "invalid job_id"}, status=http_status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is not None and job.get("status") in ("PENDING", "RUNNING"):
            return Response({"request_id": job_id, "status": job["status"],
                             "progress_percent": job.get("progress_percent")})
        if job is not None and job.get("status") == "DONE" and job.get("result"):
            return Response(job["result"])
        # Finished (or the 30-min job record expired): answer from the request files.
        if (rdir / RESULT_FILE).is_file():
            return Response(json.loads((rdir / RESULT_FILE).read_text(encoding="utf-8")))
        if (rdir / ERROR_FILE).is_file() or (job is not None and job.get("status") == "FAILED"):
            return Response(_failed_body(job_id, rdir, job, self.link_base))
        return Response({"error": "job not found or expired"}, status=http_status.HTTP_404_NOT_FOUND)

    # ------------------------------------------------------------------ job --
    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list,
                complain_type, imsi, query_time_range, **extra):
        request_id = job_id
        base = self._base()
        rdir = rf.request_dir(base, request_id)
        lock_dir = Path(settings.OUTPUT_DIR) / self.output_subdir / "locks"
        wait_s = float(_setting("RCA_RUN_SLOT_WAIT_S", 1800))
        t0 = time.monotonic()

        def queued(stage):
            last = [0.0]

            def on_wait(waited):
                if waited - last[0] >= 30 or last[0] == 0.0:   # throttle Redis writes
                    last[0] = waited or 0.001
                    job_manager.update_job(job_id, status="RUNNING",
                                           step=f"queued: waiting for a free {stage} slot ({waited:.0f}s)")
            return on_wait

        try:
            # 1) every system, through the existing orchestration (unchanged)
            with limits.slot(lock_dir, "rca_run_systems", _setting("RCA_RUN_MAX_SYSTEM_JOBS", 2),
                             wait_s, on_wait=queued("systems")):
                request_logger.info(f"[RCA-RUN] request_id={request_id} running CallFlow systems")
                request_logger.info(f"[RCA-RUN] request_id={request_id} job_type={self.job_type} "
                                    f"msisdns={len(msisdn_list)} sites={extra.get('sitelist')}")
                analysis = super().run_job(job_id, request_logger, log_path, ts, window, msisdn_list,
                                           complain_type, imsi, query_time_range, **extra)
            systems = self._report_systems(analysis.get("systems") or {})

            # 2) only start once every file that is still being written is finished
            rca_inputs = self._wait_for_files(base, request_id, request_logger)
            listing = rf.list_ready_files(base, request_id)
            if not listing.get("kpi"):
                raise RCARunError("RCA_NO_INPUT_FILES",
                                  "no KPI file is ready for the RCA tool (every KPI system failed or "
                                  "returned no data)", {"systems": systems, "rca_inputs": rca_inputs})

            # 3) the existing RCA tool, in a headless browser
            job_manager.update_job(job_id, status="RUNNING", step="rca_tool", progress_percent=95,
                                   rca_run={"status": "RUNNING"})
            with limits.slot(lock_dir, "rca_run_browser", _setting("RCA_RUN_MAX_BROWSERS", 1),
                             wait_s, on_wait=queued("browser")):
                request_logger.info(f"[RCA-RUN] request_id={request_id} starting RCA tool on "
                                    f"{[f['name'] for f in listing['kpi']]}")
                rca = run_rca_tool(
                    request_id=request_id,
                    tool_path=_tool_path(),
                    files_listing=listing,
                    resolve_file=lambda name: rf.resolve_file(base, request_id, name),
                    start_date=window["start_dt"].strftime("%Y-%m-%dT%H:%M"),
                    end_date=window["end_dt"].strftime("%Y-%m-%dT%H:%M"),
                    out_dir=rdir,
                    timeout_seconds=float(_setting("RCA_TOOL_TIMEOUT_S", 300)),
                    launch_args=_setting("RCA_BROWSER_ARGS", ["--no-sandbox", "--disable-dev-shm-usage"]),
                    log=request_logger,
                )
        except limits.SlotTimeout as e:
            self._fail(job_id, rdir, request_logger, RCARunError("RCA_QUEUE_TIMEOUT", str(e)))
        except RCARunError as e:
            self._fail(job_id, rdir, request_logger, e)

        file_status = (rca_inputs or {}).get("file_status") or {}
        failed = sorted(x for x, st in systems.items()
                        if st == "FAILED" or file_status.get(x) == rf.FILE_FAILED)
        no_data = sorted(x for x, fs in file_status.items()
                         if fs == rf.FILE_NOT_APPLICABLE and x not in failed and x in systems)
        report = build_report(request_id, rca, systems, failed, no_data,
                              request_input=self._request_input(msisdn_list, complain_type, extra),
                              link_base=self.link_base)

        # Everything else (pipeline, file loading, tool internals) goes to diagnostics only.
        _write_json(rdir / DIAGNOSTICS_FILE, {
            "request_id": request_id,
            "overall_status": analysis.get("overall_status"),
            "systems": systems,
            "summary_cards": analysis.get("summary"),
            "dashboard": analysis.get("dashboard"),
            "rca_inputs": {**rca_inputs, "files": listing},
            "rca_tool": {k: v for k, v in rca.items() if k not in ("results", "root_cause")},
            "duration_seconds": round(time.monotonic() - t0, 2),
        })
        (rdir / CSV_FILE).write_text(findings_csv(report), encoding="utf-8")
        _write_json(rdir / RESULT_FILE, report)
        job_manager.update_job(job_id, rca_run={"status": "SUCCESS"})
        request_logger.info(f"[RCA-RUN] request_id={request_id} done status={report['status']} "
                            f"findings={len(report['findings'])} rca_duration={rca.get('duration_seconds')}s")
        return report

    def _wait_for_files(self, base, request_id, request_logger):
        """Wait (bounded) until no system is still writing its RCA file."""
        deadline = time.monotonic() + float(_setting("RCA_FILES_WAIT_S", 60))
        while True:
            st = rf.rca_status(base, request_id)
            if st["status"] != rf.RCA_GENERATING or time.monotonic() >= deadline:
                if st["status"] == rf.RCA_GENERATING:
                    request_logger.warning(f"[RCA-RUN] request_id={request_id} files still generating "
                                           f"after wait; using READY files only: {st['files']}")
                return {"status": st["status"], "file_status": st["files"]}
            time.sleep(1.0)

    def _fail(self, job_id, rdir, request_logger, err):
        info = {"code": err.code, "message": err.message, "details": err.details}
        try:
            _write_json(rdir / ERROR_FILE, info)
        except Exception:
            request_logger.warning("[RCA-RUN] could not write error file", exc_info=True)
        request_logger.error(f"[RCA-RUN] request_id={job_id} failed {err.code}: {err.message}")
        job_manager.update_job(job_id, rca_run={"status": "FAILED", **info})
        raise err


def _failed_body(request_id, rdir, job=None, link_base=RCARunAPIView.link_base):
    err = {}
    p = Path(rdir) / ERROR_FILE
    if p.is_file():
        try:
            err = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            err = {}
    if not err and job is not None:
        run = job.get("rca_run") or {}
        err = {"code": run.get("code") or "PIPELINE_ERROR", "message": run.get("message") or job.get("error")}
    return {"request_id": request_id, "status": "FAILED",
            "error": {"code": err.get("code"), "message": err.get("message")},
            "links": {"diagnostics": f"{link_base}/{request_id}/diagnostics"}}


class RCASiteRunAPIView(RCARunAPIView):
    """POST /callflow/api/rca/sites/run {"sitelist": [...], "start": "...", "end": "..."}

    The same job, pipeline, RCA tool and report as the MSISDN run, without Trino:
    the requested sites go straight to NPP (entity_type="site"), Viavi and Remedy;
    Splunk is unchanged; Smartcare runs the SmartcareSiteKpiAPIView implementation
    for the sites and the request's range (rca_runner/site_mode.py).
    """
    job_type = "rca_site_run"
    log_prefix = "rca_site_run"
    link_base = "/callflow/api/rca/sites/run"
    file_systems = [s for s in FILE_SYSTEMS if s != "trino"]

    def build_log_context(self, request):
        req = site_mode.parse_site_request(request.data)
        window = parse_time_window(req["start"], req["end"])
        log_path, ts = self.get_log_file(self.log_prefix, window["start_dt"], window["end_dt"])
        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": [], "complain_type": None, "imsi": "",
            "query_time_range": self.build_analysis_time_windows(window["start_dt"], window["end_dt"]),
            "sitelist": req["sitelist"], "npp_entity_type": "site",
        }

    def _default_sources(self):
        sources = super()._default_sources()
        sources.fetch_smartcare = site_mode.fetch_smartcare   # wrapped by _build_sources as usual
        return sources

    def _build_sources(self, base, request_id, tlog, merge_dash, detail_sinks=None):
        sources = super()._build_sources(base, request_id, tlog, merge_dash, detail_sinks)
        # No Trino and no cell lookup: the sites are the NPP entities and the
        # Viavi / Remedy sites (replaces the Trino task and its file/fallback wrapper).
        sources.get_cells = site_mode.get_sites
        sources.derive_sites = site_mode.derive_sites
        return sources

    @staticmethod
    def _extract_smartcare(system, data, ctx):
        _write_json(rf.request_dir(ctx["base_output_dir"], ctx["request_id"]) / SITE_KPI_FILE,
                    (data or {}).get("site_kpis"))
        return "smartcare_filter.csv", site_mode.filter_csv(ctx["sitelist"])

    def _report_systems(self, systems):
        return {k: v for k, v in systems.items() if k != "trino"}

    def _request_input(self, msisdn_list, complain_type, extra):
        return {"mode": "site", "sites": list(extra.get("sitelist") or []), "msisdn_count": None,
                "complain_type": None}


class _RCARunFileView(BaseCallFlowAPIView):
    """Shared request-dir lookup for the CSV and diagnostics endpoints."""

    def _rdir(self, job_id):
        return rf.request_dir(Path(settings.OUTPUT_DIR) / RCARunAPIView.output_subdir, job_id)


class RCARunFindingsCSVView(_RCARunFileView):
    """GET /callflow/api/rca/run/{id}/findings.csv -- one row per RCA finding."""

    def get(self, request, job_id=None):
        try:
            rdir = self._rdir(job_id)
        except ValueError:
            return Response({"error": "invalid job_id"}, status=http_status.HTTP_400_BAD_REQUEST)
        p = rdir / CSV_FILE
        if p.is_file():
            body = p.read_text(encoding="utf-8")
        elif (rdir / RESULT_FILE).is_file():
            body = findings_csv(json.loads((rdir / RESULT_FILE).read_text(encoding="utf-8")))
        else:
            return Response({"error": "no RCA result for this request (yet)"},
                            status=http_status.HTTP_404_NOT_FOUND)
        resp = HttpResponse(body, content_type="text/csv; charset=utf-8")
        resp["Content-Disposition"] = f'attachment; filename="rca_findings_{job_id}.csv"'
        return resp


class RCARunDiagnosticsView(_RCARunFileView):
    """GET /callflow/api/rca/run/{id}/diagnostics -- everything kept out of the report."""

    def get(self, request, job_id=None):
        try:
            rdir = self._rdir(job_id)
        except ValueError:
            return Response({"error": "invalid job_id"}, status=http_status.HTTP_400_BAD_REQUEST)
        if not rdir.is_dir():
            return Response({"error": "unknown request"}, status=http_status.HTTP_404_NOT_FOUND)

        def read_json(name):
            p = rdir / name
            try:
                return json.loads(p.read_text(encoding="utf-8")) if p.is_file() else None
            except Exception as e:
                return {"unreadable": str(e)}

        console = rdir / "browser_console.log"
        job = job_manager.get_job(job_id)
        return Response({
            "request_id": job_id,
            "job": {k: (job or {}).get(k) for k in ("status", "step", "progress_percent", "error",
                                                     "systems", "rca_run")} if job else None,
            "pipeline": read_json(DIAGNOSTICS_FILE),
            "error": read_json(ERROR_FILE),
            "manifest": read_json("manifest.json"),
            "rca_tool_log": read_json("rca_tool_log.json"),
            "browser_console": console.read_text(encoding="utf-8").splitlines()[-500:]
                               if console.is_file() else [],
            "files": sorted(p.name for p in rdir.iterdir() if p.is_file()),
        })
