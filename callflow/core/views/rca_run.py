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
GET  /callflow/api/rca/run/{id}/diagnostics       -> collection, file loading and browser detail

A request does NOT run the CallFlow orchestrator plan (no system analyzers, no
summary cards). It calls the systems' existing retrieval functions
(rca_runner/collect.py), writes their raw results with the existing RCA file
writers, runs the EXISTING RCA HTML tool on that request's files in headless
Chromium (rca_runner/browser_runner.py) and returns the tool's own results.
No RCA logic is re-implemented in Python.

Files, logs and the final result for each request live under
    OUTPUT_DIR/rca_engine/rca/{request_id}/      (CSV inputs, manifest.json, rca_result.json,
                                          rca_diagnostics.json, rca_tool_log.json, browser_console.log)
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
from ..rca_runner import collect, limits, site_mode
from ..rca_runner.browser_runner import RCARunError, run_rca_tool
from ..rca_runner.report import build_report, findings_csv
from .base import BaseCallFlowAPIView, parse_time_window
from .job_base import JobBackedAPIView

logger = logging.getLogger(__name__)

RESULT_FILE = "rca_result.json"          # the public report
ERROR_FILE = "rca_error.json"
DIAGNOSTICS_FILE = "rca_diagnostics.json"  # pipeline + tool detail, never in the report
CSV_FILE = "rca_findings.csv"


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


class RCARunAPIView(JobBackedAPIView):
    """POST /callflow/api/rca/run -- MSISDN run.

    Retrieval only (rca_runner/collect.py): Trino cells, the MSISDN Smartcare
    query, NPP for the cells, Viavi/Remedy for the cells' sites, Splunk; their raw
    results become this request's RCA files; the RCA tool analyses them. The
    CallFlow orchestrator plan (system analyzers, summary cards) is NOT run.
    """
    job_type = "rca_run"
    output_subdir = "rca_engine"
    log_prefix = "rca_run"
    link_base = "/callflow/api/rca/run"
    site_run = False

    def _base(self):
        return Path(settings.OUTPUT_DIR) / self.output_subdir

    def get_log_file(self, prefix, start_date, end_date):
        # The request log lives under rca_engine/logs.
        logs_dir = self._base() / "logs"
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

    def _request_input(self, ctx):
        return {"mode": "msisdn", "sites": None, "msisdn_count": len(ctx["msisdn_list"]),
                "complain_type": ctx["complain_type"]}

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
        lock_dir = base / "locks"
        wait_s = float(_setting("RCA_RUN_SLOT_WAIT_S", 1800))
        t0 = time.monotonic()
        ctx = {
            "request_id": request_id, "msisdn_list": msisdn_list,
            "start_dt": window["start_dt"], "end_dt": window["end_dt"],
            "complain_type": complain_type, "imsi": imsi, "log_path": log_path, "ts": ts,
            "window": window, "query_time_range": query_time_range,
            "request_logger": request_logger, "base_output_dir": base,
            "_trino_sql_sink": lambda sql: request_logger.info(
                f"[RCA-COLLECT] request_id={request_id} trino SQL:\n{sql}"),
            **extra,
        }

        def queued(stage):
            last = [0.0]

            def on_wait(waited):
                if waited - last[0] >= 30 or last[0] == 0.0:   # throttle Redis writes
                    last[0] = waited or 0.001
                    job_manager.update_job(job_id, status="RUNNING",
                                           step=f"queued: waiting for a free {stage} slot ({waited:.0f}s)")
            return on_wait

        def on_change(states):
            done = sum(1 for s in states.values() if s != "RUNNING")
            job_manager.update_job(job_id, status="RUNNING", step="collecting RCA input data",
                                   progress_percent=int(90 * done / max(len(states), 1)), systems=states)

        collected = {"systems": {}}
        rca = {}
        listing = {}
        try:
            # 1) raw data from the systems -> this request's RCA files (no portal analysis)
            with limits.slot(lock_dir, "rca_run_systems", _setting("RCA_RUN_MAX_SYSTEM_JOBS", 2),
                             wait_s, on_wait=queued("systems")):
                request_logger.info(f"[RCA-RUN] request_id={request_id} job_type={self.job_type} "
                                    f"collecting RCA inputs msisdns={len(msisdn_list)} "
                                    f"sites={extra.get('sitelist')}")
                collected = collect.collect_inputs(ctx, base, request_id, self._retrievers(),
                                                   self.site_run, request_logger, on_change)
                # Server-side threshold sheet, copied into this request's folder for the tool.
                threshold_path = getattr(settings, "THRESHOLDING_SHEET_PATH", None)
                tinfo = (rf.provision_threshold(base, request_id, threshold_path)
                         if threshold_path else {"copied": False})
                request_logger.info(f"[RCA-RUN] request_id={request_id} threshold "
                                    f"copied={tinfo.get('copied')} path={tinfo.get('path')}")

            listing = rf.list_ready_files(base, request_id)
            if not listing.get("kpi"):
                raise RCARunError("RCA_NO_INPUT_FILES",
                                  "no KPI file is ready for the RCA tool (every KPI system failed or "
                                  "returned no data)", {"systems": collected["systems"]})

            # 2) the existing RCA tool, in a headless browser, on exactly these files
            job_manager.update_job(job_id, status="RUNNING", step="rca_tool", progress_percent=95,
                                   rca_run={"status": "RUNNING"})
            with limits.slot(lock_dir, "rca_run_browser", _setting("RCA_RUN_MAX_BROWSERS", 1),
                             wait_s, on_wait=queued("browser")):
                request_logger.info(f"[RCA-RUN] request_id={request_id} starting RCA tool on "
                                    f"kpi={[f['name'] for f in listing['kpi']]} "
                                    f"filter={[f['name'] for f in listing['filter']]} "
                                    f"threshold={[f['name'] for f in listing['threshold']]}")
                rca = run_rca_tool(
                    request_id=request_id,
                    tool_path=_tool_path(),
                    files_listing=listing,
                    resolve_file=lambda name: rf.resolve_file(base, request_id, name),
                    start_date=window["start_dt"].strftime("%Y-%m-%dT%H:%M"),
                    end_date=window["end_dt"].strftime("%Y-%m-%dT%H:%M"),
                    # Request times are Cairo wall-clock times (see trino_service._LOCAL_TZ).
                    timezone_id=_setting("RCA_BROWSER_TIMEZONE", "Africa/Cairo"),
                    out_dir=rdir,
                    timeout_seconds=float(_setting("RCA_TOOL_TIMEOUT_S", 300)),
                    launch_args=_setting("RCA_BROWSER_ARGS", ["--no-sandbox", "--disable-dev-shm-usage"]),
                    log=request_logger,
                )
        except limits.SlotTimeout as e:
            self._fail(job_id, rdir, request_logger, RCARunError("RCA_QUEUE_TIMEOUT", str(e)), collected)
        except RCARunError as e:
            self._fail(job_id, rdir, request_logger, e, collected)

        systems = collected["systems"]
        failed = sorted(s for s, st in systems.items() if st["status"] in collect.MISSING)
        no_data = sorted(s for s, st in systems.items() if st["status"] == collect.NO_DATA)
        notes = {s: st.get("note") or st.get("error") for s, st in systems.items()
                 if s in failed or s in no_data}
        report = build_report(request_id, rca, {s: st["status"] for s, st in systems.items()},
                              failed, no_data, request_input=self._request_input(ctx),
                              link_base=self.link_base, system_notes=notes)

        # Collection / file / tool detail goes to diagnostics only.
        _write_json(rdir / DIAGNOSTICS_FILE, {
            "request_id": request_id,
            "job_type": self.job_type,
            "collection": collected,
            "rca_inputs": listing,
            "rca_tool": {k: v for k, v in rca.items() if k not in ("results", "root_cause")},
            "duration_seconds": round(time.monotonic() - t0, 2),
        })
        (rdir / CSV_FILE).write_text(findings_csv(report), encoding="utf-8")
        _write_json(rdir / RESULT_FILE, report)
        job_manager.update_job(job_id, rca_run={"status": "SUCCESS"})
        request_logger.info(f"[RCA-RUN] request_id={request_id} done status={report['status']} "
                            f"findings={len(report['findings'])} rca_duration={rca.get('duration_seconds')}s")
        return report

    def _retrievers(self):
        return collect.default_retrievers(self.site_run)

    def _fail(self, job_id, rdir, request_logger, err, collected=None):
        info = {"code": err.code, "message": err.message, "details": err.details}
        try:
            _write_json(rdir / ERROR_FILE, info)
            if collected is not None:
                _write_json(rdir / DIAGNOSTICS_FILE, {"request_id": job_id, "job_type": self.job_type,
                                                      "collection": collected, "error": info})
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

    Retrieval only, no Trino: the requested sites go straight to NPP
    (entity_type="site"), Viavi and Remedy; Splunk is unchanged; Smartcare runs the
    SmartcareSiteKpiAPIView implementation for the sites and the request's range.
    Then the same RCA tool and report as the MSISDN run.
    """
    job_type = "rca_site_run"
    log_prefix = "rca_site_run"
    link_base = "/callflow/api/rca/sites/run"
    site_run = True

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

    def _request_input(self, ctx):
        return {"mode": "site", "sites": list(ctx["sitelist"]), "msisdn_count": None,
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
