# callflow/core/views/rca_run.py
"""
POST /callflow/api/rca/run           -> 202 {job_id, status: "PENDING"}   (job_id == request_id)
GET  /callflow/api/rca/run/{job_id}  -> job: RUNNING (step, systems, rca_run) | DONE (result) | FAILED

One request runs every CallFlow system through the EXISTING orchestration
(AnalysisOrchestratorAPIView.run_job: same DAG, services, analyzers, RCA file
writers), then runs the EXISTING RCA HTML tool on that request's files in
headless Chromium (rca_runner.browser_runner) and returns the tool's own
results. No RCA logic is re-implemented in Python.

Files, logs and the final result for each request live under
    OUTPUT_DIR/MI/rca/{request_id}/      (CSV inputs, manifest.json, logs/, rca_result.json,
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
from rest_framework import status as http_status
from rest_framework.response import Response

from ..jobs import job_manager
from ..orchestration import rca_files as rf
from ..rca_runner import limits
from ..rca_runner.browser_runner import RCARunError, run_rca_tool
from .orchestrated_analysis import AnalysisOrchestratorAPIView

logger = logging.getLogger(__name__)

RESULT_FILE = "rca_result.json"
ERROR_FILE = "rca_error.json"


def _setting(name, default):
    return getattr(settings, name, default)


def _tool_path():
    return Path(_setting("RCA_TOOL_PATH", Path(settings.BASE_DIR) / "CallFlow-Frontend" / "src" /
                         "assets" / "external" / "rca_tool_V24.html"))


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

    def get_log_file(self, prefix, start_date, end_date):
        # Same contract as the base class, but the request log lives under MI/logs.
        logs_dir = Path(settings.OUTPUT_DIR) / "rca_engine" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir / f"{prefix}.txt", f"{start_date}_{end_date}"

    def get(self, request, job_id=None):
        if not job_id:
            return Response({"error": "job_id is required"}, status=http_status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is not None:
            return Response(job)
        # The Redis job record expires after 30 min; the result file does not.
        try:
            rdir = rf.request_dir(self._base(), job_id)
        except ValueError:
            return Response({"error": "invalid job_id"}, status=http_status.HTTP_400_BAD_REQUEST)
        for name, state in ((RESULT_FILE, "DONE"), (ERROR_FILE, "FAILED")):
            p = rdir / name
            if p.is_file():
                data = json.loads(p.read_text(encoding="utf-8"))
                body = {"job_id": job_id, "job_type": self.job_type, "status": state, "source": "file"}
                body["result" if state == "DONE" else "error"] = data
                return Response(body)
        return Response({"error": "job not found or expired"}, status=http_status.HTTP_404_NOT_FOUND)

    # ------------------------------------------------------------------ job --
    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list,
                complain_type, imsi, query_time_range):
        request_id = job_id
        base = self._base()
        rdir = rf.request_dir(base, request_id)
        lock_dir = limits.default_lock_dir(settings.OUTPUT_DIR)
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
                analysis = super().run_job(job_id, request_logger, log_path, ts, window, msisdn_list,
                                           complain_type, imsi, query_time_range)
            systems = analysis.get("systems") or {}

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

        failed = sorted(s for s, st in systems.items() if st not in ("COMPLETED",))
        result = {
            "request_id": request_id,
            "status": "COMPLETED" if not failed else "PARTIAL",
            "failed_systems": failed,
            "overall_status": analysis.get("overall_status"),
            "systems": systems,
            "summary": analysis.get("summary"),
            "dashboard": analysis.get("dashboard"),
            "rca_inputs": {**rca_inputs, "files": listing},
            "rca": rca,
            "artifacts": {
                "directory": str(rdir),
                "result": str(rdir / RESULT_FILE),
                "tool_log": str(rdir / "rca_tool_log.json"),
                "browser_console": str(rdir / "browser_console.log"),
            },
            "duration_seconds": round(time.monotonic() - t0, 2),
        }
        _write_json(rdir / RESULT_FILE, result)
        job_manager.update_job(job_id, rca_run={"status": "SUCCESS"})
        request_logger.info(f"[RCA-RUN] request_id={request_id} done status={result['status']} "
                            f"rca_duration={rca.get('duration_seconds')}s")
        return result

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
