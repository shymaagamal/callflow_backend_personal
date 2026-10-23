"""
Runs one MI job end to end: slot -> heartbeat -> pipeline -> job record.
Used by the MI worker process and, when no worker is alive, by the local executors.
"""
import logging

from django import db

from . import config, jobs, outputs, pipeline
from .smartcare import to_python_deep
from .mi_logging import JobContext, setup_app_logging

logger = logging.getLogger("callflow.mi.runner")


def execute_job(job_id, params, fp=None, mode="worker", day=None, connect_fn=None):
    setup_app_logging()
    day = day or outputs.today_str()
    msisdns = params.get("msisdn_list") or []
    try:
        with JobContext(job_id, who=params.get("user", "unknown"), complain_type=params.get("Complain_Type"),
                        msisdn_count=len(msisdns) if isinstance(msisdns, (list, tuple)) else 1,
                        window={"start": params.get("start_date"), "end": params.get("end_date")},
                        mode=mode, day=day) as ctx:
            try:
                with ctx.stage("wait_for_slot"):
                    slot = jobs.Slot(config.slot_wait_s(),
                                     on_wait=lambda: jobs.update_job(job_id, step="waiting_for_slot")).__enter__()
            except jobs.SlotBusy as busy:
                ctx.status, ctx.http_status, ctx.error = "FAILED", 429, str(busy)
                jobs.update_job(job_id, status="FAILED", step="busy", http_status=429, error=str(busy),
                                result={'error': str(busy)})
                return 429, {'error': str(busy)}

            try:
                with jobs.JobHeartbeat(job_id):
                    jobs.update_job(job_id, status="RUNNING", step="analysis", progress_percent=10,
                                    started_by=mode)
                    http_status, body = pipeline.run(params, ctx, connect_fn=connect_fn)
                    body = to_python_deep(body)      # never pickle Java objects into Redis
            finally:
                slot.__exit__(None, None, None)

            ok = http_status < 400
            ctx.http_status = http_status
            ctx.status = "DONE" if ok else "FAILED"
            if not ok:
                ctx.error = body.get('error') if isinstance(body, dict) else str(body)
            try:
                outputs.atomic_write_json({"http_status": http_status, "body": body}, ctx.dir / "result.json")
            except Exception:
                ctx.exception("result_write_failed")
            jobs.update_job(
                job_id, status=ctx.status, step="completed" if ok else "failed", progress_percent=100,
                result=body, http_status=http_status, error=ctx.error,
                partial=bool(ctx.warnings), warnings=ctx.warnings,
                timings={"total_s": round(ctx.elapsed(), 3), "smartcare_s": round(ctx.smartcare_seconds, 3),
                         "stages": ctx.stage_totals()},
                output_dir=str(ctx.dir),
            )
            return http_status, body
    except Exception as e:
        logger.exception("MI job %s crashed", job_id)
        jobs.update_job(job_id, status="FAILED", step="failed", http_status=500, error=str(e),
                        result={'error': str(e)})
        return 500, {'error': str(e)}
    finally:
        if fp:
            jobs.release_inflight(fp, job_id)
        jobs.pending_remove(job_id)
        outputs.sweep_if_due()              # cheap marker check; also covers runs without the worker
        db.close_old_connections()
