"""
MI API views (thin: parse, dedup, dispatch, respond).

POST /callflow/mi/api/smartcare                      -> 202 {"job_id", "status", ...}   (default)
POST /callflow/prototype/api/smartcare               -> old synchronous body            (default)
     either path: ?async=1 / {"async": true} for 202, ?async=0 / {"async": false} to wait
GET  .../smartcare/jobs/<job_id>                     -> job record (status, step, warnings, timings, result)
GET  .../smartcare/jobs/<job_id>/result              -> the exact analysis body with its original
                                                        HTTP status once finished, 202 while running

Response headers: X-MI-Job-Id, and on finished results X-MI-Partial / X-MI-Warnings.
"""
import logging
import threading
import time

from django import db
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from . import config, jobs, outputs, response, runner, validation
from .mi_logging import setup_app_logging

logger = logging.getLogger("callflow.mi.api")
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _identity(request):
    try:
        user = getattr(request, "user", None)
        if user is not None and getattr(user, "is_authenticated", False):
            return str(user.get_username())
        return str(user)
    except Exception:
        return "unknown"


def _wants_async(request, default_mode):
    raw = request.query_params.get("async")
    if raw is None and hasattr(request.data, "get"):
        raw = request.data.get("async")
    if isinstance(raw, bool):
        return raw
    if raw is not None:
        text = str(raw).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
    return default_mode == "async"


def _with_job_headers(resp, job):
    resp["X-MI-Job-Id"] = job["job_id"]
    if job.get("status") in ("DONE", "FAILED"):
        resp["X-MI-Partial"] = "true" if job.get("partial") else "false"
        codes = sorted({w.get("code") for w in job.get("warnings") or [] if w.get("code")})
        if codes:
            resp["X-MI-Warnings"] = ",".join(codes)[:900]
    return resp


def _links(request, job_id):
    base = request.path.rstrip("/")
    return {"status_url": f"{base}/jobs/{job_id}", "result_url": f"{base}/jobs/{job_id}/result"}


def dispatch(job_id, params, fp, day, wait):
    """Hand the job to the MI worker, or run it locally per MI_EXECUTOR / MI_WORKER_FALLBACK."""
    mode = config.executor()
    if mode == "worker":
        if jobs.worker_alive() or config.worker_fallback() == "none":
            jobs.enqueue(job_id, params, fp)
            if not jobs.worker_alive():
                logger.error("MI job %s queued but no MI worker heartbeat (MI_WORKER_FALLBACK=none)", job_id)
            return "worker"
        logger.warning("MI worker heartbeat missing; running job %s in this Gunicorn process", job_id)
        mode = "thread"
    if mode == "inline" or wait:
        runner.execute_job(job_id, params, fp, mode="inline", day=day)
        return "inline"
    threading.Thread(target=runner.execute_job, args=(job_id, params, fp, "thread", day),
                     name=f"mi-job-{job_id}", daemon=True).start()
    return "thread"


def wait_for(job_id, timeout_s):
    deadline = time.monotonic() + timeout_s
    delay = 0.5
    while True:
        job = jobs.get_job(job_id)
        if job is None or job.get("status") in ("DONE", "FAILED"):
            return job
        if time.monotonic() >= deadline:
            return job
        time.sleep(delay)
        delay = min(5.0, delay * 1.5)


class _SubmitView(APIView):
    endpoint = "mi"
    legacy_errors = False

    def default_mode(self):
        return config.new_endpoint_mode()

    def post(self, request):
        setup_app_logging()
        params = {
            "msisdn_list": request.data.get("msisdn_list"),
            "start_date": request.data.get("start_date"),
            "end_date": request.data.get("end_date"),
            "Complain_Type": request.data.get("Complain_Type"),
            "imsi": request.data.get("imsi"),
            "user": _identity(request),
        }
        want_async = _wants_async(request, self.default_mode())
        # free the DB connection opened by the session/auth lookup before any waiting
        db.close_old_connections()

        try:
            req = validation.parse_request(params)
        except validation.RequestError as e:
            code = e.legacy_status if self.legacy_errors else 400
            logger.info("MI request rejected endpoint=%s status=%s error=%s", self.endpoint, code, e.message)
            return Response(response.error_body(e.message), status=code)

        params = req.to_params()
        fp = jobs.fingerprint(params)
        job_id, day = jobs.new_job_id(), outputs.today_str()
        existing = jobs.claim_inflight(fp, job_id)
        deduplicated = existing is not None
        if deduplicated:
            job_id = existing
            logger.info("MI duplicate submission -> existing job_id=%s", job_id)
        else:
            if jobs.pending_count() >= config.max_queued_jobs():
                jobs.release_inflight(fp, job_id)
                resp = Response(response.error_body("MI queue is full, retry later"),
                                status=status.HTTP_429_TOO_MANY_REQUESTS)
                resp["Retry-After"] = "120"
                return resp
            jobs.create_job(job_id, req.user, params, self.endpoint, day)
            jobs.pending_add(job_id)
            how = dispatch(job_id, params, fp, day, wait=not want_async)
            logger.info("MI job submitted job_id=%s endpoint=%s via=%s async=%s complain_type=%s msisdns=%d",
                        job_id, self.endpoint, how, want_async, req.complain_type, len(req.msisdns))

        if want_async:
            job = jobs.get_job(job_id) or {"job_id": job_id, "status": "PENDING"}
            body = {"job_id": job_id, "status": job.get("status"), **_links(request, job_id)}
            if deduplicated:
                body["deduplicated"] = True
            return _with_job_headers(Response(body, status=status.HTTP_202_ACCEPTED), job)

        job = wait_for(job_id, config.sync_wait_timeout_s())
        if job is None:
            return Response(response.error_body("MI job expired"), status=500)
        if job.get("status") not in ("DONE", "FAILED"):
            body = {"job_id": job_id, "status": job.get("status"),
                    "detail": "still running; poll result_url", **_links(request, job_id)}
            return _with_job_headers(Response(body, status=status.HTTP_202_ACCEPTED), job)
        return _with_job_headers(Response(job.get("result"), status=job.get("http_status") or 500), job)


class MISmartcareView(_SubmitView):
    """/callflow/mi/api/smartcare - async by default (MI_ENDPOINT_MODE)."""
    endpoint = "mi"


class LegacySmartcareView(_SubmitView):
    """/callflow/prototype/api/smartcare - the old contract by default (MI_LEGACY_ENDPOINT_MODE)."""
    endpoint = "prototype"
    legacy_errors = True

    def default_mode(self):
        return config.legacy_endpoint_mode()


class JobStatusView(APIView):
    def get(self, request, job_id=None):
        job = jobs.get_job(job_id) if job_id else None
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        body = dict(job)
        body["worker_alive"] = jobs.worker_alive()
        return _with_job_headers(Response(body), job)


class JobResultView(APIView):
    def get(self, request, job_id=None):
        job = jobs.get_job(job_id) if job_id else None
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        if job.get("status") not in ("DONE", "FAILED"):
            return _with_job_headers(Response({"job_id": job_id, "status": job.get("status"),
                                               "step": job.get("step")}, status=status.HTTP_202_ACCEPTED), job)
        return _with_job_headers(Response(job.get("result"), status=job.get("http_status") or 500), job)
