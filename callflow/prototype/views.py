"""
Prototype Smartcare API.

POST /callflow/prototype/api/smartcare
    Default (unchanged contract): synchronous, returns the analysis body.
    Opt-in async: send `async=1` (query string or body). Returns
        202 {"job_id": ..., "status": "PENDING"}
    and the analysis runs off the request thread.

GET  /callflow/prototype/api/smartcare/jobs/<job_id>
    Job record: status PENDING | RUNNING | DONE | FAILED, step, result
    (the exact body the sync call would have returned) and http_status.

Every response carries an `X-Prototype-Request-Id` header; the same id names
the per-request log file and output folder.

Limits (see prototype/conf.py): at most PROTOTYPE_MAX_CONCURRENT analyses
across all Gunicorn workers; sync callers wait PROTOTYPE_SYNC_SLOT_WAIT_S for
a slot then get 429; async jobs are capped at PROTOTYPE_MAX_PENDING_JOBS.
"""
import json
import logging
import threading
import uuid

from django import db
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from . import concurrency, conf, runtime
from .service import parse_dates, run_prototype_analysis  # noqa: F401  (parse_dates kept importable)

logger = logging.getLogger("callflow.prototype.views")

_TRUE = {"1", "true", "yes", "on"}


def _truthy(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUE if value is not None else False


def _identity(request):
    try:
        user = getattr(request, "user", None)
        if user is not None and getattr(user, "is_authenticated", False):
            return str(user.get_username())
        return str(user)
    except Exception:
        return "unknown"


def _params_from_request(request):
    """Snapshot everything the analysis needs, so no thread touches `request`."""
    data = request.data
    return {
        'msisdn_list': data.get('msisdn_list'),
        'start_date': data.get('start_date'),
        'end_date': data.get('end_date'),
        'Complain_Type': data.get('Complain_Type'),
        'imsi': data.get('imsi'),
        'user': _identity(request),
    }


def _new_context(params, request_id, mode):
    msisdns = params.get('msisdn_list')
    count = len(msisdns) if isinstance(msisdns, (list, tuple)) else (1 if msisdns else 0)
    return runtime.RunContext(
        request_id=request_id,
        who=params.get('user'),
        complain_type=params.get('Complain_Type'),
        msisdn_count=count,
        window={"start": params.get('start_date'), "end": params.get('end_date')},
        mode=mode,
    )


def _with_request_id(response, request_id):
    response["X-Prototype-Request-Id"] = request_id
    return response


# --------------------------------------------------------------------------
# Background execution
# --------------------------------------------------------------------------
def execute_job(job_id, params, fingerprint=None, mode="thread"):
    """Runs one queued job to completion. Used by the thread executor and the
    queue worker (callflow.prototype.worker)."""
    try:
        def waiting():
            concurrency.update_job(job_id, status="PENDING", step="waiting_for_slot")

        with _new_context(params, job_id, mode) as ctx:
            try:
                with ctx.stage("wait_for_slot"):
                    slot = concurrency.Slot(conf.async_slot_wait_s(), on_wait=waiting).__enter__()
            except concurrency.SlotBusy as busy:
                ctx.status, ctx.http_status, ctx.error = "FAILED", 429, str(busy)
                concurrency.update_job(job_id, status="FAILED", step="busy", http_status=429,
                                       error=str(busy), result={'error': str(busy)})
                return
            try:
                concurrency.update_job(job_id, status="RUNNING", step="analysis", progress_percent=10)
                http_status, body = run_prototype_analysis(params, ctx)
            finally:
                slot.__exit__(None, None, None)
            ctx.http_status = http_status
            ok = http_status < 400
            ctx.status = "DONE" if ok else "FAILED"
            if not ok:
                ctx.error = body.get('error') if isinstance(body, dict) else str(body)
            concurrency.update_job(
                job_id, status="DONE" if ok else "FAILED", step="completed" if ok else "failed",
                progress_percent=100, result=body, http_status=http_status,
                error=None if ok else ctx.error,
                timings={"total_s": round(ctx.elapsed(), 3), "stages": ctx.stage_totals()},
            )
    except Exception as e:  # never let a background thread die silently
        logger.exception("prototype job %s crashed", job_id)
        concurrency.update_job(job_id, status="FAILED", step="failed", http_status=500,
                               error=str(e), result={'error': str(e)})
    finally:
        if fingerprint:
            concurrency.release_inflight(fingerprint, job_id)
        concurrency.pending_remove(job_id)
        # this thread may have opened a Django DB connection (it should not, but
        # never leave one behind in a long-lived thread)
        db.connections.close_all()


def _dispatch(job_id, params, fingerprint):
    mode = conf.executor()
    if mode == "queue":
        if concurrency.worker_alive():
            concurrency.backend().queue_push(json.dumps(
                {"job_id": job_id, "params": params, "fingerprint": fingerprint}))
            return "queue"
        logger.warning("prototype: PROTOTYPE_EXECUTOR=queue but no live worker; using thread")
        mode = "thread"
    if mode == "inline":
        execute_job(job_id, params, fingerprint, mode="inline")
        return "inline"
    threading.Thread(target=execute_job, args=(job_id, params, fingerprint, "thread"),
                     name=f"prototype-job-{job_id}", daemon=True).start()
    return "thread"


# --------------------------------------------------------------------------
# Views
# --------------------------------------------------------------------------
class PrototypeSmartcareIntegrationAPIView(APIView):

    def post(self, request):
        print("POST prototype pid=%s", threading.get_ident())
        params = _params_from_request(request)
        request_id = uuid.uuid4().hex[:16]
        want_async = _truthy(request.query_params.get('async', request.data.get('async'))) \
            if ('async' in request.query_params or 'async' in request.data) else conf.default_async()

        # Release the Django DB connection opened by the session/auth lookup
        # (request.user) before the long analysis. With CONN_MAX_AGE=0 it was
        # otherwise held idle for the whole request.
        db.close_old_connections()

        if want_async:
            return _with_request_id(self._post_async(params, request_id), request_id)
        return _with_request_id(self._post_sync(params, request_id), request_id)

    # ---- previous contract -------------------------------------------------
    def _post_sync(self, params, request_id):
        with _new_context(params, request_id, "sync") as ctx:
            try:
                with ctx.stage("wait_for_slot"):
                    slot = concurrency.Slot(conf.sync_slot_wait_s()).__enter__()
            except concurrency.SlotBusy as busy:
                ctx.status, ctx.http_status, ctx.error = "FAILED", 429, str(busy)
                resp = Response({'error': str(busy)}, status=status.HTTP_429_TOO_MANY_REQUESTS)
                resp["Retry-After"] = "60"
                return resp
            try:
                http_status, body = run_prototype_analysis(params, ctx)
            finally:
                slot.__exit__(None, None, None)
            ctx.http_status = http_status
            if http_status >= 400:
                ctx.status = "FAILED"
                ctx.error = body.get('error') if isinstance(body, dict) else None
            resp = Response(body, status=http_status)
            resp["Server-Timing"] = ", ".join(
                f"{name};dur={sec * 1000:.0f}" for name, sec in ctx.stage_totals().items()
                if name in ("wait_for_slot", "smartcare_total", "aggregate_data",
                            "aggregate_voice", "hub_metrics", "write_csv"))
            return resp

    # ---- opt-in background job ----------------------------------------------
    def _post_async(self, params, request_id):
        # cheap validation up front so obvious errors are still synchronous 400s
        if not params.get('start_date') or not params.get('end_date'):
            return Response({'error': 'Missing start_date or end_date'}, status=400)
        if not params.get('msisdn_list'):
            return Response({'error': 'msisdn_list is required'}, status=400)

        fingerprint = concurrency.request_fingerprint(
            params.get('msisdn_list'), params.get('start_date'), params.get('end_date'),
            params.get('Complain_Type'), params.get('imsi'))
        existing = concurrency.claim_inflight(fingerprint, request_id)
        if existing:
            return Response({"job_id": existing, "status": "PENDING", "deduplicated": True},
                            status=status.HTTP_202_ACCEPTED)

        if concurrency.pending_count() >= conf.max_pending_jobs():
            concurrency.release_inflight(fingerprint, request_id)
            resp = Response({'error': 'Prototype analysis queue is full, retry later'},
                            status=status.HTTP_429_TOO_MANY_REQUESTS)
            resp["Retry-After"] = "120"
            return resp

        concurrency.create_job(params.get('user'), request_id)
        concurrency.pending_add(request_id)
        executor = _dispatch(request_id, params, fingerprint)
        logger.info("prototype job %s dispatched via %s", request_id, executor)
        return Response({"job_id": request_id, "status": "PENDING"}, status=status.HTTP_202_ACCEPTED)


class PrototypeSmartcareJobStatusAPIView(APIView):

    def get(self, request, job_id=None):
        job = concurrency.get_job(job_id) if job_id else None
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return _with_request_id(Response(job), job_id)
