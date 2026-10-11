# callflow/core/jobs/job_manager.py
"""
Lightweight job tracking, used to turn "one blocking request that takes
minutes" into "create a job, return immediately, frontend polls for
progress" -- without requiring Celery/Channels to be installed first.

This is intentionally the SIMPLEST thing that works:
  - Job state lives in Django's cache (cache.get/set), which you're
    already using elsewhere (make_smartcare_cache_key etc).
  - The actual long-running work runs in a plain background thread,
    started from the view.

IMPORTANT CAVEAT:
This works correctly out of the box ONLY if your Django cache backend is
shared across processes (e.g. django-redis backed by Redis, or
memcached). If you're on the default LocMemCache, job state is private
to whichever worker process handled the POST -- a GET that lands on a
different gunicorn/uwsgi worker won't see it. Check CACHES in settings.py;
if it's LocMemCache, either switch to django-redis (cheap, you likely
already run Redis if you use Django's cache framework seriously) or run a
single worker process until you do.

UPGRADE PATH: once you have Celery+Redis, replace
`threading.Thread(target=fn, ...).start()` in the view with
`my_task.delay(...)` -- the job_id/status contract here doesn't change,
only how the function gets invoked. You will not need to rewrite the
views again.
"""

import uuid
import time
import logging
from django.core.cache import cache

logger = logging.getLogger(__name__)

JOB_TIMEOUT_SECONDS = 60 * 30  # job status is kept for 30 minutes after creation


def _job_cache_key(job_id: str) -> str:
    return f"callflow_job:{job_id}"


def create_job(who: str, job_type: str) -> str:
    """Creates a new job in PENDING state and returns its id."""
    job_id = uuid.uuid4().hex[:16]
    job = {
        "job_id": job_id,
        "job_type": job_type,
        "who": who,
        "status": "PENDING",     # PENDING -> RUNNING -> DONE | FAILED
        "step": "queued",
        "progress_percent": 0,
        "created_at": time.time(),
        "updated_at": time.time(),
        "result": None,
        "error": None,
    }
    cache.set(_job_cache_key(job_id), job, timeout=JOB_TIMEOUT_SECONDS)
    logger.info(f"[JOB CREATED] {job_id} type={job_type} who={who}")
    return job_id


def update_job(job_id: str, **fields):
    """Merges `fields` into the existing job state. Silently no-ops if the job expired."""
    job = cache.get(_job_cache_key(job_id))
    if job is None:
        logger.warning(f"[JOB UPDATE] {job_id} not found (expired, or cache backend not shared across workers)")
        return
    job.update({k: v for k, v in fields.items() if v is not None})
    job["updated_at"] = time.time()
    cache.set(_job_cache_key(job_id), job, timeout=JOB_TIMEOUT_SECONDS)
    logger.info(f"[JOB UPDATE] {job_id} -> status={job['status']} step={job['step']} progress={job['progress_percent']}%")


def get_job(job_id: str):
    """Returns the job dict, or None if it doesn't exist / expired."""
    return cache.get(_job_cache_key(job_id))