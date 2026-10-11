# callflow/core/utils/logging_utils.py
"""
Shared logging utilities used across every view/service.

Goals this solves:
1. Every pipeline step (fetch -> aggregate -> remedy -> cache) logs a clear
   START / DONE / FAILED line with timing, instead of one giant try/except
   that only tells you SOMETHING failed somewhere.
2. Every log line says WHO triggered the request (authenticated user,
   API client header, or IP as a fallback) so you can trace "who ran this
   slow/failing analysis" without digging through request logs separately.
3. Logs still go to the per-request file you already create via
   get_log_file(), so nothing about your existing log-file-per-analysis
   convention changes -- this just makes the content of those files useful.
"""

import logging
import time
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def get_request_identity(request) -> str:
    """
    Best-effort identification of WHO made this request, for logging/audit.
    Tries, in order: authenticated Django user -> custom client header -> IP.
    Never raises -- logging must never be the thing that breaks a request.
    """
    try:
        user = getattr(request, "user", None)
        if user is not None and getattr(user, "is_authenticated", False):
            return f"user={user.get_username()}"
    except Exception:
        pass

    try:
        client_id = request.headers.get("X-Client-Id") or request.headers.get("X-Api-Client")
        if client_id:
            return f"client={client_id}"
    except Exception:
        pass

    try:
        ip = request.META.get("HTTP_X_FORWARDED_FOR", request.META.get("REMOTE_ADDR", "unknown"))
        # X-Forwarded-For can be a comma separated chain; take the first hop
        ip = ip.split(",")[0].strip()
        return f"ip={ip}"
    except Exception:
        return "who=unknown"


def build_request_logger(log_path, request_id: str, who: str) -> logging.LoggerAdapter:
    """
    Returns a LoggerAdapter that:
      - writes to the per-request log file at `log_path` (same file you
        already create with get_log_file)
      - ALSO propagates to your normal app-wide logging config (console/
        rotating file/whatever you already have), so nothing is lost
      - automatically prefixes every line with request_id and who

    Safe to call multiple times for the same request_id; we don't attach
    duplicate file handlers to the same logger instance.
    """
    base_logger = logging.getLogger(f"callflow.request.{request_id}")
    base_logger.setLevel(logging.DEBUG)
    if log_path and not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == str(log_path)
        for h in base_logger.handlers
    ):
        try:
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            base_logger.addHandler(fh)
        except Exception:
            # If the file can't be opened for some reason, fall back silently
            # to console/app-wide logging only -- never crash the request.
            logger.warning(f"Could not attach file handler at {log_path}; logging to console only")

    base_logger.propagate = True
    return logging.LoggerAdapter(base_logger, {"request_id": request_id, "who": who})


@contextmanager
def log_step(request_logger, step_name: str, job_id=None, job_manager=None, progress_percent=None):
    """
    Wrap one pipeline step with consistent START/DONE/FAILED + timing logs,
    and (optionally) push the same status to the job manager so the
    frontend can poll real progress instead of staring at a spinner.

    Usage:
        with log_step(request_logger, "fetch_smartcare_nodes", job_id, job_manager, progress_percent=30):
            nodes_data = fetch_smartcare_nodes_data(...)
    """
    start = time.perf_counter()
    request_logger.info(f"[STEP START] {step_name}")
    if job_id and job_manager:
        job_manager.update_job(job_id, status="RUNNING", step=step_name,
                                progress_percent=progress_percent)
    try:
        yield
    except Exception as e:
        elapsed = time.perf_counter() - start
        request_logger.exception(f"[STEP FAILED] {step_name} after {elapsed:.2f}s: {e}")
        if job_id and job_manager:
            job_manager.update_job(job_id, status="FAILED", step=step_name, error=str(e))
        raise
    else:
        elapsed = time.perf_counter() - start
        request_logger.info(f"[STEP DONE] {step_name} in {elapsed:.2f}s")