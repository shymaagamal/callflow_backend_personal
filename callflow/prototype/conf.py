"""
Prototype-only runtime configuration.

Every value can be overridden in settings.py (or left alone: the defaults
reproduce the previous behaviour wherever behaviour is visible to callers).
Nothing here is read by the main CallFlow app.
"""
from pathlib import Path

from django.conf import settings


def _get(name, default):
    return getattr(settings, name, default)


def output_dir() -> Path:
    return Path(_get("OUTPUT_DIR", Path(settings.BASE_DIR) / "callflow_output"))


# --------------------------------------------------------------------------
# Smartcare
# --------------------------------------------------------------------------
def smartcare_socket_timeout_ms() -> int:
    # Same value the core Smartcare connection uses. Before this refactor the
    # Prototype connection had NO effective timeout (properties were passed
    # in jaydebeapi's `libs` slot and silently ignored).
    return int(_get("PROTOTYPE_SMARTCARE_SOCKET_TIMEOUT_MS", 1_800_000))


def smartcare_connect_timeout_ms() -> int:
    return int(_get("PROTOTYPE_SMARTCARE_CONNECT_TIMEOUT_MS", 60_000))


def smartcare_max_attempts() -> int:
    # Previous behaviour: 5 attempts per query/day-table.
    return max(1, int(_get("PROTOTYPE_SMARTCARE_MAX_ATTEMPTS", 5)))


def smartcare_retry_backoff_s() -> float:
    # Previous behaviour: retry immediately. Backoff = base * 2**(attempt-1), capped.
    return float(_get("PROTOTYPE_SMARTCARE_RETRY_BACKOFF_S", 5.0))


def smartcare_retry_backoff_max_s() -> float:
    return float(_get("PROTOTYPE_SMARTCARE_RETRY_BACKOFF_MAX_S", 60.0))


def smartcare_retry_budget_s() -> float:
    # After this many seconds of Smartcare work in one request, failed queries
    # are no longer retried (first attempts still run).
    return float(_get("PROTOTYPE_SMARTCARE_RETRY_BUDGET_S", 3600.0))


def memoize_queries() -> bool:
    # Re-use the result of an identical, successful query inside the same
    # request (the fallback block used to re-run SCF_CCI_U_T_Data up to 4x and
    # S1MME up to 2x with identical parameters).
    return bool(_get("PROTOTYPE_MEMOIZE_QUERIES", True))


def skip_unused_data_queries() -> bool:
    # smartcare_nodes_REG_Voice contributes no column to the Data response and
    # contains a LEFT JOIN without ON (cartesian join). OFF by default so the
    # query set is unchanged until you decide to enable it.
    return bool(_get("PROTOTYPE_SKIP_UNUSED_DATA_QUERIES", False))


# --------------------------------------------------------------------------
# Concurrency / execution
# --------------------------------------------------------------------------
def max_concurrent() -> int:
    # How many Prototype analyses may run at once across ALL Gunicorn workers
    # (and the optional queue worker). Each one = 1 Smartcare session.
    return max(1, int(_get("PROTOTYPE_MAX_CONCURRENT", 1)))


def sync_slot_wait_s() -> float:
    # Synchronous callers wait this long for a free slot, then get HTTP 429.
    return float(_get("PROTOTYPE_SYNC_SLOT_WAIT_S", 120.0))


def async_slot_wait_s() -> float:
    # Background jobs wait this long for a free slot, then FAIL as busy.
    return float(_get("PROTOTYPE_ASYNC_SLOT_WAIT_S", 3600.0))


def max_pending_jobs() -> int:
    # Upper bound on queued + running background jobs. Beyond it: HTTP 429.
    return max(1, int(_get("PROTOTYPE_MAX_PENDING_JOBS", 3)))


def slot_ttl_s() -> int:
    # Redis slot lease. Refreshed by a heartbeat while the analysis runs, so a
    # crashed worker frees its slot after at most this long.
    return int(_get("PROTOTYPE_SLOT_TTL_S", 120))


def job_ttl_s() -> int:
    # Kept in Redis for this long after the last update (core uses 30 min,
    # which is shorter than one long Smartcare query).
    return int(_get("PROTOTYPE_JOB_TTL_S", 6 * 3600))


def default_async() -> bool:
    # Default response mode when the caller does not send `async`.
    # False = previous synchronous contract.
    return bool(_get("PROTOTYPE_DEFAULT_ASYNC", False))


def executor() -> str:
    # "thread": background thread inside the Gunicorn worker that got the POST
    #           (same pattern core already uses; no deployment change).
    # "queue" : hand the job to the separate prototype worker process
    #           (python -m callflow.prototype.worker). Falls back to "thread"
    #           when no live worker heartbeat is found.
    # "inline": run inside the request thread (tests only).
    return str(_get("PROTOTYPE_EXECUTOR", "thread")).lower()


# --------------------------------------------------------------------------
# Output files
# --------------------------------------------------------------------------
def legacy_output_dir():
    # Fixed files that existed before (sc_nodes.csv, sc_nodes_before_voice.csv,
    # sc_data.csv). Still written - atomically - for any external consumer.
    # Set to None to stop writing them.
    return _get("PROTOTYPE_LEGACY_OUTPUT_DIR", "/home/Shorouk/CallFlow")


def write_run_files() -> bool:
    # Request-specific copies under callflow_output/prototype/runs/<date>/<request_id>/
    return bool(_get("PROTOTYPE_WRITE_RUN_FILES", True))


def legacy_logs_file() -> str:
    import os
    return os.path.join(settings.STATIC_ROOT, "CallFlow/output/logs.txt")
