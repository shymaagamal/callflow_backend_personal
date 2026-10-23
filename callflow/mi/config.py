"""
MI configuration. Every value is optional: read from Django settings first,
then from the environment (the project's .env is already loaded by
settings.py), then the default shown here.

Nothing outside callflow.mi reads these names.
"""
import logging
import os
from pathlib import Path

from django.conf import settings

logger = logging.getLogger("callflow.mi.config")

_UNSET = object()


def _get(name, default):
    value = getattr(settings, name, _UNSET)
    if value is _UNSET:
        value = os.environ.get(name, _UNSET)
    return default if value is _UNSET else value


def _bool(name, default):
    value = _get(name, default)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _int(name, default):
    return int(_get(name, default))


def _float(name, default):
    return float(_get(name, default))


# ---------------------------------------------------------------- paths
def output_dir() -> Path:
    """
    CALLFLOW_OUTPUT/MI. Set MI_OUTPUT_DIR (absolute) to move it; by default it
    is <settings.OUTPUT_DIR>/MI, i.e. .../callflow_portal/callflow_output/MI.
    A relative value is resolved against BASE_DIR (and logged).
    """
    raw = _get("MI_OUTPUT_DIR", None)
    if raw:
        path = Path(str(raw)).expanduser()
        if not path.is_absolute():
            path = Path(settings.BASE_DIR) / path
            logger.warning("MI_OUTPUT_DIR is relative; using %s", path)
        return path
    base = getattr(settings, "OUTPUT_DIR", None) or (Path(settings.BASE_DIR) / "callflow_output")
    return Path(base) / "MI"


def legacy_export_dir():
    """Fixed-name CSV copies for existing consumers. None/'' disables."""
    value = _get("MI_LEGACY_EXPORT_DIR", "/home/Shorouk/CallFlow")
    return value or None


def write_latest() -> bool:
    """Also keep CALLFLOW_OUTPUT/MI/latest/<file>.csv (atomic) - the new stable path."""
    return _bool("MI_WRITE_LATEST", True)


def legacy_static_log() -> bool:
    """Previous behaviour appended to STATIC_ROOT/CallFlow/output/logs.txt. Off by default."""
    return _bool("MI_LEGACY_STATIC_LOG", False)


def log_retention_days() -> int:
    return _int("MI_LOG_RETENTION_DAYS", 14)


def job_retention_days() -> int:
    return _int("MI_JOB_RETENTION_DAYS", 7)


def log_level() -> str:
    return str(_get("MI_LOG_LEVEL", "INFO")).upper()


# ------------------------------------------------------------ Smartcare
def smartcare_user():
    return _get("STCC_SMARTCARE_DB_USER", None)


def smartcare_password():
    return _get("STCC_SMARTCARE_DB_PASSWORD", None)


def smartcare_jdbc_url() -> str:
    return str(_get("MI_SMARTCARE_JDBC_URL", "jdbc:yacht://172.30.109.95:38443/SPARK-SPARKSQL"))


def smartcare_jdbc_driver() -> str:
    return str(_get("MI_SMARTCARE_JDBC_DRIVER", "com.huawei.ryoma.yacht.client.YachtDriver"))


def smartcare_jdbc_jar() -> str:
    return str(_get("MI_SMARTCARE_JDBC_JAR", "/usr/local/drivers/yacht-jdbc-2.18.36.jar"))


def jvm_max_heap():
    """-Xmx for the JVM, applied only when MI is the first to start it (the worker)."""
    return _get("MI_JVM_MAX_HEAP", "1g") or None


def socket_timeout_ms() -> int:
    return _int("MI_SMARTCARE_SOCKET_TIMEOUT_MS", 1_800_000)


def connect_timeout_ms() -> int:
    return _int("MI_SMARTCARE_CONNECT_TIMEOUT_MS", 60_000)


def max_attempts() -> int:
    return max(1, _int("MI_SMARTCARE_MAX_ATTEMPTS", 3))


def retry_backoff_s() -> float:
    return _float("MI_SMARTCARE_RETRY_BACKOFF_S", 10.0)


def retry_backoff_max_s() -> float:
    return _float("MI_SMARTCARE_RETRY_BACKOFF_MAX_S", 120.0)


def query_parallelism() -> int:
    """
    Smartcare queries of ONE job that may run at the same time (each on its own
    JDBC session). 1 = sequential (previous behaviour). Total Smartcare sessions
    opened by MI at once = MI_MAX_CONCURRENT x MI_QUERY_PARALLELISM.
    """
    return max(1, _int("MI_QUERY_PARALLELISM", 4))


def retry_budget_s() -> float:
    """Total Smartcare seconds per job after which failed queries are not retried."""
    return _float("MI_SMARTCARE_RETRY_BUDGET_S", 2700.0)


# ----------------------------------------------------- business behaviour
def fix_midnight_tables() -> bool:
    """
    Query every calendar day touched by the window. The old loop stepped from
    the start *datetime* in 24h steps, so 23:00 -> 01:00 never read the
    second day's table.
    """
    return _bool("MI_FIX_MIDNIGHT_TABLES", True)


def skip_ineffective_queries() -> bool:
    """
    Skip smartcare_nodes_REG_Voice in the Data path: none of its columns reach
    the Data response, but they are in the sc_nodes.csv dump, so OFF by default.
    """
    return _bool("MI_SKIP_INEFFECTIVE_QUERIES", False)


def map_cs_subscriber_to_msisdn() -> bool:
    """
    Rename CS_MOC.callerno / CS_MTC.calledno to MSISDN (see catalog.SUBSCRIBER_COLUMNS).
    False = previous behaviour (CS rows without MSISDN -> every Voice count is 1).
    """
    return _bool("MI_MAP_CS_SUBSCRIBER_TO_MSISDN", True)


# ------------------------------------------------------------ validation
def max_window_minutes() -> int:
    return _int("MI_MAX_WINDOW_MINUTES", 420)


def msisdn_prefixes():
    return tuple(_get("MI_MSISDN_PREFIXES", ("10", "11", "12", "15")))


def msisdn_length() -> int:
    return _int("MI_MSISDN_LENGTH", 10)


# ------------------------------------------------------------- execution
def executor() -> str:
    """
    "worker": enqueue for the MI worker process (callflow_mi_worker.service).
    "thread": background thread in the Gunicorn worker (old core pattern).
    "inline": run in the request thread (tests).
    """
    value = str(_get("MI_EXECUTOR", "worker")).lower()
    return value if value in ("worker", "thread", "inline") else "worker"


def worker_fallback() -> str:
    """
    When MI_EXECUTOR="worker" but no worker heartbeat is found:
      "local" - run here (thread for async, inline for sync) and log a warning
      "none"  - enqueue anyway; the job waits until a worker starts
    """
    value = str(_get("MI_WORKER_FALLBACK", "local")).lower()
    return value if value in ("local", "none") else "local"


def new_endpoint_mode() -> str:
    """/callflow/mi/api/smartcare default: "async" (202 + job_id) or "sync"."""
    return str(_get("MI_ENDPOINT_MODE", "async")).lower()


def legacy_endpoint_mode() -> str:
    """/callflow/prototype/api/smartcare default: "sync" keeps the old contract."""
    return str(_get("MI_LEGACY_ENDPOINT_MODE", "sync")).lower()


def sync_wait_timeout_s() -> float:
    return _float("MI_SYNC_WAIT_TIMEOUT_S", 3600.0)


def max_concurrent() -> int:
    """Jobs allowed to run at once across ALL processes (= Smartcare sessions)."""
    return max(1, _int("MI_MAX_CONCURRENT", 1))


def slot_wait_s() -> float:
    return _float("MI_SLOT_WAIT_S", 3600.0)


def max_queued_jobs() -> int:
    return max(1, _int("MI_MAX_QUEUED_JOBS", 5))


def slot_ttl_s() -> int:
    return _int("MI_SLOT_TTL_S", 120)


def job_ttl_s() -> int:
    return _int("MI_JOB_TTL_S", 24 * 3600)


def job_stale_s() -> int:
    """A RUNNING job with no heartbeat for this long is reported as lost."""
    return _int("MI_JOB_STALE_S", 300)


def concurrency_backend() -> str:
    return str(_get("MI_CONCURRENCY_BACKEND", "auto")).lower()
