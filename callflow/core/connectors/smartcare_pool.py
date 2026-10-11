# callflow/core/connectors/smartcare_pool.py
"""
Process-wide Smartcare connection pool.

Import get_smartcare_pool() wherever you currently call
smartcareConnection(log_path) directly inside a request -- this replaces
"open one connection per request" with "borrow one from a small reusable
pool", which is created once per worker process.

IMPORTANT CAVEAT if your underlying driver is jaydebeapi/JPype (likely,
given the java.lang.Long / BigDecimal handling in  data_utils.py):
JPype requires each OS thread that touches the JVM bridge to be attached
to the JVM. If you start seeing errors like "thread not attached to the
JVM" after wiring in real parallelism (ThreadPoolExecutor), add this at
the top of the worker function that runs inside each thread:

    import jpype
    if not jpype.isThreadAttachedToJVM():
        jpype.attachThreadToJVM()

This is a one-line fix but only needed if you hit that specific error --
not all JDBC bridges require it.
"""

import logging
from pathlib import Path
from django.conf import settings

from .connection_pool import ConnectionPool
from ..utils.connections import smartcareConnection

logger = logging.getLogger(__name__)

_smartcare_pool = None
_POOL_MAX_SIZE = 6  # tune based on how many concurrent queries your Hive/Impala backend tolerates


def _pool_log_path() -> str:
    """
    Connections are now long-lived and shared across requests, so we log
    pool-level connection events (open/close) to one stable file rather
    than a per-request log file.
    """
    out_dir = Path(getattr(settings, "OUTPUT_DIR", Path(".")))
    log_dir = out_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / "smartcare_pool.log")


def get_smartcare_pool() -> ConnectionPool:
    global _smartcare_pool
    if _smartcare_pool is None:
        log_path = _pool_log_path()
        _smartcare_pool = ConnectionPool(
            factory=lambda: smartcareConnection(log_path),
            max_size=_POOL_MAX_SIZE,
            name="smartcare",
        )
        logger.info(f"[SMARTCARE POOL] initialized (max_size={_POOL_MAX_SIZE})")
    return _smartcare_pool