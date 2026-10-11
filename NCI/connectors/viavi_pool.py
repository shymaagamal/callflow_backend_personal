# NCI/connectors/viavi_pool.py
"""
Process-wide Viavi (cx_Oracle) connection pool -- same pattern as
callflow/core/connectors/smartcare_pool.py.

Needed because NCI now issues up to 3 concurrent Viavi queries (one per
period) in the same request, and cx_Oracle connections aren't safe to
share across threads running concurrent queries.
"""

import logging
from pathlib import Path
from django.conf import settings

from callflow.core.connectors.connection_pool import ConnectionPool
from callflow.core.utils.connections import viaviConnection

logger = logging.getLogger(__name__)

_viavi_pool = None
_POOL_MAX_SIZE = 6  # tune based on how many concurrent sessions Viavi/Oracle tolerates


def _pool_log_path() -> str:
    out_dir = Path(getattr(settings, "OUTPUT_DIR", Path(".")))
    log_dir = out_dir / "logs" / "viavi"
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / "viavi_pool.log")


def get_viavi_pool() -> ConnectionPool:
    global _viavi_pool
    if _viavi_pool is None:
        log_path = _pool_log_path()
        _viavi_pool = ConnectionPool(
            factory=lambda: viaviConnection(log_path),
            max_size=_POOL_MAX_SIZE,
            name="viavi",
        )
        logger.info(f"[VIAVI POOL] initialized (max_size={_POOL_MAX_SIZE})")
    return _viavi_pool