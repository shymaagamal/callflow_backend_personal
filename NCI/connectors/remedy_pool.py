# NCI/connectors/remedy_pool.py
"""
Process-wide Remedy (pyodbc via classes.Connection) connection pool.
multiple concurrent periods now query
Remedy at once, so each concurrent caller needs its own connection.
"""

import logging
from pathlib import Path
from django.conf import settings

from callflow.core.connectors.connection_pool import ConnectionPool
from callflow.core.utils.classes import Connection
from callflow.core.constants.general_constants import (
    SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
    DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD,
)

logger = logging.getLogger(__name__)

_remedy_pool = None
_POOL_MAX_SIZE = 6


def _make_connection():
    conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB, DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
    conn.connect()
    return conn


def get_remedy_pool() -> ConnectionPool:
    global _remedy_pool
    if _remedy_pool is None:
        _remedy_pool = ConnectionPool(
            factory=_make_connection,
            max_size=_POOL_MAX_SIZE,
            name="remedy",
        )
        logger.info(f"[REMEDY POOL] initialized (max_size={_POOL_MAX_SIZE})")
    return _remedy_pool