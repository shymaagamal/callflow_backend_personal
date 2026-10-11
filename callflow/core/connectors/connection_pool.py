# callflow/core/connectors/connection_pool.py
"""
Generic, thread-safe connection pool for any data source whose connection
is expensive to open (JDBC/Hive bridges, Splunk SDK sessions, ODBC, etc).

Why this exists:
 current code opens a brand new connection at the start of every single
HTTP request and closes it at the end (see smartcareConnection(log_path) /
viaviConnection(log_path) being called inside each view). For JDBC-bridge
drivers in particular (jaydebeapi/JPype-style, which is what your
java.lang.Long / BigDecimal handling implies), connection setup itself can
take real time -- and you're paying that cost on every request.

This pool creates connections lazily (up to max_size), hands them out to
whoever asks, and reuses them instead of reopening. One pool instance per
data source, created once at module-import time, lives for the lifetime of
the worker process.
"""

import queue
import threading
import logging

logger = logging.getLogger(__name__)


class ConnectionPool:
    def __init__(self, factory, max_size: int = 6, name: str = "pool"):
        """
        factory: zero-arg callable that returns a new, ready-to-use connection.
        max_size: max number of concurrent connections this pool will create.
                   Keep this <= whatever concurrency limit your backend
                   (Hive/Impala/Splunk) actually tolerates -- more isn't
                   always better.
        """
        self._factory = factory
        self._max_size = max_size
        self._name = name
        self._pool: "queue.Queue" = queue.Queue(maxsize=max_size)
        self._created = 0
        self._lock = threading.Lock()

    def acquire(self, timeout: int = 30):
        """
        Returns a connection: reuses an idle one if available, creates a new
        one if we're under max_size, otherwise blocks (up to `timeout`
        seconds) for one to free up.
        """
        try:
            conn = self._pool.get_nowait()
            logger.debug(f"[{self._name}] reused pooled connection")
            return conn
        except queue.Empty:
            pass

        with self._lock:
            if self._created < self._max_size:
                self._created += 1
                logger.info(f"[{self._name}] opening new connection ({self._created}/{self._max_size})")
                return self._factory()

        logger.warning(f"[{self._name}] pool exhausted ({self._max_size} in use) - waiting up to {timeout}s")
        return self._pool.get(timeout=timeout)

    def release(self, conn):
        """Return a healthy connection to the pool for reuse."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            # Shouldn't normally happen (we never hand out more than max_size),
            # but if it does, just close the extra connection rather than leak it.
            self._safe_close(conn)

    def invalidate(self, conn):
        """
        Call this INSTEAD of release() when a connection errored out mid-query.
        Closes it and frees a slot so a fresh connection gets created next time.
        """
        self._safe_close(conn)
        with self._lock:
            self._created = max(0, self._created - 1)

    def _safe_close(self, conn):
        try:
            conn.close()
        except Exception:
            pass