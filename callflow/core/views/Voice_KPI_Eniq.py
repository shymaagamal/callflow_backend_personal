"""
Voice KPI extraction from ENIQ (Sybase IQ).

GET /callflow/api/voice-kpi-eniq?hours=1
    Runs every query in constants/voice_kpis_Eniq_Queries.py concurrently and
    writes one CSV per KPI to
        OUTPUT_DIR/VOICE/eniq_<YYYYmmdd_HHMMSS>/<KPI_NAME>.csv

How the concurrency works
    `workers` threads are started. Each thread opens ONE ENIQ connection and
    keeps it, pulling queries from a shared queue until the queue is empty.
    So at most `workers` connections are open at a time, no matter how many
    queries there are.

    Opening connections is serialized with a lock (jaydebeapi/JPype are not
    safe for simultaneous connects) and retried with back-off. If ENIQ refuses
    extra connections (per-user connection limit), the threads that could not
    connect simply stop and the threads that did connect finish all the
    queries. A query is only marked FAILED if no connection could be opened at
    all, or the query itself raised an error.

Optional query params:
    hours         look-back window, int 1..168 (default 1)
    kpis          comma-separated subset of KPI names (default: all)
    workers       parallel connections, 1..20 (default 4)
    include_data  "true" to also return the rows in the response (default false)

HTTP 200 if at least one query succeeded, 500 if all failed.
"""
import datetime
import logging
import os
import queue
import threading
import time

import numpy as np
import pandas as pd
from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..constants.voice_kpis_Eniq_Queries import VOICE_KPI_ENIQ_QUERIES
from ..utils.connections import EniqConnection
from ..utils.data_utils import convert_java_types

logger = logging.getLogger(__name__)

DEFAULT_HOURS = 1
MAX_HOURS = 168
DEFAULT_WORKERS = 4
MAX_WORKERS = 20
CONNECT_ATTEMPTS = 3
CONNECT_BACKOFF_S = (1, 2, 4)

# One connect at a time per process: jaydebeapi initialises module-level state
# and the JVM thread context on connect, which is not safe to do concurrently.
_CONNECT_LOCK = threading.Lock()


def _int_param(raw, default, low, high, name):
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"'{name}' must be an integer")
    if not low <= value <= high:
        raise ValueError(f"'{name}' must be between {low} and {high}")
    return value


def _close(*resources):
    for res in resources:
        if res is not None:
            try:
                res.close()
            except Exception:
                pass


def _connect_with_retry(worker_id):
    """Open one ENIQ connection, retrying with back-off. Raises the last error."""
    last_err = None
    for attempt in range(CONNECT_ATTEMPTS):
        try:
            with _CONNECT_LOCK:
                return EniqConnection()
        except Exception as e:
            last_err = e
            logger.warning("[ENIQ] worker %s connect attempt %s/%s failed: %s",
                           worker_id, attempt + 1, CONNECT_ATTEMPTS, e)
            if attempt + 1 < CONNECT_ATTEMPTS:
                time.sleep(CONNECT_BACKOFF_S[min(attempt, len(CONNECT_BACKOFF_S) - 1)])
    raise last_err


def _run_query(conn, kpi_name, sql, out_dir):
    started = time.monotonic()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [str(desc[0]) for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        _close(cursor)

    df = pd.DataFrame(rows, columns=columns)
    df = convert_java_types(df)
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})

    csv_path = os.path.join(out_dir, f"{kpi_name}.csv")
    df.to_csv(csv_path, index=False)
    return {
        "kpi": kpi_name,
        "status": "SUCCESS",
        "count": len(df),
        "csv_path": csv_path,
        "duration_s": round(time.monotonic() - started, 2),
        "_df": df,
    }


def _worker(worker_id, jobs, results, out_dir, connect_errors, connected):
    """Hold one connection and run queries from `jobs` until it is empty."""
    try:
        conn = _connect_with_retry(worker_id)
        connected.append(worker_id)
    except Exception as e:
        connect_errors.append(str(e))
        logger.error("[ENIQ] worker %s could not connect, leaving queries to "
                     "other workers: %s", worker_id, e)
        return

    try:
        while True:
            try:
                kpi_name, sql = jobs.get_nowait()
            except queue.Empty:
                return
            started = time.monotonic()
            try:
                results[kpi_name] = _run_query(conn, kpi_name, sql, out_dir)
                logger.info("[ENIQ] worker %s: %s OK (%s rows)", worker_id,
                            kpi_name, results[kpi_name]["count"])
            except Exception as e:
                logger.error("[ENIQ] worker %s: %s failed: %s", worker_id,
                             kpi_name, e, exc_info=True)
                results[kpi_name] = {
                    "kpi": kpi_name,
                    "status": "FAILED",
                    "error": str(e),
                    "duration_s": round(time.monotonic() - started, 2),
                }
                # The connection may be broken after an error: replace it.
                _close(conn)
                try:
                    conn = _connect_with_retry(worker_id)
                except Exception as ce:
                    connect_errors.append(str(ce))
                    conn = None
                    return
    finally:
        _close(conn)


class SybaseQueryView(APIView):
    """Run all Voice KPI ENIQ queries concurrently and save one CSV each."""

    def get(self, request):
        try:
            hours = _int_param(request.query_params.get("hours"),
                               DEFAULT_HOURS, 1, MAX_HOURS, "hours")
            workers = _int_param(request.query_params.get("workers"),
                                 DEFAULT_WORKERS, 1, MAX_WORKERS, "workers")
            include_data = str(request.query_params.get("include_data", "")).lower() in ("1", "true", "yes")

            requested = request.query_params.get("kpis")
            if requested:
                names = [n.strip() for n in requested.split(",") if n.strip()]
                unknown = [n for n in names if n not in VOICE_KPI_ENIQ_QUERIES]
                if unknown:
                    raise ValueError(
                        f"Unknown KPI(s): {unknown}. Available: {sorted(VOICE_KPI_ENIQ_QUERIES)}"
                    )
            else:
                names = list(VOICE_KPI_ENIQ_QUERIES)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(settings.OUTPUT_DIR, "VOICE", f"eniq_{timestamp}")
        os.makedirs(out_dir, exist_ok=True)

        jobs = queue.Queue()
        for name in names:
            jobs.put((name, VOICE_KPI_ENIQ_QUERIES[name].format(hours=hours)))

        results = {}
        connect_errors = []
        connected = []
        started = time.monotonic()
        threads = [
            threading.Thread(target=_worker, name=f"eniq-{i}",
                             args=(i, jobs, results, out_dir, connect_errors, connected),
                             daemon=True)
            for i in range(min(workers, len(names)))
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Queries no worker could pick up (every connection attempt failed).
        conn_error = connect_errors[-1] if connect_errors else "not executed"
        ordered = []
        for name in names:
            res = results.get(name) or {
                "kpi": name, "status": "FAILED",
                "error": f"No ENIQ connection: {conn_error}", "duration_s": 0,
            }
            df = res.pop("_df", None)
            if include_data and df is not None:
                res["results"] = df.to_dict(orient="records")
            ordered.append(res)

        succeeded = sum(1 for r in ordered if r["status"] == "SUCCESS")
        body = {
            "hours": hours,
            "output_dir": out_dir,
            "total": len(ordered),
            "succeeded": succeeded,
            "failed": len(ordered) - succeeded,
            "workers_requested": len(threads),
            "workers_connected": len(connected),
            "connect_errors": sorted(set(connect_errors)),
            "duration_s": round(time.monotonic() - started, 2),
            "kpis": ordered,
        }
        http_status = status.HTTP_200_OK if succeeded else status.HTTP_500_INTERNAL_SERVER_ERROR
        return Response(body, status=http_status)
