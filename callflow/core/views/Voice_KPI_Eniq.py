"""
Voice KPI extraction from ENIQ (Sybase IQ).

GET /callflow/api/voice-kpi-eniq?hours=1
    Runs every query in constants/voice_kpis_Eniq_Queries.py concurrently
    (one ENIQ connection per query) and writes one CSV per KPI to
        OUTPUT_DIR/VOICE/eniq_<YYYYmmdd_HHMMSS>/<KPI_NAME>.csv

Optional query params:
    hours         look-back window, int 1..168 (default 1)
    kpis          comma-separated subset of KPI names (default: all)
    workers       max parallel queries, 1..20 (default 10)
    include_data  "true" to also return the rows in the response (default false)

A failing query does not stop the others: it is reported with status FAILED
and the rest still produce their CSVs. HTTP status is 200 if at least one
query succeeded, 500 if all failed.
"""
import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import jpype
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
DEFAULT_WORKERS = 10
MAX_WORKERS = 20


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


def _run_one_query(kpi_name, sql, out_dir):
    """Run one KPI query on its own connection and write its CSV."""
    started = time.monotonic()
    conn = None
    cursor = None
    try:
        # jaydebeapi/JPype: a worker thread must be attached to the JVM.
        try:
            if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
                jpype.attachThreadToJVM()
        except Exception:
            pass  # newer JPype versions attach threads automatically

        conn = EniqConnection()
        if conn is None:  # EniqConnection logs and returns None on failure
            raise RuntimeError("Eniq connection failed (see server log)")
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [str(desc[0]) for desc in cursor.description]
        rows = cursor.fetchall()

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
    except Exception as e:
        logger.error("[ENIQ] %s failed: %s", kpi_name, e, exc_info=True)
        return {
            "kpi": kpi_name,
            "status": "FAILED",
            "error": str(e),
            "duration_s": round(time.monotonic() - started, 2),
        }
    finally:
        for res in (cursor, conn):
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass


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

        # Start the JVM once on this thread (opening and closing one connection)
        # so the worker threads don't race to call jpype.startJVM().
        if not jpype.isJVMStarted():
            warm = EniqConnection()
            if warm is None:
                return Response({"error": "Eniq connection failed"},
                                status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            warm.close()

        started = time.monotonic()
        results = {}
        with ThreadPoolExecutor(max_workers=min(workers, len(names))) as pool:
            futures = {
                pool.submit(_run_one_query, name,
                            VOICE_KPI_ENIQ_QUERIES[name].format(hours=hours), out_dir): name
                for name in names
            }
            for fut in as_completed(futures):
                res = fut.result()
                results[res["kpi"]] = res
                logger.info("[ENIQ] %s %s (%ss)", res["kpi"], res["status"], res["duration_s"])

        # Keep the response in the same order as the constants file.
        ordered = [results[n] for n in names]
        for res in ordered:
            df = res.pop("_df", None)
            if include_data and df is not None:
                res["results"] = df.to_dict(orient="records")

        succeeded = sum(1 for r in ordered if r["status"] == "SUCCESS")
        body = {
            "hours": hours,
            "output_dir": out_dir,
            "total": len(ordered),
            "succeeded": succeeded,
            "failed": len(ordered) - succeeded,
            "duration_s": round(time.monotonic() - started, 2),
            "kpis": ordered,
        }
        http_status = status.HTTP_200_OK if succeeded else status.HTTP_500_INTERNAL_SERVER_ERROR
        return Response(body, status=http_status)
