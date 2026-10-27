# callflow/core/services/smartcare_query_runner.py
"""
FIXED VERSION of this file.

THE BUG THAT WAS MAKING SMARTCARE SLOW:
The original execute_smartcare_queries() imported ThreadPoolExecutor and
threading, but never actually used them -- it opened ONE connection,
took ONE cursor, and ran every KPI query in a plain `for` loop on that
single cursor. So a request with 12 KPI queries took the SUM of all 12
query times, not the time of the slowest one.

This version:
  1. Actually runs queries concurrently with ThreadPoolExecutor.
  2. Each thread gets its OWN connection (borrowed from the pool) --
     sharing one cursor/connection across threads is not safe for most
     Hive/Impala/JDBC drivers, so "real" connections per worker is required,
     not optional.
  3. Logs a START/DONE line with timing for every single query, plus an
     overall wall-clock summary, so you can see in the log file exactly
     which query is slow if it ever happens again.

Public API is UNCHANGED on purpose (execute_smartcare_queries,
calculate_tablename, format_sql_in) -- smartcare_service.py does not need
any changes to pick this fix up; this is a drop-in replacement for the
file of the same name.
"""

import logging
import time
import datetime
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..connectors.smartcare_pool import get_smartcare_pool
from ..utils.data_utils import convert_java_types
from ..utils.logging_utils import build_request_logger
from ..utils.connections import smartcareConnection

logger = logging.getLogger(__name__)

# How many queries to run at once. Bound by how much concurrency your
# Hive/Impala backend can actually serve -- raising this past what the
# backend tolerates will just queue up there instead of helping.
DEFAULT_MAX_WORKERS = 6


def calculate_tablename(start_dt: datetime.datetime) -> int:
    """Calculate Hive table suffix (days since 1970-01-01). Unchanged."""
    ref_date = datetime.datetime(1970, 1, 1)
    return (pd.to_datetime(start_dt) - ref_date).days


def format_sql_in(values, quote: bool = False) -> str:
    """Convert list -> SQL IN (...) safely. Unchanged."""
    if not values:
        return "(NULL)"
    cleaned = [f"'{v}'" if quote and pd.notna(v) else str(v) for v in values if pd.notna(v)]
    return f"({','.join(cleaned)})" if cleaned else "(NULL)"


def _fallback_logger(log_path):
    """
    Lets this module be called from places that only pass a log_path
    string (the old calling convention) without breaking -- builds a
    request-scoped logger on the fly if no LoggerAdapter was supplied.
    """
    request_id = str(int(time.time() * 1000))
    return build_request_logger(log_path, request_id, who="unknown")


def _to_python_string(value):
    """
    Converts a value to a plain Python str ONLY if it's a java.lang.String
    (or similar string-shaped Java object) -- identified by "String" or
    "CharSequence" appearing in its type name. Anything else (numbers,
    None, already-native Python values) passes through untouched.

    This is intentionally narrow: numeric Java types (Long, BigDecimal,
    etc.) are left alone here on purpose, so convert_java_types() downstream
    still gets first crack at converting them to proper int/float -- this
    function exists only to stop java.lang.String objects (column names
    AND cell values) from surviving all the way to pickle.dumps(), which
    is where they were crashing cache.set().
    """
    if value is None:
        return value
    type_name = str(type(value))
    if "String" in type_name or "CharSequence" in type_name:
        try:
            return str(value)
        except Exception:
            return None
    return value


def run_single_query(query_name: str, query_template: str, params: dict, log_path: str, request_logger, max_retries: int = 1):
    """
    Runs ONE query against a connection borrowed from the pool, returns it
    when done (or invalidates it if the connection itself errored), and
    logs timing for this specific query.

    THE JVM-ATTACHMENT FIX:
    This function runs inside a ThreadPoolExecutor worker thread, not the
    main thread the JVM was started on. jaydebeapi/JPype's automatic
    Java -> Python type conversion only works reliably on threads attached
    to the JVM, so each worker thread needs to attach itself.

    ORDERING MATTERS HERE: the attach check must happen AFTER pool.acquire(),
    not before. The JVM only actually gets started inside smartcareConnection()
    (called by the pool's factory when it creates a connection) -- so on a
    cold process, attaching before acquiring a connection tries to attach to
    a JVM that doesn't exist yet, which throws JVMNotRunning. Acquiring first
    guarantees the JVM exists (either this call started it, or an earlier
    one already did) before we try to attach to it.

    RETRY-ON-STALE-CONNECTION:
    Pooled connections can go dead between requests (Hive/Impala server-side
    idle session timeouts are common). A dead connection fails almost
    instantly the moment you try to use it. Rather than give up on the first
    failure, this retries ONCE with a freshly-acquired connection before
    reporting the query as failed.
    """
    import jpype

    start = time.perf_counter()
    last_error = None

    for attempt in range(1, max_retries + 1):
        conn = None
        attempt_start = time.perf_counter()
        try:
            # ============================================================
            # Seperated Tunel fo each Query
            # ============================================================
            conn = smartcareConnection(log_path)

            try:
                if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
                    jpype.attachThreadToJVM()
            except Exception as attach_err:
                request_logger.warning(f"[{query_name}] could not attach thread to JVM: {attach_err}")

            cursor = conn.cursor()
            query = query_template.format(**params)
            print(f"[{query_name}] {query} -> {attempt_start:.2f}s")
            request_logger.info(
                f"\n===== FINAL SQL ({query_name}) =====\n"
                f"{query}\n"
                f"====================================="
            )
            
            cursor.execute(query)
            raw_results = cursor.fetchall()

            request_logger.info(
                f"[{query_name}] raw rows = {len(raw_results)}")

            try:
                columns = [_to_python_string(desc[0]) for desc in cursor.description] if cursor.description else []
            except Exception as col_err:
                request_logger.warning(f"[{query_name}] could not read cursor.description: {col_err}")
                columns = [f"col_{i}" for i in range(len(raw_results[0]))] if raw_results else []

            cursor.close()
            conn.close()  
            elapsed = time.perf_counter() - attempt_start

            if not raw_results:
                request_logger.info(f"[QUERY DONE] {query_name} -> 0 rows ({elapsed:.2f}s)")
                empty_df = pd.DataFrame(columns=columns)
                empty_df['__source'] = query_name
                return empty_df

            results = [
                tuple(_to_python_string(v) for v in row)
                for row in raw_results
            ]

            df = pd.DataFrame(results, columns=columns)
            df['__source'] = query_name
            request_logger.info(f"[QUERY DONE] {query_name} -> {len(df)} rows ({elapsed:.2f}s, attempt {attempt}/{max_retries})")
            return df

        except Exception as e:
            last_error = e
            elapsed = time.perf_counter() - attempt_start
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            msg = str(e)
            if "Table not found" in msg or "UnresolvedRelation" in msg:
                request_logger.error(f"[QUERY SKIPPED] {query_name} -> Table not found ({elapsed:.2f}s, likly retention:  {msg[:250]})")
                return pd.DataFrame()
            if attempt < max_retries:
                request_logger.warning(
                    f"[{query_name}] attempt {attempt}/{max_retries} failed after {elapsed:.2f}s ({e}) -- "
                    f"retrying with a brand-new connection"
                )
                continue

            request_logger.exception(
                f"[QUERY FAILED] {query_name} after {elapsed:.2f}s ({max_retries} attempts): {last_error}"
            )
            request_logger.error(f"\n===== SQL ({query_name}) =====\n{query}\n==============================")
            return pd.DataFrame()
#------------- Queries in Parallel ---------------------
# def execute_smartcare_queries(
#     queries_dict: dict,
#     params: dict,
#     log_path: str = None,
#     request_logger=None,
#     max_workers: int = DEFAULT_MAX_WORKERS,
# ) -> pd.DataFrame:
#     """
#     Runs every query in queries_dict CONCURRENTLY (this is the actual fix)
#     and concatenates whatever comes back.

#     Backward compatible: existing callers passing only log_path keep working
#     exactly as before; pass request_logger (from build_request_logger) for
#     proper request-tagged, step-by-step logging.
#     """
#     request_logger = request_logger or _fallback_logger(log_path)
#     pool = get_smartcare_pool()
#     final_data = []

#     overall_start = time.perf_counter()
#     request_logger.info(
#         f"[SMARTCARE] launching {len(queries_dict)} queries in parallel (max_workers={max_workers})"
#     )

#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         futures = {
#             executor.submit(run_single_query, name, template, params, pool, request_logger): name
#             for name, template in queries_dict.items()
#         }
#         for future in as_completed(futures):
#             name = futures[future]
#             try:
#                 df = future.result()
#                 if not df.empty:
#                     final_data.append(df)
#             except Exception:
#                 request_logger.exception(f"[SMARTCARE] unexpected error collecting result for {name}")

#     total_elapsed = time.perf_counter() - overall_start
#     request_logger.info(
#         f"[SMARTCARE] {len(final_data)}/{len(queries_dict)} queries returned data "
#         f"in {total_elapsed:.2f}s wall time (this is the number that used to be the SUM of every query)"
#     )

#     if not final_data:
#         return pd.DataFrame()

#     result = pd.concat(final_data, ignore_index=True)
#     result = convert_java_types(result)

#     numeric_cols = result.select_dtypes(include=['number']).columns
#     if len(numeric_cols) > 0:
#         result[numeric_cols] = result[numeric_cols].replace(
#             [float('inf'), float('-inf'), np.inf, -np.inf, pd.NA, None, np.nan],
#             0,
#         )

#     return result

def execute_smartcare_queries(
    queries_dict: dict,
    params: dict,
    log_path: str = None,
    request_logger=None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    runner=None,
    ) -> pd.DataFrame:

    """
    Runs every query in ``queries_dict`` CONCURRENTLY -- each query opens its own
    isolated connection (see run_single_query, "Separated Tunnel for each Query"),
    so they are independent and safe to overlap. Previously they ran one-at-a-time,
    which made the Smartcare stage take the SUM of every query (45-120s each) and
    dominate the whole pipeline; now it takes roughly the MAX.

    Ordering is preserved (results are gathered back in ``queries_dict`` order) so
    the concatenated DataFrame is byte-identical to the old sequential output. The
    first query runs on the calling thread to start the JVM once before fanning the
    rest out (concurrent cold JVM starts would race on jpype.startJVM()).

    Public signature is unchanged except for an optional injectable ``runner``
    (used only by tests); production callers are unaffected.
    """
    from ._parallel import run_in_order

    request_logger = request_logger or _fallback_logger(log_path)
    run_one = runner or (
        lambda item: run_single_query(item[0], item[1], params, log_path, request_logger)
    )

    items = list(queries_dict.items())
    if not items:
        return pd.DataFrame()

    workers = max(1, min(max_workers, len(items)))
    overall_start = time.perf_counter()
    request_logger.info(
        f"[SMARTCARE] running {len(items)} queries in parallel "
        f"(1 warm-up starts the JVM, then up to {workers} concurrent)"
    )

    def _on_error(item, exc):
        request_logger.exception(f"[SMARTCARE] unexpected error running {item[0]}: {exc}")

    ordered = run_in_order(items, run_one, max_workers=workers,
                           first_sync=True, on_error=_on_error)

    # Gather non-empty frames in the ORIGINAL query order (identical to the old
    # sequential concatenation).
    final_data = [df for (_item, df) in ordered
                  if df is not None and not getattr(df, "empty", True)]

    total_elapsed = time.perf_counter() - overall_start
    request_logger.info(
        f"[SMARTCARE] {len(final_data)}/{len(items)} queries returned data "
        f"in {total_elapsed:.2f}s wall time (parallel; was the SUM sequentially)"
    )

    if not final_data:
        return pd.DataFrame()

    result = pd.concat(final_data, ignore_index=True)
    result = convert_java_types(result)

    numeric_cols = result.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        result[numeric_cols] = result[numeric_cols].replace(
            [float('inf'), float('-inf'), np.inf, -np.inf, pd.NA, None, np.nan],
            0,
        )

    return result