
"""
Reusable Trino cell-name retrieval.

This exists so the orchestration layer (and the existing smartcare_trino view)
can share ONE copy of the Trino cell-lookup query instead of duplicating it.
Per request section 12, the SQL text itself is NOT modified here -- it is the
exact query previously inlined in views/smartcare_trino.py, relocated verbatim.
Only application-level concerns (where/how it runs, input shaping, returning a
clean cell-name list) live here.

    Trino  ->  CELL NAMES
    Trino  ->  CUSTOMER COUNTS (unique MSISDN per cell / site, same query result)

Smartcare is intentionally NOT involved in this path.
"""

import logging
import os
import re
from datetime import datetime, timezone

import pandas as pd

from ..utils.trino_client import run_trino_query
from ..utils.mapping_cell_name import map_cell_ids

logger = logging.getLogger(__name__)


def _safe_msisdns(msisdn_list):
    safe = [re.sub(r"[^0-9]", "", str(m)) for m in (msisdn_list or [])]
    return [m for m in safe if m]


def _epochs(start_dt: datetime, end_dt: datetime):
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    return (
        int(start_dt.timestamp()),
        int(end_dt.timestamp()),
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def build_cell_query(in_clause, start_epoch, end_epoch, start_ts_literal, end_ts_literal) -> str:
    """The exact cell-lookup SQL (relocated verbatim from smartcare_trino view)."""
    return f"""SELECT
            DATE_FORMAT(FROM_UNIXTIME(CAST(time_unix AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
            MSISDN,
            Site_name AS cell_ID,
            lac_tac_dec
            FROM hive.smartcare.npl_4g
            WHERE MSISDN IN ({in_clause})
            AND CAST(time_unix AS DOUBLE) >= {start_epoch}
            AND CAST(time_unix AS DOUBLE) < {end_epoch}
        UNION ALL
            SELECT
            DATE_FORMAT(FROM_UNIXTIME(CAST(time_unix AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
            MSISDN,
            Site_name AS cell_ID,
            lac_tac_dec
            FROM iceberg.smartcare_iceberg.npl_attach_4g
            WHERE MSISDN IN ({in_clause})
            AND CAST(time_unix AS DOUBLE) >= {start_epoch}
            AND CAST(time_unix AS DOUBLE) < {end_epoch}
        UNION ALL
            SELECT
                DATE_FORMAT(FROM_UNIXTIME(CAST(calc_time AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
                msisdn,
                sai_cgi_ecgi AS cell_ID,
                NULL AS lac_tac_dec
            FROM iceberg.smartcare_iceberg.npl_ho_4g
            WHERE msisdn IN ({in_clause}) AND datetime >= TIMESTAMP '{start_ts_literal}'
            AND datetime < TIMESTAMP '{end_ts_literal}'"""


def fetch_cell_dataframe(msisdn_list, start_dt: datetime, end_dt: datetime,
                         sql_sink=None) -> pd.DataFrame:
    """Run the Trino cell query and attach a mapped ``cell_name`` column.

    ``sql_sink`` (optional): a ``callable(final_sql: str)`` invoked with the
    fully parameter-substituted SQL right before execution, so callers can log
    exactly what Trino received. The query itself is unchanged.
    """
    safe = _safe_msisdns(msisdn_list)
    if not safe:
        raise ValueError("No valid msisdn values provided for Trino cell lookup")

    start_epoch, end_epoch, start_ts_literal, end_ts_literal = _epochs(start_dt, end_dt)
    in_clause = ", ".join(f"'{m}'" for m in safe)

    final_sql = build_cell_query(in_clause, start_epoch, end_epoch, start_ts_literal, end_ts_literal)
    if sql_sink is not None:
        try:
            sql_sink(final_sql)
        except Exception:  # logging the query must never break the query
            logger.warning("[TRINO] sql_sink raised; continuing", exc_info=True)

    df = run_trino_query(final_sql)
    if not df.empty:
        df["cell_name"] = map_cell_ids(df["cell_ID"].tolist())
    else:
        df["cell_name"] = pd.Series(dtype="object")
    return df


def cell_names_from_df(df) -> list:
    """Unique, cleaned cell names from a ``fetch_cell_dataframe`` result.

    Same cleaning rule ``get_cell_names`` has always used (dropna, strip, drop
    empty / 'none'), factored out so the orchestrator can derive the cell list
    AND the customer counts from ONE Trino query.
    """
    if df is None or df.empty or "cell_name" not in df.columns:
        return []
    cells = (
        df["cell_name"].dropna().astype(str).str.strip()
    )
    cells = [c for c in cells.unique().tolist() if c and c.lower() != "none"]
    logger.info("[TRINO] resolved %d unique cell names", len(cells))
    return cells


def get_cell_names(msisdn_list, start_dt: datetime, end_dt: datetime, sql_sink=None) -> list:
    """
    Trino -> unique, cleaned cell names. This is the fast cell-name source that
    lets NPP/Viavi/Remedy retrieval start WITHOUT waiting for Smartcare.

    ``sql_sink`` is forwarded to ``fetch_cell_dataframe`` for query logging.
    """
    df = fetch_cell_dataframe(msisdn_list, start_dt, end_dt, sql_sink=sql_sink)
    return cell_names_from_df(df)


def _msisdn_column(df):
    """The MSISDN column of the Trino result (alias casing may vary by driver)."""
    for col in df.columns:
        if str(col).lower() == "msisdn":
            return col
    return None


def customer_counts_from_df(df, log_path: str = os.devnull) -> dict:
    """Customer (unique MSISDN) counts from the EXISTING Trino cell DataFrame.

    Same formula Smartcare uses in ``calculate_subs_per_cell_site_handset``:
        subs_per_cell = groupby('cell_name')['MSISDN'].nunique()
        subs_per_site = groupby('modified_site_name')['MSISDN'].nunique()
    applied to the Trino rows (already filtered to the complainant MSISDNs and
    the request window by the query). Cell names use the same cleaning as
    ``cell_names_from_df`` so every resolved cell has a count; sites use the same
    ``process_sitename`` rule as Smartcare / the master-sheet lookup.

    Returns::

        {
          "customers_per_cell": {cell_name: int},          # NPP / Viavi
          "subs_per_site": [{"label": site, "value": int}], # Remedy ranking
          "nodes_frame": DataFrame[MSISDN, cell_name, modified_site_name, ...],
                                                          # NPP hub/TAC/LAC enrichment
        }

    Empty structures when the frame has no usable rows.
    """
    empty = {"customers_per_cell": {}, "subs_per_site": [], "nodes_frame": pd.DataFrame()}
    if df is None or df.empty or "cell_name" not in df.columns:
        return empty
    msisdn_col = _msisdn_column(df)
    if msisdn_col is None:
        logger.warning("[TRINO] no MSISDN column in Trino result; customer counts unavailable")
        return empty

    frame = pd.DataFrame({
        "MSISDN": df[msisdn_col],
        "cell_name": df["cell_name"],
    }).dropna(subset=["cell_name", "MSISDN"])
    frame["cell_name"] = frame["cell_name"].astype(str).str.strip()
    frame["MSISDN"] = frame["MSISDN"].astype(str).str.strip()
    frame = frame[
        (frame["cell_name"] != "") & (frame["cell_name"].str.lower() != "none")
        & (frame["MSISDN"] != "")
    ].copy()
    if frame.empty:
        return empty

    per_cell = frame.groupby("cell_name")["MSISDN"].nunique()
    customers_per_cell = {str(c): int(n) for c, n in per_cell.items()}

    from ..utils.data_utils import process_sitename
    frame = process_sitename(frame, "cell_name", log_path)
    if "modified_site_name" in frame.columns:
        frame.loc[:, "modified_site_name"] = frame["modified_site_name"].replace("", None)
        per_site = frame.groupby("modified_site_name")["MSISDN"].nunique().dropna()
        subs_per_site = [
            {"label": site, "value": int(n)}
            for site, n in per_site.items() if pd.notna(site)
        ]
    else:
        subs_per_site = []

    logger.info("[TRINO] customer counts: %d cells, %d sites",
                len(customers_per_cell), len(subs_per_site))
    return {
        "customers_per_cell": customers_per_cell,
        "subs_per_site": subs_per_site,
        "nodes_frame": frame,
    }
