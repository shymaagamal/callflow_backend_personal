import datetime
import logging

import pandas as pd

from ..utils.data_utils import convert_java_types
from ..utils.java_sanitize import sanitize_for_cache
from ..services.smartcare_query_runner import (
    execute_smartcare_queries,
    calculate_tablename,
    format_sql_in,
)
from ..constants.smartcare_sites_queries import (
    smartcare_site_cells,
    smartcare_site_kpi_QUERIES,
)

logger = logging.getLogger(__name__)

CHUNK_HOURS = 2
SITE_COL = "LOGICAL_SITE_ID"
NCI_CACHE_GRANULARITY_MINUTES = 15


def _round_down(dt, minutes):
    discard = datetime.timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    return dt - discard


def build_nci_time_windows(now=None, start=None, end=None):
    """Default: the last 12 hours (SmartcareSiteKpiAPIView). With start/end the
    current window is exactly that range (RCA site run); the comparison windows
    are the same range 1 day and 1 week earlier in both cases."""
    if start is not None and end is not None:
        current_start, current_end = start, end
    else:
        now = _round_down(now or datetime.datetime.now(), NCI_CACHE_GRANULARITY_MINUTES)
        current_start = now - datetime.timedelta(hours=12)
        current_end = now
    logger.info("[SITE KPI] current window: %s -> %s", current_start, current_end)
    return {
        "current": {"start": current_start, "end": current_end},
        "prev_1day": {
            "start": current_start - datetime.timedelta(days=1),
            "end": current_end - datetime.timedelta(days=1),
        },
        "prev_1week": {
            "start": current_start - datetime.timedelta(days=7),
            "end": current_end - datetime.timedelta(days=7),
        },
    }


def _as_records(df):
    if df is None or df.empty:
        return []
    return sanitize_for_cache(convert_java_types(df).to_dict(orient="records"))



def _run_window(key, sql, params, start, end, log_path):
    frames = []
    bounds = [start]
    if start.date() != end.date():
        bounds.append(datetime.datetime.combine(end.date(), datetime.time.min))
    bounds.append(end)

    for chunk_start, chunk_end in zip(bounds, bounds[1:]):
        table = calculate_tablename(chunk_start)
        logger.info("[SITE KPI] %s %s -> %s table=%s", key, chunk_start, chunk_end, table)
        df = execute_smartcare_queries(
            queries_dict={key: sql},
            params={
                **params,
                "tablename": table,
                "start_date": str(chunk_start),
                "end_date": str(chunk_end),
            },
            log_path=log_path,
        )
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return (
        pd.concat(frames, ignore_index=True)
        .groupby(SITE_COL, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )


def fetch_smartcare_site_kpis(site_names, log_path="", tablename=None, start=None, end=None):
    """start/end (optional): the current window; default the last 12 hours."""
    sites = sorted({str(s).strip() for s in (site_names or []) if str(s).strip()})
    if not sites:
        raise ValueError("site_names is required")

    site_tuple = format_sql_in(sites, quote=True)
    logger.info("[SITE KPI] smartcare Query for sites %s -> %s", site_tuple, smartcare_site_cells)
    cells = execute_smartcare_queries(
        queries_dict={"cells": smartcare_site_cells},
        params={"site_tuple": site_tuple},
        log_path=log_path,
    )
    if cells is None or cells.empty or "CGISAI" not in getattr(cells, "columns", []):
        return {
            "sites": sites,
            "cells": 0,
            "windows": {},
            "error": "no cells for these sites in DIM_LOC_CGISAI",
        }

    cgi_values = cells["CGISAI"].dropna().astype(str).unique().tolist()
    logger.info("[SITE KPI] %s cells for %s", len(cgi_values), sites)
    if len(cgi_values) > 2000:
        raise ValueError(
            "site matched %s cells; check LOGICAL_SITE_ID against DIM_LOC_CGISAI" % len(cgi_values)
        )

    params = {
        "site_tuple": site_tuple,
        "cgi_tuple": format_sql_in(cgi_values, quote=True),
    }
    windows = build_nci_time_windows(start=start, end=end)
    print("[SITE KPI] windows: %s", windows)
    out = {}
    errors = {}

    for name, window in windows.items():
        out[name] = {
            "start": str(window["start"]),
            "end": str(window["end"]),
            "s1mme": [],
            "s6a": [],
        }
        for key, sql in smartcare_site_kpi_QUERIES.items():
            try:
                out[name][key] = _as_records(
                    _run_window(key, sql, params, window["start"], window["end"], log_path)
                )
            except Exception as e:
                logger.exception("[SITE KPI] %s %s failed", name, key)
                errors["%s.%s" % (name, key)] = str(e)

    return {
        "sites": sites,
        "cells": len(cgi_values),
        "windows": out,
        "errors": errors,
    }