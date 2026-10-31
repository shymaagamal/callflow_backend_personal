"""
S1-MME site KPIs for a list of sites over the NCI windows
(current / 1 day before / 1 week before).

Used by POST /callflow/api/smartcare/site-kpis (SmartcareSiteKpiAPIView) and
by the RCA site run (rca_runner/site_mode.fetch_smartcare).

Data source: Trino (no Smartcare JDBC connection any more).
Every window is split at ``now - RECENT_HOURS`` into two non-overlapping
half-open ranges:

    [window start, cutoff)  -> hive.smartcare.s1mme_kpi               (Query 2)
    [cutoff, window end)    -> clickhouse.e2e.BKPI_SIGNAL_S1MME_CISAI_15MIN (Query 1)

so the default 12-hour current window is exactly "previous 6 hours" (Query 2)
+ "last 6 hours" (Query 1). The 1-day / 1-week comparison windows are entirely
older than the cutoff, so they are read from Query 2 only.

Both query results are normalised to the same columns and types, combined,
and then rolled up per site into the SAME ``windows[<name>]["s1mme"]`` records
this endpoint always returned (LOGICAL_SITE_ID + the S1-MME counters). The
combined per-time / per-fail-cause rows are returned as well, in
``s1mme_rows``.
"""

import datetime
import logging
import re
import time

import numpy as np
import pandas as pd

from ..constants.smartcare_sites_queries import trino_s1mme_QUERIES
from ..services.trino_service import _epochs, _LOCAL_TZ
from ..utils.trino_client import run_trino_query

logger = logging.getLogger(__name__)

SITE_COL = "LOGICAL_SITE_ID"            # site key of the per-site records (unchanged)
NCI_CACHE_GRANULARITY_MINUTES = 15
RECENT_HOURS = 6                        # the 15-min ClickHouse table covers this much

RECENT_SOURCE = "clickhouse_15min"      # Query 1
HISTORY_SOURCE = "hive_s1mme_kpi"       # Query 2

# Columns of both Trino queries, in order (Trino returns them lower-cased).
QUERY_SITE_COL = "Site_Logical_Name"
COUNT_COLS = [
    "ATTACH_REQ_TIMES",
    "ATTACH_FAIL_TIMES",
    "TAU_REQ_TIMES",
    "TAU_FAIL_TIMES",
    "S1MME_AUTHENTICATION_REQ_TIMES",
    "S1MME_AUTHENTICATION_FAIL_TIMES",
]
AUTH_COLS = ["S1MME_AUTHENTICATION_REQ_TIMES", "S1MME_AUTHENTICATION_FAIL_TIMES"]
QUERY_COLS = ["Time", QUERY_SITE_COL] + COUNT_COLS + ["FAIL_CAUSE_NAME"]

# The per-site records keep the column names this endpoint returned before.
_SITE_RECORD_NAMES = {QUERY_SITE_COL: SITE_COL, "ATTACH_FAIL_TIMES": "ATTACH_Fail_TIMES"}

# Same rule as rca_runner/site_mode._SITE_RE: site ids go into SQL, so only
# plain id characters are accepted.
_SITE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")

NOTES = {
    "source": "Trino. Rows older than %d hours: hive.smartcare.s1mme_kpi; the last %d hours: "
              "clickhouse.e2e.BKPI_SIGNAL_S1MME_CISAI_15MIN." % (RECENT_HOURS, RECENT_HOURS),
    "authentication": "S1MME_AUTHENTICATION_* come from the ClickHouse 15-minute table only; "
                      "they are null for rows from hive.smartcare.s1mme_kpi, and a site total is "
                      "null when the window has no ClickHouse rows for that site.",
    "s6a": "S6a is not retrieved from Trino; 's6a' is always an empty list.",
}


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


def split_window(start, end, cutoff):
    """[(source, part_start, part_end)] -- non-overlapping half-open parts of
    [start, end): before ``cutoff`` from Hive, from ``cutoff`` on from ClickHouse."""
    parts = []
    if start < min(end, cutoff):
        parts.append((HISTORY_SOURCE, start, min(end, cutoff)))
    if max(start, cutoff) < end:
        parts.append((RECENT_SOURCE, max(start, cutoff), end))
    return parts


def sql_site_list(sites):
    """('A','B') for the SQL IN list. Ids are validated first and quotes are
    doubled, so a site id cannot change the query."""
    bad = [s for s in sites if not _SITE_RE.match(s)]
    if bad:
        raise ValueError("invalid site id(s): %s" % bad)
    return "(" + ",".join("'%s'" % s.replace("'", "''") for s in sites) + ")"


# -- normalisation ------------------------------------------------------------

def _to_local_time(series):
    """Query times -> naive Africa/Cairo datetimes (the windows' clock).
    tz-aware values are converted, naive values are taken as local time,
    numbers are epoch seconds."""
    if series.empty:
        return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    if pd.api.types.is_numeric_dtype(series):
        out = pd.to_datetime(series, unit="s", utc=True, errors="coerce")
        return out.dt.tz_convert(_LOCAL_TZ).dt.tz_localize(None).astype("datetime64[ns]")
    return series.map(_one_local_time).astype("datetime64[ns]")


def _one_local_time(v):
    """One query time -> naive Cairo time (NaT when unusable). A datetime whose
    tzinfo reports no UTC offset is treated as naive local time."""
    if v is None or (not isinstance(v, (datetime.datetime, str)) and pd.isna(v)):
        return pd.NaT
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return pd.Timestamp(v, unit="s", tz="UTC").tz_convert(_LOCAL_TZ).tz_localize(None)
    if isinstance(v, datetime.datetime):
        offset = v.utcoffset() if v.tzinfo is not None else None
        if offset is None:
            return pd.Timestamp(v.replace(tzinfo=None))
        return pd.Timestamp((v - offset).replace(tzinfo=None), tz="UTC").tz_convert(_LOCAL_TZ).tz_localize(None)
    try:
        t = pd.Timestamp(v)
    except (ValueError, TypeError):
        return pd.NaT
    if t.tzinfo is not None and t.utcoffset() is not None:
        return t.tz_convert(_LOCAL_TZ).tz_localize(None)
    return t.tz_localize(None) if t.tzinfo is not None else t


def normalize_frame(df, source):
    """One query result -> QUERY_COLS (+ SOURCE) with fixed types:
    Time datetime64, site/fail cause str (or None), counters nullable Int64."""
    if df is None:
        df = pd.DataFrame()
    by_lower = {str(c).lower(): c for c in df.columns}
    missing = [c for c in QUERY_COLS if c.lower() not in by_lower]
    if missing and not df.empty:
        raise ValueError("%s result is missing column(s) %s (got %s)"
                         % (source, missing, list(df.columns)))
    out = pd.DataFrame(index=df.index)
    for col in QUERY_COLS:
        out[col] = df[by_lower[col.lower()]] if col.lower() in by_lower else pd.Series(index=df.index, dtype=object)

    out["Time"] = _to_local_time(out["Time"])
    for col in (QUERY_SITE_COL, "FAIL_CAUSE_NAME"):
        out[col] = out[col].map(lambda v: None if v is None or pd.isna(v) else str(v).strip()).astype(object)
    for col in COUNT_COLS:
        out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    if source == HISTORY_SOURCE:
        for col in AUTH_COLS:
            out[col] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["SOURCE"] = source
    return out.reset_index(drop=True)


def _empty_frame():
    return normalize_frame(pd.DataFrame(columns=QUERY_COLS), RECENT_SOURCE).iloc[0:0]


def combine_parts(frames):
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return _empty_frame()
    return pd.concat(frames, ignore_index=True).sort_values(
        ["Time", QUERY_SITE_COL], kind="stable", na_position="last").reset_index(drop=True)


def per_site(combined):
    """Combined rows -> one row per site, the record shape this endpoint
    always returned. A counter total is null when none of its rows has a value."""
    if combined.empty:
        return pd.DataFrame(columns=[SITE_COL] + [_SITE_RECORD_NAMES.get(c, c) for c in COUNT_COLS])
    grouped = combined.dropna(subset=[QUERY_SITE_COL]).groupby(QUERY_SITE_COL, sort=True)[COUNT_COLS]
    agg = grouped.sum(min_count=1).reset_index()
    return agg.rename(columns=_SITE_RECORD_NAMES)


def _records(df):
    """JSON-safe records: Int64/NaN -> int/None, Time -> 'YYYY-MM-DD HH:MM:SS'."""
    if df is None or df.empty:
        return []
    out = df.copy()
    if "Time" in out.columns:
        out["Time"] = out["Time"].map(lambda t: None if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M:%S"))
    return [{k: _py(v) for k, v in row.items()} for row in out.astype(object).to_dict(orient="records")]


def _py(v):
    if v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and v != v):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


# -- Trino ----------------------------------------------------------------------

def run_part(window_name, source, site_list, start, end, request_logger=None):
    """Run one query for [start, end). Returns (normalised frame, query log entry).
    Errors are raised after they are logged."""
    log = request_logger or logger
    start_epoch, end_epoch, start_lit, end_lit = _epochs(start, end)
    sql = trino_s1mme_QUERIES[source].format(
        site_list=site_list, start_epoch=start_epoch, end_epoch=end_epoch)
    entry = {"window": window_name, "source": source,
             "start": start_lit, "end": end_lit,
             "start_epoch": start_epoch, "end_epoch": end_epoch}
    log.info("[SITE KPI] %s %s range [%s, %s) epoch [%s, %s)",
             window_name, source, start_lit, end_lit, start_epoch, end_epoch)
    log.info("[SITE KPI] %s %s SQL:\n%s", window_name, source, sql)
    t0 = time.monotonic()
    try:
        frame = normalize_frame(run_trino_query(sql), source)
    except Exception as e:
        entry.update(rows=None, duration_s=round(time.monotonic() - t0, 2), error=str(e))
        log.error("[SITE KPI] %s %s failed after %.2fs: %s", window_name, source, entry["duration_s"], e)
        raise
    entry.update(rows=len(frame), duration_s=round(time.monotonic() - t0, 2))
    log.info("[SITE KPI] %s %s returned %d row(s) in %.2fs",
             window_name, source, entry["rows"], entry["duration_s"])
    if frame.empty:
        log.warning("[SITE KPI] %s %s returned no rows for [%s, %s)", window_name, source, start_lit, end_lit)
    return frame, entry


def fetch_smartcare_site_kpis(site_names, log_path="", tablename=None, start=None, end=None,
                              now=None, request_logger=None, window_names=None):
    """S1-MME KPIs per site for the three NCI windows, from Trino.

    start/end (optional): the current window; default the last 12 hours.
    now (optional, tests): the clock used for the default window and the
    ClickHouse/Hive cutoff. ``log_path`` and ``tablename`` are kept for the
    existing callers and are not used.
    window_names (optional): only these windows (e.g. ("current", "prev_1day"));
    default all three.

    A window with a failed query has no ``s1mme`` records (no partial totals);
    the failure is in ``errors`` and ``queries``.
    """
    sites = sorted({str(s).strip() for s in (site_names or []) if str(s).strip()})
    if not sites:
        raise ValueError("site_names is required")
    site_list = sql_site_list(sites)
    log = request_logger or logger

    now = _round_down(now or datetime.datetime.now(), NCI_CACHE_GRANULARITY_MINUTES)
    cutoff = now - datetime.timedelta(hours=RECENT_HOURS)
    windows = build_nci_time_windows(now=now, start=start, end=end)
    if window_names:
        windows = {k: v for k, v in windows.items() if k in window_names}
    log.info("[SITE KPI] Trino S1-MME for %d site(s) %s; ClickHouse from %s, Hive before",
             len(sites), sites, cutoff)

    out, rows, queries, errors = {}, {}, [], {}
    for name, window in windows.items():
        out[name] = {"start": str(window["start"]), "end": str(window["end"]), "s1mme": [], "s6a": []}
        rows[name] = []
        frames, failed = [], False
        for source, part_start, part_end in split_window(window["start"], window["end"], cutoff):
            try:
                frame, entry = run_part(name, source, site_list, part_start, part_end, log)
                frames.append(frame)
            except Exception as e:
                failed = True
                errors["%s.s1mme.%s" % (name, source)] = str(e)
                entry = {"window": name, "source": source, "start": str(part_start),
                         "end": str(part_end), "rows": None, "error": str(e)}
            queries.append(entry)
        if failed:
            log.error("[SITE KPI] %s: a query failed; no S1-MME totals for this window", name)
            continue
        combined = combine_parts(frames)
        out[name]["s1mme"] = _records(per_site(combined))
        rows[name] = _records(combined)
        missing = sorted(set(sites) - set(combined[QUERY_SITE_COL].dropna()))
        log.info("[SITE KPI] %s: %d combined row(s), %d site(s) with data, no data for %s",
                 name, len(combined), len(out[name]["s1mme"]), missing or "none")

    result = {
        "sites": sites,
        "cells": None,              # Smartcare cell lookup no longer used
        "windows": out,
        "s1mme_rows": rows,
        "queries": queries,
        "notes": NOTES,
        "errors": errors,
    }
    if not errors and not any(w["s1mme"] for w in out.values()):
        result["error"] = "no S1-MME rows for these sites in Trino"
        log.warning("[SITE KPI] %s", result["error"])
    return result
