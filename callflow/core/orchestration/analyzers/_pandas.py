"""
Small pandas helpers used by the analyzers' DEFAULT providers (time-series and
Splunk subscriber mapping). Kept isolated and lazy so importing an analyzer for
unit tests never requires pandas — tests inject fakes instead.
"""

from __future__ import annotations


def build_time_series(df, value_col, time_col, start, end):
    """Chart-ready [{datetime, value}] aggregated (mean) per timestamp in range.

    Mirrors the old abnormal_kpis._build_time_series_data exactly.
    """
    try:
        import pandas as pd  # noqa: F401
        if df is None or getattr(df, "empty", True):
            return []
        if value_col not in df.columns or time_col not in df.columns:
            return []
        filtered = df[(df[time_col] >= start) & (df[time_col] <= end)][[time_col, value_col]].dropna(subset=[value_col])
        if filtered.empty:
            return []
        agg = filtered.groupby(time_col)[value_col].mean().reset_index().sort_values(time_col)
        return [
            {"datetime": row[time_col].isoformat() + "Z", "value": round(float(row[value_col]), 3)}
            for _, row in agg.iterrows()
        ]
    except Exception:
        return []


def npp_series_provider(raw_df, ctx):
    qtr = ctx.get("query_time_range", {})
    start = qtr.get("query_start_1day"); end = qtr.get("query_end_1day")
    cell_col = None
    try:
        if raw_df is not None and not getattr(raw_df, "empty", True):
            cols = raw_df.columns
            cell_col = "cell" if "cell" in cols else ("Cell" if "Cell" in cols else None)
    except Exception:
        cell_col = None

    def provider(cell, kpi):
        if cell_col is None:
            return []
        try:
            return build_time_series(raw_df[raw_df[cell_col] == cell], kpi, "DATETIME_ID", start, end)
        except Exception:
            return []
    return provider


def viavi_series_provider(raw_df, ctx):
    qtr = ctx.get("query_time_range", {})
    start = qtr.get("query_start_1day"); end = qtr.get("query_end_1day")

    def provider(flwid, kpi):
        try:
            if raw_df is None or getattr(raw_df, "empty", True) or "FLWID" not in raw_df.columns:
                return []
            import pandas as pd
            sub = raw_df[raw_df["FLWID"] == flwid].copy()
            if "PERIOD_TIMESTAMP" in sub.columns:
                sub["PERIOD_TIMESTAMP"] = pd.to_datetime(sub["PERIOD_TIMESTAMP"], errors="coerce")
            return build_time_series(sub, kpi, "PERIOD_TIMESTAMP", start, end)
        except Exception:
            return []
    return provider


def splunk_series_provider(raw_by_node, ctx):
    qtr = ctx.get("query_time_range", {})
    start = qtr.get("query_start_1day"); end = qtr.get("query_end_1day")

    def provider(node, kpi_col):
        try:
            df = (raw_by_node or {}).get(node)
            if df is None or getattr(df, "empty", True):
                return []
            d = df.reset_index() if getattr(df.index, "name", None) == "timestamp" else df
            return build_time_series(d, kpi_col, "timestamp", start, end)
        except Exception:
            return []
    return provider


def splunk_subs_mapper(smartcare, ctx):
    processed = smartcare.get("processed_data") if isinstance(smartcare, dict) else None
    if processed is None or getattr(processed, "empty", True):
        return None
    try:
        from ...utils.data_utils import map_splunk_kpi_to_smartcare
        from ...constants.endpoints import NODE_COLUMNS_FROM_SMARTCARE
    except Exception:
        return None

    def mapper(kpi_col):
        try:
            mapped_value, column, _suffix = map_splunk_kpi_to_smartcare(kpi_col, processed)
            if mapped_value is None or column is None:
                return (None, None, None)
            node_type = NODE_COLUMNS_FROM_SMARTCARE.get(column)
            impacted = processed[
                processed[column].astype(str).str.contains(str(mapped_value), case=False, na=False)
            ]["MSISDN"].nunique()
            return (mapped_value, int(impacted), node_type)
        except Exception:
            return (None, None, None)
    return mapper
