"""
Voice KPI calculation from ENIQ counter rows.

Input:  one DataFrame per ENIQ query, as returned by the Voice KPI ENIQ view
        (columns labelled exactly as ENIQ returns them).
Output: one table per query with the green columns of its sample workbook
        (time, node, green counters, calculated KPIs), one row per node and
        15-minute period, plus per-KPI status and mapping. The counters that
        only feed a formula are not returned.

The KPI definitions (query, counters, formula, output columns) are in
constants/voice_kpis_Eniq_Formulas.py.

Missing data rules
    - A KPI whose counters or node column are not in the query result fails
      on its own with a message naming the missing labels; other KPIs go on.
    - A row where every counter of the KPI is null carries no data for it and
      is skipped.
    - Otherwise a null counter counts as 0 in sums (as the Excel samples do
      with blank cells), except for averaged gauges, where null rows are left
      out of the average.
    - A ratio whose denominator is 0 or below gives value None, never 0 or inf.
"""

import logging
import math

import numpy as np
import pandas as pd

from ..constants.voice_kpis_Eniq_Formulas import (
    DATETIME_COLUMNS, QUERY_OUTPUT_NOTES, QUERY_OUTPUTS, VOICE_KPI_SPECS,
)

logger = logging.getLogger("callflow_logger")

LEVELS = ("node", "object")
VALUE_DECIMALS = 6


class MissingColumns(ValueError):
    pass


def _resolve(columns, candidates):
    """Return the result column that matches one of the candidate labels (case-insensitive)."""
    by_upper = {str(c).upper(): c for c in columns}
    for label in candidates:
        hit = by_upper.get(label.upper())
        if hit is not None:
            return hit
    return None


def _resolve_many(columns, candidates_list):
    """First match for each candidate tuple, skipping those that are absent."""
    out = []
    for candidates in candidates_list:
        col = _resolve(columns, candidates)
        if col is not None and col not in out:
            out.append(col)
    return out


def _format_time(value):
    if value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _clean_value(value):
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return round(v, VALUE_DECIMALS)


def _prepare(spec, df):
    """Map ENIQ labels to formula variables and dimensions; return the working frame."""
    cols = list(df.columns)
    missing = []

    time_col = _resolve(cols, DATETIME_COLUMNS)
    if time_col is None:
        missing.append(" / ".join(DATETIME_COLUMNS))
    node_col = _resolve(cols, spec.node)
    if node_col is None:
        missing.append(" / ".join(spec.node))

    counter_cols = {}
    for var, candidates in spec.counters.items():
        col = _resolve(cols, candidates)
        if col is None:
            missing.append(" / ".join(candidates))
        else:
            counter_cols[var] = col
    if missing:
        raise MissingColumns(
            f"{spec.query} result has no column for: {missing}. Returned columns: {cols}"
        )

    extra_cols = _resolve_many(cols, [spec.extra_dims]) if spec.extra_dims else []
    object_cols = [c for c in _resolve_many(cols, [(d,) for d in spec.object_dims])
                   if c not in (node_col, time_col)]
    # object_dims lists alternative spellings of the same column; keep one.
    object_cols = object_cols[:1]

    work = df
    if spec.row_filter is not None:
        mask = spec.row_filter(df)
        if mask is not None:
            work = df[mask]

    out = pd.DataFrame(index=work.index)
    out["_time"] = pd.to_datetime(work[time_col], errors="coerce")
    out["_time"] = out["_time"].where(out["_time"].notna(), work[time_col])
    out["_node"] = work[node_col]
    for c in extra_cols:
        out[c] = work[c]
    for c in object_cols:
        out[c] = work[c]
    for var, col in counter_cols.items():
        out[var] = pd.to_numeric(work[col], errors="coerce")

    mapping = {
        "datetime": time_col,
        "node": node_col,
        "counters": counter_cols,
        "extra_dims": extra_cols,
        "object_dims": object_cols,
    }
    return out, list(counter_cols), extra_cols, object_cols, mapping


def _compute(spec, work, counters, group_cols):
    """Apply the formula to `work`, grouped by `group_cols` (or per row if None)."""
    has_data = work[counters].notna().any(axis=1)
    work = work[has_data]
    skipped = int((~has_data).sum())
    if work.empty:
        return pd.DataFrame(columns=(group_cols or []) + ["value"]), skipped

    if spec.kind == "ratio":
        c = {v: work[v].fillna(0) for v in counters}
        frame = work.drop(columns=counters).copy()
        frame["_num"] = spec.num(c)
        frame["_den"] = spec.den(c)
        if group_cols:
            frame = frame.groupby(group_cols, dropna=False, sort=True)[["_num", "_den"]].sum().reset_index()
        den = frame["_den"]
        ratio = frame["_num"] / den.where(den > 0)
        frame["value"] = spec.post(ratio)
        return frame.drop(columns=["_num", "_den"]), skipped

    # kind == "value"
    if spec.agg == "mean":
        c = {v: work[v] for v in counters}
    else:
        c = {v: work[v].fillna(0) for v in counters}
    frame = work.drop(columns=counters).copy()
    frame["value"] = spec.expr(c)
    if group_cols:
        frame = frame.groupby(group_cols, dropna=False, sort=True)["value"].agg(spec.agg).reset_index()
    return frame, skipped


def calculate_kpi(spec, df, level="node"):
    """Calculate one KPI from its query result. Returns the KPI block for the response."""
    block = {
        "kpi": spec.name,
        "query": spec.query,
        "unit": spec.unit,
        "formula": spec.formula,
        "level": level,
    }
    if spec.notes:
        block["notes"] = list(spec.notes)

    work, counters, extra_cols, object_cols, mapping = _prepare(spec, df)
    block["mapping"] = mapping

    group_cols = ["_time", "_node"] + extra_cols
    if level == "object":
        group_cols = None

    result, skipped = _compute(spec, work, counters, group_cols)

    # Key columns + value, kept for the per-query output tables (removed
    # from the block before it is returned to the client).
    keys = ["_time", "_node"] + extra_cols
    if level == "object":
        result = result.assign(_row=result.index)
        keys = keys + object_cols + ["_row"]
    block["_frame"] = result.reindex(columns=keys + ["value"])
    block["_keys"] = {"extra": extra_cols, "object": object_cols if level == "object" else []}

    rows = []
    for rec in result.to_dict(orient="records"):
        row = {"datetime": _format_time(rec.get("_time")), "node": rec.get("_node")}
        for c in extra_cols:
            row[c] = _format_time(rec.get(c)) if "DATETIME" in c.upper() else rec.get(c)
        if level == "object":
            for c in object_cols:
                row[c] = rec.get(c)
        row["value"] = _clean_value(rec.get("value"))
        rows.append(row)
    rows.sort(key=lambda r: (str(r["datetime"]), str(r["node"])))

    block.update({
        "status": "SUCCESS",
        "count": len(rows),
        "null_values": sum(1 for r in rows if r["value"] is None),
        "input_rows": int(len(df)),
        "rows_without_counters": skipped,
        "results": rows,
    })
    return block


def select_specs(tokens=None, specs=VOICE_KPI_SPECS):
    """
    Pick KPI specs from request tokens. A token is a KPI name or a query name
    (all KPIs of that query), case-insensitive. Returns (specs, unknown_tokens).
    """
    if not tokens:
        return list(specs), []
    chosen, unknown = [], []
    for token in tokens:
        t = token.strip().upper()
        hits = [s for s in specs if s.name.upper() == t or s.query.upper() == t]
        if not hits:
            unknown.append(token)
        for s in hits:
            if s not in chosen:
                chosen.append(s)
    return chosen, unknown


def required_queries(specs):
    out = []
    for s in specs:
        if s.query not in out:
            out.append(s.query)
    return out


def calculate_voice_kpis(frames, specs=VOICE_KPI_SPECS, level="node", query_errors=None):
    """
    frames:        {query_name: DataFrame} for the queries that succeeded
    query_errors:  {query_name: error text} for the queries that failed
    Returns one block per spec, in spec order. A failing KPI never stops the others.
    """
    if level not in LEVELS:
        raise ValueError(f"level must be one of {LEVELS}")
    query_errors = query_errors or {}
    blocks = []
    for spec in specs:
        base = {"kpi": spec.name, "query": spec.query, "unit": spec.unit,
                "formula": spec.formula, "level": level}
        df = frames.get(spec.query)
        if df is None:
            err = query_errors.get(spec.query, "query was not executed")
            blocks.append({**base, "status": "FAILED", "error": f"ENIQ query {spec.query} failed: {err}",
                           "count": 0, "results": []})
            continue
        try:
            blocks.append(calculate_kpi(spec, df, level=level))
        except MissingColumns as e:
            logger.error("[ENIQ KPI] %s: %s", spec.name, e)
            blocks.append({**base, "status": "FAILED", "error": str(e), "count": 0, "results": []})
        except Exception as e:
            logger.error("[ENIQ KPI] %s calculation failed: %s", spec.name, e, exc_info=True)
            blocks.append({**base, "status": "FAILED", "error": f"calculation failed: {e}",
                           "count": 0, "results": []})
    return blocks


def _table_for_query(query, blocks, layout, level):
    """Merge the KPIs of one query into the sample's green-column table."""
    ok = [b for b in blocks if b["status"] == "SUCCESS"]
    failed = [b for b in blocks if b["status"] != "SUCCESS"]
    wanted = {b["kpi"] for b in blocks}
    # Only the columns of the KPIs that were requested.
    layout = [(h, src) for h, src in layout
              if not src.startswith("kpi:") or src[4:] in wanted]

    table = {"query": query, "level": level}
    if QUERY_OUTPUT_NOTES.get(query):
        table["note"] = QUERY_OUTPUT_NOTES[query]
    if failed:
        table["errors"] = {b["kpi"]: b.get("error") for b in failed}
    if not ok:
        table.update({"status": "FAILED", "columns": [h for h, _ in layout],
                      "count": 0, "results": []})
        return table, None

    extra = ok[0]["_keys"]["extra"]
    objects = ok[0]["_keys"]["object"]
    keys = ["_time", "_node"] + extra + (objects + ["_row"] if level == "object" else [])

    merged = None
    for b in ok:
        f = b["_frame"].rename(columns={"value": f"kpi:{b['kpi']}"})
        merged = f if merged is None else merged.merge(f, on=keys, how="outer")
    merged = merged.sort_values(["_time", "_node"], kind="stable", na_position="last")

    source_col = {"datetime": "_time", "node": "_node", **{c.upper(): c for c in extra}}
    out = pd.DataFrame(index=merged.index)
    columns = []
    for header, src in layout:
        if src.startswith("kpi:"):
            col = merged[src] if src in merged.columns else pd.Series(None, index=merged.index)
            out[header] = [_clean_value(v) for v in col]
        else:
            name = source_col.get(src.upper(), source_col.get(src))
            if name is None:  # dimension not returned by ENIQ
                out[header] = None
            elif name == "_node":
                out[header] = merged[name]
            else:
                out[header] = [_format_time(v) for v in merged[name]]
        columns.append(header)
        if src == "node" and level == "object":
            for c in objects:
                out[c] = merged[c]
                columns.append(c)
    out = out[columns].astype(object).where(out[columns].notna(), None)

    table.update({
        "status": "SUCCESS" if not failed else "PARTIAL",
        "columns": columns,
        "count": int(len(out)),
        "results": out.to_dict(orient="records"),
    })
    return table, out


def build_query_outputs(blocks, level="node"):
    """
    One output table per query, with the green columns of its sample workbook:
    time, node, green counters and calculated KPIs (no raw formula inputs).

    Returns (tables, frames): tables for the response, {query: DataFrame} for CSV.
    Removes the internal frames from `blocks`.
    """
    by_query = {}
    for b in blocks:
        by_query.setdefault(b["query"], []).append(b)
    tables, frames = [], {}
    for query, qblocks in by_query.items():
        layout = QUERY_OUTPUTS.get(query) or [("DATETIMEID", "datetime"), ("NODE", "node")] + [
            (b["kpi"], f"kpi:{b['kpi']}") for b in qblocks]
        table, df = _table_for_query(query, qblocks, layout, level)
        tables.append(table)
        if df is not None:
            frames[query] = df
    for b in blocks:
        b.pop("_frame", None)
        b.pop("_keys", None)
    return tables, frames
