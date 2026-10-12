"""
Python port of the RCA HTML tool's KPI anomaly rule — the single source of truth
for "is a KPI abnormal".

This mirrors, line-for-line, ``checkThreshold(value, baseline, threshold,
sensFactor, columnIsCount)`` in ``assets/external/rca_tool_V24.html`` so the
Python backend and the standalone RCA tool produce the SAME anomaly decision for
the same value / baseline / rule / sensitivity. Because the RCA HTML must stay
standalone (no Python at runtime), equivalence is locked by the regression tests
in ``tests/test_rca_threshold.py`` (port these expectations if the RCA JS ever
changes).

RCA rule shape (from the thresholds sheet -> dynamicThresholds):
    {
        "operator": ">" | "<",          # absolute comparison
        "value":    <number>,           # absolute threshold
        "devDev":   "drop" | "rise",    # directional deviation vs baseline
        "devPct":   <number>,           # deviation limit (percent)
        "useDeviation": bool,           # legacy abs-percent deviation
        "severity": "CRITICAL" | ...,   # CRITICAL bypasses the sensitivity factor
        "logic":    "AND" | "OR" | "SINGLE",
    }

Sensitivity factors (RCA UI): strict=1.0, normal=0.9 (default), relaxed=0.5.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

SENS_FACTORS = {"strict": 1.0, "normal": 0.9, "relaxed": 0.5}
DEFAULT_SENS_FACTOR = 0.9  # 'normal', matching the RCA UI default


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def check_threshold(value, baseline, rule: Optional[Dict[str, Any]],
                    sens_factor: float = DEFAULT_SENS_FACTOR,
                    column_is_count: bool = False) -> Dict[str, Any]:
    """Exact port of RCA checkThreshold(). Returns
    ``{is_anomaly, severity, reason?, description?}``."""
    if not rule:
        return {"is_anomaly": False}

    logic = rule.get("logic") or "OR"
    severity = rule.get("severity") or "ANOMALY"
    result: Dict[str, Any] = {"is_anomaly": False, "severity": severity}

    value = _num(value)

    # Skip zero-value rows with no baseline — likely missing data, not a real event.
    if value == 0 and baseline is None:
        return {"is_anomaly": False}

    has_baseline = baseline is not None and _num(baseline) != 0
    signed = ((value - _num(baseline)) / abs(_num(baseline)) * 100) if has_baseline else None

    dev_dev = rule.get("devDev")
    # Improvement guard: KPI clearly moving in the GOOD direction -> suppress abs check.
    improving = False
    if has_baseline and dev_dev:
        improving = (dev_dev == "rise" and signed < -2) or (dev_dev == "drop" and signed > 2)

    # Absolute condition (with count-vs-% guard + severity/sensitivity adjustment).
    abs_breached = False
    abs_applicable = False
    op = rule.get("operator")
    tval = rule.get("value")
    if op and tval is not None:
        is_percent_threshold = tval <= 100
        value_is_count = is_percent_threshold and (column_is_count or value > 101)
        if not value_is_count and not improving:
            abs_applicable = True
            adj = tval * (1 if severity == "CRITICAL" else sens_factor)
            abs_breached = (op == ">" and value > adj) or (op == "<" and value < adj)

    # Directional deviation condition.
    dev_breached = False
    dev_pct = rule.get("devPct")
    dev_applicable = bool(dev_dev and dev_pct is not None)
    if dev_applicable and has_baseline:
        limit = dev_pct * (1 if severity == "CRITICAL" else sens_factor)
        dev_breached = (dev_dev == "drop" and signed < -limit) or (dev_dev == "rise" and signed > limit)

    # Fire decision (AND / OR / SINGLE).
    fire = False
    if logic == "AND":
        if abs_applicable and dev_applicable:
            fire = (abs_breached and dev_breached) if has_baseline else abs_breached
        elif abs_applicable:
            fire = abs_breached
        elif dev_applicable:
            fire = dev_breached
    else:
        if abs_applicable and abs_breached:
            fire = True
        if dev_applicable and dev_breached:
            fire = True

    if fire:
        result["is_anomaly"] = True
        result["reason"] = "absolute" if abs_breached else "deviation"
        if abs_breached and op:
            result["description"] = f"{op} {tval}" + ("" if has_baseline else " (no baseline)")

    # Legacy useDeviation (abs percent deviation, backwards compat).
    if not result["is_anomaly"] and rule.get("useDeviation") and not dev_dev and has_baseline:
        abs_d = abs(signed)
        limit = (dev_pct if dev_pct is not None else 20) * (1 if severity == "CRITICAL" else sens_factor)
        if abs_d > limit:
            result["is_anomaly"] = True
            result["reason"] = "deviation"

    return result


def rule_from_config(config_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Adapt the backend's KPIThresholds config (a list of
    ``{type: 'threshold'|'deviation', operator, value}`` conds) into a single
    RCA rule so ``check_threshold`` can evaluate it.

    - a ``threshold`` cond -> RCA absolute (operator/value)
    - a ``deviation`` cond with value>0 -> RCA directional 'rise' (devPct=value)
    - a ``deviation`` cond with value<0 -> RCA directional 'drop' (devPct=|value|)
    The backend evaluates conds as OR (any breach fires), so logic='OR'.
    """
    rule: Dict[str, Any] = {"logic": "OR"}
    for cond in (config_list or []):
        ctype = cond.get("type")
        val = cond.get("value")
        op = cond.get("operator")
        if val is None:
            continue
        if ctype == "threshold":
            rule["operator"] = op
            rule["value"] = val
        elif ctype == "deviation":
            if val < 0:
                rule["devDev"] = "drop"
                rule["devPct"] = abs(val)
            else:
                rule["devDev"] = "rise"
                rule["devPct"] = val
    return rule


def normalize_kpi(name: str) -> str:
    return str(name or "").replace(" ", "_").lower()


def find_config(node_thresholds: Dict[str, Any], kpi_name: str):
    """RCA-equivalent KPI->rule lookup: exact -> normalized-equality -> substring
    (longest wins). Prevents silently missing a threshold due to formatting."""
    if not node_thresholds:
        return None
    if kpi_name in node_thresholds:
        return node_thresholds[kpi_name]
    norm = normalize_kpi(kpi_name)
    for key, rules in node_thresholds.items():
        if normalize_kpi(key) == norm:
            return rules
    matched_key = None
    for key in node_thresholds:
        if (key in kpi_name or kpi_name in key) and (not matched_key or len(key) > len(matched_key)):
            matched_key = key
    return node_thresholds.get(matched_key) if matched_key else None
