"""
Severity selection — analyze High first, then Medium; skip the rest.

Severity is DETERMINED by the unchanged old logic (KPIAnalyzer.get_degradation_insight:
high >= 30% deviation, medium >= 15%, low >= 5%, minor < 5%, plus the special
"increase" for KPIs where a rise is bad, and "none"). This module only SELECTS
which analyzed entities to keep, matching the requested priority:

    HIGH  -> keep
    MEDIUM-> keep
    (increase == high-equivalent) -> keep
    low / minor / none -> drop

The gate is applied right after the KPI/threshold step and BEFORE the expensive
downstream work (hub/TAC/LAC enrichment, time-series building, impacted-subs
mapping, correlations), so we never spend that effort on non-significant cells.

Ranking mirrors NCI's analysis_runner (_SEVERITY_RANK / _CRITICAL_SEVERITIES).
Pure/stdlib -> fully unit-testable.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

SEVERITY_RANK = {"none": 0, "minor": 1, "low": 1, "medium": 2, "high": 3, "increase": 3}

# High + Medium (and 'increase' which the old code treats as high-equivalent).
SIGNIFICANT_SEVERITIES = frozenset({"high", "increase", "medium"})


def is_significant(severity, significant=SIGNIFICANT_SEVERITIES) -> bool:
    return str(severity or "").strip().lower() in significant


def severity_counts(details: Dict[str, Any]) -> Dict[str, int]:
    """Count KPIs per severity across an ``{entity: {kpis: {kpi: {severity}}}}`` map."""
    counts: Dict[str, int] = {}
    for _entity, info in (details or {}).items():
        for _kpi, k in (info.get("kpis") or {}).items():
            sev = str(k.get("severity") or "").strip().lower() or "unknown"
            counts[sev] = counts.get(sev, 0) + 1
    return counts


def filter_entities_by_severity(
    details: Dict[str, Any], significant=SIGNIFICANT_SEVERITIES
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Keep only KPIs whose severity is significant; drop entities left with none.

    Works for NPP ``affected_cells_details`` and Viavi ``affected_sites_details``
    (both are ``{entity: {kpis: {...}, ...}}``). Returns (filtered, counts_before).
    De-duplication across complaints is inherent: entities are keyed uniquely, and
    an entity with both a high and a minor KPI keeps the high and drops the minor.
    """
    counts = severity_counts(details)
    out: Dict[str, Any] = {}
    for entity, info in (details or {}).items():
        kept = {
            kpi: k for kpi, k in (info.get("kpis") or {}).items()
            if is_significant(k.get("severity"), significant)
        }
        if kept:
            new_info = dict(info)
            new_info["kpis"] = kept
            out[entity] = new_info
    return out, counts


def filter_alarms_by_severity(
    alarms_by_node: Dict[str, List[dict]], significant=SIGNIFICANT_SEVERITIES
) -> Tuple[Dict[str, List[dict]], Dict[str, int]]:
    """Keep only significant Splunk alarms per node. Returns (filtered, counts_before)."""
    counts: Dict[str, int] = {}
    out: Dict[str, List[dict]] = {}
    for node, alarms in (alarms_by_node or {}).items():
        kept = []
        for a in (alarms or []):
            sev = str(a.get("severity") or "").strip().lower() or "unknown"
            counts[sev] = counts.get(sev, 0) + 1
            if is_significant(sev, significant):
                kept.append(a)
        out[node] = kept
    return out, counts
