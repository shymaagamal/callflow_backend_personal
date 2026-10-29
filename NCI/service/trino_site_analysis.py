# NCI/service/trino_site_analysis.py
"""
Trino S1-MME site KPIs as a 4th NCI source, next to NPP / Viavi / Remedy.

Retrieval reuses fetch_smartcare_site_kpis (the SmartcareSiteKpiAPIView code,
now on Trino) for NCI's own three windows. Analysis reuses KPIAnalyzer's
threshold check (_check_thresholds -> the RCA rule port) with the
S1MME_SITE thresholds, for the same three comparisons as NPP / Viavi. The
result has the analyzer shape build_site_summary already reads:

    {comparison: {"affected_sites_list": [...],
                  "affected_sites_details": {site: {"kpis": {kpi: {...}}}}}}

KPIs per site and window (from the per-site S1-MME totals):
    S1MME_ATTACH_FAILURE_RATE = ATTACH_Fail_TIMES / ATTACH_REQ_TIMES * 100
    S1MME_TAU_SR              = (TAU_REQ - TAU_FAIL) / TAU_REQ * 100
    S1MME_AUTH_SR             = (AUTH_REQ - AUTH_FAIL) / AUTH_REQ * 100
A KPI is only computed when its request count is at least MIN_ATTEMPTS, so a
handful of attempts cannot produce a 33 % failure rate alarm. A KPI without a
value in the baseline window (e.g. authentication: only the ClickHouse 15-min
table has it, so only the last 6 hours of the current window) is checked
against the absolute threshold only, and its message says so.
"""

import hashlib
import logging

from callflow.core.utils.file_cache import cache_get, cache_set
from callflow.core.services.smartcare_site_kpi_service import fetch_smartcare_site_kpis

logger = logging.getLogger(__name__)

TRINO_CACHE_TTL = 900          # same as Remedy; the ClickHouse table refreshes every 15 min
THRESHOLD_SYSTEM = "S1MME_SITE"
MIN_ATTEMPTS = 50

# kpi -> (request column, failure column, kind)
KPI_DEFS = {
    "S1MME_ATTACH_FAILURE_RATE": ("ATTACH_REQ_TIMES", "ATTACH_Fail_TIMES", "failure_rate"),
    "S1MME_TAU_SR": ("TAU_REQ_TIMES", "TAU_FAIL_TIMES", "success_rate"),
    "S1MME_AUTH_SR": ("S1MME_AUTHENTICATION_REQ_TIMES", "S1MME_AUTHENTICATION_FAIL_TIMES", "success_rate"),
}

_EMPTY = {"affected_sites_list": [], "affected_sites_details": {}}


def _cache_key(sites, windows):
    w = windows["current"]
    raw = "nci_trino|%s|%s|%s" % (",".join(sorted(sites)), w["start"], w["end"])
    return "nci_trino_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def fetch_trino_site_kpis(sites, windows, log_path=""):
    """S1-MME site KPIs for NCI's three windows (current = the NCI 12 hours).
    Cached only when every query succeeded and something came back."""
    if not sites:
        return {"windows": {}, "errors": {}}
    key = _cache_key(sites, windows)
    cached = cache_get(key)
    if cached is not None:
        return cached
    current = windows["current"]
    result = fetch_smartcare_site_kpis(sites, log_path, start=current["start"], end=current["end"],
                                       now=current["end"])
    if not result.get("errors") and not result.get("error"):
        cache_set(key, result, ttl_seconds=TRINO_CACHE_TTL)
    return result


def trino_errors(result, display_key):
    """The service's per-query errors as NCI error entries."""
    out = []
    for key, msg in (result or {}).get("errors", {}).items():
        period, _, source = key.split(".", 2)
        out.append({"period": display_key.get(period, period), "source": source, "error": msg})
    return out


def _rate(row, kpi):
    req_col, fail_col, kind = KPI_DEFS[kpi]
    req, fail = row.get(req_col), row.get(fail_col)
    if req is None or fail is None or req < MIN_ATTEMPTS:
        return None
    fail_pct = 100.0 * fail / req
    return round(fail_pct if kind == "failure_rate" else 100.0 - fail_pct, 4)


def site_kpis_by_period(result, sites):
    """{period: {site: {kpi: value or None, <request col>: n}}} for the requested sites."""
    out = {}
    wanted = {s.upper(): s for s in sites}
    for period, window in ((result or {}).get("windows") or {}).items():
        per_site = {}
        for row in window.get("s1mme") or []:
            site = wanted.get(str(row.get("LOGICAL_SITE_ID", "")).upper())
            if site is None:
                continue
            values = {kpi: _rate(row, kpi) for kpi in KPI_DEFS}
            values.update({KPI_DEFS[k][0]: row.get(KPI_DEFS[k][0]) for k in KPI_DEFS})
            per_site[site] = values
        out[period] = per_site
    return out


def analyze_comparison(analyzer, thresholds, current, baseline):
    """One comparison: current vs baseline per-site KPI values."""
    details = {}
    for site, values in current.items():
        site_kpis = {}
        for kpi, config in thresholds.items():
            curr = values.get(kpi)
            if curr is None:
                continue
            prev = (baseline.get(site) or {}).get(kpi)
            exceeded, _, insight = analyzer._check_thresholds(curr, prev if prev is not None else 0.0,
                                                              config, kpi)
            if not exceeded:
                continue
            message, severity = insight.get("message"), insight.get("severity")
            if prev is None:
                # absolute threshold breached with nothing to compare against
                message = f"{kpi} is {curr:.2f}% (no baseline value; absolute threshold only)"
                severity = "high"
            site_kpis[kpi] = {
                "current_value": curr,
                "previous_value": prev,
                "requests": values.get(KPI_DEFS[kpi][0]),
                "deviation_pct": insight.get("deviation_pct") if prev is not None else None,
                "deviation_direction": insight.get("direction") if prev is not None else None,
                "severity": severity,
                "insight_message": message,
                "message": message,
            }
        if site_kpis:
            details[site] = {"kpis": site_kpis}
    return {"affected_sites_list": sorted(details), "affected_sites_details": details}


def run_trino_comparisons(analyzer, result, sites, comparisons):
    """{display_key: analyzer-shaped result} for the NCI comparisons."""
    thresholds = analyzer.thresholds.get_thresholds_for_system(THRESHOLD_SYSTEM)
    by_period = site_kpis_by_period(result, sites)
    raw = {}
    for display_key, period_key, baseline_key in comparisons:
        current = by_period.get(period_key) or {}
        if not current or not thresholds:
            raw[display_key] = dict(_EMPTY, affected_sites_details={})
            continue
        raw[display_key] = analyze_comparison(analyzer, thresholds, current, by_period.get(baseline_key) or {})
        logger.info("[NCI] trino %s: %d affected site(s) %s", display_key,
                    len(raw[display_key]["affected_sites_list"]), raw[display_key]["affected_sites_list"])
    return raw
