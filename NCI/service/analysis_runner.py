# NCI/service/analysis_runner.py
"""
NCI data access shared by the NCI endpoints.

POST /NCI/analysis/  run_nci_analysis -> rca_pipeline.run_nci_rca: the sites go
                     through the existing RCA tool (rca_tool_V24.html); the old
                     3-period KPIAnalyzer comparisons are no longer used there.
POST /NCI/rca/       rca_site_runner, which still uses the fetchers,
                     canonicalization and build_site_summary below.

Canonicalization: Viavi (FLWID LIKE '%site%') and Remedy (site LIKE '%site%')
match on SUBSTRING, so the raw identifier they return is often not the exact
input string (e.g. "0160" comes back as "CU0160"). _canonicalize maps a raw
identifier back to the requested site it contains.
"""

import logging
from threading import Semaphore

import pandas as pd

from callflow.core.utils.file_cache import (
    cache_get, cache_set,
    make_npp_cache_key, make_viavi_cache_key, make_remedy_cache_key,
)
from callflow.core.services.npp_service import NPP_CACHE_TTL, nppIntegration
from callflow.core.services.viavi_service import viaviQueries, VIAVI_CACHE_TTL
from callflow.core.services.remedy_service import remedyTicketsData
from callflow.core.services.kpi_analyzer import KPIAnalyzer

from ..connectors.viavi_pool import get_viavi_pool
from ..connectors.remedy_pool import get_remedy_pool

logger = logging.getLogger(__name__)

REMEDY_CACHE_TTL = 900
_NPP_SEMAPHORE = Semaphore(8)
_analyzer = KPIAnalyzer()

# NPP is queried with sitePhysical populated -- see analyze_npp's
# entity_type="site" handling, which groups by this same column.
NPP_ENTITY_COL = "sitePhysical"

_COMPARISONS = [
    ("current_vs_1daybefore", "current",   "prev_1day"),
    ("current_vs_1week",      "current",   "prev_1week"),
    ("1daybefore_vs_1week",   "prev_1day", "prev_1week"),
]

_DISPLAY_KEY = {
    "current": "current",
    "prev_1day": "1day before",
    "prev_1week": "1 week before",
}

_CRITICAL_SEVERITIES = {"high", "increase"}
_SEVERITY_RANK = {"none": 0, "minor": 1, "low": 1, "medium": 2, "high": 3, "increase": 3}


def _severity_bucket(severity: str) -> str:
    return "critical" if severity in _CRITICAL_SEVERITIES else "minor"


def _severity_rank(severity: str) -> int:
    return _SEVERITY_RANK.get(severity, 1)


# -- Canonicalization: map any raw identifier back to a requested site ----

def _canonicalize(raw_value, requested_sites: list) -> str:
    """
    Maps a raw identifier (Viavi FLWID, Remedy 'site', NPP sitePhysical)
    back to whichever REQUESTED site string it contains. Viavi/Remedy
    both query with a SQL `LIKE '%site%'`, so what comes back is often a
    longer string that merely contains the site code, not an exact match.

    If more than one requested site's code could match (rare), the
    LONGEST matching site string wins, since a short code can accidentally
    be a substring of a longer one (e.g. "160" inside "0160").

    If nothing matches at all, the raw value is returned unchanged rather
    than dropped -- so a genuinely unexpected identifier still shows up
    somewhere visible instead of silently vanishing from the result.
    """
    raw_str = str(raw_value).strip()
    raw_upper = raw_str.upper()
    matches = [s for s in requested_sites if s.upper() in raw_upper]
    if not matches:
        return raw_str
    return max(matches, key=len)


def _normalize_npp_df(df: pd.DataFrame, sites: list) -> pd.DataFrame:
    if df.empty or NPP_ENTITY_COL not in df.columns:
        return df
    df = df.copy()
    df[NPP_ENTITY_COL] = df[NPP_ENTITY_COL].apply(lambda v: _canonicalize(v, sites))
    return df


def _normalize_viavi_df(df: pd.DataFrame, sites: list) -> pd.DataFrame:
    if df.empty or "FLWID" not in df.columns:
        return df
    df = df.copy()
    df["FLWID"] = df["FLWID"].apply(lambda v: _canonicalize(v, sites))
    return df


def _normalize_remedy_output(remedy_output: dict, sites: list) -> dict:
    """Merges Remedy sub-results whose raw 'site' key canonicalizes to the
    same requested site (e.g. "G1797" and "1797" both -> "1797")."""
    if not remedy_output:
        return remedy_output
    merged: dict = {}
    for raw_site, site_data in remedy_output.items():
        canon = _canonicalize(raw_site, sites)
        bucket = merged.setdefault(canon, {"total": 0, "status": {}, "tickets": []})
        bucket["total"] += site_data.get("total", 0)
        for status, count in site_data.get("status", {}).items():
            bucket["status"][status] = bucket["status"].get(status, 0) + count
        bucket["tickets"].extend(site_data.get("tickets", []))
    return merged


# -- Per-source fetchers ----------------------------------------------------

def fetch_npp_one_site(site, window, log_path, ts, resolution="hourly"):
    cache_key = make_npp_cache_key([site], window["start"], window["end"], resolution, entity_type="site")
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    with _NPP_SEMAPHORE:
        df = nppIntegration([site], window["start"], window["end"], log_path, ts, resolution, entity_type="site")

    if not df.empty:
        cache_set(cache_key, df, ttl_seconds=NPP_CACHE_TTL)
    return df


def fetch_viavi(sites, window, log_path, ts):
    if not sites:
        return pd.DataFrame(columns=['PERIOD_TIMESTAMP', 'FLWID', 'AVG_FD_MS', 'AVG_FDV_MS', 'ONEW_FLR_AZ', 'ONEW_FLR_ZA'])

    cache_key = make_viavi_cache_key(sites, window["formatted_start"], window["formatted_end"])
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    pool = get_viavi_pool()
    conn = pool.acquire()
    try:
        df = viaviQueries(conn, window["formatted_start"], window["formatted_end"], sites, log_path, ts)
        pool.release(conn)
    except Exception:
        pool.invalidate(conn)
        raise

    if not df.empty:
        cache_set(cache_key, df, ttl_seconds=VIAVI_CACHE_TTL)
    return df


def fetch_remedy(sites, window, log_path, ts):
    if not sites:
        return {}, 0

    cache_key = make_remedy_cache_key(sites, str(window["start"]), str(window["end"]))
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    pool = get_remedy_pool()
    conn = pool.acquire()
    try:
        remedy_output, total_tickets = remedyTicketsData(
            conn, window["start"], window["end"], sites, log_path, ts,
        )
        pool.release(conn)
    except Exception:
        pool.invalidate(conn)
        raise

    result = (remedy_output, total_tickets)
    cache_set(cache_key, result, ttl_seconds=REMEDY_CACHE_TTL)
    return result


# -- The deliverable: one line per requested site ----------------------------

def _ingest_npp_issues(npp_raw_by_comparison: dict, site_issues: dict) -> None:
    for display_key, raw in npp_raw_by_comparison.items():
        for site, info in raw["affected_cells_details"].items():
            bucket = site_issues.setdefault(site, {})
            for kpi_name, kpi_info in info.get("kpis", {}).items():
                severity = kpi_info.get("severity", "minor")
                key = (kpi_name, site)
                existing = bucket.get(key)
                if existing is None or _severity_rank(severity) > _severity_rank(existing["severity"]):
                    bucket[key] = {
                        "severity": severity,
                        "message": kpi_info.get("message") or kpi_info.get("insight_message"),
                        "source": "npp",
                        "site": site,
                    }


def _ingest_viavi_issues(viavi_raw_by_comparison: dict, site_issues: dict) -> None:
    for display_key, raw in viavi_raw_by_comparison.items():
        for site, info in raw["affected_sites_details"].items():
            bucket = site_issues.setdefault(site, {})
            for kpi_name, kpi_info in info.get("kpis", {}).items():
                severity = kpi_info.get("severity", "minor")
                key = (kpi_name, site)
                existing = bucket.get(key)
                if existing is None or _severity_rank(severity) > _severity_rank(existing["severity"]):
                    bucket[key] = {
                        "severity": severity,
                        "message": kpi_info.get("message") or kpi_info.get("insight_message"),
                        "source": "viavi",
                        "site": site,
                    }


def build_site_summary(npp_raw_by_comparison: dict, viavi_raw_by_comparison: dict,
                        remedy_by_period: dict, all_sites: list) -> dict:
    """Used by the NCI RCA endpoint (rca_site_runner); POST /NCI/analysis/ no
    longer uses it -- its counts come from the RCA tool (rca_pipeline)."""
    site_issues: dict = {}
    _ingest_npp_issues(npp_raw_by_comparison, site_issues)
    _ingest_viavi_issues(viavi_raw_by_comparison, site_issues)
    logger.info(f"[NCI] build_site_summary: {len(site_issues)} sites with issues, {sum(len(v) for v in site_issues.values())} total issues")
    site_ticket_ids: dict = {}
    for period_key, (remedy_output, _total) in remedy_by_period.items():
        for site, site_data in (remedy_output or {}).items():
            ids = site_ticket_ids.setdefault(site, set())
            for ticket in site_data.get("tickets", []):
                incident_number = ticket.get("incident_number")
                if incident_number:
                    ids.add(incident_number)

    # Report EXACTLY the requested sites -- canonicalization already
    # folded any raw-identifier variants (FLWID/site-with-prefix) into
    # these keys, so there's nothing left over to add here.
    result = {}
    for site in sorted(all_sites):
        issues = list(site_issues.get(site, {}).values())
        critical = [i for i in issues if _severity_bucket(i["severity"]) == "critical"]
        minor = [i for i in issues if _severity_bucket(i["severity"]) == "minor"]
        result[site] = {
            "critical_cases": len(critical),
            "minor_cases": len(minor),
            "trouble_tickets": len(site_ticket_ids.get(site, set())),
            # "critical_details": [i["message"] for i in critical if i.get("message")],
            # "minor_details": [i["message"] for i in minor if i.get("message")],
        }
    return result


# -- Orchestration -----------------------------------------------------------

def run_nci_analysis(sites, request_logger, log_path, ts, job_id=None):
    """POST /NCI/analysis/: retrieval -> RCA files -> the existing RCA tool ->
    its per-site results in the NCI response shape. See rca_pipeline.py."""
    from .rca_pipeline import run_nci_rca
    return run_nci_rca(sites, request_logger, log_path, ts, job_id=job_id)
