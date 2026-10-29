# NCI/service/analysis_runner.py
"""
Orchestrates NCI's 3-period x 4-source analysis (NPP, Viavi, Remedy and the
Trino S1-MME site KPIs -- see trino_site_analysis.py), then rolls everything up
into ONE line per requested site.

KEY FIX in this version: canonicalization. Viavi (FLWID LIKE '%site%') and
Remedy (site LIKE '%site%') both match on SUBSTRING, so the raw identifier
they return is often NOT your exact input string -- e.g. requested site
"0160" comes back from Viavi as "CU0160", "GU0160",
"QT-RMD-P2-InnerIP-HUK0160-BEDataQueue", etc, and from Remedy as "G1797"
for requested site "1797". Left unfixed, each of those becomes its own
bogus "site" in the output.

Fix: right after each source is fetched, every raw identifier is mapped
back to whichever of the ORIGINALLY REQUESTED sites it contains, and
results for the same requested site are merged. This happens ONCE, at
the fetch boundary -- every downstream function (summarize_npp,
summarize_viavi, summarize_remedy, build_site_summary) then just works
with clean, canonical site keys without needing its own fix.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

import pandas as pd
from callflow.core.jobs import job_manager

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
from ..utils.helpers import build_nci_time_windows
from .trino_site_analysis import fetch_trino_site_kpis, run_trino_comparisons, trino_errors

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


# -- Run each comparison ONCE, share the raw (severity-carrying) result ----

def _run_npp_comparisons(npp_by_period: dict, windows: dict, ts: str) -> dict:
    raw = {}
    for display_key, period_key, baseline_key in _COMPARISONS:
        current_df = npp_by_period.get(period_key, pd.DataFrame())
        baseline_df = npp_by_period.get(baseline_key, pd.DataFrame())
        combined = pd.concat([current_df, baseline_df], ignore_index=True) if not current_df.empty else pd.DataFrame()
        if combined.empty:
            raw[display_key] = {"affected_cells_list": [], "affected_cells_details": {}}
        else:
            raw[display_key] = _analyzer.analyze_npp(
                combined, "hourly",
                windows[period_key]["start"], windows[period_key]["end"],
                windows[baseline_key]["start"], windows[baseline_key]["end"],
                ts,
                entity_type="site",
            )
    return raw


def _run_viavi_comparisons(viavi_by_period: dict, all_sites: list, windows: dict, ts: str) -> dict:
    raw = {}
    for display_key, period_key, baseline_key in _COMPARISONS:
        current_df = viavi_by_period.get(period_key, pd.DataFrame())
        baseline_df = viavi_by_period.get(baseline_key, pd.DataFrame())
        combined = pd.concat([current_df, baseline_df], ignore_index=True) if not current_df.empty else pd.DataFrame()
        if combined.empty:
            raw[display_key] = {"affected_sites_list": [], "affected_sites_details": {}}
        else:
            raw[display_key] = _analyzer.analyze_viavi(
                combined, all_sites,
                windows[period_key]["start"], windows[period_key]["end"],
                windows[baseline_key]["start"], windows[baseline_key]["end"],
                ts,
            )
    return raw


# -- Slim per-system/per-comparison views (message-only, no severity) -----

def _messages_only(affected_details: dict) -> dict:
    out = {}
    for entity_id, info in affected_details.items():
        kpis = info.get("kpis", {})
        messages = [(k.get("message") or k.get("insight_message")) for k in kpis.values()]
        out[entity_id] = [m for m in messages if m]
    return out


def summarize_npp(npp_raw_by_comparison: dict, npp_by_period: dict, all_sites: list) -> dict:
    out = {}
    for display_key, period_key, baseline_key in _COMPARISONS:
        current_df = npp_by_period.get(period_key, pd.DataFrame())
        sites_with_data = set(current_df[NPP_ENTITY_COL].unique()) if (not current_df.empty and NPP_ENTITY_COL in current_df.columns) else set()
        no_data_sites = sorted(set(all_sites) - sites_with_data)

        raw = npp_raw_by_comparison[display_key]
        abnormal_details = _messages_only(raw["affected_cells_details"])
        abnormal_sites = sorted(abnormal_details.keys())
        healthy_sites = sorted(sites_with_data - set(abnormal_sites))

        out[display_key] = {
            "abnormal_sites_count": len(abnormal_sites),
            "abnormal_sites": abnormal_sites,
            "abnormal_sites_details": abnormal_details,
            "healthy_sites_count": len(healthy_sites),
            "healthy_sites": healthy_sites,
            "no_data_sites_count": len(no_data_sites),
            "no_data_sites": no_data_sites,
        }
    return out


def summarize_viavi(viavi_raw_by_comparison: dict, viavi_by_period: dict, all_sites: list) -> dict:
    out = {}
    for display_key, period_key, baseline_key in _COMPARISONS:
        current_df = viavi_by_period.get(period_key, pd.DataFrame())
        sites_with_data = set(current_df["FLWID"].unique()) if not current_df.empty else set()
        no_data_sites = sorted(set(all_sites) - sites_with_data)

        raw = viavi_raw_by_comparison[display_key]
        abnormal_details = _messages_only(raw["affected_sites_details"])
        abnormal_sites = sorted(abnormal_details.keys())
        healthy_sites = sorted(sites_with_data - set(abnormal_sites))

        out[display_key] = {
            "abnormal_sites_count": len(abnormal_sites),
            "abnormal_sites": abnormal_sites,
            "abnormal_sites_details": abnormal_details,
            "healthy_sites_count": len(healthy_sites),
            "healthy_sites": healthy_sites,
            "no_data_sites_count": len(no_data_sites),
            "no_data_sites": no_data_sites,
        }
    return out


def summarize_remedy(remedy_by_period: dict) -> dict:
    out = {}
    for period_key, display_key in _DISPLAY_KEY.items():
        remedy_output, total_tickets = remedy_by_period.get(period_key, ({}, 0))
        out[display_key] = {
            "total_tickets": total_tickets,
            "sites_with_tickets": list(remedy_output.keys()) if remedy_output else [],
        }
    return out


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


def _ingest_trino_issues(trino_raw_by_comparison: dict, site_issues: dict) -> None:
    for display_key, raw in (trino_raw_by_comparison or {}).items():
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
                        "source": "trino",
                        "site": site,
                    }


def build_site_summary(npp_raw_by_comparison: dict, viavi_raw_by_comparison: dict,
                        remedy_by_period: dict, all_sites: list,
                        trino_raw_by_comparison: dict = None) -> dict:
    site_issues: dict = {}
    _ingest_npp_issues(npp_raw_by_comparison, site_issues)
    _ingest_viavi_issues(viavi_raw_by_comparison, site_issues)
    _ingest_trino_issues(trino_raw_by_comparison, site_issues)
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
    windows = build_nci_time_windows()

    max_workers = min(len(sites) * 3 + 6, 30)

    tasks = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for period_name, window in windows.items():
            for site in sites:
                fut = executor.submit(fetch_npp_one_site, site, window, log_path, ts)
                tasks[fut] = (period_name, "npp", site)

            fut = executor.submit(fetch_viavi, sites, window, log_path, ts)
            tasks[fut] = (period_name, "viavi", None)

            fut = executor.submit(fetch_remedy, sites, window, log_path, ts)
            tasks[fut] = (period_name, "remedy", None)

        # Trino S1-MME site KPIs: one task covers all three windows.
        fut = executor.submit(fetch_trino_site_kpis, sites, windows, log_path)
        tasks[fut] = ("current", "trino", None)

        npp_by_period_lists = {p: [] for p in windows}
        viavi_by_period = {}
        remedy_by_period = {}
        trino_result = {}
        errors = {"npp": [], "viavi": [], "remedy": [], "trino": []}

        done = 0
        total = len(tasks)
        for future in as_completed(tasks):
            period_name, source, site = tasks[future]
            try:
                data = future.result()
                if source == "npp":
                    npp_by_period_lists[period_name].append(data)
                elif source == "viavi":
                    viavi_by_period[period_name] = data
                elif source == "remedy":
                    remedy_by_period[period_name] = data
                elif source == "trino":
                    trino_result = data
                    errors["trino"].extend(trino_errors(data, _DISPLAY_KEY))
                    if data.get("error"):
                        request_logger.warning(f"[NCI] trino: {data['error']}")
            except Exception as e:
                label = f"{source}" + (f"/{site}" if site else "")
                request_logger.error(f"[NCI] {label}/{period_name} failed: {e}")
                entry = {"period": "all" if source == "trino" else _DISPLAY_KEY[period_name], "error": str(e)}
                if site:
                    entry["site"] = site
                errors[source].append(entry)

            done += 1
            if job_id is not None:
                job_manager.update_job(
                    job_id, status="RUNNING",
                    step=f"{source}/{period_name}",
                    progress_percent=int(done / total * 100),
                )

    # Concat NPP's per-site parts, then canonicalize ALL THREE sources
    # against the requested site list -- see module docstring.
    npp_by_period = {}
    for period_name, parts in npp_by_period_lists.items():
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        npp_by_period[period_name] = _normalize_npp_df(df, sites)

    viavi_by_period = {
        period_name: _normalize_viavi_df(df, sites)
        for period_name, df in viavi_by_period.items()
    }

    remedy_by_period = {
        period_name: (_normalize_remedy_output(remedy_output, sites), total_tickets)
        for period_name, (remedy_output, total_tickets) in remedy_by_period.items()
    }

    npp_raw = _run_npp_comparisons(npp_by_period, windows, ts)
    viavi_raw = _run_viavi_comparisons(viavi_by_period, sites, windows, ts)
    trino_raw = run_trino_comparisons(_analyzer, trino_result, sites, _COMPARISONS)

    return {
        "sites_summary": build_site_summary(npp_raw, viavi_raw, remedy_by_period, sites, trino_raw),
        # "npp": summarize_npp(npp_raw, npp_by_period, sites),
        # "viavi": summarize_viavi(viavi_raw, viavi_by_period, sites),
        # "remedy": summarize_remedy(remedy_by_period),
        "errors": errors,
    }