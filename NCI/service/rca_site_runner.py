# NCI/service/rca_site_runner.py
"""
RCA analysis for a list of sites -- the server-side equivalent of the RCA
tool in CallFlow-Frontend/src/assets/external/rca_tool_V24.html.

Everything that already exists in the backend is reused as-is:
  - data:      fetch_npp_one_site / fetch_viavi / fetch_remedy  (live systems)
  - analysis:  KPIAnalyzer.analyze_npp / analyze_viavi  with KPIThresholds
  - mapping:   _canonicalize / _normalize_npp_df / _normalize_remedy_output
  - summary:   build_site_summary  (critical_cases / minor_cases / trouble_tickets,
               same fields as POST /NCI/analysis/)

Only the RCA-specific parts the backend doesn't have live here:
  1. _pick_baseline   -- RCA "auto" baseline: 2h before, else yesterday same time
  2. _fold_to_sites   -- per-cell / per-route results rolled up to requested sites
  3. _rca_site_score  -- RCA site score, status and ranking
"""

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from callflow.core.jobs import job_manager

from .analysis_runner import (
    _analyzer, _canonicalize, _normalize_npp_df, _normalize_remedy_output,
    _severity_bucket, _severity_rank,
    fetch_npp_one_site, fetch_viavi, fetch_remedy, build_site_summary,
)
from ..utils.helpers import _add_formatted

logger = logging.getLogger(__name__)

VALID_BASELINE_METHODS = ("auto", "2h_before", "yesterday")
DEFAULT_WINDOW_HOURS = 3

# The RCA tool reads the 15-min NPP export (npp_15min_kpi_*.csv). In the backend
# that is nppIntegration(resolution="daily") analysed with "15m" thresholds --
# the same pairing NppIntegration15minAPIView uses.
NPP_FETCH_RESOLUTION = "daily"
NPP_ANALYZER_RESOLUTION = "15m"


# ── RCA helper 1: baseline window ─────────────────────────────────────────────

def _pick_baseline(df: pd.DataFrame, ts_col: str, start, end, method: str):
    two_h = (start - datetime.timedelta(hours=2), start - datetime.timedelta(microseconds=1))
    yesterday = (start - datetime.timedelta(days=1), end - datetime.timedelta(days=1))
    if method == "2h_before":
        return two_h
    if method == "yesterday":
        return yesterday
    # auto: 2h before if the source has data there, otherwise yesterday
    if not df.empty and ts_col in df.columns and df[ts_col].between(*two_h).any():
        return two_h
    return yesterday


# ── RCA helper 2: roll per-cell / per-route results up to requested sites ─────

def _fold_to_sites(details: dict, entity_to_site, sites: list):
    """
    details: affected_*_details from KPIAnalyzer, keyed by cell or FLWID.
    Returns (site_keyed_details, entities_by_site):
      - site_keyed_details: {site: {"kpis": {kpi: worst-severity info}}}, the
        shape build_site_summary expects
      - entities_by_site: {site: {entity: {kpi: info}}}, kept per entity
        because the RCA counts every cell+KPI / route+KPI separately
    """
    site_keyed, by_site = {}, {}
    for entity, info in details.items():
        site = entity_to_site(entity)
        if site not in sites:
            continue
        kpis = info.get("kpis", {})
        by_site.setdefault(site, {})[entity] = kpis
        merged = site_keyed.setdefault(site, {"kpis": {}})["kpis"]
        for kpi, k in kpis.items():
            old = merged.get(kpi)
            if old is None or _severity_rank(k.get("severity")) > _severity_rank(old.get("severity")):
                merged[kpi] = k
    return site_keyed, by_site


# ── RCA helper 3: site score (correlateAnomalies, no subscriber terms) ────────

def _rca_site_score(cells: dict, routes: dict, tickets: int, has_data: bool) -> dict:
    # every cell+KPI and route+KPI is one issue; TX issues also count as KPI issues (as in the RCA tool)
    issues = [k for kpis in list(cells.values()) + list(routes.values()) for k in kpis.items()]
    critical = sum(1 for _, k in issues if _severity_bucket(k.get("severity")) == "critical")
    anomaly = len(issues) - critical
    tx = sum(len(kpis) for kpis in routes.values())
    kpi_names = {name for name, _ in issues}

    kpi_score = critical * 150 + anomaly * 50 + tx * 120
    has_kpi_evidence = (critical + anomaly) > 0
    ticket_score = round(min(tickets * 600, 2.0 * max(kpi_score, 1))) if has_kpi_evidence else tickets * 100
    evidence = sum([has_kpi_evidence, tx > 0, tickets > 0])
    convergence = {0: 1.0, 1: 1.0, 2: 1.3}.get(evidence, 1.6)
    score = round((kpi_score + len(kpi_names) * 40 + ticket_score) * convergence)

    status = ("CRITICAL" if critical else "DEGRADED" if anomaly else
              "HEALTHY" if has_data else "NO_DATA")
    return {
        "status": status,
        "is_degraded": status in ("CRITICAL", "DEGRADED"),
        "rca_score": score,
        "rca_critical_issues": critical,
        "rca_anomaly_issues": anomaly,
        "rca_transport_issues": tx,
        "degraded_cells": {c: _kpi_messages(k) for c, k in cells.items()},
        "degraded_routes": {r: _kpi_messages(k) for r, k in routes.items()},
    }


def _kpi_messages(kpis: dict) -> list:
    return [{"kpi": name, "severity": k.get("severity"),
             "message": k.get("message") or k.get("insight_message")} for name, k in kpis.items()]


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_rca_site_analysis(sites, start, end, request_logger, log_path, ts,
                          baseline_method="auto", job_id=None) -> dict:
    windows = {
        "primary": {"start": start - datetime.timedelta(hours=2), "end": end},
        "yesterday": {"start": start - datetime.timedelta(days=1), "end": end - datetime.timedelta(days=1)},
    }
    if baseline_method == "2h_before":
        windows.pop("yesterday")
    for w in windows.values():
        _add_formatted(w)

    npp_parts, viavi_parts, remedy = [], [], ({}, 0)
    errors = {"npp": [], "viavi": [], "remedy": []}
    tasks = {}
    with ThreadPoolExecutor(max_workers=min(len(sites) * len(windows) + 4, 30)) as ex:
        for wname, w in windows.items():
            for site in sites:
                fut = ex.submit(fetch_npp_one_site, site, w, log_path, ts, NPP_FETCH_RESOLUTION)
                tasks[fut] = ("npp", wname, site)
            tasks[ex.submit(fetch_viavi, sites, w, log_path, ts)] = ("viavi", wname, None)
        tasks[ex.submit(fetch_remedy, sites, windows["primary"], log_path, ts)] = ("remedy", "primary", None)

        for done, fut in enumerate(as_completed(tasks), 1):
            source, wname, site = tasks[fut]
            try:
                data = fut.result()
                if source == "npp":
                    npp_parts.append(data)
                elif source == "viavi":
                    viavi_parts.append(data)
                else:
                    remedy = data
            except Exception as e:
                request_logger.error(f"[NCI-RCA] {source}/{wname}{'/' + site if site else ''} failed: {e}")
                errors[source].append({"window": wname, "error": str(e), **({"site": site} if site else {})})
            if job_id is not None:
                job_manager.update_job(job_id, status="RUNNING", step=f"{source}/{wname}",
                                       progress_percent=int(done / len(tasks) * 90))

    npp_df = pd.concat([d for d in npp_parts if not d.empty], ignore_index=True).drop_duplicates() \
        if any(not d.empty for d in npp_parts) else pd.DataFrame()
    viavi_df = pd.concat([d for d in viavi_parts if not d.empty], ignore_index=True).drop_duplicates() \
        if any(not d.empty for d in viavi_parts) else pd.DataFrame()
    npp_df = _normalize_npp_df(npp_df, sites)          # sitePhysical -> requested site
    if not viavi_df.empty:
        viavi_df["PERIOD_TIMESTAMP"] = pd.to_datetime(viavi_df["PERIOD_TIMESTAMP"])

    # ── NPP: existing analyzer, per cell, 15m thresholds ──
    npp_base = _pick_baseline(npp_df, "DATETIME_ID", start, end, baseline_method)
    npp_raw = _analyzer.analyze_npp(npp_df, NPP_ANALYZER_RESOLUTION, start, end, *npp_base, ts,
                                    entity_type="cell") if not npp_df.empty else {"affected_cells_details": {}}
    cell_site = npp_df.groupby("cell")["sitePhysical"].first().to_dict() \
        if {"cell", "sitePhysical"} <= set(npp_df.columns) else {}
    npp_sites, npp_by_site = _fold_to_sites(
        npp_raw["affected_cells_details"],
        lambda c: cell_site.get(c) or _canonicalize(c, sites), sites)

    # ── Viavi: existing analyzer, per route (FLWID) ──
    viavi_base = _pick_baseline(viavi_df, "PERIOD_TIMESTAMP", start, end, baseline_method)
    has_current_tx = not viavi_df.empty and viavi_df["PERIOD_TIMESTAMP"].between(start, end).any()
    viavi_raw = _analyzer.analyze_viavi(viavi_df, sites, start, end, *viavi_base, ts) \
        if has_current_tx else {"affected_sites_details": {}}
    viavi_sites, viavi_by_site = _fold_to_sites(
        viavi_raw["affected_sites_details"], lambda r: _canonicalize(r, sites), sites)

    # ── Existing NCI site summary: critical_cases / minor_cases / trouble_tickets ──
    remedy_output, remedy_total = remedy
    summary = build_site_summary(
        {"rca": {"affected_cells_details": npp_sites}},
        {"rca": {"affected_sites_details": viavi_sites}},
        {"current": (_normalize_remedy_output(remedy_output, sites), remedy_total)},
        sites,
    )

    # ── RCA score / status / ranking on top ──
    sites_with_npp = set(npp_df.loc[npp_df["DATETIME_ID"].between(start, end), "sitePhysical"]) \
        if {"DATETIME_ID", "sitePhysical"} <= set(npp_df.columns) else set()
    sites_with_tx = {_canonicalize(r, sites) for r in
                     viavi_df.loc[viavi_df["PERIOD_TIMESTAMP"].between(start, end), "FLWID"]} \
        if has_current_tx else set()
    for site, row in summary.items():
        row.update(_rca_site_score(npp_by_site.get(site, {}), viavi_by_site.get(site, {}),
                                   row["trouble_tickets"], site in sites_with_npp | sites_with_tx))

    ranking = sorted(summary, key=lambda s: -summary[s]["rca_score"])
    for i, site in enumerate(ranking, 1):
        summary[site]["rank"] = i

    return {
        "sites_summary": summary,
        "ranking": ranking,
        "degraded_sites": [s for s in ranking if summary[s]["is_degraded"]],
        "window": {
            "complaint_start": start.isoformat(), "complaint_end": end.isoformat(),
            "baseline_method": baseline_method,
            "npp_baseline": [npp_base[0].isoformat(), npp_base[1].isoformat()],
            "viavi_baseline": [viavi_base[0].isoformat(), viavi_base[1].isoformat()],
        },
        "errors": errors,
    }
