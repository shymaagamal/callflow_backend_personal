# NCI/service/portal_result.py
"""
Turns one NCI analysis result into the job shape the EXISTING CallFlow
Summary page already reads (``systems`` / ``dashboard`` / ``summary`` /
``details``), so the portal can open an NCI request without a second
Summary page or a second analysis architecture.

Nothing here re-analyses anything. It only re-shapes what
``run_nci_analysis(..., include_raw=True)`` already computed:

  * the per-comparison NPP / Viavi analyzer output (KPIAnalyzer, same
    thresholds and severities NCI already uses), merged across the three
    comparisons with the SAME "highest severity per (site, KPI) wins" rule
    ``build_site_summary`` uses -- so the portal counts match the API's
    ``sites_summary`` / ``affected_sites`` exactly;
  * the Remedy tickets of the three periods, de-duplicated by incident
    number exactly like ``build_site_summary`` counts ``trouble_tickets``.

Chart rows are built with CallFlow's own ``dashboard_shaping`` helpers.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .analysis_runner import _COMPARISONS, _severity_rank

# Systems that NCI actually analyses. Everything else (Trino, Smartcare,
# Splunk, RCA) is not applicable in the NCI portal context.
NCI_SYSTEMS = ("npp", "viavi", "remedy")


# -- API response helpers ---------------------------------------------------

def affected_sites(sites_summary: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sites with at least one NPP/Viavi KPI issue (critical or minor)."""
    out = []
    for site, s in (sites_summary or {}).items():
        if (s.get("critical_cases", 0) or 0) + (s.get("minor_cases", 0) or 0) > 0:
            out.append({"site": site, **s})
    return out


def build_portal_url(sites: List[str], request=None, nci_id: Optional[str] = None) -> str:
    """URL of the existing Summary page in NCI mode, carrying the sites and
    (when given) the id of the stored NCI result, so the portal SHOWS that
    result instead of running the analysis again.

    Base = ``settings.CALLFLOW_PORTAL_URL`` (the Angular portal); falls back
    to this Django host when the setting is missing.
    """
    from django.conf import settings
    base = getattr(settings, "CALLFLOW_PORTAL_URL", None)
    if not base and request is not None:
        base = request.build_absolute_uri("/")
    params = {"mode": "nci"}
    if nci_id:
        params["nci_id"] = nci_id
    params["sites"] = ",".join(str(s) for s in sites)
    return f"{(base or '').rstrip('/')}/summary?{urlencode(params)}"


# -- Stored NCI results (what the portal_url opens) --------------------------
# The NCI API stores the portal view of ITS OWN result, already finished, as
# a DONE job; the portal reads it through the existing GET
# /NCI/portal/analysis/<id>. Stored as a JSON file (not only the 30-minute job
# cache) so the link keeps working across workers and restarts.

_ID_RE = re.compile(r"^[0-9a-f]{16}$")


def _results_dir() -> Path:
    from django.conf import settings
    d = Path(getattr(settings, "NCI_RESULTS_DIR", None) or (Path(settings.OUTPUT_DIR) / "nci_results"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def store_portal_job(portal: Dict[str, Any], who: str = "") -> str:
    """Saves a JSON-safe ``build_portal_job`` output as a finished job.
    Returns its id (the ``nci_id`` in the portal_url)."""
    nci_id = uuid.uuid4().hex[:16]
    now = time.time()
    job = {
        "job_id": nci_id, "job_type": "nci_result", "who": who,
        "status": "DONE", "step": "completed", "progress_percent": 100,
        "created_at": now, "updated_at": now, "error": None,
        "systems": portal["systems"], "dashboard": portal["dashboard"],
        "summary": portal["summary"], "details": portal["details"],
        "result": {"request_id": nci_id, **portal},
    }
    path = _results_dir() / f"{nci_id}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(job), encoding="utf-8")
    tmp.replace(path)
    return nci_id


def load_portal_job(nci_id: str) -> Optional[Dict[str, Any]]:
    """The stored job for ``nci_id``, or None (unknown id / bad id)."""
    if not nci_id or not _ID_RE.match(str(nci_id)):
        return None
    path = _results_dir() / f"{nci_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# -- Merging (same rule as analysis_runner.build_site_summary) --------------

def _merge_comparisons(raw_by_comparison: Dict[str, Any], details_key: str) -> Dict[str, Any]:
    """{site: {"kpis": {kpi: entry}}} keeping the highest-severity entry per
    (site, KPI) across the three comparisons (first one wins on a tie)."""
    merged: Dict[str, Any] = {}
    for display_key, _period, _baseline in _COMPARISONS:
        raw = (raw_by_comparison or {}).get(display_key) or {}
        for site, info in (raw.get(details_key) or {}).items():
            kpis = merged.setdefault(site, {"site_name": site, "kpis": {}})["kpis"]
            for kpi_name, kpi_info in (info.get("kpis") or {}).items():
                existing = kpis.get(kpi_name)
                if existing is None or _severity_rank(kpi_info.get("severity", "minor")) > \
                        _severity_rank(existing.get("severity", "minor")):
                    kpis[kpi_name] = {**kpi_info, "comparison": display_key}
    return merged


def _merge_remedy(remedy_by_period: Dict[str, Any]) -> Dict[str, Any]:
    """{site: {total, status, tickets}} over all periods, tickets unique by
    incident_number (as build_site_summary counts trouble_tickets)."""
    merged: Dict[str, Any] = {}
    seen: Dict[str, set] = {}
    for _period, value in (remedy_by_period or {}).items():
        remedy_output = value[0] if isinstance(value, (list, tuple)) else value
        for site, site_data in (remedy_output or {}).items():
            bucket = merged.setdefault(site, {"total": 0, "status": {}, "tickets": []})
            ids = seen.setdefault(site, set())
            for ticket in site_data.get("tickets", []):
                inc = ticket.get("incident_number")
                if inc and inc in ids:
                    continue
                if inc:
                    ids.add(inc)
                bucket["tickets"].append(ticket)
                st = ticket.get("status")
                if st is not None:
                    bucket["status"][st] = bucket["status"].get(st, 0) + 1
            bucket["total"] = len(bucket["tickets"])
    return merged


def _system_status(errors: Dict[str, list], source: str, expected: int) -> str:
    """FAILED only when every fetch of that source failed; otherwise COMPLETED."""
    return "FAILED" if len((errors or {}).get(source) or []) >= expected else "COMPLETED"


def _card(status_ok: bool, value: int) -> Dict[str, Any]:
    if not status_ok:
        return {"status": "FAILED", "value": None}
    return {"status": "READY_WITH_DATA" if value > 0 else "READY_EMPTY", "value": value}


# -- The portal job ---------------------------------------------------------

def build_portal_job(result: Dict[str, Any], sites: List[str]) -> Dict[str, Any]:
    """NCI result (with ``_raw``) -> the fields the Summary page reads."""
    from callflow.core.orchestration import dashboard_shaping as ds
    from callflow.core.orchestration.analyzers.base import compute_state
    try:
        from callflow.core.constants.endpoints import VIAVIE_KPIS_MAPPING as name_map
    except Exception:
        name_map = {}

    raw = result.get("_raw") or {}
    errors = result.get("errors") or {}
    periods = len(_COMPARISONS)

    systems = {
        "npp": _system_status(errors, "npp", periods * max(len(sites), 1)),
        "viavi": _system_status(errors, "viavi", periods),
        "remedy": _system_status(errors, "remedy", periods),
    }

    npp_details = _merge_comparisons(raw.get("npp"), "affected_cells_details")
    viavi_details = _merge_comparisons(raw.get("viavi"), "affected_sites_details")

    # Chart rows via CallFlow's own shaping. NCI has no customer counts, so
    # impacted_customers stays None (NPP) / 0 (Viavi) -- the portal plots the
    # number of degraded KPIs instead in NCI mode.
    abnormal = {
        "npp": ds.shape_npp_abnormal(npp_details, {}, None),
        "viavi": ds.shape_viavi_abnormal(viavi_details, {}, name_map=name_map),
        "splunk": [],
    }

    # "Sites Affected" popup: every affected site with its NPP + Viavi KPIs,
    # i.e. exactly the sites counted in affected_sites.
    site_details: Dict[str, Any] = {}
    for site, info in npp_details.items():
        site_details[site] = {"site_name": site, "impacted_customers": None, "kpis": dict(info["kpis"])}
    for site, info in viavi_details.items():
        entry = site_details.setdefault(site, {"site_name": site, "impacted_customers": None, "kpis": {}})
        for kpi_name, kpi in info["kpis"].items():
            entry["kpis"][name_map.get(kpi_name, kpi_name)] = kpi

    affected = affected_sites(result.get("sites_summary") or {})
    affected_list = [a["site"] for a in affected]

    remedy = ds.shape_remedy({"top_remedy": _merge_remedy(raw.get("remedy"))},
                             affected_sites=list(sites))

    states = {}
    for system, details in (("npp", npp_details), ("viavi", viavi_details)):
        run = systems[system] != "FAILED"
        state, message = compute_state(run, len(sites) if run else 0, len(details))
        states[system] = {
            "state": state, "message": message, "analysis_run": run,
            "entities_analyzed": len(sites), "affected_count": len(details),
            "has_abnormal_kpis": len(details) > 0,
            "all_clean": run and len(details) == 0,
        }

    kpi_ok = systems["npp"] != "FAILED" or systems["viavi"] != "FAILED"
    dashboard = {
        "cellsAffected": len(affected_list),
        "totalCells": len(sites),
        "remedyAffected": remedy.get("remedy_count", 0),
    }
    summary = {
        "affected_cells": _card(kpi_ok, len(affected_list)),
        "tickets": _card(systems["remedy"] != "FAILED", dashboard["remedyAffected"]),
    }
    ok = [s for s in systems.values() if s == "COMPLETED"]
    overall = "COMPLETED" if len(ok) == len(systems) else ("PARTIAL_SUCCESS" if ok else "FAILED")

    return {
        "mode": "nci",
        "sites": list(sites),
        "overall_status": overall,
        "systems": systems,
        "dashboard": dashboard,
        "summary": summary,
        "details": {
            "npp_data": {
                "cells_affected_count": len(affected_list),
                "total_cells": len(sites),
                "cells_affected_list": affected_list,
                "affected_cells_details": site_details,
                "affected_metrics": {},
            },
            "smartcare_data_metrics": {"remedy": remedy},
            "abnormal": abnormal,
            "states": states,
        },
    }
