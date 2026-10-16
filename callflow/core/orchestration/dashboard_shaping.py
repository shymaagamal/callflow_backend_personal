"""
Shape orchestration analyzer outputs into the structures the Angular summary /
popups / abnormal-KPI chart already consume.

Why this exists
---------------
The analyzers (KPIAnalyzer.analyze_npp / analyze_viavi / analyze_core_node) and
the Smartcare/NPP-enrichment services DO produce all the data the dashboard
needs, but the orchestrated result previously exposed only the RAW analyzer
output. The frontend expects specific shapes:

  * ``npp_data``           -> affected_cells_details + affected_metrics
  * ``smartcare_data_metrics`` -> metrics + packetNodes + remedy
  * abnormal arrays        -> [{cell_name/site_name/mapped_value,
                                impacted_customers/impacted_subs, degraded_kpis}]

These functions are PURE (no pandas/Django). Anything that needs the raw
DataFrame (KPI time-series, Splunk->Smartcare subscriber mapping) is injected as
a small callable by the caller, so the shaping itself is fully unit-testable.
The abnormal shapes mirror the existing abnormal_kpis.py output exactly, so the
frontend consumes them unchanged.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional


def _num(x):
    if x is None or x == "":
        return None
    try:
        return round(float(x), 2)
    except (TypeError, ValueError):
        return x


def customers_per_cell_from_smartcare(smartcare) -> Dict[str, int]:
    """subs_per_cell ([{label,value}] or {label:value}) -> {cell: count}."""
    if not isinstance(smartcare, dict):
        return {}
    metrics = smartcare.get("metrics") or {}
    spc = metrics.get("subs_per_cell")
    out: Dict[str, int] = {}
    if isinstance(spc, list):
        for item in spc:
            if isinstance(item, dict) and item.get("label") is not None:
                out[str(item["label"])] = item.get("value", 0)
    elif isinstance(spc, dict):
        out = {str(k): v for k, v in spc.items()}
    return out


def shape_smartcare_metrics(smartcare, remedy=None) -> Dict[str, Any]:
    """Assemble the ``smartcare_data_metrics`` object the popups read."""
    if not isinstance(smartcare, dict):
        smartcare = {}
    out = {
        "metrics": smartcare.get("metrics", {}) or {},
        "packetNodes": smartcare.get("packetNodes", {}) or {},
        "suspected_cells_raw": smartcare.get("suspected_cells_raw"),
        "query_category": smartcare.get("query_category"),
    }
    if remedy is not None:
        out["remedy"] = remedy
    return out


def shape_npp_data(npp_analysis) -> Dict[str, Any]:
    """Assemble the ``npp_data`` object the cards + popups read."""
    if not isinstance(npp_analysis, dict):
        return {"cells_affected_count": 0, "cells_affected_list": [],
                "affected_cells_details": {}, "affected_metrics": {}}
    metrics = npp_analysis.get("metrics", {}) or {}
    return {
        "cells_affected_count": metrics.get("affected_cells_count", 0),
        "cells_affected_list": metrics.get("affected_cells_list", []),
        "affected_cells_details": metrics.get("affected_cells_details", {}) or {},
        "affected_metrics": npp_analysis.get("affected_metrics", {}) or {},
    }


def shape_npp_abnormal(
    affected_cells_details: Dict[str, Any],
    customers_per_cell: Optional[Dict[str, int]] = None,
    kpi_series: Optional[Callable[[str, str], List[dict]]] = None,
) -> List[Dict[str, Any]]:
    """[{cell_name, system:'NPP', impacted_customers, degraded_kpis:[...]}].

    ``impacted_customers`` is None when the cell is absent from Smartcare's
    subs_per_cell (genuinely unknown -> UI shows N/A), never a misleading 0.
    """
    customers_per_cell = customers_per_cell or {}
    results = []
    for cell, details in (affected_cells_details or {}).items():
        degraded = []
        for kpi_name, k in (details.get("kpis") or {}).items():
            degraded.append({
                "name": kpi_name,
                "current_value": _num(k.get("current_value")),
                "previous_value": _num(k.get("previous_value")),
                "deviation_pct": k.get("deviation_pct"),
                "severity": k.get("severity", "unknown"),
                "message": k.get("message") or k.get("insight_message") or "",
                "kpi_data": kpi_series(cell, kpi_name) if kpi_series else [],
                "cell_name": cell,
            })
        impacted = customers_per_cell.get(str(cell))
        results.append({
            "cell_name": cell,
            "system": "NPP",
            "impacted_customers": impacted if impacted is not None else None,
            "degraded_kpis": degraded,
        })
    return results


def site_from_cell(cell: str) -> str:
    """Extract the site code from a cell name, mirroring the old
    data_utils.process_sitename rule (split on '_L' -> first '_' token ->
    letters + zero-padded digits, e.g. 'L09_L1323_01' -> '1323')."""
    if not cell:
        return ""
    raw = str(cell)
    seg = raw.split("_L")[-1].split("_")[0]
    digits = "".join(ch for ch in seg if ch.isdigit())
    letters = "".join(ch for ch in seg if not ch.isdigit())
    if digits == "" and letters == "":
        return seg
    return f"{letters}{digits.zfill(4)}" if digits else letters


def sites_from_cells(cells) -> List[str]:
    out = []
    for c in (cells or []):
        s = site_from_cell(c)
        if s and s not in out:
            out.append(s)
    return out


def filter_remedy_by_sites(remedy_output: Dict[str, Any], affected_sites) -> Dict[str, Any]:
    """Keep only remedy entries whose site key is related to an affected site.

    Matching mirrors the old calculate_top_remedy_with_subs rule
    (``str(site) in str(remedy_key)`` substring, case-insensitive)."""
    if not remedy_output:
        return {}
    sites = [str(s).strip() for s in (affected_sites or []) if str(s).strip()]
    if not sites:
        return {}
    out = {}
    for key, val in remedy_output.items():
        k = str(key).upper()
        if any(s.upper() in k for s in sites):
            out[key] = val
    return out


def _site_code(flwid: str) -> Optional[str]:
    m = re.search(r'(ERD|HU|HUK|ERK)\d+', str(flwid))
    return m.group(0) if m else None


def shape_viavi_abnormal(
    affected_sites_details: Dict[str, Any],
    customers_per_cell: Optional[Dict[str, int]] = None,
    name_map: Optional[Dict[str, str]] = None,
    kpi_series: Optional[Callable[[str, str], List[dict]]] = None,
) -> List[Dict[str, Any]]:
    """[{site_name, system:'Viavi', impacted_customers, degraded_kpis:[...]}]."""
    customers_per_cell = customers_per_cell or {}
    name_map = name_map or {}
    results = []
    for flwid, details in (affected_sites_details or {}).items():
        code = _site_code(flwid)
        search_key = code[2:] if code else str(flwid)
        matched_cell = next(
            (c for c in customers_per_cell if search_key and search_key in c), None
        )
        impacted = customers_per_cell.get(matched_cell, 0) if matched_cell else 0
        degraded = []
        for kpi_name, k in (details.get("kpis") or {}).items():
            degraded.append({
                "name": name_map.get(kpi_name, kpi_name),
                "current_value": _num(k.get("current_value")),
                "previous_value": _num(k.get("previous_value")),
                "deviation_pct": k.get("deviation_pct"),
                "severity": k.get("severity", "unknown"),
                "message": k.get("message") or k.get("insight_message") or "",
                "kpi_data": kpi_series(flwid, kpi_name) if kpi_series else [],
                "site_name": search_key,
            })
        results.append({
            "site_name": search_key,
            "system": "Viavi",
            "impacted_customers": impacted,
            "degraded_kpis": degraded,
        })
    return results


def shape_splunk_abnormal(
    alarms_by_node: Dict[str, List[dict]],
    subs_mapper: Optional[Callable[[str], "tuple"]] = None,
    kpi_series: Optional[Callable[[str, str], List[dict]]] = None,
) -> List[Dict[str, Any]]:
    """[{node, mapped_value, impacted_subs, Kpi_Column, degraded_kpis, system:'splunk'}].

    ``subs_mapper(kpi_column) -> (mapped_value, impacted_subs, node_type[, serving_status])``
    supplies the Smartcare-based mapping (pandas lives in the caller). Without it, DNS-style
    passthrough is used (mapped_value == kpi_column, impacted_subs == None).
    ``serving_status`` (serving | unverified | not_serving | no_mapping) is copied onto
    the row so the portal can separate alarms linked to the complainants from the rest.
    """
    results = []
    for node, alarms in (alarms_by_node or {}).items():
        for alarm in (alarms or []):
            kpi_col = alarm.get("kpi_column")
            kpi_entry = {
                "name": None,  # set below
                "current_value": _num(alarm.get("current_value")),
                "previous_value": _num(alarm.get("prev_value")),
                "deviation_pct": alarm.get("deviation_percentage"),
                "severity": alarm.get("severity"),
                "message": alarm.get("message"),
                "kpi_data": kpi_series(node, kpi_col) if kpi_series else [],
            }
            if node == "DNS" or subs_mapper is None:
                kpi_entry["name"] = kpi_col
                results.append({
                    "node": node,
                    "splunk_node": node,
                    "mapped_value": kpi_col,
                    "impacted_subs": None,
                    "Kpi_Column": kpi_col,
                    "degraded_kpis": [kpi_entry],
                    "system": "splunk",
                })
                continue
            mapped = subs_mapper(kpi_col)
            mapped_value, impacted_subs, node_type = mapped[:3]
            if mapped_value is None:
                continue
            kpi_entry["name"] = mapped_value
            results.append({
                "node": node_type or node,
                "splunk_node": node,
                "serving_status": mapped[3] if len(mapped) > 3 else "serving",
                "mapped_value": mapped_value,
                "impacted_subs": impacted_subs,
                "Kpi_Column": kpi_col,
                "degraded_kpis": [kpi_entry],
                "system": "splunk",
            })
    return sorted(results, key=lambda x: x.get("impacted_subs") or 0, reverse=True)


def shape_remedy(remedy_analysis, remedy_retrieval=None, affected_sites=None) -> Dict[str, Any]:
    """Assemble the ``remedy`` object the popup reads: {remedy_count, remedy_output}.

    When ``affected_sites`` is provided, the ticket set is filtered to tickets
    related to those sites (one source of truth = the affected cells/sites), and
    the count reflects only those tickets (never unrelated ones). When it is None
    (affected set unknown, e.g. NPP failed), the full set is returned as a
    fallback so the card is not blank.
    """
    top = {}
    if isinstance(remedy_analysis, dict):
        top = remedy_analysis.get("top_remedy") or {}

    if affected_sites is not None:
        top = filter_remedy_by_sites(top, affected_sites)
        return {
            "remedy_count": _remedy_ticket_total(top),
            "remedy_output": top,
            "filtered_by_affected": True,
        }

    # Fallback (no affected set available): keep prior behaviour.
    count = 0
    if isinstance(remedy_analysis, dict) and isinstance(remedy_analysis.get("status"), int):
        count = remedy_analysis["status"]
    elif isinstance(remedy_retrieval, dict):
        status = remedy_retrieval.get("status")
        if isinstance(status, int):
            count = status
        else:
            tickets = remedy_retrieval.get("remedy_tickets", {})
            count = len(tickets) if isinstance(tickets, dict) else 0
    return {"remedy_count": count, "remedy_output": top, "filtered_by_affected": False}


def _remedy_ticket_total(remedy_output: Dict[str, Any]) -> int:
    """Sum the ticket counts across the (filtered) remedy sites."""
    total = 0
    for _site, data in (remedy_output or {}).items():
        if isinstance(data, dict) and isinstance(data.get("total"), int):
            total += data["total"]
        elif isinstance(data, dict) and isinstance(data.get("tickets"), list):
            total += len(data["tickets"])
        else:
            total += 1
    return total
