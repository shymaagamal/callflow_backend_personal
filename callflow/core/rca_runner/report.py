# callflow/core/rca_runner/report.py
"""
Shape the RCA tool's own result objects into the public RCA report.

Pure mapping: every value comes from what rca_tool_V24.html produced
(AppState.correlationResults and performRCA(...)). Nothing is recomputed:
no scores, severities, counts or findings are derived here. The only
transformations are renaming, numeric strings -> numbers (the tool formats
values with toFixed), and linking suspects to findings by id instead of
repeating the anomaly objects.

Source -> report
    performRCA(r).hypothesis          -> root_cause (rootCause, mechanism, confidence,
                                         evidenceChain, impactPath, actions, nextSteps,
                                         differentials, ticketInsights, kbPattern)
    performRCA(r).primary/secondary/
      layerConf/scope/symptoms/
      layerRanking                    -> summary + root_cause.layer_ranking
    r.summary / r.subscriberStats     -> summary.counts / impact
    r.allAnomalies + r.txAnomalies    -> findings (one row each, ids F1..Fn)
    r.suspects / r.nodeSuspects /
      r.tacSuspects / r.chains        -> suspects.sites / .nodes / .tacs
    r.allTickets                      -> impact.tickets
"""

import csv
import io
import re

REPORT_VERSION = "1.0"

FINDING_COLUMNS = ["id", "system", "kpi", "node", "site", "severity", "observed", "baseline",
                   "deviation_pct", "direction", "threshold"]


def _num(v):
    """'2.500' -> 2.5; None/''/'-'/non-numeric -> None (never 0 for a missing value)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        s = str(v).strip()
        if s in ("", "-", "null", "undefined", "NaN"):
            return None
        f = float(s)
        return int(f) if f.is_integer() and "." not in s else f
    except (TypeError, ValueError):
        return None


def _snake(d):
    if not isinstance(d, dict):
        return None
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower(): v for k, v in d.items()}


def _txt(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _findings(results):
    out = []
    for a in results.get("anomalies") or []:
        if not _txt(a.get("kpiName")):        # the tool's own summary ignores these too
            continue
        out.append({
            "id": f"F{len(out) + 1}",
            "system": _txt(a.get("nodeType")),
            "kpi": _txt(a.get("displayKpi")) or _txt(a.get("kpiName")),
            "node": _txt(a.get("instance")),
            "site": _txt(a.get("site")),
            "severity": _txt(a.get("severity")),
            "observed": _num(a.get("complaintVal")),
            "baseline": _num(a.get("baselineVal")),
            "deviation_pct": _num(a.get("devPct")),
            "direction": _txt(a.get("devDev")),
            "threshold": _txt(a.get("threshold")),
        })
    for t in results.get("txAnomalies") or []:
        out.append({
            "id": f"F{len(out) + 1}",
            "system": "TX",
            "kpi": _txt(t.get("kpiName")),
            "node": _txt(t.get("route")),
            "site": _txt(t.get("site")),
            "severity": _txt(t.get("severity")),
            "observed": _num(t.get("value")),
            "baseline": _num(t.get("baseline")),
            "deviation_pct": _num(t.get("devPct")),
            "direction": None,
            "threshold": None,
        })
    return out


def _ids(findings, pred):
    return [f["id"] for f in findings if pred(f)]


def _suspects(results, findings):
    chains = {c.get("site"): c for c in results.get("chains") or []}
    sites = []
    for i, s in enumerate(results.get("suspects") or [], 1):
        st = s.get("stats") or {}
        name = s.get("name")
        sites.append({
            "rank": i,
            "type": _txt(s.get("type")),
            "site": name,
            "score": _num(s.get("score")),
            "is_primary": bool(s.get("primary")),
            "evidence": _txt(s.get("evidence")),
            "critical_issues": _num(st.get("criticalIssues")),
            "anomaly_issues": _num(st.get("anomalyIssues")),
            "tx_issues": _num(st.get("txIssues")),
            "subscribers": _num(st.get("subscribers")),
            "tickets": _num(st.get("tickets")),
            "node_types": _txt(st.get("nodeTypes")),
            "cells": list((chains.get(name) or {}).get("cells") or []),
            "score_breakdown": _snake(st.get("scoreBreakdown")),
            "finding_ids": _ids(findings, lambda f: f["site"] == name),
        })
    nodes = []
    for i, n in enumerate(results.get("nodeSuspects") or [], 1):
        nodes.append({
            "rank": i,
            "node": n.get("name"),
            "node_type": _txt(n.get("nodeType")),
            "score": _num(n.get("score")),
            "evidence": _txt(n.get("evidence")),
            "critical_issues": _num(n.get("criticalIssues")),
            "anomaly_issues": _num(n.get("anomalyIssues")),
            "affected_sites": _num(n.get("affectedSites")),
            "subscribers": _num(n.get("subscribers")),
            "sites": list(n.get("siteList") or []),
            "finding_ids": _ids(findings, lambda f: f["node"] == n.get("name")
                                and f["system"] == n.get("nodeType")),
        })
    tacs = []
    for i, t in enumerate(results.get("tacSuspects") or [], 1):
        tacs.append({
            "rank": i,
            "tac": _txt(t.get("tac")),
            "score": _num(t.get("score")),
            "evidence": _txt(t.get("evidence")),
            "affected_sites": _num(t.get("affectedSites")),
            "total_sites": _num(t.get("totalSites")),
            "subscribers": _num(t.get("subscribers")),
            "critical_issues": _num(t.get("criticalIssues")),
            "anomaly_issues": _num(t.get("anomalyIssues")),
            "tickets": _num(t.get("tickets")),
            "sites": list(t.get("siteList") or []),
        })
    return {"sites": sites, "nodes": nodes, "tacs": tacs}


def _tickets(results):
    seen, out = set(), []
    for t in results.get("tickets") or []:
        tid = None
        for k in ("incident_number", "ticket_number", "incident_id", "ticket_id", "Ticket_ID", "ID", "id"):
            if t.get(k):
                tid = str(t[k])
                break
        key = tid or repr(sorted(t.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append({"id": tid, "site": _txt(t.get("site")), "status": _txt(t.get("status")),
                    "fault_time": _txt(t.get("actual_fault_time")), "summary": _txt(t.get("summary"))})
    return out


def _root_cause(rc):
    if not rc:
        return None
    h = rc.get("hypothesis") or {}
    return {
        "hypothesis": _txt(h.get("rootCause")),
        "confidence": _txt(h.get("confidence")),
        "kb_pattern": _txt(h.get("kbPattern")),
        "mechanism": _txt(h.get("mechanism")),
        "evidence_chain": list(h.get("evidenceChain") or []),
        "impact_path": list(h.get("impactPath") or []),
        "recommended_actions": [
            {"priority": _num(a.get("priority")), "urgency": _txt(a.get("urgency")),
             "action": _txt(a.get("action")), "rationale": _txt(a.get("rationale"))}
            for a in (h.get("actions") or [])
        ],
        "next_steps": list(h.get("nextSteps") or []),
        "differentials": list(h.get("differentials") or []),
        "ticket_insights": list(((h.get("ticketInsights") or {}).get("snippets")) or []),
        "layer_ranking": [{"layer": l[0], "score": _num(l[1])} for l in (rc.get("layerRanking") or [])
                          if isinstance(l, (list, tuple)) and len(l) >= 2],
    }


def build_report(request_id, rca, systems, failed_systems, no_data_systems=()):
    """rca: browser_runner.run_rca_tool() output. systems: {system: status} from the run.
    failed_systems: systems whose data could not be retrieved (-> status PARTIAL).
    no_data_systems: systems that ran but returned no rows (reported, not a failure)."""
    results = rca.get("results") or {}
    rc = rca.get("root_cause") or {}
    h = rc.get("hypothesis") or {}
    s = results.get("summary") or {}
    params = results.get("params") or {}
    findings = _findings(results)

    warnings = []
    for sysname in failed_systems:
        warnings.append({"code": "SYSTEM_UNAVAILABLE", "system": sysname,
                         "message": f"{sysname} data was not available; findings from it are missing."})
    for sysname in no_data_systems:
        warnings.append({"code": "NO_DATA", "system": sysname,
                         "message": f"{sysname} returned no data for this request."})
    for w in rca.get("warnings") or []:
        code = w.get("code")
        if code == "FILES_NOT_LOADED":
            names = ", ".join(f.get("name", "?") for f in w.get("files") or [])
            warnings.append({"code": code, "message": f"The RCA tool could not load: {names}."})
        elif code == "FEWER_KPI_FILES_LOADED":
            missing = sorted(set(w.get("offered") or []) - set(w.get("loaded") or []))
            warnings.append({"code": code, "message": f"KPI files not loaded by the RCA tool: {', '.join(missing)}."})
        elif code == "ROOT_CAUSE_FAILED":
            warnings.append({"code": code, "message": "The RCA tool produced findings but no root-cause "
                                                      "hypothesis: " + str(w.get("message"))})
        # PAGE_ERRORS and similar stay in diagnostics only
    if not rc:
        warnings.append({"code": "NO_ROOT_CAUSE", "message": "No root-cause hypothesis was produced."})

    return {
        "report_version": REPORT_VERSION,
        "request_id": request_id,
        "status": "PARTIAL" if failed_systems else "COMPLETED",
        "analysis_window": {
            "start": params.get("startDate"), "end": params.get("endDate"),
            "baseline_method": params.get("baselineMethod"), "sensitivity": params.get("sensitivity"),
        },
        "data_coverage": {
            "systems_available": sorted(k for k in (systems or {})
                                        if k not in failed_systems and k not in no_data_systems),
            "systems_missing": list(failed_systems),
            "systems_without_data": list(no_data_systems),
        },
        "summary": {
            "primary_root_cause": _txt(h.get("rootCause")),
            "primary_layer": _txt(rc.get("primary")),
            "secondary_layer": _txt(rc.get("secondary")),
            "confidence": _txt(h.get("confidence")),
            "layer_confidence_pct": _num(rc.get("layerConf")),
            "scope": _txt(rc.get("scope")),
            "symptoms": list(rc.get("symptoms") or []),
            "counts": {
                "critical_findings": _num(s.get("critical")),
                "anomaly_findings": _num(s.get("anomalies")),
                "tx_findings": _num(s.get("txAnomalies")),
                "sites_affected": _num(s.get("sites")),
                "cells_in_scope": _num(s.get("cells")),
                "affected_subscribers": _num(s.get("affectedSubscribers")),
                "tickets": _num(s.get("tickets")),
            },
        },
        "root_cause": _root_cause(rc),
        "findings": findings,
        "suspects": _suspects(results, findings),
        "impact": {
            "affected_subscribers": _num((results.get("subscriberStats") or {}).get("totalUnique")),
            "sites_with_subscribers": _num((results.get("subscriberStats") or {}).get("sitesWithSubs")),
            "cells_in_scope": _num(s.get("cells")),
            "affected_sites": [c.get("site") for c in results.get("chains") or [] if c.get("site")],
            "node_types_detected": list(results.get("detectedNodeTypes") or []),
            "tickets": _tickets(results),
        },
        "warnings": warnings,
        "links": {
            "findings_csv": f"/callflow/api/rca/run/{request_id}/findings.csv",
            "diagnostics": f"/callflow/api/rca/run/{request_id}/diagnostics",
        },
    }


def findings_csv(report):
    """One row per finding, fixed columns, plus request_id. Root cause stays in JSON."""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["request_id"] + FINDING_COLUMNS, extrasaction="ignore",
                       lineterminator="\n")
    w.writeheader()
    for f in report.get("findings") or []:
        w.writerow({"request_id": report.get("request_id"),
                    **{k: ("" if f.get(k) is None else f.get(k)) for k in FINDING_COLUMNS}})
    return buf.getvalue()
