"""
Per-card Summary readiness

The Summary page must NOT wait for the whole job to reach COMPLETED. Each card
becomes ready as soon as ITS OWN backing analysis task finishes, and shows a
truthful state that distinguishes "still running" from "ran and found nothing"
from "failed":

    PENDING          -> backing task not finished yet (UI shows spinner)
    READY_WITH_DATA  -> backing task succeeded and produced a non-zero result
    READY_EMPTY      -> backing task succeeded and produced zero (a real 0)
    FAILED           -> backing task failed / timed out / was skipped

Crucially, readiness and VALUES are derived from the SAME backend analysis result
that RCA/details use (``details.states`` + ``dashboard``), never re-computed — so
the Summary cards, the RCA/abnormal data and the details popups always agree
(section 17: one source of truth).

Pure/stdlib: no Django/pandas. ``task_status`` is the scheduler's per-task status
map; ``details``/``dashboard`` are the structures the view already streams.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Splunk rows returned for display only (not linked to the complainants).
NOT_LINKED = ("not_serving", "no_mapping")

# Card readiness states.
PENDING = "PENDING"
READY_WITH_DATA = "READY_WITH_DATA"
READY_EMPTY = "READY_EMPTY"
FAILED = "FAILED"

# Scheduler task names (kept in sync with pipeline.py; duplicated as plain
# strings so this module stays import-light).
T_SMARTCARE = "smartcare"
T_NPP_AN = "npp_analysis"
T_VIAVI_AN = "viavi_analysis"
T_REMEDY_AN = "remedy_analysis"
T_SPLUNK_AN = "splunk_analysis"

_FAIL = {"FAILED", "TIMEOUT", "SKIPPED"}
_DONE_OK = "SUCCESS"


def _task_state(task_status: Dict[str, str], task: str) -> str:
    return task_status.get(task, "PENDING")


def _card(task_status: Dict[str, str], task: str, value: Optional[int]) -> Dict[str, Any]:
    """Map one backing task + its value into a truthful card readiness."""
    st = _task_state(task_status, task)
    if st in _FAIL:
        return {"status": FAILED, "value": None, "task": task, "task_status": st}
    if st != _DONE_OK:
        # Never emit 0 while pending — value stays None so the UI shows a spinner.
        return {"status": PENDING, "value": None, "task": task, "task_status": st}
    # Succeeded: a real number, even if 0.
    v = value or 0
    return {
        "status": READY_WITH_DATA if v > 0 else READY_EMPTY,
        "value": v,
        "task": task,
        "task_status": st,
    }


def _state_val(details: Dict[str, Any], system: str, key: str) -> Optional[int]:
    states = (details or {}).get("states") or {}
    sysd = states.get(system) or {}
    return sysd.get(key)


def summary_cards(task_status: Dict[str, str],
                  details: Optional[Dict[str, Any]] = None,
                  dashboard: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return per-card readiness derived from task status + streamed analysis.

    Cards and their REAL backing dependency (from the DAG, not the whole job):
      * affected_cells -> npp_analysis        (NPP KPI/threshold analysis)
      * tickets        -> remedy_analysis     (Remedy retrieval+filtering)
      * viavi          -> viavi_analysis
      * splunk         -> splunk_analysis
      * abnormal_kpis  -> npp + splunk + viavi analyses (progressive union)
      * smartcare      -> smartcare retrieval
    """
    details = details or {}
    dashboard = dashboard or {}

    cards: Dict[str, Any] = {}

    # Affected cells: NPP analysis only (NOT Smartcare/Viavi/Splunk/Remedy).
    cards["affected_cells"] = _card(
        task_status, T_NPP_AN, _state_val(details, "npp", "affected_count"))

    # Tickets: Remedy analysis. Count comes from the (affected-site filtered)
    # dashboard value the view already computed — same source RCA/popup use.
    cards["tickets"] = _card(task_status, T_REMEDY_AN, dashboard.get("remedyAffected"))

    # Viavi / Splunk affected counts.
    cards["viavi"] = _card(task_status, T_VIAVI_AN, _state_val(details, "viavi", "affected_count"))
    cards["splunk"] = _card(task_status, T_SPLUNK_AN, _state_val(details, "splunk", "affected_count"))

    # Smartcare readiness (retrieval).
    sc_st = _task_state(task_status, T_SMARTCARE)
    if sc_st in _FAIL:
        cards["smartcare"] = {"status": FAILED, "task": T_SMARTCARE, "task_status": sc_st}
    elif sc_st == _DONE_OK:
        cards["smartcare"] = {"status": READY_WITH_DATA, "task": T_SMARTCARE, "task_status": sc_st}
    else:
        cards["smartcare"] = {"status": PENDING, "task": T_SMARTCARE, "task_status": sc_st}

    # Abnormal KPIs: progressive union across NPP/Splunk/Viavi analyses. Each
    # system contributes as soon as its OWN analysis finishes.
    abnormal = (details.get("abnormal") or {})
    ready_systems: List[str] = []
    pending_systems: List[str] = []
    failed_systems: List[str] = []
    total_items = 0
    for system, task in (("npp", T_NPP_AN), ("splunk", T_SPLUNK_AN), ("viavi", T_VIAVI_AN)):
        st = _task_state(task_status, task)
        if st in _FAIL:
            failed_systems.append(system)
        elif st == _DONE_OK:
            ready_systems.append(system)
            total_items += len([r for r in (abnormal.get(system) or [])
                                if not (isinstance(r, dict) and r.get("serving_status") in NOT_LINKED)])
        else:
            pending_systems.append(system)

    if pending_systems:
        ak_status = PENDING if total_items == 0 else READY_WITH_DATA
    elif total_items > 0:
        ak_status = READY_WITH_DATA
    elif ready_systems:
        ak_status = READY_EMPTY
    else:
        ak_status = FAILED  # all three failed/skipped

    cards["abnormal_kpis"] = {
        "status": ak_status,
        "count": total_items,
        "ready_systems": ready_systems,
        "pending_systems": pending_systems,
        "failed_systems": failed_systems,
    }

    return cards
