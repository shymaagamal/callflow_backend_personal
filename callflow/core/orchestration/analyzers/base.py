"""
Shared analysis result contract for every system analyzer.

Common contract (system-specific fields live under the relevant keys):

    {
        "status": "success" | "failed",
        "system": "npp" | "splunk" | "viavi" | "remedy" | "smartcare",
        "summary": { headline counts, e.g. cells_affected_count / total_cells },
        "metrics": { system-specific metrics, e.g. affected_metrics },
        "affected": { affected entity lists, e.g. cells_list / sites_list },
        "details": { per-entity detail, e.g. affected_cells_details },
        "abnormal_kpis": [ chart-ready rows the dashboard/correlations consume ],
        "insights": [ short human-readable strings ],
        "error": None | str,
    }

The dashboard/RCA/correlations consume THIS result; they never re-derive the
business logic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

SUCCESS = "success"
FAILED = "failed"

# Explicit, unambiguous analysis states (section 4/5/8). An empty abnormal list
# NEVER implies "analysis didn't run": SUCCESS_ALL_CLEAN and NO_DATA are distinct.
STATE_NOT_RUN = "NOT_RUN"
STATE_SUCCESS_WITH_ABNORMAL = "SUCCESS_WITH_ABNORMAL_KPIS"
STATE_SUCCESS_ALL_CLEAN = "SUCCESS_ALL_CLEAN"
STATE_NO_DATA = "NO_DATA"
STATE_FAILED = "FAILED"


def build_result(
    system: str,
    *,
    summary: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    affected: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
    abnormal_kpis: Optional[List[Dict[str, Any]]] = None,
    insights: Optional[List[str]] = None,
    status: str = SUCCESS,
    error: Optional[str] = None,
    state: Optional[str] = None,
    message: Optional[str] = None,
    analysis_run: bool = True,
) -> Dict[str, Any]:
    return {
        "status": status,
        "system": system,
        "state": state,
        "message": message,
        "analysis_run": analysis_run,
        "summary": summary or {},
        "metrics": metrics or {},
        "affected": affected or {},
        "details": details or {},
        "abnormal_kpis": abnormal_kpis or [],
        "insights": insights or [],
        "error": error,
    }


def failed_result(system: str, error) -> Dict[str, Any]:
    return build_result(system, status=FAILED, state=STATE_FAILED,
                        message=f"Analysis failed: {error}", analysis_run=False,
                        error=str(error))


def compute_state(analysis_run: bool, entities_analyzed: int, affected_count: int):
    """Derive (state, message) from run/analyzed/affected counts."""
    if not analysis_run:
        return STATE_NOT_RUN, "Analysis did not run."
    if entities_analyzed == 0:
        return STATE_NO_DATA, "Analysis ran but no usable data was returned."
    if affected_count > 0:
        return (STATE_SUCCESS_WITH_ABNORMAL,
                f"Analysis completed. {affected_count} of {entities_analyzed} "
                f"analyzed entities are affected.")
    return (STATE_SUCCESS_ALL_CLEAN,
            f"Analysis completed. All {entities_analyzed} analyzed entities are "
            f"within thresholds. No abnormal KPIs detected.")
