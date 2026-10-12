# callflow/core/views/stcc_analysis.py
"""
STCC endpoint: NPP / Viavi / Remedy analysis for a complaint, as a job
(same job pattern as every other CallFlow endpoint).

    POST /callflow/api/stcc/analysis
    body: same as the portal analysis
        { "msisdn_list": [...], "start_date": "...", "end_date": "...",
          "Complain_Type": "Data", "imsi": "" }
    -> 202 { "job_id": "...", "status": "PENDING" }

    GET  /callflow/api/stcc/analysis/<job_id>
    -> { status: "RUNNING", progress_percent, systems: {npp, viavi, remedy} }
    -> { status: "DONE", result: { overall_status, systems, cells, sites,
                                   npp, viavi, remedy, ... } }
    -> { status: "FAILED", error }

Reuses the EXISTING CallFlow orchestration unchanged: the same DAG
(build_callflow_plan), the same services (default_sources) and the same
analyzers. It only runs the subset of tasks NPP / Viavi / Remedy need:

    trino_cells -> npp_retrieval -> npp_analysis
                -> sites -> viavi_retrieval  -> viavi_analysis
                         -> remedy_retrieval -> remedy_analysis

Smartcare and Splunk are not run (and never waited for). Trino runs because
the cells/sites of the MSISDNs come from it. No RCA files.
"""

import datetime
import logging
import math
import time

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..orchestration.pipeline import (
    build_callflow_plan, default_sources, rollup_all,
    T_TRINO, T_SITES, T_NPP_GET, T_VIAVI_GET, T_REMEDY_GET,
    T_NPP_AN, T_VIAVI_AN, T_REMEDY_AN,
)
from ..orchestration.scheduler import run_plan

logger = logging.getLogger(__name__)

STCC_SYSTEMS = ("npp", "viavi", "remedy")
STCC_TASKS = {T_TRINO, T_SITES, T_NPP_GET, T_VIAVI_GET, T_REMEDY_GET,
              T_NPP_AN, T_VIAVI_AN, T_REMEDY_AN}
_ANALYSIS_TASK = {"npp": T_NPP_AN, "viavi": T_VIAVI_AN, "remedy": T_REMEDY_AN}


def stcc_plan(sources):
    """The CallFlow DAG restricted to NPP / Viavi / Remedy (and what they need)."""
    return [t for t in build_callflow_plan(sources) if t.name in STCC_TASKS]


def to_json(value):
    """Make analyzer output JSON-safe (DataFrames, numpy, NaN, datetimes)."""
    try:
        import pandas as pd
        import numpy as np
    except ImportError:  # pragma: no cover
        pd = np = None
    if isinstance(value, dict):
        return {str(k): to_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json(v) for v in value]
    if pd is not None:
        if isinstance(value, pd.DataFrame):
            return to_json(value.to_dict(orient="records"))
        if isinstance(value, pd.Series):
            return to_json(value.to_dict())
        if isinstance(value, pd.Timestamp):
            return None if pd.isna(value) else value.isoformat()
    if np is not None:
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            value = float(value)
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.ndarray):
            return to_json(value.tolist())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value


def build_stcc_response(result, context, wall_time):
    """PlanResult -> the STCC JSON (per-system analyzer result, unchanged)."""
    task_status = {name: oc.status.value for name, oc in result.outcomes.items()}
    all_systems = rollup_all(task_status)
    systems = {s: all_systems[s] for s in STCC_SYSTEMS}

    def system_payload(system):
        oc = result.outcomes[_ANALYSIS_TASK[system]]
        data = oc.result if oc.ok else None
        error = oc.error
        if not oc.ok:
            # analysis skipped because its retrieval (or Trino) failed -> report
            # the real cause, e.g. "NPP API 400", not just "skipped".
            for cause in ({"npp": T_NPP_GET, "viavi": T_VIAVI_GET, "remedy": T_REMEDY_GET}[system],
                          T_SITES, T_TRINO):
                cause_error = result.outcomes[cause].error
                if cause_error and not cause_error.startswith("skipped"):
                    error = f"{cause}: {cause_error}"
                    break
        return {
            "status": systems[system],
            "analysis": data,          # the analyzer's common result contract
            "error": error,
            "duration_seconds": oc.duration,
        }

    completed = [s for s in systems.values() if s == "COMPLETED"]
    if len(completed) == len(systems):
        overall = "COMPLETED"
    elif completed:
        overall = "PARTIAL_SUCCESS"
    else:
        overall = "FAILED"

    cells = result.outcomes[T_TRINO].result if result.outcomes[T_TRINO].ok else []
    sites = result.outcomes[T_SITES].result if result.outcomes[T_SITES].ok else []
    return to_json({
        "overall_status": overall,
        "systems": systems,
        "cells": cells or [],
        "sites": sites or [],
        "npp": system_payload("npp"),
        "viavi": system_payload("viavi"),
        "remedy": system_payload("remedy"),
        "errors": result.errors(),
        "duration_seconds": round(wall_time, 2),
    })


class STCCAnalysisAPIView(JobBackedAPIView):
    """NPP / Viavi / Remedy analysis for STCC: POST -> job_id, GET <job_id> -> result."""

    job_type = "stcc_analysis"
    sources_factory = staticmethod(default_sources)   # overridable in tests

    def build_log_context(self, request):
        # Fast validation only (runs in the request); ValueError -> 400.
        window = self.parse_request_time_range(request.data)
        msisdn_list = request.data.get("msisdn_list")
        if not msisdn_list:
            raise ValueError("msisdn_list is required")
        log_path, ts = self.get_log_file("stcc", window["start_dt"], window["end_dt"])
        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": msisdn_list,
            "complain_type": request.data.get("Complain_Type", "Data"),
            "imsi": request.data.get("imsi", ""),
            "query_time_range": self.build_analysis_time_windows(
                window["start_dt"], window["end_dt"]),
        }

    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list,
                complain_type, imsi, query_time_range):
        request_logger.info(f"[STCC] job={job_id} msisdns={len(msisdn_list)}")
        # Same context keys the portal orchestrator gives the services/analyzers.
        context = {
            "request_id": job_id,
            "msisdn_list": msisdn_list,
            "start_dt": window["start_dt"], "end_dt": window["end_dt"],
            "complain_type": complain_type, "imsi": imsi,
            "log_path": log_path, "ts": ts, "window": window,
            "query_time_range": query_time_range,
            "request_logger": request_logger,
        }

        # Live per-system status while RUNNING (npp / viavi / remedy only).
        task_status = {}

        def push(task_name, st):
            task_status[task_name] = st
            all_systems = rollup_all(task_status)
            done = sum(1 for v in task_status.values()
                       if v in ("SUCCESS", "FAILED", "TIMEOUT", "SKIPPED"))
            job_manager.update_job(
                job_id, status="RUNNING", step="analysis",
                progress_percent=int(done / len(STCC_TASKS) * 100),
                systems={s: all_systems[s] for s in STCC_SYSTEMS},
            )

        job_manager.update_job(job_id, status="RUNNING", step="analysis", progress_percent=0,
                               systems={s: "PENDING" for s in STCC_SYSTEMS})
        started = time.perf_counter()
        result = run_plan(
            stcc_plan(self.sources_factory()), context=context,
            on_start=lambda name: push(name, "RUNNING"),
            progress_cb=lambda done, total, name, st: push(name, st),
            logger_override=request_logger,
        )
        body = build_stcc_response(result, context, time.perf_counter() - started)
        body["request_id"] = job_id
        request_logger.info(f"[STCC] done job={job_id} overall={body['overall_status']} "
                            f"systems={body['systems']} in {body['duration_seconds']}s")
        # Final systems on the job too; the base class then marks DONE with result=body.
        job_manager.update_job(job_id, systems=body["systems"])
        return body
