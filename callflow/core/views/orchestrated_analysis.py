"""
Orchestrated analysis endpoint + RCA readiness/serving endpoints.

Built entirely on the existing data-availability orchestration
(orchestration/scheduler.py + pipeline.py). No second orchestration mechanism
and NO change to the DAG/task topology. It consumes the existing scheduler and
adds, around it:

  * structured, per-request + per-system logging (task_logging) -- every event
    carries request_id / system / task / status / duration / error, and each
    system gets its own log file under rca/<request_id>/logs/<system>.log so
    concurrent requests never interleave (see task_logging.py);
  * the final, parameter-substituted Trino SQL logged for the trino task;
  * a Trino -> Smartcare cell-name fallback (cell_resolution) that keeps the DAG
    unchanged: the trino task, ONLY when Trino returns no cells, waits for the
    already-running Smartcare task and reuses its cell column, with clear errors;
  * incremental per-card dashboard values pushed into the job as each system
    finishes, so the UI shows real values instead of dummy defaults.

Endpoints (unchanged from before):
  POST /callflow/api/analysis                         -> { job_id, status }
  GET  /callflow/api/analysis/<request_id>            -> job (+ systems, rca, dashboard, result)
  GET  /callflow/api/analysis/<request_id>/rca-status -> readiness contract
  GET  /callflow/api/analysis/<request_id>/rca-files  -> RCA-tool file list
  GET  /callflow/api/analysis/<request_id>/file/<filename> -> request-scoped artifact
"""

import logging
import threading
import time

from django.conf import settings
from django.http import FileResponse, Http404
from rest_framework.response import Response
from rest_framework import status as http_status

from .base import BaseCallFlowAPIView
from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..orchestration.pipeline import (
    default_sources, run_callflow_analysis, TASK_SYSTEM, TASK_DEPS, FILE_SYSTEMS,
    TRINO_FRAME_KEY, rollup_all, classify_overall,
)
from ..orchestration import rca_files as rf
from ..orchestration import cell_resolution as cr
from ..orchestration import dashboard_shaping as ds
from ..orchestration.task_logging import TaskEventLogger
from ..orchestration.task_logging import render_pipeline_summary

logger = logging.getLogger(__name__)

# How long the trino task will wait for Smartcare before giving up on the
# fallback. Kept just under the trino task timeout so the scheduler doesn't
# kill the task mid-wait. Does not affect the normal (Trino-found) path.
FALLBACK_WAIT_SECONDS = 110


def _output_dir():
    return settings.OUTPUT_DIR


class AnalysisOrchestratorAPIView(JobBackedAPIView):
    job_type = "orchestrated_analysis"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        msisdn_list = request.data.get("msisdn_list")
        if not msisdn_list:
            raise ValueError("msisdn_list is required")
        complain_type = request.data.get("Complain_Type", "Data")
        imsi = request.data.get("imsi", "")
        log_path, ts = self.get_log_file("orchestrated", window["start_dt"], window["end_dt"])
        query_time_range = self.build_analysis_time_windows(window["start_dt"], window["end_dt"])
        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": msisdn_list, "complain_type": complain_type,
            "imsi": imsi, "query_time_range": query_time_range,
        }

    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list,
                complain_type, imsi, query_time_range):
        request_id = job_id  # request_id IS the job id -> naturally isolated
        base = _output_dir()
        tlog = TaskEventLogger(request_id, request_logger, base)
        tlog.event("ORCHESTRATOR", "analysis", "STARTED",
                   detail=f"msisdns={len(msisdn_list)}")

        rf.init_request(base, request_id, FILE_SYSTEMS)

        # Auto-provision the server-side threshold file into the request dir so the
        # RCA tool receives it without a manual browse. Path is settings-derived
        # (portable), never hardcoded.
        threshold_path = getattr(settings, "THRESHOLDING_SHEET_PATH", None)
        tinfo = rf.provision_threshold(base, request_id, threshold_path) if threshold_path else {"exists": False}
        tlog.event(
            "RCA", "threshold", "READY" if tinfo.get("copied") else "FAILED",
            detail=f"path={tinfo.get('path')} exists={tinfo.get('exists')} size={tinfo.get('size', 0)}",
            error=None if tinfo.get("copied") else "threshold file could not be loaded",
        )

        context = {
            "request_id": request_id,
            "msisdn_list": msisdn_list,
            "start_dt": window["start_dt"], "end_dt": window["end_dt"],
            "complain_type": complain_type, "imsi": imsi,
            "log_path": log_path, "ts": ts, "window": window,
            "query_time_range": query_time_range,
            "request_logger": request_logger,
            "base_output_dir": base,
            # Trino task logs the final SQL through this sink (request-scoped).
            "_trino_sql_sink": lambda sql: tlog.query("TRINO", "trino_cells", sql),
            # Fallback coordination (does NOT add a DAG edge):
            "_smartcare_ready": threading.Event(),
            "_smartcare_holder": {},
            "_fallback_wait_seconds": FALLBACK_WAIT_SECONDS,
        }

        # -- live dashboard values (pushed incrementally as systems finish) --
        dashboard = {}
        dash_lock = threading.Lock()

        def merge_dash(contrib):
            if not contrib:
                return
            with dash_lock:
                dashboard.update({k: v for k, v in contrib.items() if v is not None})
                job_manager.update_job(job_id, dashboard=dict(dashboard))

        # Total complaints is known from the input immediately.
        merge_dash({"totalComplaints": len(msisdn_list)})

        # -- detailed structures the popups + abnormal chart consume ---------
        # Streamed into the job as each system finishes so details load
        # independently (never dummy). Shapes match what the frontend expects.
        details = {
            "smartcare_data_metrics": None,
            "npp_data": None,
            "abnormal": {"npp": [], "splunk": [], "viavi": []},
            "states": {},   # per-system {state, message, analysis_run, ...}
        }
        details_lock = threading.Lock()
        # Holders for the one-source-of-truth remedy filtering (affected cells ->
        # sites -> related tickets only). Recomputed whenever either side arrives.
        holder = {"affected_cells": None, "remedy_top": None}

        def push_details():
            job_manager.update_job(job_id, details={
                "smartcare_data_metrics": details["smartcare_data_metrics"],
                "npp_data": details["npp_data"],
                "abnormal": {k: list(v) for k, v in details["abnormal"].items()},
                "states": dict(details["states"]),
            })

        def set_smartcare_metrics(shaped):
            with details_lock:
                base_m = details.get("smartcare_data_metrics") or {}
                base_m.update(shaped)
                details["smartcare_data_metrics"] = base_m
                push_details()

        def _recompute_remedy_locked():
            top = holder["remedy_top"]
            if top is None:
                return
            sites = ds.sites_from_cells(holder["affected_cells"] or [])
            shaped = ds.shape_remedy({"top_remedy": top}, affected_sites=(sites or None))
            base_m = details.get("smartcare_data_metrics") or {}
            base_m["remedy"] = shaped
            details["smartcare_data_metrics"] = base_m
            push_details()
            merge_dash({"remedyAffected": shaped.get("remedy_count")})

        def set_remedy_full(top_remedy):
            with details_lock:
                holder["remedy_top"] = top_remedy or {}
                _recompute_remedy_locked()

        def _log_abnormal_pushed(system):
            # Diagnostic: proves the per-system abnormal result reached the job
            # (and therefore the GET /analysis/<id> response) while RUNNING.
            request_logger.info(
                "[ABNORMAL] request_id=%s system=%s abnormal_count=%d pushed_to_job=true",
                request_id, system, len(details["abnormal"].get(system) or []),
            )

        def set_npp_details(npp_data, abnormal):
            with details_lock:
                details["npp_data"] = npp_data
                details["abnormal"]["npp"] = abnormal
                holder["affected_cells"] = (npp_data or {}).get("cells_affected_list") or []
                push_details()
                _log_abnormal_pushed("npp")
                _recompute_remedy_locked()  # re-filter tickets by affected sites

        def set_abnormal(system, arr):
            with details_lock:
                details["abnormal"][system] = arr
                push_details()
                _log_abnormal_pushed(system)

        def set_state(system, contract):
            with details_lock:
                summary = contract.get("summary", {}) or {}
                details["states"][system] = {
                    "state": contract.get("state"),
                    "message": contract.get("message"),
                    "analysis_run": contract.get("analysis_run"),
                    "entities_analyzed": summary.get("entities_analyzed"),
                    "affected_count": summary.get("affected_count"),
                    "has_abnormal_kpis": summary.get("has_abnormal_kpis"),
                    "all_clean": summary.get("all_clean"),
                }
                push_details()

        detail_sinks = {
            "smartcare": set_smartcare_metrics,
            "remedy_full": set_remedy_full,
            "npp": set_npp_details,
            "abnormal": set_abnormal,
            "state": set_state,
        }

        sources = self._build_sources(base, request_id, tlog, merge_dash, detail_sinks)

        # -- truthful, event-driven status tracking --------------------------
        task_status = {}
        task_started_at = {}
        task_ended_at = {}
        lock = threading.Lock()

        def _summary_snapshot():
            """Per-card readiness from the SAME streamed analysis result (one
            source of truth). Snapshots details/dashboard so no cross-lock hold."""
            try:
                from ..orchestration.summary_readiness import summary_cards
                det = {"states": dict(details.get("states") or {}),
                       "abnormal": {k: list(v) for k, v in (details.get("abnormal") or {}).items()}}
                return summary_cards(dict(task_status), det, dict(dashboard))
            except Exception:
                return {}

        last_card_status = {}

        def push_job():
            systems = rollup_all(task_status)
            rca = rf.rca_status(base, request_id)
            done = sum(1 for s in task_status.values()
                       if s in ("SUCCESS", "FAILED", "TIMEOUT", "SKIPPED"))
            total = len(TASK_SYSTEM)
            cards = _summary_snapshot()
            # Log a card only when its readiness CHANGES (avoids log spam).
            for name, card in cards.items():
                st = card.get("status", "PENDING")
                if last_card_status.get(name) != st:
                    last_card_status[name] = st
                    tlog.event("SUMMARY", f"card.{name}", st,
                               detail=f"value={card.get('value', card.get('count'))}")
            job_manager.update_job(
                job_id, status="RUNNING", step="analysis",
                progress_percent=int(done / total * 100) if total else 0,
                systems=systems, rca=rca, summary=cards,
            )

        def _dep_wait_detail(task_name):
            """Human-readable 'WAITING FOR: dep ✓ ...' + how long this task waited
            after its last dependency finished (request section 11)."""
            deps = TASK_DEPS.get(task_name, [])
            if not deps:
                return None
            now = time.perf_counter()
            marks = []
            last_dep_end = None
            for d in deps:
                st = task_status.get(d, "PENDING")
                ok = "OK" if st == "SUCCESS" else st
                marks.append(f"{d}={ok}")
                end = task_ended_at.get(d)
                if end is not None:
                    last_dep_end = end if last_dep_end is None else max(last_dep_end, end)
            waited = f"{now - last_dep_end:.2f}s" if last_dep_end is not None else "0.00s"
            return f"waiting_for=[{', '.join(marks)}] waited_after_deps={waited}"

        def on_start(task_name):
            sysname, task = TASK_SYSTEM.get(task_name, ("orchestrator", task_name))
            with lock:
                task_status[task_name] = "RUNNING"
                task_started_at[task_name] = time.perf_counter()
                tlog.event(sysname, task_name, "STARTED", detail=_dep_wait_detail(task_name))
                push_job()

        def progress_cb(done, total, task_name, status):
            sysname, task = TASK_SYSTEM.get(task_name, ("orchestrator", task_name))
            with lock:
                task_status[task_name] = status
                task_ended_at[task_name] = time.perf_counter()
                duration = None
                if task_name in task_started_at:
                    duration = task_ended_at[task_name] - task_started_at[task_name]
                tlog.event(sysname, task_name, status, duration=duration)
                push_job()

        result = run_callflow_analysis(
            context, sources=sources,
            progress_cb=progress_cb, on_start=on_start,
            logger_override=request_logger,
        )

        # End-of-request timing table so the bottleneck is obvious at a glance.
        try:
            diag = (result or {}).get("diagnostics", {}) or {}
            summary_txt = render_pipeline_summary(
                request_id, diag.get("timings", {}) or {},
                diag.get("wall_time_seconds"),
                order=["trino_cells", "sites", "smartcare", "npp_retrieval",
                       "viavi_retrieval", "remedy_retrieval", "splunk_retrieval",
                       "npp_analysis", "viavi_analysis", "remedy_analysis",
                       "splunk_analysis"],
            )
            request_logger.info(summary_txt)
        except Exception:  # summary logging must never break the request
            pass

        systems = rollup_all(task_status)
        overall = classify_overall(systems)
        rca = rf.rca_status(base, request_id)

        tlog.event("ORCHESTRATOR", "analysis", "FINISHED",
                   detail=f"overall={overall} rca={rca['status']}")
        if rca["status"] in (rf.RCA_READY, rf.RCA_PARTIAL):
            tlog.event("ORCHESTRATOR", "rca", "READY", detail=rca["status"])

        return {
            "request_id": request_id,
            "overall_status": overall,
            "systems": systems,
            "summary": _summary_snapshot(),
            "rca": rca,
            "dashboard": dict(dashboard),
            "details": {
                "smartcare_data_metrics": details["smartcare_data_metrics"],
                "npp_data": details["npp_data"],
                "abnormal": {k: list(v) for k, v in details["abnormal"].items()},
                "states": dict(details["states"]),
            },
            "analysis": result,
        }

    # -- Source wrappers: RCA files + dashboard + logging + cell fallback -----
    def _build_sources(self, base, request_id, tlog, merge_dash, detail_sinks=None):
        sources = default_sources()
        detail_sinks = detail_sinks or {}

        def write_file_safe(system, extract, data, ctx):
            try:
                result = extract(system, data, ctx)
                if isinstance(result, list):  # several files (Splunk: one per node)
                    if result:
                        rf.write_system_files(base, request_id, system, result)
                    else:
                        rf.mark_not_applicable(base, request_id, system)
                    return
                filename, content = result
                if content is None:
                    rf.mark_not_applicable(base, request_id, system)
                else:
                    rf.write_system_file(base, request_id, system, filename, content)
            except Exception as e:  # file generation must never break retrieval
                tlog.event(system, f"{system}_rca_file", "ERROR", error=str(e))
                rf.mark_file_failed(base, request_id, system, str(e))

        def retrieval_wrapper(system, orig, extract, dash=None):
            def _wrapped(*args):
                ctx = args[-1]
                rf.set_data_status(base, request_id, system, rf.DATA_PROCESSING)
                data = orig(*args)
                rf.set_data_status(base, request_id, system, rf.DATA_COMPLETED)
                write_file_safe(system, extract, data, ctx)
                if dash:
                    try:
                        merge_dash(dash(data))
                    except Exception:
                        tlog.event(system, f"{system}_dashboard", "ERROR")
                return data
            return _wrapped

        # ---- Smartcare: retrieval + file + dashboard + fallback signalling ---
        orig_smartcare = sources.fetch_smartcare

        def wrapped_smartcare(ctx):
            system = "smartcare"
            holder = ctx.get("_smartcare_holder")
            ready = ctx.get("_smartcare_ready")
            rf.set_data_status(base, request_id, system, rf.DATA_PROCESSING)
            try:
                data = orig_smartcare(ctx)
                if holder is not None:
                    holder["result"] = data
                rf.set_data_status(base, request_id, system, rf.DATA_COMPLETED)
                write_file_safe(system, self._extract_smartcare, data, ctx)
                merge_dash(self._dash_smartcare(data))
                if detail_sinks.get("smartcare"):
                    try:
                        detail_sinks["smartcare"](ds.shape_smartcare_metrics(data))
                    except Exception:
                        tlog.event(system, "smartcare_details", "ERROR")
                return data
            except Exception as e:
                if holder is not None:
                    holder["error"] = str(e)
                rf.set_data_status(base, request_id, system, rf.DATA_FAILED, str(e))
                raise
            finally:
                # Unblock any Trino fallback that is waiting, success or failure.
                if ready is not None:
                    ready.set()

        sources.fetch_smartcare = wrapped_smartcare

        # ---- Trino cells: normal path + Smartcare fallback (DAG unchanged) ----
        orig_get_cells = sources.get_cells

        def wrapped_get_cells(ctx):
            rf.set_data_status(base, request_id, "trino", rf.DATA_PROCESSING)
            try:
                trino_cells = orig_get_cells(ctx)  # logs the final SQL via sink
            except Exception as e:
                rf.set_data_status(base, request_id, "trino", rf.DATA_FAILED, str(e))
                raise
            rf.set_data_status(base, request_id, "trino", rf.DATA_COMPLETED)
            # Raw Trino rows -> this request's RCA dir (trino_cells.csv), same
            # writer as the other systems; no rows -> NOT_APPLICABLE.
            write_file_safe("trino", self._extract_df_csv("trino", "cells"),
                            ctx.get(TRINO_FRAME_KEY), ctx)
            resolution = cr.resolve_cells(trino_cells)

            if not resolution.resolved:
                # Trino empty -> wait for the concurrently-running Smartcare task,
                # then reuse its cell column. No DAG edge added.
                ready = ctx.get("_smartcare_ready")
                if ready is not None:
                    tlog.event("TRINO", "trino_cells", "FALLBACK",
                               detail="Trino returned no cells; waiting for Smartcare")
                    ready.wait(timeout=ctx.get("_fallback_wait_seconds", FALLBACK_WAIT_SECONDS))
                holder = ctx.get("_smartcare_holder") or {}
                sc_result = holder.get("result")
                available, has_col, sc_cells = cr.smartcare_cell_facts(sc_result)
                resolution = cr.resolve_cells(
                    [], smartcare_available=available,
                    smartcare_has_column=has_col, smartcare_cells=sc_cells,
                )

            if resolution.resolved:
                tlog.event("TRINO", "trino_cells", "SUCCESS",
                           detail=f"{resolution.message} source={resolution.source} "
                                  f"count={len(resolution.cells)}")
                merge_dash({"totalCells": len(resolution.cells),
                            "cellSource": resolution.source})
                return resolution.cells

            # Neither source could resolve the cell name -> clear, explicit error.
            tlog.event("TRINO", "trino_cells", "FAILED", error=resolution.message)
            raise ValueError(resolution.message)

        sources.get_cells = wrapped_get_cells

        # ---- Other retrievals: file + (remedy) dashboard --------------------
        sources.retrieve_npp = retrieval_wrapper(
            "npp", sources.retrieve_npp, self._extract_df_csv("npp"))
        sources.retrieve_viavi = retrieval_wrapper(
            "viavi", sources.retrieve_viavi, self._extract_df_csv("viavi"))
        # remedyAffected is set AFTER analysis (filtered to affected sites), so we
        # do not set an unfiltered count at retrieval time.
        sources.retrieve_remedy = retrieval_wrapper(
            "remedy", sources.retrieve_remedy, self._extract_remedy)
        sources.retrieve_splunk = retrieval_wrapper(
            "splunk", sources.retrieve_splunk, self._extract_splunk)

        # ---- Analysis wrappers: consume each analyzer's result CONTRACT ------
        # The analyzers (orchestration/analyzers/*) own the business logic and
        # return the common contract; these wrappers only MAP the contract to the
        # frontend structures (job.details + dashboard). No KPI math here.
        orig_npp_an = sources.analyze_npp

        def _emit_state(system, contract):
            if detail_sinks.get("state"):
                detail_sinks["state"](system, contract)

        def wrapped_npp_analysis(raw, smartcare, ctx):
            contract = orig_npp_an(raw, smartcare, ctx)
            try:
                summary = contract.get("summary", {}) or {}
                affected_metrics = (contract.get("metrics", {}) or {}).get("affected_metrics", {}) or {}
                total = summary.get("total_cells")
                affected = summary.get("cells_affected_count")
                merge_dash(self._dash_from_npp(summary, affected_metrics))
                npp_data = {
                    "cells_affected_count": affected,
                    "total_cells": total,
                    "cells_affected_list": (contract.get("affected", {}) or {}).get("cells_list", []),
                    # affected_cells_details now carries impacted_customers per cell.
                    "affected_cells_details": (contract.get("details", {}) or {}).get("affected_cells_details", {}),
                    "affected_metrics": affected_metrics,
                }
                if detail_sinks.get("npp"):
                    detail_sinks["npp"](npp_data, contract.get("abnormal_kpis", []))
                _emit_state("npp", contract)
            except Exception:
                tlog.event("npp", "npp_details", "ERROR")
            return contract

        sources.analyze_npp = wrapped_npp_analysis

        orig_viavi_an = sources.analyze_viavi

        def wrapped_viavi_analysis(raw, smartcare, ctx):
            contract = orig_viavi_an(raw, smartcare, ctx)
            try:
                if detail_sinks.get("abnormal"):
                    detail_sinks["abnormal"]("viavi", contract.get("abnormal_kpis", []))
                _emit_state("viavi", contract)
            except Exception:
                tlog.event("viavi", "viavi_details", "ERROR")
            return contract

        sources.analyze_viavi = wrapped_viavi_analysis

        orig_splunk_an = sources.analyze_splunk

        def wrapped_splunk_analysis(raw, smartcare, ctx):
            contract = orig_splunk_an(raw, smartcare, ctx)
            try:
                if detail_sinks.get("abnormal"):
                    detail_sinks["abnormal"]("splunk", contract.get("abnormal_kpis", []))
                _emit_state("splunk", contract)
            except Exception:
                tlog.event("splunk", "splunk_details", "ERROR")
            return contract

        sources.analyze_splunk = wrapped_splunk_analysis

        orig_remedy_an = sources.analyze_remedy

        def wrapped_remedy_analysis(raw, smartcare, ctx):
            contract = orig_remedy_an(raw, smartcare, ctx)
            try:
                # Hand the FULL ticket map to the view; it filters to the affected
                # sites (one source of truth) once NPP affected cells are known.
                top_full = (contract.get("details", {}) or {}).get("top_remedy") or {}
                if detail_sinks.get("remedy_full"):
                    detail_sinks["remedy_full"](top_full)
                _emit_state("remedy", contract)
            except Exception:
                tlog.event("remedy", "remedy_details", "ERROR")
            return contract

        sources.analyze_remedy = wrapped_remedy_analysis
        return sources

    @staticmethod
    def _dash_from_npp(summary, affected_metrics):
        """Derive the NPP dashboard cards from the analyzer contract. Cities/Hubs/
        TAC-LAC are omitted (card shows '—') unless the master sheet matched
        (matched_sites_count > 0), so a no-match is never shown as a real 0."""
        out = {
            "cellsAffected": summary.get("cells_affected_count"),
            "totalCells": summary.get("total_cells"),
        }
        aff = affected_metrics or {}
        matched = aff.get("matched_sites_count") or 0
        if matched > 0:
            cities = aff.get("subs_per_region")
            tac = aff.get("tacs_affected_count")
            lac = aff.get("lacs_affected_count")
            out["hubsAffected"] = aff.get("hubs_affected_count")
            out["tacsLacsAffected"] = (
                (tac or 0) + (lac or 0) if (tac is not None or lac is not None) else None
            )
            out["citiesSpanned"] = len(cities) if isinstance(cities, (list, dict)) else None
        return out

    # -- dashboard extractors (guarded; null when not computable) -------------
    @staticmethod
    def _dash_smartcare(data):
        if not isinstance(data, dict):
            return {}
        metrics = data.get("metrics", {}) or {}
        packet = data.get("packetNodes", {}) or {}
        handset = metrics.get("subs_per_handset_type")
        return {
            "handsetTypes": len(handset) if isinstance(handset, (list, dict)) else None,
            "packetCoreNodes": len(packet) if isinstance(packet, dict) else None,
        }

    @staticmethod
    def _dash_npp_UNUSED(data):
        if not isinstance(data, dict):
            return {}
        metrics = data.get("metrics", {}) or {}
        aff = data.get("affected_metrics", {}) or {}
        out = {"cellsAffected": metrics.get("affected_cells_count")}
        return out  # deprecated: NPP dashboard now derived from contract in _dash_from_npp

        # Hubs / TAC-LAC / Cities are only meaningful when the NPP-affected cells
        # actually matched the master sheet. If nothing matched
        # (matched_sites_count == 0), the values are genuinely UNAVAILABLE, so we
        # OMIT them (card shows "—") instead of emitting a misleading 0.
        matched = aff.get("matched_sites_count") or 0
        if matched > 0:
            cities = aff.get("subs_per_region")
            tac = aff.get("tacs_affected_count")
            lac = aff.get("lacs_affected_count")
            out["hubsAffected"] = aff.get("hubs_affected_count")
            out["tacsLacsAffected"] = (
                (tac or 0) + (lac or 0)
                if (tac is not None or lac is not None) else None
            )
            out["citiesSpanned"] = len(cities) if isinstance(cities, (list, dict)) else None
        return out

    @staticmethod
    def _dash_remedy(data):
        if not isinstance(data, dict):
            return {}
        status = data.get("status")
        if isinstance(status, int):
            return {"remedyAffected": status}
        tickets = data.get("remedy_tickets", {})
        return {"remedyAffected": len(tickets) if isinstance(tickets, dict) else None}

    # -- RCA file extractors (unchanged) -------------------------------------
    @staticmethod
    def _extract_smartcare(system, data, ctx):
        processed = data.get("processed_data") if isinstance(data, dict) else None
        if processed is None or getattr(processed, "empty", True):
            return "smartcare_filter.csv", None
        return "smartcare_filter.csv", processed.to_csv(index=False)

    @staticmethod
    def _extract_df_csv(system, suffix="kpi"):
        def _extract(sysname, data, ctx):
            df = data
            if df is None or getattr(df, "empty", True):
                return f"{sysname}_{suffix}.csv", None
            return f"{sysname}_{suffix}.csv", df.to_csv(index=False)
        return _extract

    @staticmethod
    def _extract_splunk(system, data, ctx):
        # One file per core node (splunk_mme_kpi.csv, splunk_ggsn_kpi.csv, ...):
        # the RCA tool detects the node type (MME/SGSN/GGSN/DSC/DNS) from the
        # filename, so a single combined file was parsed entirely as SGSN.
        files = []
        for node, df in (data or {}).items():
            if df is None or getattr(df, "empty", True):
                continue
            files.append((f"splunk_{str(node).lower()}_kpi.csv", df.to_csv(index=False)))
        return files

    @staticmethod
    def _extract_remedy(system, data, ctx):
        import pandas as pd
        tickets = (data or {}).get("remedy_tickets", {}) if isinstance(data, dict) else {}
        rows = []
        for site, sdata in (tickets or {}).items():
            for ticket in sdata.get("tickets", []):
                ticket = dict(ticket)
                ticket["site"] = site
                rows.append(ticket)
        if not rows:
            return "remedy_kpi.csv", None
        return "remedy_kpi.csv", pd.DataFrame(rows).to_csv(index=False)


class RCAStatusAPIView(BaseCallFlowAPIView):
    """GET /callflow/api/analysis/<request_id>/rca-status -- readiness contract."""

    def get(self, request, request_id=None):
        if not request_id:
            return Response({"error": "request_id is required"},
                            status=http_status.HTTP_400_BAD_REQUEST)
        return Response(rf.rca_status(_output_dir(), request_id))


class RCAFilesAPIView(BaseCallFlowAPIView):
    """GET /callflow/api/analysis/<request_id>/rca-files -- RCA-tool file list."""

    def get(self, request, request_id=None):
        if not request_id:
            return Response({"error": "request_id is required"},
                            status=http_status.HTTP_400_BAD_REQUEST)
        return Response(rf.list_ready_files(_output_dir(), request_id))


class RCAFileDownloadAPIView(BaseCallFlowAPIView):
    """GET /callflow/api/analysis/<request_id>/file/<filename> -- request-scoped."""

    def get(self, request, request_id=None, filename=None):
        try:
            path = rf.resolve_file(_output_dir(), request_id, filename or "")
        except (ValueError, FileNotFoundError):
            raise Http404("file not found")
        return FileResponse(open(path, "rb"), as_attachment=True, filename=path.name)


