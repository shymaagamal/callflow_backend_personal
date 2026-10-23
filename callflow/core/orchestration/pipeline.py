
"""
CallFlow analysis pipeline expressed as a data-availability DAG.

This encodes the verified dependency graph from ANALYSIS.md onto the generic
scheduler:

    trino_cells ─┬─> npp_retrieval ──────────────────> npp_analysis
    (+ customer  └─> sites ─┬─> viavi_retrieval ─────> viavi_analysis
       counts)              └─> remedy_retrieval ────> remedy_analysis
    splunk_retrieval ──┐
    smartcare ─────────┴─────────────────────────────> splunk_analysis

Key properties this buys us (contrast with today's Smartcare-first flow):
  * retrieval for NPP/Viavi/Remedy/Splunk no longer waits for Smartcare;
  * NPP/Viavi/Remedy analysis do not wait for Smartcare either: their customer
    counts come from the Trino cell rows (see customer_counts.py);
  * Smartcare only gates Splunk analysis (per-core-node subscriber counts need
    Smartcare's node columns) and its own card/RCA file;
  * a slow/failed system is isolated -- its analysis is skipped, others proceed.

The concrete data sources are injected via ``DataSources`` so the DAG wiring is
unit-testable without a live JVM/Trino/Oracle/Splunk. ``default_sources()`` lazily
binds the real CallFlow service functions (no heavy imports at module load).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .scheduler import Task, PlanResult, run_plan
from ..services.trino_service import (
fetch_cell_dataframe, cell_names_from_df, customer_counts_from_df,
)
from .customer_counts import CTX_KEY
from ..services.smartcare_service import fetch_smartcare_nodes_data
import pandas as pd
from ..utils.data_utils import process_sitename
from ..services.npp_service import nppIntegration
from ..services.viavi_service import viaviQueries
from ..utils.connections import viaviConnection
from ..services.remedy_service import remedyTicketsData
from ..utils.classes import Connection
from ..constants.general_constants import (
SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.conf import settings
from ..services.splunk_service import splunkIntegration
from .analyzers import NppAnalyzer
from .analyzers import ViaviAnalyzer
from .analyzers import RemedyAnalyzer
from .analyzers import SplunkAnalyzer
from .analyzers._pandas import npp_series_provider

logger = logging.getLogger(__name__)

# The core-node list Splunk retrieval iterates (matches abnormal_kpis.py).
DEFAULT_SPLUNK_NODES = ["SGSN", "MME", "GGSN", "DSC", "DNS"]

# Task names (stable identifiers used in progress events and result lookup).
T_TRINO = "trino_cells"
T_SMARTCARE = "smartcare"
T_SITES = "sites"
T_NPP_GET = "npp_retrieval"
T_VIAVI_GET = "viavi_retrieval"
T_REMEDY_GET = "remedy_retrieval"
T_SPLUNK_GET = "splunk_retrieval"
T_NPP_AN = "npp_analysis"
T_VIAVI_AN = "viavi_analysis"
T_REMEDY_AN = "remedy_analysis"
T_SPLUNK_AN = "splunk_analysis"


# Maps each scheduler task to (system, phase) so consumers can roll task-level
# events up into per-system status without re-deriving the DAG.
TASK_SYSTEM = {
    T_TRINO: ("trino", "retrieval"),
    T_SITES: ("trino", "derive"),
    T_SMARTCARE: ("smartcare", "retrieval"),
    T_NPP_GET: ("npp", "retrieval"),
    T_NPP_AN: ("npp", "analysis"),
    T_VIAVI_GET: ("viavi", "retrieval"),
    T_VIAVI_AN: ("viavi", "analysis"),
    T_REMEDY_GET: ("remedy", "retrieval"),
    T_REMEDY_AN: ("remedy", "analysis"),
    T_SPLUNK_GET: ("splunk", "retrieval"),
    T_SPLUNK_AN: ("splunk", "analysis"),
}

# Systems reported to the frontend, in display order.
REPORTED_SYSTEMS = ["trino", "smartcare", "npp", "splunk", "viavi", "remedy"]
# Systems whose analysis defines COMPLETED/PARTIAL_SUCCESS/FAILED (section 22).
ANALYSIS_SYSTEMS = ["npp", "splunk", "viavi", "remedy"]
# Systems that produce an RCA file.
FILE_SYSTEMS = ["trino", "smartcare", "npp", "splunk", "viavi", "remedy"]

# Per-request context key holding the raw Trino cell DataFrame, so the view can
# write it as this request's RCA file (same pattern as customer_counts.CTX_KEY).
TRINO_FRAME_KEY = "_trino_frame"

# Static dependency map (mirrors build_callflow_plan) so consumers can log what a
# task waited for without re-deriving the DAG.
TASK_DEPS = {
    T_TRINO: [],
    T_SMARTCARE: [],
    T_SITES: [T_TRINO],
    T_NPP_GET: [T_TRINO],
    T_VIAVI_GET: [T_SITES],
    T_REMEDY_GET: [T_SITES],
    T_SPLUNK_GET: [],
    T_NPP_AN: [T_NPP_GET],
    T_VIAVI_AN: [T_VIAVI_GET],
    T_REMEDY_AN: [T_REMEDY_GET],
    T_SPLUNK_AN: [T_SPLUNK_GET, T_SMARTCARE],
}

# Per-system task groups, for rolling up status.
SYSTEM_TASKS = {
    "trino": [T_TRINO, T_SITES],
    "smartcare": [T_SMARTCARE],
    "npp": [T_NPP_GET, T_NPP_AN],
    "splunk": [T_SPLUNK_GET, T_SPLUNK_AN],
    "viavi": [T_VIAVI_GET, T_VIAVI_AN],
    "remedy": [T_REMEDY_GET, T_REMEDY_AN],
}


@dataclass
class DataSources:
    """Callables the pipeline needs. Signatures keep tasks thin and testable."""
    get_cells: Callable[[Dict[str, Any]], List[str]]
    fetch_smartcare: Callable[[Dict[str, Any]], Any]
    derive_sites: Callable[[List[str], Dict[str, Any]], List[str]]
    retrieve_npp: Callable[[List[str], Dict[str, Any]], Any]
    retrieve_viavi: Callable[[List[str], Dict[str, Any]], Any]
    retrieve_remedy: Callable[[List[str], Dict[str, Any]], Any]
    retrieve_splunk: Callable[[Dict[str, Any]], Any]
    analyze_npp: Callable[[Any, Any, Dict[str, Any]], Any]
    analyze_viavi: Callable[[Any, Any, Dict[str, Any]], Any]
    analyze_remedy: Callable[[Any, Any, Dict[str, Any]], Any]
    analyze_splunk: Callable[[Any, Any, Dict[str, Any]], Any]


# Frontend-facing per-system states (section 6 -- kept minimal & truthful).
SYS_PENDING = "PENDING"
SYS_PROCESSING = "PROCESSING"
SYS_COMPLETED = "COMPLETED"
SYS_FAILED = "FAILED"
SYS_PARTIAL = "PARTIAL"  # data retrieved but analysis unavailable

_TERMINAL_FAIL = {"FAILED", "TIMEOUT"}


def _rollup_system(system: str, task_status: Dict[str, str]) -> str:
    """Roll a system's task statuses up into one truthful frontend status."""
    tasks = SYSTEM_TASKS[system]

    if system == "trino":
        sts = [task_status.get(t, "PENDING") for t in tasks]
        if any(s in _TERMINAL_FAIL or s == "SKIPPED" for s in sts):
            return SYS_FAILED
        if all(s == "SUCCESS" for s in sts):
            return SYS_COMPLETED
        if any(s == "RUNNING" for s in sts) or any(s == "SUCCESS" for s in sts):
            return SYS_PROCESSING
        return SYS_PENDING

    if system == "smartcare":
        s = task_status.get(tasks[0], "PENDING")
        if s in _TERMINAL_FAIL:
            return SYS_FAILED
        if s == "SUCCESS":
            return SYS_COMPLETED
        if s == "RUNNING":
            return SYS_PROCESSING
        return SYS_PENDING

    # npp / splunk / viavi / remedy: retrieval + analysis
    retrieval, analysis = tasks[0], tasks[1]
    r = task_status.get(retrieval, "PENDING")
    a = task_status.get(analysis, "PENDING")

    if r in _TERMINAL_FAIL or r == "SKIPPED":
        return SYS_FAILED  # no data could be retrieved
    if a == "SUCCESS":
        return SYS_COMPLETED
    if a in _TERMINAL_FAIL:
        return SYS_FAILED
    if a == "SKIPPED":
        # data retrieved, but analysis could not run (e.g. Smartcare failed)
        return SYS_PARTIAL
    if r == "RUNNING" or a == "RUNNING" or r == "SUCCESS":
        return SYS_PROCESSING
    return SYS_PENDING


def rollup_all(task_status: Dict[str, str]) -> Dict[str, str]:
    """Map raw scheduler task statuses to per-system frontend statuses."""
    return {s: _rollup_system(s, task_status) for s in REPORTED_SYSTEMS}


def classify_overall(system_status: Dict[str, str]) -> str:
    """COMPLETED / PARTIAL_SUCCESS / FAILED for the whole job (section 22)."""
    analysis = [system_status.get(s, SYS_PENDING) for s in ANALYSIS_SYSTEMS]
    completed = [s for s in analysis if s == SYS_COMPLETED]
    failed = [s for s in analysis if s in (SYS_FAILED, SYS_PARTIAL)]
    if completed and not failed:
        return "COMPLETED"
    if completed and failed:
        return "PARTIAL_SUCCESS"
    return "FAILED"


@dataclass
class Timeouts:
    """Per-stage application-level timeouts (seconds). None disables."""
    trino: Optional[float] = 1800
    smartcare: Optional[float] = 1800
    retrieval: Optional[float] = 600
    analysis: Optional[float] = 300


def build_callflow_plan(sources: DataSources, timeouts: Optional[Timeouts] = None) -> List[Task]:
    """Return the CallFlow task DAG (does not execute it)."""
    to = timeouts or Timeouts()

    return [
        # ---- Retrieval layer: gated only on real data inputs, NOT Smartcare ----
        Task(T_TRINO, lambda ctx, d: sources.get_cells(ctx),
             deps=[], timeout=to.trino, label="Trino cell-name retrieval"),

        Task(T_SMARTCARE, lambda ctx, d: sources.fetch_smartcare(ctx),
             deps=[], timeout=to.smartcare, label="Smartcare retrieval"),

        Task(T_SITES, lambda ctx, d: sources.derive_sites(d[T_TRINO], ctx),
             deps=[T_TRINO], timeout=to.trino, label="Derive site names from cells"),

        Task(T_NPP_GET, lambda ctx, d: sources.retrieve_npp(d[T_TRINO], ctx),
             deps=[T_TRINO], timeout=to.retrieval, label="NPP data retrieval"),

        Task(T_VIAVI_GET, lambda ctx, d: sources.retrieve_viavi(d[T_SITES], ctx),
             deps=[T_SITES], timeout=to.retrieval, label="Viavi data retrieval"),

        Task(T_REMEDY_GET, lambda ctx, d: sources.retrieve_remedy(d[T_SITES], ctx),
             deps=[T_SITES], timeout=to.retrieval, label="Remedy data retrieval"),

        Task(T_SPLUNK_GET, lambda ctx, d: sources.retrieve_splunk(ctx),
             deps=[], timeout=to.retrieval, label="Splunk data retrieval"),

        # ---- Analysis layer ----------------------------------------------------
        # NPP / Viavi / Remedy wait only for their own retrieval. Their customer
        # counts come from the Trino task (ctx, see customer_counts.py), which
        # every one of them already depends on transitively. The analyze
        # signatures are unchanged; smartcare is passed as None.
        Task(T_NPP_AN,
             lambda ctx, d: sources.analyze_npp(d[T_NPP_GET], None, ctx),
             deps=[T_NPP_GET], timeout=to.analysis, label="NPP analysis"),

        Task(T_VIAVI_AN,
             lambda ctx, d: sources.analyze_viavi(d[T_VIAVI_GET], None, ctx),
             deps=[T_VIAVI_GET], timeout=to.analysis, label="Viavi analysis"),

        Task(T_REMEDY_AN,
             lambda ctx, d: sources.analyze_remedy(d[T_REMEDY_GET], None, ctx),
             deps=[T_REMEDY_GET], timeout=to.analysis, label="Remedy analysis"),

        # Splunk still needs Smartcare: per-core-node subscriber counts use
        # Smartcare's node-name columns, which Trino does not have.

        Task(T_SPLUNK_AN,
             lambda ctx, d: sources.analyze_splunk(d[T_SPLUNK_GET], d[T_SMARTCARE], ctx),
             deps=[T_SPLUNK_GET, T_SMARTCARE], timeout=to.analysis, label="Splunk analysis"),
    ]


def run_callflow_analysis(
    context: Dict[str, Any],
    sources: Optional[DataSources] = None,
    timeouts: Optional[Timeouts] = None,
    max_workers: int = 12,
    progress_cb: Optional[Callable[[int, int, str, str], None]] = None,
    on_start: Optional[Callable[[str], None]] = None,
    logger_override: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Execute the CallFlow analysis DAG and assemble the per-system insights.

    ``context`` must carry the request inputs the sources need, e.g.:
        msisdn_list, start_dt, end_dt, complain_type, imsi, log_path, ts,
        query_time_range, splunk_nodes.

    Returns a dict with per-system analysis payloads (or an error per system),
    plus timing/diagnostics. Existing endpoints are unaffected; this is an
    additive orchestration entry point.
    """
    sources = sources or default_sources()
    plan = build_callflow_plan(sources, timeouts)
    logger_override.info(f"[orchestrator] running CallFlow plan with {len(plan)} tasks and plan is {plan}")
    result: PlanResult = run_plan(
        plan, context=context, max_workers=max_workers,
        progress_cb=progress_cb, on_start=on_start,
        logger_override=logger_override,
    )

    def payload(analysis_task: str) -> Dict[str, Any]:
        oc = result.outcomes[analysis_task]
        return {
            "status": "success" if oc.ok else oc.status.value.lower(),
            "data": oc.result if oc.ok else None,
            "error": oc.error,
            "duration_seconds": oc.duration,
        }

    return {
        "npp": payload(T_NPP_AN),
        "viavi": payload(T_VIAVI_AN),
        "remedy": payload(T_REMEDY_AN),
        "splunk": payload(T_SPLUNK_AN),
        "diagnostics": {
            "wall_time_seconds": result.wall_time,
            "succeeded": result.succeeded(),
            "failed": result.failed(),
            "skipped": result.skipped(),
            "errors": result.errors(),
            "timings": {
                name: oc.duration for name, oc in result.outcomes.items()
            },
        },
    }


# ---------------------------------------------------------------------------
# Default wiring onto the real CallFlow services (lazy imports).
# ---------------------------------------------------------------------------

def default_sources() -> DataSources:
    """Bind the DAG to the existing service functions. Imports are deferred so
    importing this module (e.g. for tests with fake sources) never drags in
    Django/pandas/JVM."""

    def get_cells(ctx):

        # ctx may carry a sink to log the final Trino SQL (request-scoped).
        df = fetch_cell_dataframe(
            ctx["msisdn_list"], ctx["start_dt"], ctx["end_dt"],
            sql_sink=ctx.get("_trino_sql_sink"),
        )
        ctx[TRINO_FRAME_KEY] = df  # raw rows for the request-scoped RCA file
        # Customer counts from the SAME Trino rows (no extra query). Written
        # before this task returns, so every downstream analysis sees it.
        # A counting failure must never lose the cell list.
        try:
            ctx[CTX_KEY] = customer_counts_from_df(df, ctx.get("log_path") or os.devnull)
        except Exception:
            logger.warning("[orchestrator] Trino customer counts failed", exc_info=True)
            ctx[CTX_KEY] = None
        return cell_names_from_df(df)

    def fetch_smartcare(ctx):
        return fetch_smartcare_nodes_data(
            ctx["msisdn_list"], ctx["start_dt"], ctx["end_dt"],
            ctx.get("imsi", ""), ctx.get("complain_type", "Data"),
            ctx.get("log_path", ""), "orchestrator", ctx.get("ts", ""),
        )

    def derive_sites(cells, ctx):

        if not cells:
            return []
        df = pd.DataFrame({"cell_name": list(cells)})
        df = process_sitename(df, "cell_name", ctx.get("log_path", ""))
        if "modified_site_name" not in df.columns:
            return []
        return [
            s for s in df["modified_site_name"].dropna().astype(str).unique().tolist()
            if s
        ]

    def _npp_fetch(cells, start, end, ctx, stats=None):
        return nppIntegration(
            cells, start_date=start, end_date=end,
            logs_file=ctx.get("log_path", ""), timestamp=ctx.get("ts", ""),
            resolution=ctx.get("npp_resolution", "15min"), stats=stats,
            # "site" for the RCA site run (NPP site endpoint); cells otherwise.
            entity_type=ctx.get("npp_entity_type", "cell"),
        )

    def retrieve_npp(cells, ctx):
        # NPP is one HTTP request per cell per hour. KPIAnalyzer.analyze_npp only
        # reads the current window and the same window 1 day earlier, so fetch
        # just those hours (~8 per cell) instead of the whole 28h span
        # (query_start_1day..query_end_1day) -- that span is only used to draw
        # trend charts for AFFECTED cells, fetched in analyze_npp below.
        qtr = ctx["query_time_range"]
        rlog = ctx.get("request_logger", logger)
        rid = ctx.get("request_id")
        windows = {
            "current": (qtr["current_start"], qtr["current_end"]),
            "prev_1day": (qtr["prev_start_1day"], qtr["prev_end_1day"]),
        }
        stats = {name: {} for name in windows}
        frames, errors = [], {}
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=len(windows)) as ex:
            futs = {ex.submit(_npp_fetch, cells, a, b, ctx, stats[name]): name
                    for name, (a, b) in windows.items()}
            for fut in as_completed(futs):
                name = futs[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        frames.append((name, df))
                except Exception as e:  # one window failing must not lose the other
                    errors[name] = str(e)
        for name in windows:
            rlog.info("[NPP] request_id=%s window=%s %s->%s cells=%d stats=%s error=%s",
                      rid, name, windows[name][0], windows[name][1], len(cells),
                      stats[name], errors.get(name))
        if len(errors) == len(windows):
            # same semantics as before: fail only when NOTHING could be fetched
            raise RuntimeError(f"NPP retrieval failed for all windows: {errors}")
        frames = [df for name in windows for (n, df) in frames if n == name]  # stable order
        # windows are 1 day apart; drop_duplicates only matters if they ever overlap
        out = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
        rlog.info("[NPP] request_id=%s retrieval done cells=%d rows=%d duration=%.1fs",
                  rid, len(cells), len(out), time.time() - t0)
        return out

    def _npp_chart_series(npp_raw, ctx):
        """Trend-chart provider for the NPP abnormal-KPI popup. Charts span
        query_start_1day..query_end_1day; retrieval fetched only the analysis
        windows, so fetch the full span once per CHARTED (affected) cell, within
        a time budget. Past the budget (or on error) the chart uses the
        analysis-window data instead of failing the analysis."""
        qtr = ctx["query_time_range"]
        rlog = ctx.get("request_logger", logger)
        rid = ctx.get("request_id")
        budget_s = getattr(settings, "NPP_CHART_FETCH_BUDGET_S", 120)
        deadline = time.time() + budget_s
        fallback = npp_series_provider(npp_raw, ctx)
        per_cell = {}

        def provider(cell, kpi):
            if cell not in per_cell:
                if time.time() >= deadline:
                    rlog.info("[NPP] request_id=%s chart cell=%s full-range fetch skipped "
                              "(budget %ss used); chart shows analysis windows", rid, cell, budget_s)
                    per_cell[cell] = fallback
                else:
                    st = {}
                    try:
                        df = _npp_fetch([cell], qtr["query_start_1day"], qtr["query_end_1day"], ctx, st)
                        per_cell[cell] = npp_series_provider(df, ctx) if df is not None and not df.empty else fallback
                    except Exception as e:
                        rlog.warning("[NPP] request_id=%s chart cell=%s full-range fetch failed: %s", rid, cell, e)
                        per_cell[cell] = fallback
                    rlog.info("[NPP] request_id=%s chart cell=%s stats=%s", rid, cell, st)
            return per_cell[cell](cell, kpi)
        return provider

    def retrieve_viavi(sites, ctx):
   
        # Stash sites so the analysis stage (which needs the requested site list)
        # can read them; only this task writes the key and analysis depends on it.
        ctx["_viavi_sites"] = sites
        conn = viaviConnection(ctx.get("log_path", ""))
        if not conn:
            raise RuntimeError("Viavi database connection failed")
        return viaviQueries(
            conn, ctx["window"]["formatted_startDate"],
            ctx["window"]["formatted_endDate"], sites,
            ctx.get("log_path", ""), ctx.get("ts", ""),
        )

    def retrieve_remedy(sites, ctx):
     
    
        conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
                          DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
        conn.connect()
        remedy_output, status_count = remedyTicketsData(
            conn, ctx["window"]["start_dt"], ctx["window"]["end_dt"],
            sites, ctx.get("log_path", ""), ctx.get("ts", ""),
        )
        return {"remedy_tickets": remedy_output, "status": status_count}

    def retrieve_splunk(ctx):

        qtr = ctx["query_time_range"]
        nodes = ctx.get("splunk_nodes", DEFAULT_SPLUNK_NODES)
        request_logger = ctx.get("request_logger", logger)

        def one(node):
            return node, splunkIntegration(
                settings.SPLUNK_USER, settings.SPLUNK_PASSWORD,
                qtr["query_start_1day_str"], qtr["query_end_1day_str"],
                ctx.get("log_path", ""), node, request_logger,
            )

        out = {}
        with ThreadPoolExecutor(max_workers=len(nodes)) as ex:
            for fut in as_completed([ex.submit(one, n) for n in nodes]):
                try:
                    node, df = fut.result()
                    out[node] = df
                except Exception as e:  # per-node isolation
                    request_logger.error(f"[orchestrator] splunk node failed: {e}")
        return out

    # Analysis tasks delegate to the per-system analyzers, which hold the
    # (unchanged) business logic and return the common result contract. The
    # pipeline only expresses the dependency; it contains no KPI math.
    def analyze_npp(npp_raw, smartcare, ctx):
        return NppAnalyzer(series_provider=_npp_chart_series).analyze(npp_raw, smartcare, ctx)

    def analyze_viavi(viavi_raw, smartcare, ctx):
        return ViaviAnalyzer().analyze(viavi_raw, smartcare, ctx)

    def analyze_remedy(remedy_raw, smartcare, ctx):
        return RemedyAnalyzer().analyze(remedy_raw, smartcare, ctx)

    def analyze_splunk(splunk_raw, smartcare, ctx):
        return SplunkAnalyzer().analyze(splunk_raw, smartcare, ctx)

    return DataSources(
        get_cells=get_cells,
        fetch_smartcare=fetch_smartcare,
        derive_sites=derive_sites,
        retrieve_npp=retrieve_npp,
        retrieve_viavi=retrieve_viavi,
        retrieve_remedy=retrieve_remedy,
        retrieve_splunk=retrieve_splunk,
        analyze_npp=analyze_npp,
        analyze_viavi=analyze_viavi,
        analyze_remedy=analyze_remedy,
        analyze_splunk=analyze_splunk,
    )


def _smartcare_processed(smartcare):
    if isinstance(smartcare, dict):
        return smartcare.get("processed_data")
    return None


def _smartcare_metrics(smartcare):
    if isinstance(smartcare, dict):
        return smartcare.get("metrics", {})
    return {}
