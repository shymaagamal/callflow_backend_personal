# callflow/core/rca_runner/collect.py
"""
Raw-data collection for the RCA run endpoints: retrieval + RCA input files ONLY.

The RCA endpoints do not run the CallFlow orchestrator plan. No system analysis
(NPP/Viavi/Remedy/Splunk analyzers), no threshold evaluation, no customer
counts, no summary cards. They call the existing retrieval functions, write
the raw results with the existing RCA file writers, and hand the files to the
RCA tool, which does all the analysis.

    MSISDN run (POST /rca/run)
        Smartcare   fetch_smartcare_nodes_data      (existing MSISDN query)   -> smartcare_filter.csv
        Splunk      splunkIntegration per core node                           -> splunk_<node>_kpi.csv
        Trino       fetch_cell_dataframe (existing query)                     -> trino_cells.csv
                      no cells -> the existing Smartcare cell-name fallback (cell_resolution)
        NPP         nppIntegration(cells)                                      -> npp_kpi.csv
        Viavi       viaviQueries(sites derived from the cells)                 -> viavi_kpi.csv
        Remedy      remedyTicketsData(sites derived from the cells)            -> remedy_kpi.csv

    Site run (POST /rca/sites/run)
        Smartcare   fetch_smartcare_site_kpis (SmartcareSiteKpiAPIView's code) -> smartcare_site_kpis.json
        Splunk      unchanged                                                  -> splunk_<node>_kpi.csv
        NPP         nppIntegration(sites, entity_type="site")                  -> npp_kpi.csv
        Viavi       viaviQueries(sites)                                        -> viavi_kpi.csv
        Remedy      remedyTicketsData(sites)                                   -> remedy_kpi.csv
        scope       the requested sites                                        -> smartcare_filter.csv
        (no Trino, no cell or site derivation)

Files are written by the collecting thread only after a retrieval has returned
inside its time limit, so a retrieval that times out can never write a file
later, while the RCA tool is reading this request's folder.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from django.conf import settings

from ..orchestration import cell_resolution as cr
from ..orchestration import rca_files as rf
from ..orchestration.pipeline import default_sources
from ..services.trino_service import cell_names_from_df, fetch_cell_dataframe
from ..views.orchestrated_analysis import AnalysisOrchestratorAPIView as _Writers
from . import site_mode

logger = logging.getLogger(__name__)

COMPLETED = "COMPLETED"   # data retrieved, RCA file written
NO_DATA = "NO_DATA"       # retrieval worked, nothing returned
FAILED = "FAILED"
TIMEOUT = "TIMEOUT"
SKIPPED = "SKIPPED"       # an input it needs (cells / sites) is missing
MISSING = (FAILED, TIMEOUT, SKIPPED)

MSISDN_SYSTEMS = ["trino", "smartcare", "npp", "splunk", "viavi", "remedy"]
SITE_SYSTEMS = ["smartcare", "npp", "splunk", "viavi", "remedy"]

SITE_KPI_FILE = "smartcare_site_kpis.json"


@dataclass
class Retrievers:
    """The existing retrieval functions (no analysis)."""
    trino: Callable[[Dict[str, Any]], Any]            # ctx -> Trino rows (DataFrame)
    smartcare: Callable[[Dict[str, Any]], Any]
    derive_sites: Callable[[List[str], Dict[str, Any]], List[str]]
    npp: Callable[[List[str], Dict[str, Any]], Any]
    viavi: Callable[[List[str], Dict[str, Any]], Any]
    remedy: Callable[[List[str], Dict[str, Any]], Any]
    splunk: Callable[[Dict[str, Any]], Any]


def default_retrievers(site_run: bool) -> Retrievers:
    src = default_sources()   # only its retrieval functions are used
    return Retrievers(
        trino=lambda ctx: fetch_cell_dataframe(ctx["msisdn_list"], ctx["start_dt"], ctx["end_dt"],
                                               sql_sink=ctx.get("_trino_sql_sink")),
        smartcare=site_mode.fetch_smartcare if site_run else src.fetch_smartcare,
        derive_sites=src.derive_sites,
        npp=src.retrieve_npp,
        viavi=src.retrieve_viavi,
        remedy=src.retrieve_remedy,
        splunk=src.retrieve_splunk,
    )


# ---------------------------------------------------------------- row counts --
def _df_rows(df):
    return 0 if df is None or getattr(df, "empty", True) else len(df)


def _remedy_rows(data):
    tickets = (data or {}).get("remedy_tickets") or {}
    return sum(len((v or {}).get("tickets") or []) for v in tickets.values())


def _splunk_rows(data):
    return sum(_df_rows(df) for df in (data or {}).values())


def _smartcare_rows(data):
    return _df_rows(data.get("processed_data")) if isinstance(data, dict) else 0


def _site_kpi_rows(data):
    windows = ((data or {}).get("site_kpis") or {}).get("windows") or {}
    return sum(len(v) for w in windows.values() for v in w.values() if isinstance(v, list))


def _timeout(name, default):
    return float(getattr(settings, name, default))


class _Collector:
    def __init__(self, base, request_id, ctx, log, on_change):
        self.base, self.rid, self.ctx, self.log = base, request_id, ctx, log
        self.on_change = on_change
        self.systems: Dict[str, Dict[str, Any]] = {}
        self.pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix=f"rca-{request_id[:8]}")
        self._handles = {}
        self._results = {}

    def _set(self, name, status, **info):
        self.systems[name] = {"status": status, **{k: v for k, v in info.items() if v is not None}}
        self.log.info("[RCA-COLLECT] request_id=%s system=%s status=%s %s", self.rid, name, status,
                      " ".join(f"{k}={v}" for k, v in info.items() if v is not None))
        self.on_change({k: v["status"] for k, v in self.systems.items()})

    def start(self, name, fn, *args, timeout):
        self._set(name, "RUNNING")
        rf.set_data_status(self.base, self.rid, name, rf.DATA_PROCESSING)
        self._handles[name] = (self.pool.submit(fn, *args), time.monotonic(), timeout)

    def skip(self, name, reason):
        rf.set_data_status(self.base, self.rid, name, rf.DATA_FAILED, reason)
        rf.mark_file_failed(self.base, self.rid, name, reason)
        self._set(name, SKIPPED, error=reason)

    def finish(self, name, rows, extract=None):
        """Wait for one retrieval (within its limit), write its RCA file, record status.
        extract=None: the system has no RCA file of its own. Returns the data or None."""
        if name in self._results:
            return self._results[name]
        fut, started, timeout = self._handles[name]
        data = None
        try:
            data = fut.result(timeout=max(0.0, started + timeout - time.monotonic()))
        except FutureTimeout:
            msg = f"no response within {timeout:.0f}s"
            rf.set_data_status(self.base, self.rid, name, rf.DATA_FAILED, msg)
            if extract is not None:
                rf.mark_file_failed(self.base, self.rid, name, msg)
            self._set(name, TIMEOUT, error=msg)
        except Exception as e:
            rf.set_data_status(self.base, self.rid, name, rf.DATA_FAILED, str(e))
            if extract is not None:
                rf.mark_file_failed(self.base, self.rid, name, str(e))
            self._set(name, FAILED, error=str(e) or type(e).__name__)
        else:
            rf.set_data_status(self.base, self.rid, name, rf.DATA_COMPLETED)
            n = rows(data)
            duration = round(time.monotonic() - started, 2)
            files = None
            if extract is not None:
                files = self._write(name, extract, data) if n else None
                if not n:
                    rf.mark_not_applicable(self.base, self.rid, name)
            self._set(name, COMPLETED if n else NO_DATA, rows=n, files=files, duration_s=duration)
        self._results[name] = data
        return data

    def _write(self, system, extract, data):
        result = extract(system, data, self.ctx)
        if isinstance(result, list):              # Splunk: one file per core node
            if result:
                rf.write_system_files(self.base, self.rid, system, result)
                return [f for f, _ in result]
            rf.mark_not_applicable(self.base, self.rid, system)
            return None
        filename, content = result
        if content is None:
            rf.mark_not_applicable(self.base, self.rid, system)
            return None
        rf.write_system_file(self.base, self.rid, system, filename, content)
        return [filename]

    def close(self):
        # Retrievals that timed out keep running in their thread, but nothing
        # they return is written: only finish() writes, and it has given up.
        self.pool.shutdown(wait=False)


def collect_inputs(ctx, base, request_id, retrievers: Retrievers, site_run: bool, log,
                   on_change: Optional[Callable[[Dict[str, str]], None]] = None):
    """Run the retrievals for one RCA request and write its RCA input files.

    Returns {"systems": {system: {status, rows?, files?, duration_s?, error?}},
             "cells": {...} (MSISDN run), "sites": [...]}.
    """
    on_change = on_change or (lambda s: None)
    t_db = _timeout("RCA_TRINO_TIMEOUT_S", 1800)        # pipeline.Timeouts defaults
    t_sc = _timeout("RCA_SMARTCARE_TIMEOUT_S", 1800)
    t_ret = _timeout("RCA_RETRIEVAL_TIMEOUT_S", 600)
    rf.init_request(base, request_id, SITE_SYSTEMS if site_run else MSISDN_SYSTEMS)
    c = _Collector(base, request_id, ctx, log, on_change)
    out = {}
    try:
        if site_run:
            sites = list(ctx["sitelist"])
            # The request's sites are the RCA scope (filter file); Smartcare's own
            # result is saved separately, its status reflects what it returned.
            rf.write_system_file(base, request_id, "smartcare", "smartcare_filter.csv",
                                 site_mode.filter_csv(sites))
            c.start("smartcare", retrievers.smartcare, ctx, timeout=t_sc)
            c.start("splunk", retrievers.splunk, ctx, timeout=t_ret)
            c.start("npp", retrievers.npp, sites, ctx, timeout=t_ret)
            c.start("viavi", retrievers.viavi, sites, ctx, timeout=t_ret)
            c.start("remedy", retrievers.remedy, sites, ctx, timeout=t_ret)
            sc = c.finish("smartcare", _site_kpi_rows)
            if sc is not None:
                _save_json(rf.request_dir(base, request_id) / SITE_KPI_FILE, sc.get("site_kpis"))
                note = (sc.get("site_kpis") or {}).get("error")
                if note and c.systems["smartcare"]["status"] == NO_DATA:
                    c.systems["smartcare"]["note"] = note
            out["sites"] = sites
        else:
            c.start("smartcare", retrievers.smartcare, ctx, timeout=t_sc)
            c.start("splunk", retrievers.splunk, ctx, timeout=t_ret)
            c.start("trino", retrievers.trino, ctx, timeout=t_db)
            frame = c.finish("trino", _df_rows, _Writers._extract_df_csv("trino", "cells"))
            cells = cell_names_from_df(frame) if frame is not None else []
            resolution = cr.resolve_cells(cells)
            if not resolution.resolved and c.systems["trino"]["status"] == NO_DATA:
                # Existing behaviour: Trino found no cells -> Smartcare's cell column.
                sc = c.finish("smartcare", _smartcare_rows, _Writers._extract_smartcare)
                available, has_col, sc_cells = cr.smartcare_cell_facts(sc)
                resolution = cr.resolve_cells([], smartcare_available=available,
                                              smartcare_has_column=has_col, smartcare_cells=sc_cells)
            out["cells"] = {"source": resolution.source, "count": len(resolution.cells),
                            "message": resolution.message}
            log.info("[RCA-COLLECT] request_id=%s cells: %s", request_id, resolution.message)
            cells = resolution.cells
            sites = retrievers.derive_sites(cells, ctx) if cells else []
            out["sites"] = sites
            if cells:
                c.start("npp", retrievers.npp, cells, ctx, timeout=t_ret)
            else:
                c.skip("npp", f"no cells: {resolution.message}")
            for name in ("viavi", "remedy"):
                if sites:
                    c.start(name, getattr(retrievers, name), sites, ctx, timeout=t_ret)
                else:
                    c.skip(name, "no sites (no cells resolved)")
            c.finish("smartcare", _smartcare_rows, _Writers._extract_smartcare)

        c.finish("splunk", _splunk_rows, _Writers._extract_splunk)
        for name, rows, extract in (("npp", _df_rows, _Writers._extract_df_csv("npp")),
                                    ("viavi", _df_rows, _Writers._extract_df_csv("viavi")),
                                    ("remedy", _remedy_rows, _Writers._extract_remedy)):
            if name in c._handles:
                c.finish(name, rows, extract)
    finally:
        c.close()
    out["systems"] = c.systems
    return out


def _save_json(path, data):
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)
