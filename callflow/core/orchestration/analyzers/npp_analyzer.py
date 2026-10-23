"""
NPP analyzer — preserves the old NPP business logic:

  * KPI/threshold evaluation -> affected cells      (KPIAnalyzer.analyze_npp)
  * hub / TAC / LAC / region enrichment             (npp_service.calculate_affected_hub_taclac_metrics)
  * impacted customers + degraded-KPI chart rows    (dashboard_shaping.shape_npp_abnormal)

Data dependency: the hub/TAC/LAC enrichment and per-cell impacted customers only
need unique MSISDNs per cell. Those come from the Trino cell rows (computed by
the Trino task, see customer_counts.py), so NPP analysis does NOT wait for
Smartcare. On the Trino-empty fallback the cells (and counts) come from
Smartcare, which the Trino task already waited for.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from .base import build_result, compute_state
from .severity import filter_entities_by_severity, SIGNIFICANT_SEVERITIES
from .. import dashboard_shaping as ds
from ..customer_counts import resolve as resolve_customer_counts
from ...services.kpi_analyzer import KPIAnalyzer
from ...services.npp_service import calculate_affected_hub_taclac_metrics
from ._pandas import npp_series_provider
from ..task_logging import StageLog

logger = logging.getLogger(__name__)


class NppAnalyzer:
    system = "npp"

    def __init__(self, *, analyze_fn: Optional[Callable] = None,
                 enrich_fn: Optional[Callable] = None,
                 series_provider: Optional[Callable] = None,
                 log=None):
        self._analyze_fn = analyze_fn
        self._enrich_fn = enrich_fn
        self._series_provider = series_provider
        self.log = log or logger

    # -- delegation to the existing (unchanged) business logic ---------------
    def _analyze(self, npp_raw, ctx) -> Dict[str, Any]:
        if self._analyze_fn is not None:
            return self._analyze_fn(npp_raw, ctx)
        qtr = ctx["query_time_range"]
        return KPIAnalyzer().analyze_npp(
            npp_raw, ctx.get("analyzer_freq", "1H"),
            qtr["current_start"], qtr["current_end"],
            qtr["prev_start_1day"], qtr["prev_end_1day"], ctx.get("ts", ""),"site"
        )

    def _enrich(self, smartcare, affected_list, ctx, counts=None) -> Dict[str, Any]:
        if self._enrich_fn is not None:
            return self._enrich_fn(smartcare, affected_list, ctx)
        # MSISDN + cell_name rows from the customer-count source (Trino rows, or
        # Smartcare processed_data on the Trino-empty fallback). The enrichment
        # function itself is unchanged.
        if counts is None:
            counts = resolve_customer_counts(smartcare, ctx)
        processed = counts.nodes_frame
        if processed is None or getattr(processed, "empty", True):
            return {}
        return calculate_affected_hub_taclac_metrics(processed, affected_list, ctx.get("ts", ""))

    def _series(self, npp_raw, ctx):
        if self._series_provider is not None:
            return self._series_provider(npp_raw, ctx)
        return npp_series_provider(npp_raw, ctx)

    @staticmethod
    def _retrieved_count(npp_raw):
        try:
            if npp_raw is None or getattr(npp_raw, "empty", True):
                return 0
            col = "cell" if "cell" in npp_raw.columns else ("Cell" if "Cell" in npp_raw.columns else None)
            if col is None:
                return None
            return int(npp_raw[col].dropna().nunique())
        except Exception:
            return None

    def analyze(self, npp_raw, smartcare, ctx) -> Dict[str, Any]:
        slog = StageLog(ctx.get("request_logger") or self.log,
                        ctx.get("request_id"), self.system)
        total = self._retrieved_count(npp_raw)
        slog.note("start NPP analysis", marker="START", records=total)

        with slog.stage("evaluating KPI thresholds") as s:
            metrics = self._analyze(npp_raw, ctx) or {}
            raw_details = metrics.get("affected_cells_details", {}) or {}
            s.counts(cells=total, affected_raw=len(raw_details))

        # SEVERITY GATE: keep only High + Medium cells before the expensive
        # enrichment / time-series / abnormal shaping (skip low/minor/none).
        significant = ctx.get("significant_severities", SIGNIFICANT_SEVERITIES)
        with slog.stage("severity gate (High+Medium)") as s:
            details, severity_counts = filter_entities_by_severity(raw_details, significant)
            affected_list = list(details.keys())
            affected_count = len(affected_list)
            s.counts(affected=affected_count, dropped=len(raw_details) - affected_count)

        counts = resolve_customer_counts(smartcare, ctx)
        enrichment = {}
        with slog.stage(f"Hub/TAC/LAC enrichment (source={counts.source})") as s:
            try:
                enrichment = self._enrich(smartcare, affected_list, ctx, counts=counts) or {}
                s.counts(matched_sites=enrichment.get("matched_sites_count"))
            except Exception as e:  # enrichment failure must not lose the KPI result
                self.log.warning("NPP enrichment failed: %s", e)

        with slog.stage(f"customer counts per affected cell (source={counts.source})") as s:
            cpc = counts.customers_per_cell
            matched = 0
            # Annotate each affected cell with its customer count (N/A when unknown).
            for cell, info in details.items():
                val = cpc.get(str(cell))
                info["impacted_customers"] = val
                if val is not None:
                    matched += 1
            s.counts(matched=matched, unmatched=affected_count - matched)

        abnormal = []
        with slog.stage("building abnormal-KPI chart rows") as s:
            try:
                abnormal = ds.shape_npp_abnormal(details, cpc, kpi_series=self._series(npp_raw, ctx))
                s.counts(abnormal=len(abnormal))
            except Exception as e:
                self.log.warning("NPP abnormal shaping failed: %s", e)

        analysis_run = total is not None
        entities_analyzed = total or 0
        state, message = compute_state(analysis_run, entities_analyzed, affected_count)
        slog.note("NPP analysis complete", marker="COMPLETE",
                  cells=total, affected=affected_count, abnormal=len(abnormal), state=state)
        return build_result(
            self.system,
            state=state, message=message, analysis_run=analysis_run,
            summary={
                "cells_affected_count": affected_count,
                "total_cells": total,
                "entities_analyzed": entities_analyzed,
                "affected_count": affected_count,
                "has_abnormal_kpis": affected_count > 0,
                "all_clean": analysis_run and entities_analyzed > 0 and affected_count == 0,
                "severity_counts": severity_counts,
            },
            metrics={"affected_metrics": enrichment},
            affected={"cells_list": affected_list},
            details={"affected_cells_details": details},
            abnormal_kpis=abnormal,
            insights=[message],
        )
