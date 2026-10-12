"""
Viavi / Transmission analyzer — preserves the old site-level business logic:

  * KPI/threshold evaluation -> affected sites   (KPIAnalyzer.analyze_viavi)
  * impacted customers + degraded-KPI chart rows (dashboard_shaping.shape_viavi_abnormal)

Data dependency: the KPI/threshold step needs only Viavi raw data + the site
list; the impacted-customers enrichment needs unique MSISDNs per cell, which
come from the Trino cell rows (customer_counts.py). Viavi analysis does NOT wait
for Smartcare.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from .base import build_result, compute_state
from .severity import filter_entities_by_severity, SIGNIFICANT_SEVERITIES
from .. import dashboard_shaping as ds
from ...services.kpi_analyzer import KPIAnalyzer
from ...constants.endpoints import VIAVIE_KPIS_MAPPING
from ._pandas import viavi_series_provider
from ..task_logging import StageLog
from ..customer_counts import resolve as resolve_customer_counts

logger = logging.getLogger(__name__)


class ViaviAnalyzer:
    system = "viavi"

    def __init__(self, *, analyze_fn: Optional[Callable] = None,
                 name_map_provider: Optional[Callable] = None,
                 series_provider: Optional[Callable] = None,
                 log=None):
        self._analyze_fn = analyze_fn
        self._name_map_provider = name_map_provider
        self._series_provider = series_provider
        self.log = log or logger

    def _analyze(self, viavi_raw, ctx) -> Dict[str, Any]:
        if self._analyze_fn is not None:
            return self._analyze_fn(viavi_raw, ctx)
        qtr = ctx["query_time_range"]
        sites = ctx.get("_viavi_sites", [])
        return KPIAnalyzer().analyze_viavi(
            viavi_raw, sites,
            qtr["current_start"], qtr["current_end"],
            qtr["prev_start_1day"], qtr["prev_end_1day"], ctx.get("ts", ""),
        )

    def _name_map(self):
        if self._name_map_provider is not None:
            return self._name_map_provider()
        try:
            return dict(VIAVIE_KPIS_MAPPING)
        except Exception:
            return {}

    def _series(self, viavi_raw, ctx):
        if self._series_provider is not None:
            return self._series_provider(viavi_raw, ctx)
        return viavi_series_provider(viavi_raw, ctx)

    @staticmethod
    def _analyzed_sites(viavi_raw, ctx):
        """Sites actually analyzed = requested sites (ctx) or unique FLWIDs in raw."""
        sites = ctx.get("_viavi_sites")
        if sites:
            return list(sites)
        try:
            if viavi_raw is not None and not getattr(viavi_raw, "empty", True) and "FLWID" in viavi_raw.columns:
                return list(viavi_raw["FLWID"].dropna().unique())
        except Exception:
            pass
        return []

    def analyze(self, viavi_raw, smartcare, ctx) -> Dict[str, Any]:
        slog = StageLog(ctx.get("request_logger") or self.log,
                        ctx.get("request_id"), self.system)
        analyzed_sites = self._analyzed_sites(viavi_raw, ctx)
        slog.note("start Viavi analysis", marker="START", sites=len(analyzed_sites))

        with slog.stage("evaluating KPI thresholds (site-level)") as s:
            result = self._analyze(viavi_raw, ctx) or {}
            raw_details = result.get("affected_sites_details", {}) or {}
            s.counts(sites=len(analyzed_sites), affected_raw=len(raw_details))

        # SEVERITY GATE: keep only High + Medium affected sites.
        significant = ctx.get("significant_severities", SIGNIFICANT_SEVERITIES)
        with slog.stage("severity gate (High+Medium)") as s:
            details, severity_counts = filter_entities_by_severity(raw_details, significant)
            sites_list = list(details.keys())
            count = len(sites_list)
            s.counts(affected=count, dropped=len(raw_details) - count)

        abnormal = []
        with slog.stage("building abnormal-KPI chart rows (customer counts)") as s:
            try:
                counts = resolve_customer_counts(smartcare, ctx)
                cpc = counts.customers_per_cell
                s.counts(customer_source=counts.source)
                abnormal = ds.shape_viavi_abnormal(
                    details, cpc, name_map=self._name_map(),
                    kpi_series=self._series(viavi_raw, ctx),
                )
                s.counts(abnormal=len(abnormal))
            except Exception as e:
                self.log.warning("Viavi abnormal shaping failed: %s", e)

        entities_analyzed = len(analyzed_sites)
        raw_empty = viavi_raw is None or getattr(viavi_raw, "empty", True)
        analysis_run = not raw_empty or entities_analyzed > 0
        # If retrieval returned no rows, that's NO_DATA (distinct from clean).
        if raw_empty:
            entities_analyzed = 0
        state, message = compute_state(analysis_run, entities_analyzed, count)
        slog.note("Viavi analysis complete", marker="COMPLETE",
                  sites=entities_analyzed, affected=count, abnormal=len(abnormal), state=state)
        return build_result(
            self.system,
            state=state, message=message, analysis_run=analysis_run,
            summary={
                "sites_affected_count": count,
                "entities_analyzed": entities_analyzed,
                "affected_count": count,
                "has_abnormal_kpis": count > 0,
                "all_clean": analysis_run and entities_analyzed > 0 and count == 0,
                "severity_counts": severity_counts,
            },
            metrics={},
            affected={"sites_list": sites_list},
            details={"affected_sites_details": details},
            abnormal_kpis=abnormal,
            insights=[message],
        )
