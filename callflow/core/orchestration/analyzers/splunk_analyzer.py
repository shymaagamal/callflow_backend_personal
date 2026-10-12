"""
Splunk analyzer — preserves the old core-node business logic:

  * per-node KPI/threshold alarms                    (KPIAnalyzer.analyze_core_node)
  * Smartcare impacted-subscriber mapping + chart    (dashboard_shaping.shape_splunk_abnormal
                                                       with a Smartcare-backed subs mapper)

Real data dependency: the alarm detection does NOT need Smartcare, but the
dashboard result (impacted subscribers per KPI) does, so the analysis depends on
Smartcare in the DAG.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from .base import build_result, compute_state
from .severity import filter_alarms_by_severity, SIGNIFICANT_SEVERITIES
from .. import dashboard_shaping as ds
from ...services.kpi_analyzer import KPIAnalyzer
from ..smartcare_index import from_context
from ._pandas import splunk_series_provider
from ..task_logging import StageLog

logger = logging.getLogger(__name__)

# Configured core nodes (kept in sync with abnormal_kpis / retrieval).
CONFIGURED_NODES = ["SGSN", "MME", "GGSN", "DSC", "DNS"]


class SplunkAnalyzer:
    system = "splunk"

    def __init__(self, *, analyze_node_fn: Optional[Callable] = None,
                 subs_mapper_factory: Optional[Callable] = None,
                 series_provider: Optional[Callable] = None,
                 log=None):
        self._analyze_node_fn = analyze_node_fn
        self._subs_mapper_factory = subs_mapper_factory
        self._series_provider = series_provider
        self.log = log or logger

    def _alarms_by_node(self, splunk_raw, ctx) -> Dict[str, Any]:
        if self._analyze_node_fn is not None:
            return self._analyze_node_fn(splunk_raw, ctx)
        qtr = ctx["query_time_range"]
        analyzer = KPIAnalyzer()
        out = {}
        for node, df in (splunk_raw or {}).items():
            if df is None or getattr(df, "empty", True):
                continue
            out[node] = analyzer.analyze_core_node(
                df=df, node=node,
                current_start=qtr["current_start"], current_end=qtr["current_end"],
                prev_start=qtr["prev_start_1day"], prev_end=qtr["prev_end_1day"],
            )
        return out

    def _subs_mapper(self, smartcare, ctx):
        if self._subs_mapper_factory is not None:
            return self._subs_mapper_factory(smartcare, ctx)
        # Reuse the one per-request Smartcare index (prepared node columns) so the
        # per-KPI subscriber lookup does not re-scan the whole frame each call.
        return from_context(smartcare, ctx).splunk_subs_mapper()

    def _series(self, splunk_raw, ctx):
        if self._series_provider is not None:
            return self._series_provider(splunk_raw, ctx)
        return splunk_series_provider(splunk_raw, ctx)

    @staticmethod
    def _nodes_with_data(splunk_raw):
        out = []
        for node, df in (splunk_raw or {}).items():
            if df is not None and not getattr(df, "empty", True):
                out.append(node)
        return out

    def analyze(self, splunk_raw, smartcare, ctx) -> Dict[str, Any]:
        slog = StageLog(ctx.get("request_logger") or self.log,
                        ctx.get("request_id"), self.system)
        configured = ctx.get("splunk_nodes", CONFIGURED_NODES)
        nodes_with_data = self._nodes_with_data(splunk_raw)
        slog.note("start Splunk analysis", marker="START",
                  configured=len(configured), with_data=len(nodes_with_data))

        with slog.stage("per-node KPI/threshold alarms") as s:
            raw_alarms = self._alarms_by_node(splunk_raw, ctx) or {}
            s.counts(nodes=len(raw_alarms),
                     alarms_raw=sum(len(a or []) for a in raw_alarms.values()))

        # SEVERITY GATE: keep only High + Medium alarms.
        significant = ctx.get("significant_severities", SIGNIFICANT_SEVERITIES)
        with slog.stage("severity gate (High+Medium)") as s:
            alarms_by_node, severity_counts = filter_alarms_by_severity(raw_alarms, significant)
            s.counts(alarms=sum(len(a or []) for a in alarms_by_node.values()))

        abnormal = []
        with slog.stage("Smartcare subscriber mapping + chart rows") as s:
            try:
                abnormal = ds.shape_splunk_abnormal(
                    alarms_by_node,
                    subs_mapper=self._subs_mapper(smartcare, ctx),
                    kpi_series=self._series(splunk_raw, ctx),
                )
                s.counts(abnormal=len(abnormal))
            except Exception as e:
                self.log.warning("Splunk abnormal shaping failed: %s", e)

        abnormal_nodes = sorted(n for n, a in alarms_by_node.items() if a)
        alarm_count = sum(len(a or []) for a in alarms_by_node.values())
        analyzed = self._nodes_with_data(splunk_raw) or list(raw_alarms.keys())
        entities_analyzed = len(analyzed)
        analysis_run = True  # analyze always runs over the configured node set
        state, message = compute_state(analysis_run, entities_analyzed, len(abnormal_nodes))
        slog.note("Splunk analysis complete", marker="COMPLETE",
                  nodes=entities_analyzed, abnormal_nodes=len(abnormal_nodes),
                  alarms=alarm_count, state=state)
        return build_result(
            self.system,
            state=state, message=message, analysis_run=analysis_run,
            summary={
                "configured_nodes": list(configured),
                "nodes_with_data": nodes_with_data,
                "abnormal_nodes": abnormal_nodes,
                "alarm_count": alarm_count,
                "entities_analyzed": entities_analyzed,
                "affected_count": len(abnormal_nodes),
                "has_abnormal_kpis": len(abnormal_nodes) > 0,
                "all_clean": analysis_run and entities_analyzed > 0 and len(abnormal_nodes) == 0,
                "severity_counts": severity_counts,
            },
            metrics={"alarms_by_node": alarms_by_node},
            affected={"nodes": abnormal_nodes},
            details={"alarms_by_node": alarms_by_node},
            abnormal_kpis=abnormal,
            insights=[message],
        )
