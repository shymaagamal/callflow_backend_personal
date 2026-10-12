"""
Smartcare analyzer.

Smartcare's "analysis" is the processing already performed during retrieval
(``fetch_smartcare_nodes_data`` -> process_smartcare_data: metrics, packetNodes,
suspected_cells_raw). This analyzer shapes that result into the common contract
and the ``smartcare_data_metrics`` object the popups consume, without recomputing
anything. It does not depend on any other system.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from .base import build_result
from .. import dashboard_shaping as ds

logger = logging.getLogger(__name__)


class SmartcareAnalyzer:
    system = "smartcare"

    def analyze(self, smartcare, ctx, remedy=None) -> Dict[str, Any]:
        sc = smartcare if isinstance(smartcare, dict) else {}
        metrics = sc.get("metrics", {}) or {}
        packet = sc.get("packetNodes", {}) or {}
        handset = metrics.get("subs_per_handset_type")

        smartcare_data_metrics = ds.shape_smartcare_metrics(sc, remedy=remedy)
        summary = {
            "handset_types": len(handset) if isinstance(handset, (list, dict)) else None,
            "packet_core_nodes": len(packet) if isinstance(packet, dict) else None,
            "suspected_cells": len(sc.get("suspected_cells_raw") or [])
                if sc.get("suspected_cells_raw") is not None else None,
        }
        insights = [
            f"{summary['handset_types'] or 0} handset type(s), "
            f"{summary['packet_core_nodes'] or 0} packet-core node type(s)."
        ]
        return build_result(
            self.system,
            summary=summary,
            metrics=metrics,
            details={"packetNodes": packet, "smartcare_data_metrics": smartcare_data_metrics},
            insights=insights,
        )
