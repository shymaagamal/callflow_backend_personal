"""
Remedy analyzer — preserves the old ticket business logic:

  * top-remedy ranking with Smartcare subscriber counts
    (smartcare_service.calculate_top_remedy_with_subs)
  * ticket counts + status info + the popup's {remedy_count, remedy_output}
    (dashboard_shaping.shape_remedy)

Data dependency: the ranking needs unique MSISDNs per site (``subs_per_site``),
which come from the Trino cell rows (customer_counts.py), so Remedy analysis
does NOT wait for Smartcare. The ranking function itself is unchanged.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from .base import build_result
from .. import dashboard_shaping as ds
from ...services.smartcare_service import calculate_top_remedy_with_subs
from ..customer_counts import resolve as resolve_customer_counts

logger = logging.getLogger(__name__)


class RemedyAnalyzer:
    system = "remedy"

    def __init__(self, *, top_remedy_fn: Optional[Callable] = None, log=None):
        self._top_remedy_fn = top_remedy_fn
        self.log = log or logger

    def _top_remedy(self, smartcare_metrics, remedy_output):
        if self._top_remedy_fn is not None:
            return self._top_remedy_fn(smartcare_metrics, remedy_output)
        return calculate_top_remedy_with_subs(smartcare_metrics, remedy_output)

    def analyze(self, remedy_raw, smartcare, ctx) -> Dict[str, Any]:
        remedy_output = (remedy_raw or {}).get("remedy_tickets", {}) if isinstance(remedy_raw, dict) else {}
        status_count = (remedy_raw or {}).get("status", {}) if isinstance(remedy_raw, dict) else {}
        sc_metrics = resolve_customer_counts(smartcare, ctx).remedy_metrics()

        top = {}
        try:
            top = self._top_remedy(sc_metrics, remedy_output) or {}
        except Exception as e:
            self.log.warning("Remedy top-ranking failed: %s", e)

        remedy_shaped = ds.shape_remedy({"top_remedy": top, "status": status_count},
                                        remedy_retrieval=remedy_raw)
        total = remedy_shaped.get("remedy_count", 0)
        insights = [f"{total} remedy ticket(s) across affected sites."]
        return build_result(
            self.system,
            summary={"remedy_count": total},
            metrics={"status": status_count},
            affected={"sites_with_tickets": list((remedy_output or {}).keys())},
            details={"remedy": remedy_shaped, "top_remedy": top},
            insights=insights,
        )
