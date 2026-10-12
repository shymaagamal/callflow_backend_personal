"""
Per-request customer-count source for the NPP / Viavi / Remedy analyses.

Customer counts are unique complainant MSISDNs per cell (NPP, Viavi) and per
site (Remedy ranking), plus the MSISDN/cell rows NPP's hub/TAC/LAC enrichment
groups. Smartcare used to be the only source, so those three analyses waited
for the slow Smartcare task. The Trino cell query already returns the same
MSISDN + cell rows, so the Trino task now computes the counts once
(``trino_service.customer_counts_from_df``) and stores them on the shared
context under ``CTX_KEY``. The analyses read them from here.

Source selection (no DAG edge needed for either path):
  * Trino resolved the cells  -> Trino counts.
  * Trino returned no cells   -> the cells came from Smartcare (cell_resolution
    fallback, where the Trino task already WAITED for Smartcare), so the
    Smartcare result is in ``ctx['_smartcare_holder']`` -> Smartcare counts,
    exactly as before.
  * neither                   -> empty (NPP shows N/A, Viavi 0: unchanged rules).

The shaping/business functions that consume these values are unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from .dashboard_shaping import customers_per_cell_from_smartcare

CTX_KEY = "_trino_customers"

SOURCE_TRINO = "trino"
SOURCE_SMARTCARE = "smartcare"


@dataclass
class CustomerCounts:
    customers_per_cell: Dict[str, Any] = field(default_factory=dict)
    subs_per_site: List[Dict[str, Any]] = field(default_factory=list)
    nodes_frame: Any = None          # DataFrame with MSISDN + cell_name (or None)
    source: Optional[str] = None     # 'trino' | 'smartcare' | None
    smartcare_metrics: Optional[Dict[str, Any]] = None  # untouched, Smartcare path only

    def remedy_metrics(self) -> Dict[str, Any]:
        """The ``metrics`` shape ``calculate_top_remedy_with_subs`` reads.
        Smartcare path: the original Smartcare metrics dict, passed as before."""
        if self.smartcare_metrics is not None:
            return self.smartcare_metrics
        return {"subs_per_site": self.subs_per_site}


def _smartcare_result(smartcare, ctx):
    if isinstance(smartcare, dict):
        return smartcare
    holder = (ctx or {}).get("_smartcare_holder") or {}
    result = holder.get("result")
    return result if isinstance(result, dict) else None


def resolve(smartcare, ctx: Optional[Dict[str, Any]]) -> CustomerCounts:
    """Pick the customer-count source for this request (see module docstring)."""
    trino = (ctx or {}).get(CTX_KEY)
    if isinstance(trino, dict) and trino.get("customers_per_cell"):
        return CustomerCounts(
            customers_per_cell=trino.get("customers_per_cell") or {},
            subs_per_site=trino.get("subs_per_site") or [],
            nodes_frame=trino.get("nodes_frame"),
            source=SOURCE_TRINO,
        )
    sc = _smartcare_result(smartcare, ctx)
    if sc is not None:
        metrics = sc.get("metrics") or {}
        return CustomerCounts(
            customers_per_cell=customers_per_cell_from_smartcare(sc),
            subs_per_site=metrics.get("subs_per_site") or [],
            nodes_frame=sc.get("processed_data"),
            source=SOURCE_SMARTCARE,
            smartcare_metrics=metrics,
        )
    return CustomerCounts()
