
"""
One-time, per-request Smartcare lookup index.

Motivation (request sections 3 & 4): NPP, Viavi and Splunk analysis all need
customer information from the SAME Smartcare dataset. Previously each analysis
re-derived what it needed from the raw Smartcare frame, and the Splunk mapper in
particular re-ran ``.astype(str).str.contains(...)`` over an entire node column
*twice per abnormal KPI* (once to find the matching column, once to count
subscribers). On a large Smartcare frame that repeated full-column scanning is
the dominant cost.

This module builds ONE index from the Smartcare result and lets every system
reuse it:

    Smartcare result
          │  build once
          ▼
    SmartcareIndex
      ├─ customers_per_cell     (cell  -> unique subscriber count)   NPP / Viavi
      └─ match_and_count(value) (node value -> column, node_type,    Splunk
                                 impacted subscriber count)

IMPORTANT — business logic is preserved, not changed:
  * ``customers_per_cell`` is exactly Smartcare's precomputed
    ``metrics.subs_per_cell`` (same numbers the old dashboard used).
  * ``match_and_count`` reproduces the old
    ``map_splunk_kpi_to_smartcare`` + count behaviour (first node column whose
    values contain the mapped value, case-insensitive substring, then
    ``MSISDN.nunique()`` of the matching rows) — only the per-column string
    conversion is done ONCE and the find+count are folded into a single scan.

The index is cached on the shared task context so the three concurrent analyses
build it at most once (see ``from_context``).
"""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Tuple

_CTX_KEY = "_smartcare_index"
_CTX_LOCK_KEY = "_smartcare_index_lock"
_INSTALL_LOCK = threading.Lock()  # guards lazy creation of the per-ctx lock


def customers_per_cell_from_smartcare(smartcare) -> Dict[str, Any]:
    """subs_per_cell ([{label,value}] or {label:value}) -> {cell: count}.

    Delegates to the single pure implementation in ``dashboard_shaping`` so the
    numbers can never diverge between the index and the dashboard shaping.
    """
    from .dashboard_shaping import customers_per_cell_from_smartcare as _impl
    return _impl(smartcare)


class SmartcareIndex:
    """Prepared, reusable view over one request's Smartcare data."""

    def __init__(
        self,
        customers_per_cell: Dict[str, Any],
        prepared_columns: "Optional[Dict[str, Any]]" = None,
        msisdn_series: Any = None,
        node_columns: Optional[List[str]] = None,
        node_type_map: Optional[Dict[str, str]] = None,
    ):
        self.customers_per_cell = customers_per_cell or {}
        self._prepared = prepared_columns or {}     # column -> astype(str) Series
        self._msisdn = msisdn_series                 # MSISDN Series (aligned)
        self._node_columns = node_columns or []
        self._node_type_map = node_type_map or {}

    # ---- construction ------------------------------------------------------
    @classmethod
    def build(
        cls,
        smartcare,
        node_columns: Optional[List[str]] = None,
        node_type_map: Optional[Dict[str, str]] = None,
        msisdn_col: str = "MSISDN",
    ) -> "SmartcareIndex":
        cpc = customers_per_cell_from_smartcare(smartcare)
        processed = smartcare.get("processed_data") if isinstance(smartcare, dict) else None

        prepared: Dict[str, Any] = {}
        msisdn_series = None
        cols: List[str] = []
        if processed is not None and not getattr(processed, "empty", True):
            if node_columns is None:
                # Default to the production node columns; import lazily so tests
                # that pass explicit columns need no Django/endpoints import.
                try:
                    from ..constants.endpoints import NODE_COLUMNS_FROM_SMARTCARE
                    node_columns = list(NODE_COLUMNS_FROM_SMARTCARE.keys())
                    node_type_map = node_type_map or dict(NODE_COLUMNS_FROM_SMARTCARE)
                except Exception:
                    node_columns = []
            if msisdn_col in getattr(processed, "columns", []):
                msisdn_series = processed[msisdn_col]
            for col in (node_columns or []):
                if col in getattr(processed, "columns", []):
                    # One string conversion per column, reused for every query.
                    prepared[col] = processed[col].astype(str)
                    cols.append(col)

        return cls(
            customers_per_cell=cpc,
            prepared_columns=prepared,
            msisdn_series=msisdn_series,
            node_columns=cols,
            node_type_map=node_type_map or {},
        )

    # ---- lookups -----------------------------------------------------------
    def customers_for_cell(self, cell) -> Optional[int]:
        return self.customers_per_cell.get(str(cell))

    def match_and_count(self, mapped_value) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """First node column whose values contain ``mapped_value`` (case-insensitive
        substring), and the number of unique MSISDNs on the matching rows.

        Returns ``(column, node_type, impacted_count)`` or ``(None, None, None)``.
        Mirrors the old map+count exactly, but scans each column at most once.
        """
        if not self._prepared or self._msisdn is None or mapped_value is None:
            return (None, None, None)
        needle = str(mapped_value)
        for col in self._node_columns:
            series = self._prepared.get(col)
            if series is None:
                continue
            try:
                mask = series.str.contains(needle, case=False, na=False)
            except Exception:
                continue
            if bool(mask.any()):
                impacted = int(self._msisdn[mask.values].nunique())
                return (col, self._node_type_map.get(col), impacted)
        return (None, None, None)

    def splunk_subs_mapper(self):
        """Return the ``(mapped_value, impacted_subs, node_type)`` mapper the
        Splunk shaping expects, reproducing the old semantics via this index."""
        if not self._prepared or self._msisdn is None:
            return None
        try:
            from ..utils.data_utils import extract_suffix
            from ..constants.endpoints import SPLUNK_SUFFIX_TO_SMARTCARE_NODE
        except Exception:
            return None

        def mapper(kpi_col):
            try:
                suffix = extract_suffix(kpi_col).lower()
                mapped_value = SPLUNK_SUFFIX_TO_SMARTCARE_NODE.get(suffix)
                if not mapped_value:
                    return (None, None, None)
                column, node_type, impacted = self.match_and_count(mapped_value)
                if column is None:
                    return (None, None, None)
                return (mapped_value, impacted, node_type)
            except Exception:
                return (None, None, None)

        return mapper


def from_context(smartcare, ctx: Optional[Dict[str, Any]]) -> SmartcareIndex:
    """Build the index once and cache it on the shared context.

    Only Splunk analysis still depends on Smartcare (NPP/Viavi now read their
    customer counts from customer_counts.py). The cache + lock are kept so any
    concurrent caller still builds the index at most once, race-safe.
    """
    if ctx is None:
        return SmartcareIndex.build(smartcare)
    existing = ctx.get(_CTX_KEY)
    if isinstance(existing, SmartcareIndex):
        return existing
    lock = ctx.get(_CTX_LOCK_KEY)
    if lock is None:
        # Install a per-ctx lock exactly once, even under concurrent callers.
        with _INSTALL_LOCK:
            lock = ctx.get(_CTX_LOCK_KEY)
            if lock is None:
                lock = threading.Lock()
                ctx[_CTX_LOCK_KEY] = lock
    with lock:
        existing = ctx.get(_CTX_KEY)
        if isinstance(existing, SmartcareIndex):
            return existing
        idx = SmartcareIndex.build(smartcare)
        ctx[_CTX_KEY] = idx
        return idx
