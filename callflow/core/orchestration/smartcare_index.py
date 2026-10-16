
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

# Smartcare columns that hold the customers of each Splunk node type.
#   MME  (4G)    -> S1-MME MME_Name, S6a MME_NAME
#   SGSN (2G/3G) -> GBIUPS SGSN_MME_NAME
#   GGSN         -> SGW/PGW names; SGW_PGW_NAME is the S1-MME/DNS SGW column after
#                   smartcareOutput_Aggregated renamed it and removed '_SGW-PGW_'.
# Other node types (DSC, DNS, ...) search every node column, as before.
SPLUNK_NODE_COLUMNS = {
    "MME": ("MME_Name", "MME_NAME"),
    "SGSN": ("SGSN_MME_NAME",),
    "GGSN": ("SGSN_SGW_NAME", "GGSN_PGW_NAME", "SGW_PGW_NAME"),
}
_STRIPPED_SGW_COLUMN = "SGW_PGW_NAME"
_SGW_STRIP = ("_vSGW-vPGW_", "_SGW-PGW_")   # same replacements as smartcareOutput_Aggregated
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
            # Per-node-type columns that are not in the generic search order.
            for type_cols in SPLUNK_NODE_COLUMNS.values():
                for col in type_cols:
                    if col not in prepared and col in getattr(processed, "columns", []):
                        prepared[col] = processed[col].astype(str)

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

    def match_and_count_in(self, mapped_value, columns) -> Tuple[List[str], Optional[int]]:
        """Columns (of ``columns``) whose values contain ``mapped_value``, and the
        unique MSISDNs over ALL matching rows of those columns together.

        Unlike ``match_and_count`` (first matching column only), customers seen on
        the node through several interfaces are all counted, once each.
        Returns ``([], None)`` when nothing matches.
        """
        if not self._prepared or self._msisdn is None or mapped_value is None:
            return ([], None)
        needle = str(mapped_value)
        stripped = needle
        for token in _SGW_STRIP:
            stripped = stripped.replace(token, "")
        union = None
        matched: List[str] = []
        for col in columns:
            series = self._prepared.get(col)
            if series is None:
                continue
            try:
                if col == _STRIPPED_SGW_COLUMN:
                    if stripped == needle:
                        continue  # not an SGW/PGW name; this column cannot hold it
                    mask = series.str.strip().str.lower() == stripped.lower()
                else:
                    mask = series.str.contains(needle, case=False, na=False, regex=False)
            except Exception:
                continue
            if bool(mask.any()):
                matched.append(col)
                union = mask if union is None else (union | mask)
        if union is None:
            return ([], None)
        return (matched, int(self._msisdn[union.values].nunique()))

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

        # Suffix -> Smartcare node lookup. The table mixes key casing ('sgsn06',
        # 'ALX-GGSN_CI-10', 'RMD' vs 'rmd') and Splunk sends 'SGSN6' for 'sgsn06',
        # so a plain .get(suffix.lower()) never matches the mixed-case keys
        # (all GGSN nodes). Exact key first, then case-insensitive, then
        # leading-zero-insensitive; keys that become ambiguous are skipped.
        import re

        def _zero_norm(k):
            return re.sub(r"\d+", lambda m: str(int(m.group())), k)

        def _unique_index(keyfn):
            idx, clash = {}, set()
            for k, v in SPLUNK_SUFFIX_TO_SMARTCARE_NODE.items():
                nk = keyfn(str(k).lower())
                if nk in idx and idx[nk] != v:
                    clash.add(nk)
                idx[nk] = v
            return {k: v for k, v in idx.items() if k not in clash}

        by_lower = _unique_index(lambda k: k)
        by_zero = _unique_index(_zero_norm)

        def lookup(suffix):
            low = suffix.lower()
            return (SPLUNK_SUFFIX_TO_SMARTCARE_NODE.get(suffix)
                    or SPLUNK_SUFFIX_TO_SMARTCARE_NODE.get(low)
                    or by_lower.get(low) or by_zero.get(_zero_norm(low)))

        # Smartcare columns that can confirm (or rule out) a Splunk node type.
        expected_cols = dict(SPLUNK_NODE_COLUMNS, DSC=("DRA_NAME",))
        empty_vals = {"", "nan", "none", "<na>", "nat"}

        def has_node_data(cols):
            for c in cols:
                series = self._prepared.get(c)
                if series is not None and not series.str.strip().str.lower().isin(empty_vals).all():
                    return True
            return False

        # Alarms are no longer dropped: each is labelled so the portal can show
        # the ones not linked to the complainants separately.
        stats = {"serving": 0, "unverified": 0, "no_mapping": 0,
                 "not_serving": 0, "dropped_error": 0}
        dropped = []

        def mapper(kpi_col):
            suffix = None
            try:
                suffix = str(extract_suffix(kpi_col) or "")
                mapped_value = lookup(suffix)
                node = str(kpi_col).split("_")[0].upper()
                if not mapped_value:
                    stats["no_mapping"] += 1
                    dropped.append({"kpi": kpi_col, "suffix": suffix, "reason": "no_mapping"})
                    return (suffix or kpi_col, None, node, "no_mapping")
                search_cols = SPLUNK_NODE_COLUMNS.get(node) or self._node_columns
                matched, impacted = self.match_and_count_in(mapped_value, search_cols)
                column = matched[0] if matched else None
                node_type = (self._node_type_map.get(column) or node) if column else None
                if column is None:
                    cols = expected_cols.get(node)
                    if cols and not has_node_data(cols):
                        # Smartcare has no data for this node type (e.g. its query
                        # failed), so "served the complainants?" cannot be checked:
                        # keep the alarm with unknown impact instead of hiding it.
                        stats["unverified"] += 1
                        return (mapped_value, None, node, "unverified")
                    stats["not_serving"] += 1
                    dropped.append({"kpi": kpi_col, "mapped": mapped_value, "reason": "not_serving"})
                    return (mapped_value, None, node, "not_serving")
                stats["serving"] += 1
                return (mapped_value, impacted, node_type, "serving")
            except Exception as e:
                stats["dropped_error"] += 1
                dropped.append({"kpi": kpi_col, "suffix": suffix, "reason": "error: %s" % e})
                return (None, None, None)

        mapper.stats = stats
        mapper.dropped = dropped
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
