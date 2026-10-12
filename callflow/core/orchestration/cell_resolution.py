"""
Trino -> Smartcare cell-name fallback resolution.

The DAG is unchanged: ``trino_cells`` still produces the cell list that NPP /
sites / Viavi / Remedy retrieval depend on. This module only decides WHAT that
list is when Trino comes back empty, and produces a clear, debuggable status:

    Trino
      ├── cell(s) found        -> use Trino
      └── none found
            └── Smartcare
                  ├── column missing              -> clear failure
                  ├── column present, no cells    -> clear failure (distinct)
                  └── cells present               -> use Smartcare

The pure ``resolve_cells`` function takes plain values (lists/bools) so it is
fully unit-testable without pandas/Django. The caller (pipeline/view) extracts
``smartcare_available`` / ``smartcare_has_column`` / ``smartcare_cells`` from the
real Smartcare result DataFrame and passes them in.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

# The Smartcare processed_data column that holds cell names
# (see smartcare_service.smartcareOutput_Aggregated -> 'cell_name').
SMARTCARE_CELL_COLUMN = "cell_name"

# smartcare_status values
SC_NOT_CHECKED = "not_checked"     # Trino found cells; Smartcare not consulted
SC_UNAVAILABLE = "unavailable"     # Smartcare data/result not available (failed/none)
SC_COLUMN_MISSING = "column_missing"   # result present but the cell column is absent
SC_NO_MATCH = "no_matching_cell"   # column present but no usable cell values
SC_FOUND = "found"                 # cells taken from Smartcare


@dataclass
class CellResolution:
    cells: List[str] = field(default_factory=list)
    source: Optional[str] = None          # 'trino' | 'smartcare' | None
    trino_found: bool = False
    smartcare_status: str = SC_NOT_CHECKED
    message: str = ""

    @property
    def resolved(self) -> bool:
        return bool(self.cells) and self.source is not None


def resolve_cells(
    trino_cells,
    smartcare_available: bool = False,
    smartcare_has_column: bool = False,
    smartcare_cells=None,
    column: str = SMARTCARE_CELL_COLUMN,
) -> CellResolution:
    """Decide the effective cell list and a clear status message.

    Args:
        trino_cells: cell names returned by Trino (possibly empty/None).
        smartcare_available: True if a usable Smartcare result exists.
        smartcare_has_column: True if the Smartcare result has the cell column.
        smartcare_cells: usable (non-empty, de-duplicated) cell values from
            the Smartcare column, when it exists.
        column: the Smartcare column consulted (for messaging).
    """
    trino_cells = [c for c in (trino_cells or []) if str(c).strip() and str(c).strip().lower() != "none"]
    if trino_cells:
        return CellResolution(
            cells=list(dict.fromkeys(trino_cells)),
            source="trino",
            trino_found=True,
            smartcare_status=SC_NOT_CHECKED,
            message=f"Resolved {len(set(trino_cells))} cell(s) from Trino.",
        )

    # Trino returned nothing -> consult Smartcare.
    if not smartcare_available:
        return CellResolution(
            cells=[], source=None, trino_found=False,
            smartcare_status=SC_UNAVAILABLE,
            message="Cell name could not be resolved. Trino: not found. "
                    "Smartcare: data not available.",
        )

    if not smartcare_has_column:
        return CellResolution(
            cells=[], source=None, trino_found=False,
            smartcare_status=SC_COLUMN_MISSING,
            message=f"Cell name could not be resolved. Trino: not found. "
                    f"Smartcare: column '{column}' does not exist.",
        )

    usable = [c for c in (smartcare_cells or [])
              if str(c).strip() and str(c).strip().lower() != "none"]
    if not usable:
        return CellResolution(
            cells=[], source=None, trino_found=False,
            smartcare_status=SC_NO_MATCH,
            message=f"Cell name could not be resolved. Trino: not found. "
                    f"Smartcare: column '{column}' exists but no matching cell found.",
        )

    unique = list(dict.fromkeys(str(c).strip() for c in usable))
    return CellResolution(
        cells=unique, source="smartcare", trino_found=False,
        smartcare_status=SC_FOUND,
        message=f"Trino: not found. Resolved {len(unique)} cell(s) from Smartcare "
                f"column '{column}'.",
    )


def smartcare_cell_facts(smartcare_result, column: str = SMARTCARE_CELL_COLUMN):
    """Extract (available, has_column, cells) from a real Smartcare result dict.

    Kept tiny and defensive; the pandas access is guarded so a malformed result
    never raises here. Returns plain Python types suitable for ``resolve_cells``.
    """
    if not isinstance(smartcare_result, dict):
        return False, False, []
    processed = smartcare_result.get("processed_data")
    if processed is None:
        return False, False, []
    columns = getattr(processed, "columns", None)
    if columns is None:
        return True, False, []
    if column not in list(columns):
        return True, False, []
    try:
        values = processed[column].dropna().astype(str).tolist()
    except Exception:
        values = []
    cells = [v.strip() for v in values if v and v.strip() and v.strip().lower() != "none"]
    return True, True, list(dict.fromkeys(cells))
