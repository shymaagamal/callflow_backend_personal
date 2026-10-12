"""
Structured, per-request/per-system logging for the orchestrated analysis.

Why this exists
---------------
The orchestrated job reuses the existing per-request logger from
``logging_utils.build_request_logger`` (a single file per request via
``get_log_file``). But that file's prefix is shared across orchestrated
requests, so concurrent runs interleave. This module adds:

  * a consistent structured line format that always carries
    request_id / system / task / status / duration / error, and
  * a dedicated log file PER SYSTEM within a PER-REQUEST directory, so a
    reviewer can open exactly one file to trace one system for one request
    without any interleaving from other requests.

It reuses (does not replace) ``build_request_logger``: structured lines are
emitted to BOTH the request logger (app-wide + the per-request file) and the
per-system file. Rendering is pure and unit-testable; no Django/pandas import.

Line format (matches the requested shape):

    [request_id=abc123][system=TRINO][task=trino_cells]
    STARTED

    [request_id=abc123][system=TRINO][task=trino_cells]
    SUCCESS - retrieved 25 cells - duration=3.2s

    [request_id=abc123][system=NPP][task=npp_retrieval]
    FAILED - connection timeout - duration=30.1s
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

RCA_ROOT_DIRNAME = "rca"
LOGS_DIRNAME = "logs"

_FAIL_STATUSES = {"FAILED", "TIMEOUT", "ERROR"}


def render_event(request_id, system, task, status,
                 detail=None, duration=None, error=None) -> str:
    """Pure renderer for one structured event line-block."""
    header = f"[request_id={request_id}][system={str(system).upper()}][task={task}]"
    body = str(status).upper()
    extras = []
    if detail is not None and str(detail) != "":
        extras.append(str(detail))
    if duration is not None:
        try:
            extras.append(f"duration={float(duration):.2f}s")
        except (TypeError, ValueError):
            pass
    if error:
        extras.append(f"error={error}")
    if extras:
        body += " - " + " - ".join(extras)
    return f"{header}\n{body}"


def render_query(request_id, system, task, sql: str) -> str:
    """Pure renderer for a full (already parameter-substituted) SQL query."""
    header = f"[request_id={request_id}][system={str(system).upper()}][task={task}]"
    return f"{header}\nQUERY:\n{sql}"


def _fmt_counts(counts: Optional[dict]) -> str:
    if not counts:
        return ""
    parts = []
    for k, v in counts.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    return " ".join(parts)


def render_stage(request_id, system, step, marker="STEP",
                 counts: Optional[dict] = None, detail=None, duration=None) -> str:
    """Pure renderer for one analysis-stage line.

    Example:
        [request_id=abc][system=NPP]
        [STEP] extracting affected cells - cells=63 - duration=0.70s
    """
    header = f"[request_id={request_id}][system={str(system).upper()}]"
    body = f"[{marker}] {step}"
    extras = []
    c = _fmt_counts(counts)
    if c:
        extras.append(c)
    if detail is not None and str(detail) != "":
        extras.append(str(detail))
    if duration is not None:
        try:
            extras.append(f"duration={float(duration):.2f}s")
        except (TypeError, ValueError):
            pass
    if extras:
        body += " - " + " - ".join(extras)
    return f"{header}\n{body}"


def render_pipeline_summary(request_id, timings: Dict[str, Optional[float]],
                            wall_time: Optional[float] = None,
                            order=None) -> str:
    """Pure renderer for the end-of-request timing table (request section 10).

    ``timings`` maps stage/task name -> seconds (None if it never ran). The table
    makes the bottleneck obvious at a glance.
    """
    names = list(order) if order else list(timings.keys())
    for n in timings:
        if n not in names:
            names.append(n)
    width = max([len(n) for n in names] + [len("WALL")]) + 2
    lines = [f"========== PIPELINE SUMMARY [request_id={request_id}] =========="]
    for n in names:
        d = timings.get(n)
        val = f"{d:7.2f}s" if isinstance(d, (int, float)) else "     -- "
        lines.append(f"{n.ljust(width)}{val}")
    lines.append("-" * (width + 9))
    if wall_time is not None:
        lines.append(f"{'WALL'.ljust(width)}{wall_time:7.2f}s")
    lines.append("=" * (width + 9))
    return "\n".join(lines)


class StageLog:
    """Lightweight per-stage timing logger bound to (request_id, system).

    Emits a ``[STEP]`` line when a stage starts and a ``[DONE]`` line with the
    measured duration when it ends. Counts (records, matched, affected, ...) are
    logged, never the underlying data (request section 12).

    Usage:
        slog = StageLog(request_logger, request_id, "NPP")
        with slog.stage("extracting affected cells") as s:
            ...
            s.counts(cells=len(cells))
    """

    def __init__(self, log, request_id, system):
        self.log = log or logger
        self.request_id = request_id
        self.system = system

    def note(self, step, marker="STEP", **counts) -> None:
        try:
            self.log.info(render_stage(self.request_id, self.system, step,
                                       marker=marker, counts=counts or None))
        except Exception:  # pragma: no cover - logging must never break analysis
            pass

    @contextmanager
    def stage(self, step, **counts):
        self.note(step, marker="STEP", **counts)
        t0 = time.perf_counter()
        collected: Dict[str, object] = {}

        class _Handle:
            def counts(self, **kw):
                collected.update(kw)

        try:
            yield _Handle()
        finally:
            dur = time.perf_counter() - t0
            try:
                self.log.info(render_stage(self.request_id, self.system, step,
                                           marker="DONE", counts=collected or None,
                                           duration=dur))
            except Exception:  # pragma: no cover
                pass


def _system_log_path(base_output_dir, request_id: str, system: str) -> Path:
    d = Path(base_output_dir) / RCA_ROOT_DIRNAME / str(request_id) / LOGS_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{str(system).lower()}.log"


def get_system_logger(base_output_dir, request_id: str, system: str) -> logging.Logger:
    """A logger writing to ``rca/<request_id>/logs/<system>.log`` (request-scoped,
    per-system). Handlers are de-duplicated so repeated calls are safe."""
    name = f"callflow.request.{request_id}.system.{str(system).lower()}"
    lg = logging.getLogger(name)
    lg.setLevel(logging.DEBUG)
    path = _system_log_path(base_output_dir, request_id, system)
    already = any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == str(path)
        for h in lg.handlers
    )
    if not already:
        try:
            fh = logging.FileHandler(path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            lg.addHandler(fh)
        except Exception:  # pragma: no cover - never let logging break a request
            logger.warning("Could not attach per-system log handler at %s", path)
    lg.propagate = True
    return lg


class TaskEventLogger:
    """Emits structured events to the request logger AND a per-system file.

    Bind once per request; call ``event(...)`` / ``query(...)`` from anywhere
    (view callbacks, retrieval/analysis wrappers, services).
    """

    def __init__(self, request_id: str, request_logger=None, base_output_dir=None):
        self.request_id = request_id
        self.request_logger = request_logger
        self.base_output_dir = base_output_dir

    def _emit(self, system: str, level: int, message: str) -> None:
        if self.request_logger is not None:
            try:
                self.request_logger.log(level, message)
            except Exception:  # pragma: no cover
                pass
        if self.base_output_dir is not None:
            try:
                get_system_logger(self.base_output_dir, self.request_id, system).log(level, message)
            except Exception:  # pragma: no cover
                pass

    def event(self, system, task, status, detail=None, duration=None, error=None) -> None:
        msg = render_event(self.request_id, system, task, status,
                           detail=detail, duration=duration, error=error)
        level = logging.ERROR if str(status).upper() in _FAIL_STATUSES else logging.INFO
        self._emit(system, level, msg)

    def query(self, system, task, sql: str) -> None:
        self._emit(system, logging.INFO, render_query(self.request_id, system, task, sql))
