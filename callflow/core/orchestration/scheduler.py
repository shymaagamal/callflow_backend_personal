"""
A tiny dependency-driven task scheduler.

Goal (see request section 9): "run every operation as soon as all of its
required inputs are available" -- NOT "run everything in parallel". Each task
declares the tasks it depends on; the scheduler starts a task the moment all of
its dependencies have completed successfully, running independent tasks
concurrently on a shared thread pool.

Design constraints:
  * stdlib only (no Django / pandas / JVM) so it is unit-testable in isolation;
  * failure isolation -- one task failing only skips the tasks that transitively
    depend on it, never the whole plan;
  * best-effort per-task timeout at the application level (the worker thread is
    left to finish in the background, but the plan stops waiting on it);
  * upstream results are passed into downstream tasks, so a task can consume what
    its dependencies produced (e.g. NPP retrieval consumes the Trino cell list);
  * a progress callback fires on every task completion, which the views wire to
    job_manager for live frontend status.

This module deliberately knows nothing about CallFlow's data sources; see
pipeline.py for the concrete Trino/Smartcare/NPP/Viavi/Remedy/Splunk wiring.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"  # a dependency failed/was skipped, so this never ran


# A task function receives the shared context and a {dep_name: result} mapping
# for its (successful) dependencies, and returns whatever it produced.
TaskFn = Callable[[Dict[str, Any], Dict[str, Any]], Any]


@dataclass
class Task:
    name: str
    fn: TaskFn
    deps: List[str] = field(default_factory=list)
    timeout: Optional[float] = None  # seconds; None = no app-level timeout
    label: Optional[str] = None      # human-friendly label for logs / progress

    def display(self) -> str:
        return self.label or self.name


@dataclass
class TaskOutcome:
    name: str
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    ended_at: Optional[float] = None

    @property
    def duration(self) -> Optional[float]:
        if self.started_at is None or self.ended_at is None:
            return None
        return self.ended_at - self.started_at

    @property
    def ok(self) -> bool:
        return self.status == TaskStatus.SUCCESS


@dataclass
class PlanResult:
    outcomes: Dict[str, TaskOutcome]
    wall_time: float

    def result_of(self, name: str) -> Any:
        return self.outcomes[name].result

    def succeeded(self) -> List[str]:
        return [n for n, o in self.outcomes.items() if o.status == TaskStatus.SUCCESS]

    def failed(self) -> List[str]:
        return [
            n for n, o in self.outcomes.items()
            if o.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT)
        ]

    def skipped(self) -> List[str]:
        return [n for n, o in self.outcomes.items() if o.status == TaskStatus.SKIPPED]

    def errors(self) -> Dict[str, str]:
        return {
            n: o.error for n, o in self.outcomes.items()
            if o.error is not None
        }


# Sentinel used internally so a progress callback that raises can never break
# the scheduler.
def _safe_progress(progress_cb, *args) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(*args)
    except Exception:  # pragma: no cover - defensive only
        logger.exception("[orchestration] progress callback raised; ignoring")


def _validate(tasks: List[Task]) -> Dict[str, Task]:
    by_name: Dict[str, Task] = {}
    for t in tasks:
        if t.name in by_name:
            raise ValueError(f"duplicate task name: {t.name!r}")
        by_name[t.name] = t

    for t in tasks:
        for dep in t.deps:
            if dep not in by_name:
                raise ValueError(f"task {t.name!r} depends on unknown task {dep!r}")
            if dep == t.name:
                raise ValueError(f"task {t.name!r} depends on itself")

    # Cycle detection (DFS with colors).
    WHITE, GREY, BLACK = 0, 1, 2
    color = {t.name: WHITE for t in tasks}

    def visit(node: str, stack: List[str]) -> None:
        color[node] = GREY
        for dep in by_name[node].deps:
            if color[dep] == GREY:
                cycle = " -> ".join(stack + [node, dep])
                raise ValueError(f"dependency cycle detected: {cycle}")
            if color[dep] == WHITE:
                visit(dep, stack + [node])
        color[node] = BLACK

    for t in tasks:
        if color[t.name] == WHITE:
            visit(t.name, [])

    return by_name


def run_plan(
    tasks: List[Task],
    context: Optional[Dict[str, Any]] = None,
    max_workers: int = 8,
    progress_cb: Optional[Callable[[int, int, str, str], None]] = None,
    on_start: Optional[Callable[[str], None]] = None,
    logger_override: Optional[logging.Logger] = None,
) -> PlanResult:
    """
    Execute a task DAG by data availability.

    Args:
        tasks: the task list (a DAG via each task's ``deps``).
        context: shared read-only-ish context passed to every task fn.
        max_workers: thread-pool size (bounds concurrency).
        progress_cb: called as ``progress_cb(done, total, task_name, status)``
            after each task settles. Wire this to job_manager.update_job.
        on_start: optional ``on_start(task_name)`` fired when a task transitions
            to RUNNING, so consumers can show truthful "processing" states
            (not faked progress). Defaults to None (backward compatible).
        logger_override: use a request-scoped logger instead of the module one.

    Returns:
        PlanResult with a per-task TaskOutcome.
    """
    log = logger_override or logger
    context = context or {}
    by_name = _validate(tasks)
    total = len(tasks)

    outcomes: Dict[str, TaskOutcome] = {name: TaskOutcome(name=name) for name in by_name}
    done_count = 0
    wall_start = time.perf_counter()

    def deps_state(task: Task) -> str:
        """'ready' if all deps succeeded, 'blocked' if any dep failed/skipped,
        'waiting' otherwise."""
        blocked = False
        ready = True
        for dep in task.deps:
            st = outcomes[dep].status
            if st in (TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.SKIPPED):
                blocked = True
            elif st != TaskStatus.SUCCESS:
                ready = False
        if blocked:
            return "blocked"
        return "ready" if ready else "waiting"

    def _run_one(task: Task) -> Any:
        dep_results = {dep: outcomes[dep].result for dep in task.deps}
        return task.fn(context, dep_results)

    # NOTE: we do NOT use `with ThreadPoolExecutor(...)`, because its __exit__
    # calls shutdown(wait=True) and would block on a task that we abandoned due
    # to an app-level timeout. We shut down with wait=False in `finally` so a
    # single hung task never holds up the whole plan's return.
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        running: Dict[Future, Task] = {}
        deadlines: Dict[str, Optional[float]] = {}

        def submit_ready() -> None:
            for name, task in by_name.items():
                if outcomes[name].status != TaskStatus.PENDING:
                    continue
                state = deps_state(task)
                if state == "blocked":
                    failed_deps = [
                        d for d in task.deps
                        if outcomes[d].status in (
                            TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.SKIPPED
                        )
                    ]
                    outcomes[name].status = TaskStatus.SKIPPED
                    outcomes[name].error = (
                        f"skipped: dependency not satisfied ({', '.join(failed_deps)})"
                    )
                    log.warning(
                        "[orchestration] SKIP %s (deps failed: %s)",
                        task.display(), ", ".join(failed_deps),
                    )
                    _mark_settled(name)
                elif state == "ready":
                    outcomes[name].status = TaskStatus.RUNNING
                    outcomes[name].started_at = time.perf_counter()
                    fut = executor.submit(_run_one, task)
                    running[fut] = task
                    deadlines[name] = (
                        time.perf_counter() + task.timeout if task.timeout else None
                    )
                    log.info("[orchestration] START %s (deps: %s)",
                             task.display(), ", ".join(task.deps) or "-")
                    if on_start is not None:
                        try:
                            on_start(name)
                        except Exception:  # pragma: no cover - defensive
                            log.exception("[orchestration] on_start cb raised; ignoring")

        def _mark_settled(name: str) -> None:
            nonlocal done_count
            done_count += 1
            _safe_progress(
                progress_cb, done_count, total, name, outcomes[name].status.value
            )

        submit_ready()

        while running:
            # Wake up at the earliest task deadline so app-level timeouts fire
            # even if nothing else completes.
            now = time.perf_counter()
            pending_deadlines = [
                deadlines[t.name] for t in running.values()
                if deadlines.get(t.name) is not None
            ]
            wait_timeout = None
            if pending_deadlines:
                wait_timeout = max(0.0, min(pending_deadlines) - now)

            done, _ = wait(running.keys(), timeout=wait_timeout,
                           return_when=FIRST_COMPLETED)

            for fut in done:
                task = running.pop(fut)
                oc = outcomes[task.name]
                oc.ended_at = time.perf_counter()
                deadlines.pop(task.name, None)
                try:
                    oc.result = fut.result()
                    oc.status = TaskStatus.SUCCESS
                    log.info("[orchestration] DONE  %s (%.2fs)",
                             task.display(), oc.duration or 0.0)
                except Exception as e:  # failure isolation
                    oc.status = TaskStatus.FAILED
                    oc.error = str(e)
                    log.error("[orchestration] FAIL  %s (%.2fs): %s",
                              task.display(), oc.duration or 0.0, e)
                _mark_settled(task.name)

            # Enforce best-effort per-task timeouts for still-running tasks.
            now = time.perf_counter()
            timed_out = [
                (fut, task) for fut, task in list(running.items())
                if deadlines.get(task.name) is not None and now >= deadlines[task.name]
            ]
            for fut, task in timed_out:
                running.pop(fut, None)
                deadlines.pop(task.name, None)
                oc = outcomes[task.name]
                oc.ended_at = time.perf_counter()
                oc.status = TaskStatus.TIMEOUT
                oc.error = f"timed out after {task.timeout:.0f}s"
                # The worker thread is a pool thread; we stop waiting on it but
                # cannot force-kill it. Cancel is a no-op once running.
                fut.cancel()
                log.error("[orchestration] TIMEOUT %s (>%.0fs)",
                          task.display(), task.timeout or 0.0)
                _mark_settled(task.name)

            submit_ready()

        # Any tasks still PENDING here were transitively blocked by failures.
        for name, oc in outcomes.items():
            if oc.status == TaskStatus.PENDING:
                oc.status = TaskStatus.SKIPPED
                oc.error = "skipped: never became runnable (upstream failure)"
                _mark_settled(name)
    finally:
        # Return promptly even if a timed-out worker is still running; the
        # abandoned thread finishes on its own without holding up the plan.
        executor.shutdown(wait=False)

    wall = time.perf_counter() - wall_start
    log.info(
        "[orchestration] plan complete in %.2fs: %d ok, %d failed, %d skipped",
        wall,
        sum(1 for o in outcomes.values() if o.status == TaskStatus.SUCCESS),
        sum(1 for o in outcomes.values()
            if o.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT)),
        sum(1 for o in outcomes.values() if o.status == TaskStatus.SKIPPED),
    )
    return PlanResult(outcomes=outcomes, wall_time=wall)
