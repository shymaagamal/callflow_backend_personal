"""
Data-availability driven orchestration for CallFlow.

See ANALYSIS.md in this package for the code-verified dependency analysis and
the target execution model. The public surface is intentionally small:

    from callflow.core.orchestration import Task, run_plan, TaskStatus

`scheduler` is stdlib-only and has no Django/pandas/JVM imports, so it can be
unit-tested in isolation. `pipeline` wires the real CallFlow services onto the
scheduler and is imported lazily to keep those heavy dependencies optional.
"""

from .scheduler import Task, TaskStatus, TaskOutcome, PlanResult, run_plan

__all__ = [
    "Task",
    "TaskStatus",
    "TaskOutcome",
    "PlanResult",
    "run_plan",
]
