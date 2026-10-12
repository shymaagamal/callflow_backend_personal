"""
Small, dependency-free helper for running independent work items concurrently
while preserving the original ordering of results.

Extracted so it can be unit-tested without pandas / jpype / a live JVM. Used by
the Smartcare query runner to execute its per-node queries in parallel instead of
one-at-a-time (each query already opens its own isolated connection), which was
the dominant cost in the pipeline (each node query took 45-120s and they ran
sequentially -> the Smartcare stage took the SUM, not the MAX).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, List, Optional, Tuple


def run_in_order(
    items: List[Any],
    run_one: Callable[[Any], Any],
    max_workers: int = 6,
    first_sync: bool = True,
    on_error: Optional[Callable[[Any, Exception], None]] = None,
) -> List[Tuple[Any, Any]]:
    """Run ``run_one(item)`` for every item, concurrently, and return
    ``[(item, result), ...]`` in the SAME order as ``items``.

    Preserving order matters: the caller concatenates the results, so keeping the
    original order guarantees byte-identical output to the old sequential loop.

    ``first_sync=True`` runs the first item on the calling thread before fanning
    the rest out. This is deliberate: the Smartcare connection factory starts the
    JVM lazily on first use, and concurrent cold starts would race on
    ``jpype.startJVM()``. Running the first item synchronously starts the JVM once,
    after which the remaining items can safely open their own connections in
    parallel. (For work with no such cold-start concern, pass ``first_sync=False``.)

    On a failure, ``on_error(item, exc)`` is invoked (if given) and that item's
    result is ``None`` — one failure never aborts the others (task isolation).
    """
    n = len(items)
    if n == 0:
        return []

    results: dict = {}
    start = 0
    if first_sync:
        try:
            results[0] = run_one(items[0])
        except Exception as e:  # noqa: BLE001
            if on_error:
                on_error(items[0], e)
            results[0] = None
        start = 1

    rest = list(range(start, n))
    if rest:
        workers = max(1, min(max_workers, len(rest)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(run_one, items[i]): i for i in rest}
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    results[i] = fut.result()
                except Exception as e:  # noqa: BLE001
                    if on_error:
                        on_error(items[i], e)
                    results[i] = None

    return [(items[i], results.get(i)) for i in range(n)]
