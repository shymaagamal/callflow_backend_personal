# callflow/core/rca_runner/limits.py
"""
Concurrency slots shared by every Gunicorn worker on the host.

A plain threading.Semaphore only limits one worker process; Gunicorn runs 3.
Each slot is a lock file under CALLFLOW_OUTPUT/rca_engine/locks/ held with fcntl.flock,
so at most N holders exist across all processes. The OS releases the lock if a
worker dies, so a crashed job can never leave a slot taken.

On platforms without fcntl (Windows development machines) it falls back to a
per-process semaphore, which is enough for a single runserver process.
"""

import contextlib
import os
import threading
import time
from pathlib import Path

try:
    import fcntl  # POSIX only
except ImportError:  # pragma: no cover - Windows dev
    fcntl = None

_local_sems = {}
_local_guard = threading.Lock()


class SlotTimeout(RuntimeError):
    """No slot became free within the wait limit."""


def _local_sem(name, size):
    with _local_guard:
        sem = _local_sems.get(name)
        if sem is None:
            sem = _local_sems[name] = threading.BoundedSemaphore(size)
        return sem


@contextlib.contextmanager
def slot(lock_dir, name, size, wait_seconds, on_wait=None, poll_seconds=2.0):
    """Hold one of ``size`` slots called ``name``; wait up to ``wait_seconds``.

    ``on_wait(waited_seconds)`` is called while queued (e.g. to update the job step).
    Raises SlotTimeout if no slot frees up in time.
    """
    size = max(1, int(size))
    deadline = time.monotonic() + wait_seconds
    if fcntl is None:
        sem = _local_sem(name, size)
        started = time.monotonic()
        while not sem.acquire(timeout=poll_seconds):
            if on_wait:
                on_wait(time.monotonic() - started)
            if time.monotonic() >= deadline:
                raise SlotTimeout(f"no free '{name}' slot after {wait_seconds:.0f}s")
        try:
            yield 0
        finally:
            sem.release()
        return

    Path(lock_dir).mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    while True:
        for i in range(size):
            fh = open(Path(lock_dir) / f"{name}_{i}.lock", "a+")
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                fh.close()
                continue
            try:
                yield i
            finally:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                finally:
                    fh.close()
            return
        if on_wait:
            on_wait(time.monotonic() - started)
        if time.monotonic() >= deadline:
            raise SlotTimeout(f"no free '{name}' slot after {wait_seconds:.0f}s")
        time.sleep(poll_seconds)


def default_lock_dir(output_dir):
    return Path(output_dir) / "rca_engine" / "locks"
