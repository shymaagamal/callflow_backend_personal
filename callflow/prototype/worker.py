"""
Optional dedicated Prototype worker process (PROTOTYPE_EXECUTOR = "queue").

Runs Prototype analyses OUTSIDE Gunicorn, one at a time, so a long Smartcare
analysis never shares a process (CPU, memory, GIL, JVM) with the CORE/NCI API
workers. Uses the Redis that Django's cache already points at; no new Python
dependency.

    cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
    ../portalenv/bin/python -m callflow.prototype.worker

Run it under its own systemd unit (see PROTOTYPE_DEPLOYMENT.md). While it is
alive it publishes a heartbeat; the API only enqueues when the heartbeat is
present and otherwise falls back to the in-process thread executor.
"""
import json
import logging
import os
import signal
import socket
import sys
import threading
import time


def _setup_django():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "callflow_portal.settings")
    import django
    django.setup()


def main(argv=None):
    _setup_django()
    from callflow.prototype import concurrency
    from callflow.prototype.views import execute_job

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log = logging.getLogger("callflow.prototype.worker")

    stop = threading.Event()

    def _graceful(signum, _frame):
        log.info("signal %s received: finishing current job then exiting", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    backend = concurrency.backend()
    if backend.name != "redis":
        log.error("Redis is required for the queue worker (got %s backend)", backend.name)
        return 2

    ident = f"{socket.gethostname()}:{os.getpid()}"
    hb_ttl = 30

    def heartbeat():
        while not stop.is_set():
            try:
                backend.r.set(concurrency.WORKER_HEARTBEAT_KEY, ident, ex=hb_ttl)
            except Exception:
                log.warning("heartbeat failed", exc_info=True)
            stop.wait(hb_ttl / 3)
        try:
            backend.delete_if(concurrency.WORKER_HEARTBEAT_KEY, ident)
        except Exception:
            pass

    threading.Thread(target=heartbeat, name="prototype-worker-hb", daemon=True).start()
    log.info("prototype worker %s started", ident)

    while not stop.is_set():
        try:
            raw = backend.queue_pop(timeout=5)
        except Exception:
            log.warning("queue read failed; retrying in 5s", exc_info=True)
            time.sleep(5)
            continue
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except ValueError:
            log.error("dropping malformed queue item")
            continue
        log.info("job %s started", item.get("job_id"))
        execute_job(item["job_id"], item["params"], item.get("fingerprint"), mode="queue")
        log.info("job %s finished", item.get("job_id"))

    log.info("prototype worker %s stopped", ident)
    return 0


if __name__ == "__main__":
    sys.exit(main())
