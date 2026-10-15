"""
MI worker: runs queued MI jobs outside Gunicorn, one at a time.

    cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
    ../portalenv/bin/python -m callflow.mi.worker

Managed by systemd (deploy/callflow_mi_worker.service). Same code, settings
and virtualenv as the portal - no new dependency. Publishes a heartbeat in
Redis; the API enqueues only while it sees one (MI_WORKER_FALLBACK decides
what happens otherwise). Also runs the hourly retention sweep.

SIGTERM/SIGINT: finish the current job, then exit.
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


def main(argv=None, stop_event=None, install_signals=True, allow_local=False):
    _setup_django()
    from django import db
    from callflow.mi import jobs, outputs, runner
    from callflow.mi.mi_logging import setup_app_logging

    setup_app_logging(stream=True)
    log = logging.getLogger("callflow.mi.worker")
    stop = stop_event or threading.Event()

    def _graceful(signum, _frame):
        log.info("signal %s: finishing current job, then exiting", signum)
        stop.set()

    if install_signals:
        signal.signal(signal.SIGTERM, _graceful)
        signal.signal(signal.SIGINT, _graceful)

    backend = jobs.backend()
    if backend.name != "redis" and not allow_local:
        log.error("MI worker needs Redis (Django CACHES 'default'); got %s backend", backend.name)
        return 2

    ident = f"{socket.gethostname()}:{os.getpid()}"
    hb_ttl = 30

    def heartbeat():
        while not stop.is_set():
            try:
                backend.set(jobs.WORKER_HEARTBEAT_KEY, ident, hb_ttl)
            except Exception:
                log.warning("worker heartbeat failed", exc_info=True)
            stop.wait(hb_ttl / 3)
        try:
            backend.delete_if(jobs.WORKER_HEARTBEAT_KEY, ident)
        except Exception:
            pass

    threading.Thread(target=heartbeat, name="mi-worker-heartbeat", daemon=True).start()
    log.info("MI worker %s started (queue=%s)", ident, jobs.QUEUE_KEY)
    outputs.sweep_if_due()

    while not stop.is_set():
        try:
            raw = backend.queue_pop(timeout=5)
        except Exception:
            log.warning("queue read failed; retrying in 5s", exc_info=True)
            time.sleep(5)
            continue
        if not raw:
            outputs.sweep_if_due()
            continue
        try:
            item = json.loads(raw)
            job_id = item["job_id"]
        except (ValueError, KeyError):
            log.error("dropping malformed queue item")
            continue
        job = jobs.get_job(job_id)
        if job is None:
            log.warning("job %s expired before it was picked up", job_id)
            jobs.pending_remove(job_id)
            continue
        log.info("job %s picked up (queued %.0fs)", job_id, time.time() - (job.get("created_at") or time.time()))
        runner.execute_job(job_id, item["params"], item.get("fingerprint"), mode="worker", day=job.get("day"))
        db.close_old_connections()
        log.info("job %s finished", job_id)

    log.info("MI worker %s stopped", ident)
    return 0


if __name__ == "__main__":
    sys.exit(main())
