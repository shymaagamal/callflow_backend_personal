"""
Deployment self-check for MI.

    ../portalenv/bin/python -m callflow.mi.selfcheck              # config, folders, Redis, worker
    ../portalenv/bin/python -m callflow.mi.selfcheck --smartcare  # + open a Smartcare connection

Exit code 0 = all required checks passed.
"""
import os
import sys
import tempfile


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "callflow_portal.settings")
    import django
    django.setup()
    from callflow.mi import config, jobs, outputs, smartcare

    failures = 0

    def check(label, ok, detail="", required=True):
        nonlocal failures
        mark = "OK  " if ok else ("FAIL" if required else "WARN")
        if not ok and required:
            failures += 1
        print(f"[{mark}] {label}{(': ' + detail) if detail else ''}")

    root = config.output_dir()
    check("MI output dir is absolute", root.is_absolute(), str(root))
    try:
        outputs.ensure_dir(root / "logs")
        with tempfile.NamedTemporaryFile(dir=str(root), prefix=".selfcheck.", delete=True):
            pass
        check("MI output dir writable", True, str(root))
    except Exception as e:
        check("MI output dir writable", False, f"{root}: {e}")


    check("Smartcare credentials configured (MI_SMARTCARE_USER/PASSWORD)",
          bool(config.smartcare_user() and config.smartcare_password()))
    check("JDBC driver jar present", os.path.isfile(config.smartcare_jdbc_jar()), config.smartcare_jdbc_jar())

    try:
        b = jobs.backend()
        check("Redis backend", b.name == "redis", b.name)
        if b.name == "redis":
            check("queue length", True, str(b.queue_length()), required=False)
    except Exception as e:
        check("Redis backend", False, str(e))
    check("MI worker heartbeat", jobs.worker_alive(), "callflow_mi_worker.service",
          required=config.executor() == "worker" and config.worker_fallback() == "none")
    print(f"executor={config.executor()} fallback={config.worker_fallback()} "
          f"max_concurrent={config.max_concurrent()} legacy_mode={config.legacy_endpoint_mode()} "
          f"new_mode={config.new_endpoint_mode()} voice_ims_filter={config.voice_ims_filter()}")

    if "--smartcare" in argv:
        try:
            cursor = smartcare.connect()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            smartcare.close_cursor(cursor)
            check("Smartcare connection + SELECT 1", True)
        except Exception as e:
            check("Smartcare connection + SELECT 1", False, smartcare.short_error(e))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
