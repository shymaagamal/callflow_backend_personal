"""
MI logging.

* App log - logger "callflow.mi" -> MI/logs/<date>/mi.log
  DailyFileHandler opens the file for the *current* date in append mode and
  switches at midnight. Several Gunicorn processes plus the worker can write
  to it safely (O_APPEND, one write per record, no rename-based rotation).
* Job log - one file per job, MI/jobs/<date>/<job_id>/job.log, DEBUG level,
  full tracebacks. Its handler is removed when the job ends.
* Every line is key=value with job_id, so `grep job_id=<id>` works across
  both files. Each finished job also appends one JSON line to
  MI/logs/<date>/timings.jsonl and writes summary.json in its folder.
"""
import json
import logging
import os
import sys
import threading
import time
import traceback
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime

from . import config, outputs

APP_LOGGER = "callflow.mi"
LINE_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | pid=%(process)d | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_setup_lock = threading.Lock()
_summary_lock = threading.Lock()


class DailyFileHandler(logging.Handler):
    """Writes to <base>/<YYYY-MM-DD>/<filename>; re-targets when the date changes."""

    def __init__(self, base_dir_fn, filename, level=logging.NOTSET):
        super().__init__(level)
        self._base_dir_fn = base_dir_fn
        self._filename = filename
        self._day = None
        self._stream = None

    def _target(self):
        day = datetime.now().strftime(outputs.DATE_FMT)
        if day != self._day or self._stream is None:
            if self._stream is not None:
                try:
                    self._stream.close()
                except Exception:
                    pass
            directory = outputs.ensure_dir(self._base_dir_fn() / day)
            self._stream = open(directory / self._filename, "a", encoding="utf-8")
            self._day = day
        return self._stream

    def emit(self, record):
        try:
            msg = self.format(record) + "\n"
            with self.lock:
                stream = self._target()
                stream.write(msg)
                stream.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        with self.lock:
            if self._stream is not None:
                try:
                    self._stream.close()
                finally:
                    self._stream = None
        super().close()


def setup_app_logging(stream=False):
    """Idempotent per process. `stream=True` also logs to stdout (worker -> journald)."""
    root = logging.getLogger(APP_LOGGER)
    with _setup_lock:
        if getattr(root, "_mi_configured", False):
            return root
        root.setLevel(getattr(logging, config.log_level(), logging.INFO))
        formatter = logging.Formatter(LINE_FORMAT, DATE_FORMAT)
        try:
            handler = DailyFileHandler(lambda: config.output_dir() / "logs", "mi.log")
            handler.setFormatter(formatter)
            root.addHandler(handler)
        except Exception:
            logging.getLogger(__name__).warning("MI app log unavailable", exc_info=True)
        if stream:
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(formatter)
            root.addHandler(sh)
        root.propagate = False
        root._mi_configured = True
    return root


def reset_app_logging_for_tests():
    root = logging.getLogger(APP_LOGGER)
    with _setup_lock:
        for h in list(root.handlers):
            root.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass
        root._mi_configured = False


def _fmt(value):
    if isinstance(value, float):
        return f"{value:.3f}"
    text = str(value)
    if not text or any(c in text for c in (" ", "=", '"', "|", "\n")):
        return json.dumps(text, ensure_ascii=False)
    return text


class JobContext:
    """Logging, timings and output folder of ONE job."""

    def __init__(self, job_id, who="unknown", complain_type=None, msisdn_count=0,
                 window=None, mode="worker", day=None):
        setup_app_logging()
        self.job_id = job_id
        self.who = who
        self.complain_type = complain_type
        self.msisdn_count = msisdn_count
        self.window = window or {}
        self.mode = mode
        self.day = day or outputs.today_str()
        self.dir = outputs.job_dir(job_id, self.day)
        self.stages = []
        self.queries = []
        self.warnings = []
        self.status = "RUNNING"
        self.http_status = None
        self.error = None
        self.smartcare_seconds = 0.0
        self._t0 = time.perf_counter()
        self.started_at = datetime.now().isoformat(timespec="seconds")
        self.app_log = logging.getLogger(APP_LOGGER + ".job")
        self.log = self._job_logger()

    def _job_logger(self):
        lg = logging.getLogger(f"{APP_LOGGER}.jobs.{self.job_id}")
        lg.setLevel(logging.DEBUG)
        lg.propagate = False
        self._handler = None
        try:
            outputs.ensure_dir(self.dir)
            fh = logging.FileHandler(self.dir / "job.log", encoding="utf-8")
            fh.setFormatter(logging.Formatter(LINE_FORMAT, DATE_FORMAT))
            lg.addHandler(fh)
            self._handler = fh
        except Exception:
            logging.getLogger(APP_LOGGER).warning("job log unavailable job_id=%s", self.job_id, exc_info=True)
        return lg

    # ---------------------------------------------------------- lifecycle
    def __enter__(self):
        self.event("job_start", who=self.who, mode=self.mode, pid=os.getpid(),
                   complain_type=self.complain_type, msisdn_count=self.msisdn_count,
                   **{f"window_{k}": v for k, v in self.window.items()})
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            self.status, self.error = "FAILED", f"{exc_type.__name__}: {exc}"
            self.exception("job_crashed", exc_info=(exc_type, exc, tb))
        elif self.status == "RUNNING":
            self.status = "DONE"
        try:
            self.write_summary()
        finally:
            self.close()
        return False

    def close(self):
        if self._handler is not None:
            self.log.removeHandler(self._handler)
            try:
                self._handler.close()
            except Exception:
                pass
            self._handler = None
        logging.Logger.manager.loggerDict.pop(self.log.name, None)

    # ------------------------------------------------------------ logging
    def event(self, _event, level=logging.INFO, **fields):
        line = " ".join([f"job_id={self.job_id}", f"event={_event}"] +
                        [f"{k}={_fmt(v)}" for k, v in fields.items()])
        self.log.log(level, line)
        if level >= logging.INFO:
            self.app_log.log(level, line)

    def debug(self, _event, **fields):
        self.event(_event, level=logging.DEBUG, **fields)

    def warning(self, _event, **fields):
        self.event(_event, level=logging.WARNING, **fields)

    def exception(self, _event, exc_info=None, **fields):
        exc_info = exc_info or sys.exc_info()
        err = exc_info[1]
        self.event(_event, level=logging.ERROR, error_type=type(err).__name__ if err else None,
                   error=str(err)[:1000] if err else None, **fields)
        if err is not None:
            self.log.error("job_id=%s traceback:\n%s", self.job_id,
                           "".join(traceback.format_exception(*exc_info)))

    def add_warning(self, code, **detail):
        """A reason the result may be partial; surfaced in the job record."""
        item = {"code": code, **detail}
        self.warnings.append(item)
        self.warning(code, **detail)

    @contextmanager
    def stage(self, name, **extra):
        start = time.perf_counter()
        self.event("stage_start", stage=name, **extra)
        record = {"stage": name, **extra}
        try:
            yield record
        except Exception:
            record["seconds"] = round(time.perf_counter() - start, 3)
            record["status"] = "failed"
            self.stages.append(record)
            self.exception("stage_failed", stage=name, seconds=record["seconds"])
            raise
        record["seconds"] = round(time.perf_counter() - start, 3)
        record.setdefault("status", "ok")
        self.stages.append(record)
        self.event("stage_done", **record)

    def record_query(self, **fields):
        self.queries.append(fields)
        level = logging.INFO if fields.get("ok", True) else logging.WARNING
        self.event("smartcare_query", level=level, **fields)

    # ------------------------------------------------------------ summary
    def elapsed(self):
        return time.perf_counter() - self._t0

    def stage_totals(self):
        totals = OrderedDict()
        for s in self.stages:
            totals[s["stage"]] = round(totals.get(s["stage"], 0.0) + s.get("seconds", 0.0), 3)
        return totals

    def summary(self):
        totals = self.stage_totals()
        leaves = {k: v for k, v in totals.items() if k not in ("query_execution", "job")}
        top = max(leaves.items(), key=lambda kv: kv[1]) if leaves else (None, 0)
        executed = [q for q in self.queries if not q.get("memo_hit") and not q.get("skipped")]
        slowest = max(executed, key=lambda q: q.get("execute_s", 0) + q.get("fetch_s", 0) + q.get("seconds", 0),
                      default=None)
        return {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "job_id": self.job_id,
            "who": self.who,
            "mode": self.mode,
            "pid": os.getpid(),
            "complain_type": self.complain_type,
            "msisdn_count": self.msisdn_count,
            "window": self.window,
            "started_at": self.started_at,
            "status": self.status,
            "http_status": self.http_status,
            "error": self.error,
            "partial": bool(self.warnings),
            "warnings": self.warnings,
            "total_s": round(self.elapsed(), 3),
            "smartcare_s": round(self.smartcare_seconds, 3),
            "smartcare_executions": len(executed),
            "top_stage": top[0],
            "top_stage_s": top[1],
            "slowest_query": slowest,
            "stages": totals,
            "queries": self.queries,
        }

    def write_summary(self):
        summary = self.summary()
        self.event("job_done", status=summary["status"], http_status=summary["http_status"],
                   total_s=summary["total_s"], smartcare_s=summary["smartcare_s"],
                   executions=summary["smartcare_executions"], partial=summary["partial"],
                   top_stage=summary["top_stage"], top_stage_s=summary["top_stage_s"])
        try:
            outputs.atomic_write_json(summary, self.dir / "summary.json")
        except Exception:
            self.app_log.warning("summary.json write failed job_id=%s", self.job_id, exc_info=True)
        try:
            path = outputs.ensure_dir(outputs.logs_dir()) / "timings.jsonl"
            line = json.dumps(outputs._json_keys(summary), default=outputs._json_default,
                              ensure_ascii=False) + "\n"
            with _summary_lock, open(path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            self.app_log.warning("timings.jsonl write failed job_id=%s", self.job_id, exc_info=True)
        return summary
