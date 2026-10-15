"""
Per-request runtime for the Prototype Smartcare analysis.

One RunContext per request gives:
  * a request_id that appears on every log line and every output file,
  * a dedicated log file  callflow_output/logs/prototype/<date>/<request_id>.log
    (handler removed when the request ends - no handler/file-descriptor leak),
  * stage timings, written as ONE JSON line per request to
    callflow_output/logs/prototype_timings.jsonl  (grep/jq friendly),
  * request-specific output files under
    callflow_output/prototype/runs/<date>/<request_id>/ plus an atomic refresh
    of the legacy fixed files (sc_nodes.csv ...) for existing consumers.

Only callflow.prototype imports this module.
"""
import contextvars
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import conf

module_logger = logging.getLogger("callflow.prototype")

_current = contextvars.ContextVar("prototype_run_context", default=None)
_summary_lock = threading.Lock()


def current():
    """The RunContext of the request running in this thread, or None."""
    return _current.get()


def _fmt(value):
    if isinstance(value, float):
        return f"{value:.3f}"
    text = str(value)
    if any(c in text for c in (" ", "=", '"', "|")):
        return json.dumps(text, ensure_ascii=False)
    return text


class RunContext:
    def __init__(self, request_id=None, who="unknown", complain_type=None,
                 msisdn_count=0, window=None, mode="sync"):
        self.request_id = request_id or uuid.uuid4().hex[:16]
        self.who = who
        self.complain_type = complain_type
        self.msisdn_count = msisdn_count
        self.window = window or {}
        self.mode = mode
        self.started_at = time.time()
        self._t0 = time.perf_counter()
        self.stages = []          # [{"stage", "seconds", ...extra}]
        self.queries = []         # one entry per Smartcare execution
        self.status = "RUNNING"
        self.http_status = None
        self.error = None
        self.smartcare_seconds = 0.0
        self.date_dir = datetime.now().strftime("%Y-%m-%d")
        self._token = None
        self._handler = None
        self.logger = self._build_logger()

    # ------------------------------------------------------------------ setup
    def _build_logger(self):
        name = f"callflow.prototype.run.{self.request_id}"
        lg = logging.getLogger(name)
        lg.setLevel(logging.DEBUG)
        lg.propagate = False
        try:
            log_dir = conf.output_dir() / "logs" / "prototype" / self.date_dir
            log_dir.mkdir(parents=True, exist_ok=True)
            self.log_path = log_dir / f"{self.request_id}.log"
            fh = logging.FileHandler(self.log_path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            lg.addHandler(fh)
            self._handler = fh
        except Exception:  # logging must never break the request
            self.log_path = None
            module_logger.warning("prototype: cannot open per-request log file", exc_info=True)
        return lg

    @property
    def run_dir(self) -> Path:
        return conf.output_dir() / "prototype" / "runs" / self.date_dir / self.request_id

    def __enter__(self):
        self._token = _current.set(self)
        self.event("request_start", who=self.who, mode=self.mode, pid=os.getpid(),
                   thread=threading.current_thread().name,
                   complain_type=self.complain_type, msisdn_count=self.msisdn_count,
                   **{f"window_{k}": v for k, v in self.window.items()})
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None and self.status == "RUNNING":
            self.status, self.error = "FAILED", str(exc)
        if self.status == "RUNNING":
            self.status = "DONE"
        try:
            self.write_summary()
        finally:
            if self._token is not None:
                _current.reset(self._token)
            self.close()
        return False

    def close(self):
        if self._handler is not None:
            self.logger.removeHandler(self._handler)
            try:
                self._handler.close()
            except Exception:
                pass
            self._handler = None
        logging.Logger.manager.loggerDict.pop(self.logger.name, None)

    # ---------------------------------------------------------------- logging
    def event(self, _event, level=logging.INFO, **fields):
        parts = [f"rid={self.request_id}", f"event={_event}"]
        parts += [f"{k}={_fmt(v)}" for k, v in fields.items()]
        line = " ".join(parts)
        self.logger.log(level, line)
        if level >= logging.WARNING:
            module_logger.log(level, line)

    def warning(self, _event, **fields):
        self.event(_event, level=logging.WARNING, **fields)

    def exception(self, _event, **fields):
        self.event(_event, level=logging.ERROR, **fields)
        self.logger.exception("traceback")

    @contextmanager
    def stage(self, name, **extra):
        start = time.perf_counter()
        self.event("stage_start", stage=name, **extra)
        record = {"stage": name, **extra}
        try:
            yield record
        except Exception as e:
            record["seconds"] = round(time.perf_counter() - start, 3)
            record["error"] = str(e)
            self.stages.append(record)
            self.event("stage_failed", level=logging.ERROR, stage=name,
                       seconds=record["seconds"], error=str(e))
            raise
        record["seconds"] = round(time.perf_counter() - start, 3)
        self.stages.append(record)
        self.event("stage_done", **{k: v for k, v in record.items()})

    def record_query(self, **fields):
        self.queries.append(fields)
        self.event("smartcare_query", **fields)

    # ---------------------------------------------------------------- summary
    def elapsed(self):
        return time.perf_counter() - self._t0

    def stage_totals(self):
        totals = OrderedDict()
        for s in self.stages:
            totals[s["stage"]] = round(totals.get(s["stage"], 0.0) + s.get("seconds", 0.0), 3)
        return totals

    def summary(self):
        totals = self.stage_totals()
        # smartcare_total wraps the per-query stages; rank the leaf stages
        leaves = {k: v for k, v in totals.items() if k != "smartcare_total"}
        top = max(leaves.items(), key=lambda kv: kv[1]) if leaves else (None, 0)
        executed = [q for q in self.queries if not q.get("memo_hit")]
        slowest = max(executed, key=lambda q: q.get("execute_s", 0) + q.get("fetch_s", 0)
                      + q.get("seconds", 0), default=None)
        return {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "request_id": self.request_id,
            "who": self.who,
            "mode": self.mode,
            "pid": os.getpid(),
            "complain_type": self.complain_type,
            "msisdn_count": self.msisdn_count,
            "window": self.window,
            "status": self.status,
            "http_status": self.http_status,
            "error": self.error,
            "total_s": round(self.elapsed(), 3),
            "smartcare_s": round(self.smartcare_seconds, 3),
            "top_stage": top[0],
            "top_stage_s": top[1],
            "slowest_query": slowest,
            "smartcare_executions": len(executed),
            "smartcare_memo_hits": len(self.queries) - len(executed),
            "stages": totals,
            "queries": self.queries,
        }

    def write_summary(self):
        summary = self.summary()
        self.event("request_done", status=summary["status"], http_status=summary["http_status"],
                   total_s=summary["total_s"], smartcare_s=summary["smartcare_s"],
                   top_stage=summary["top_stage"], top_stage_s=summary["top_stage_s"])
        try:
            path = conf.output_dir() / "logs" / "prototype_timings.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(summary, default=str, ensure_ascii=False) + "\n"
            with _summary_lock, open(path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            module_logger.warning("prototype: cannot write timing summary", exc_info=True)
        return summary

    # ----------------------------------------------------------------- output
    def dump_csv(self, df: pd.DataFrame, filename: str, **to_csv_kwargs):
        """
        Write `df` to the request-specific run folder and atomically refresh the
        legacy fixed file of the same name. Failures are logged, never raised:
        these files are debug/consumer dumps, not part of the API response.
        """
        with self.stage("write_csv", file=filename, rows=len(df)):
            written = None
            if conf.write_run_files():
                try:
                    self.run_dir.mkdir(parents=True, exist_ok=True)
                    written = self.run_dir / filename
                    df.to_csv(written, **to_csv_kwargs)
                except Exception as e:
                    written = None
                    self.warning("run_file_write_failed", file=filename, error=str(e))
            legacy_dir = conf.legacy_output_dir()
            if legacy_dir:
                try:
                    atomic_write_csv(df, Path(legacy_dir) / filename, source=written, **to_csv_kwargs)
                except Exception as e:
                    self.warning("legacy_file_write_failed", file=filename, dir=legacy_dir, error=str(e))


def atomic_write_csv(df, target: Path, source: Path = None, **to_csv_kwargs):
    """
    Write to a temp file in the target directory then os.replace() it, so a
    concurrent reader sees either the previous complete file or the new
    complete file, never a half-written one. If `source` (an already written
    identical CSV) is given it is copied instead of serialising again.
    """
    target = Path(target)
    fd, tmp = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        if source is not None and Path(source).exists():
            with os.fdopen(fd, "wb") as out, open(source, "rb") as src:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
        else:
            os.close(fd)
            df.to_csv(tmp, **to_csv_kwargs)
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def legacy_log(logs_file, text):
    """Append to the pre-existing shared logs.txt (kept for compatibility)."""
    try:
        with open(logs_file, "a") as f:
            f.write(text)
    except Exception:
        module_logger.warning("prototype: cannot append to %s", logs_file, exc_info=True)


# --------------------------------------------------------------------------
# Master sheet cache (was pd.read_excel on every request)
# --------------------------------------------------------------------------
_master_lock = threading.Lock()
_master_cache = {}   # path -> (mtime, size, DataFrame)


def get_master_sheet(path):
    """
    Same frame the old code built (read_excel + SITE_ID as stripped str),
    re-read only when the file's mtime/size changes. Callers only filter it.
    """
    path = str(path)
    st = os.stat(path)
    key = (st.st_mtime, st.st_size)
    with _master_lock:
        cached = _master_cache.get(path)
        if cached and cached[0] == key:
            return cached[1]
    df = pd.read_excel(path)
    df["SITE_ID"] = df["SITE_ID"].astype(str).str.strip()
    with _master_lock:
        _master_cache[path] = (key, df)
    return df


def clear_master_cache():
    with _master_lock:
        _master_cache.clear()
