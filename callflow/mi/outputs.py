"""
Everything MI writes lives under CALLFLOW_OUTPUT/MI (config.output_dir()):

    MI/
      logs/<YYYY-MM-DD>/mi.log            application log, all processes (append-only)
      logs/<YYYY-MM-DD>/timings.jsonl     one JSON line per finished job
      jobs/<YYYY-MM-DD>/<job_id>/         one folder per job, never shared
          job.log  request.json  summary.json  result.json
          sc_nodes.csv  sc_nodes_before_voice.csv  sc_data.csv  matched_master.csv
          sitename_errors.log
      latest/<file>.csv                   newest copy of each export (atomic replace)

plus, for existing consumers, atomic copies of the three fixed-name CSVs in
MI_LEGACY_EXPORT_DIR (default /home/Shorouk/CallFlow).

Date-named folders mean no file is ever rotated by one process while another
is writing it (Gunicorn runs several processes); retention deletes whole
folders older than MI_LOG_RETENTION_DAYS / MI_JOB_RETENTION_DAYS.
"""
import json
import logging
import os
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from . import config

logger = logging.getLogger("callflow.mi.outputs")

DATE_FMT = "%Y-%m-%d"
LEGACY_EXPORT_NAMES = ("sc_nodes.csv", "sc_nodes_before_voice.csv", "sc_data.csv")


def today_str():
    return datetime.now().strftime(DATE_FMT)


def ensure_dir(path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def logs_dir(day=None) -> Path:
    return config.output_dir() / "logs" / (day or today_str())


def job_dir(job_id, day) -> Path:
    if not job_id or "/" in job_id or "\\" in job_id or job_id.startswith("."):
        raise ValueError("invalid job id")
    return config.output_dir() / "jobs" / day / job_id


def latest_dir() -> Path:
    return config.output_dir() / "latest"


# --------------------------------------------------------------- atomic IO
def _atomic(target: Path, write):
    target = Path(target)
    ensure_dir(target.parent)
    fd, tmp = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    os.close(fd)
    try:
        write(tmp)
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_csv(df, target, **to_csv_kwargs):
    _atomic(target, lambda tmp: df.to_csv(tmp, **to_csv_kwargs))


def atomic_copy(source, target):
    _atomic(target, lambda tmp: shutil.copyfile(source, tmp))


def _json_default(value):
    for attr in ("item", "tolist"):
        if hasattr(value, attr):
            try:
                return getattr(value, attr)()
            except Exception:
                pass
    return str(value)


def _json_keys(value):
    """json.dumps rejects numpy keys; convert keys that are not JSON-native."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if not isinstance(k, (str, int, float, bool)) and k is not None:
                k = _json_default(k)
                if not isinstance(k, (str, int, float, bool)):
                    k = str(k)
            out[k] = _json_keys(v)
        return out
    if isinstance(value, (list, tuple)):
        return [_json_keys(v) for v in value]
    return value


def atomic_write_json(obj, target):
    text = json.dumps(_json_keys(obj), default=_json_default, ensure_ascii=False, indent=1)
    _atomic(target, lambda tmp: Path(tmp).write_text(text, encoding="utf-8"))


# ------------------------------------------------------------ job exports
def export_csv(df, filename, directory: Path, job_log=None, **to_csv_kwargs):
    """
    Write `df` into the job folder, then refresh MI/latest/<filename> and (for
    the three legacy names) the legacy fixed path - both via temp file +
    os.replace, so a reader never sees a half-written file. Export failures
    are logged and never fail the job.
    """
    written = None
    try:
        written = ensure_dir(directory) / filename
        df.to_csv(written, **to_csv_kwargs)
    except Exception as e:
        written = None
        (job_log or logger).warning("export_failed file=%s dir=%s error=%s", filename, directory, e)
        return None

    if config.write_latest():
        try:
            atomic_copy(written, latest_dir() / filename)
        except Exception as e:
            (job_log or logger).warning("latest_export_failed file=%s error=%s", filename, e)

    legacy = config.legacy_export_dir()
    if legacy and filename in LEGACY_EXPORT_NAMES:
        try:
            if not Path(legacy).is_dir():
                raise FileNotFoundError(f"{legacy} does not exist")
            atomic_copy(written, Path(legacy) / filename)
        except Exception as e:
            (job_log or logger).warning("legacy_export_failed file=%s dir=%s error=%s", filename, legacy, e)
    return written


def legacy_static_log(text):
    """Optional append to the old STATIC_ROOT/CallFlow/output/logs.txt (off by default)."""
    if not config.legacy_static_log():
        return
    try:
        from django.conf import settings
        path = Path(settings.STATIC_ROOT) / "CallFlow" / "output" / "logs.txt"
        with open(path, "a") as f:
            f.write(text)
    except Exception:
        logger.warning("legacy static log write failed", exc_info=True)


# --------------------------------------------------------------- retention
def _dated_children(parent: Path):
    if not parent.is_dir():
        return
    for child in parent.iterdir():
        try:
            day = datetime.strptime(child.name, DATE_FMT).date()
        except ValueError:
            continue
        yield day, child


def sweep_retention(now=None):
    """Delete dated folders past their retention. Returns the removed paths."""
    now = now or date.today()
    removed = []
    root = config.output_dir()
    plans = [(root / "logs", config.log_retention_days()),
             (root / "jobs", config.job_retention_days())]
    for parent, days in plans:
        if days <= 0:
            continue
        cutoff = now - timedelta(days=days)
        for day, child in _dated_children(parent):
            if day < cutoff:
                shutil.rmtree(child, ignore_errors=True)
                removed.append(str(child))
    if removed:
        logger.info("retention_sweep removed=%d", len(removed))
    return removed


def sweep_if_due(interval_s=3600):
    """Run the sweep at most once per interval across all processes (marker file)."""
    marker = config.output_dir() / ".retention_last_run"
    try:
        if marker.exists() and time.time() - marker.stat().st_mtime < interval_s:
            return []
        ensure_dir(marker.parent)
        marker.touch()
        return sweep_retention()
    except Exception:
        logger.warning("retention sweep failed", exc_info=True)
        return []
