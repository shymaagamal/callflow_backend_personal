"""
Request-scoped RCA artifact management + readiness contract.

This solves three concrete problems identified in the existing flow:

  1. RCA loads files while they are still being written  -> ATOMIC writes
     (write ``<name>.tmp`` then ``os.replace`` -> the reader only ever sees a
     complete file), plus an explicit per-system FILE status separate from the
     DATA status (see request sections 15 & 17).

  2. RCA is opened before files exist / requires manual refresh -> an explicit
     readiness manifest + ``rca_status`` the frontend can poll until READY
     (sections 12, 16, 18).

  3. Concurrent requests overwrite each other / RCA loads the wrong request's
     files -> every request writes into its OWN directory keyed by request_id
     (== the job_id), so REQ-101 and REQ-102 never collide (sections 14 & 19).

This module is import-light (stdlib only: json/os/pathlib/threading/logging) so
it is fully unit-testable without Django/pandas/JVM. The Django view passes in a
base output directory; tests pass a tmp dir.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

RCA_ROOT_DIRNAME = "rca"
MANIFEST_NAME = "manifest.json"

# Per-system DATA lifecycle (mirrors the scheduler task outcome).
DATA_PENDING = "PENDING"
DATA_PROCESSING = "PROCESSING"
DATA_COMPLETED = "COMPLETED"
DATA_FAILED = "FAILED"
DATA_SKIPPED = "SKIPPED"

# Per-system FILE lifecycle (separate from data -- section 15).
FILE_PENDING = "PENDING"
FILE_GENERATING = "GENERATING"
FILE_READY = "READY"
FILE_FAILED = "FAILED"
FILE_NOT_APPLICABLE = "NOT_APPLICABLE"  # system produced no file (e.g. no data)

# Overall RCA readiness (section 23).
RCA_NOT_STARTED = "NOT_STARTED"
RCA_GENERATING = "GENERATING"
RCA_READY = "READY"
RCA_PARTIAL = "PARTIAL"
RCA_FAILED = "FAILED"

# How the RCA HTML tool buckets files it consumes.
KPI_SYSTEMS = ("npp", "splunk", "viavi", "remedy")
FILTER_SYSTEMS = ("smartcare", "scope")   # scope: NCI site scope file (NCI/service/rca_pipeline.py)
THRESHOLD_SYSTEMS = ("threshold",)
THRESHOLD_SYSTEM = "threshold"

_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(request_id: str) -> threading.Lock:
    with _locks_guard:
        lk = _locks.get(request_id)
        if lk is None:
            lk = threading.Lock()
            _locks[request_id] = lk
        return lk


def _safe_component(value: str) -> str:
    """Reject path traversal in request_id / filename components."""
    name = os.path.basename(str(value))
    if name in ("", ".", "..") or "/" in str(value) or "\\" in str(value):
        raise ValueError(f"unsafe path component: {value!r}")
    return name


def request_dir(base_output_dir, request_id: str) -> Path:
    rid = _safe_component(request_id)
    d = Path(base_output_dir) / RCA_ROOT_DIRNAME / rid
    return d


def _manifest_path(base_output_dir, request_id: str) -> Path:
    return request_dir(base_output_dir, request_id) / MANIFEST_NAME


def _atomic_write_bytes(path: Path, content) -> None:
    """Write via a temp file + atomic replace so readers never see a partial
    file (section 17)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        content = content.encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # atomic on POSIX/NTFS within the same filesystem


def _read_manifest_raw(base_output_dir, request_id: str) -> Optional[dict]:
    p = _manifest_path(base_output_dir, request_id)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("[rca] manifest read error for %s: %s", request_id, e)
        return None


def _write_manifest(base_output_dir, request_id: str, manifest: dict) -> None:
    _atomic_write_bytes(
        _manifest_path(base_output_dir, request_id),
        json.dumps(manifest, indent=2, default=str),
    )


def init_request(base_output_dir, request_id: str, systems: List[str]) -> dict:
    """Create the request dir and a manifest with every system PENDING."""
    with _lock_for(request_id):
        d = request_dir(base_output_dir, request_id)
        d.mkdir(parents=True, exist_ok=True)
        manifest = {
            "request_id": request_id,
            "created_at": time.time(),
            "systems": {
                s: {
                    "data": DATA_PENDING,
                    "file": FILE_PENDING,
                    "filename": None,
                    "error": None,
                }
                for s in systems
            },
        }
        _write_manifest(base_output_dir, request_id, manifest)
        logger.info("request_id=%s event=rca_manifest_initialized systems=%s",
                    request_id, ",".join(systems))
        return manifest


def _update_system(base_output_dir, request_id: str, system: str, **fields) -> dict:
    with _lock_for(request_id):
        manifest = _read_manifest_raw(base_output_dir, request_id) or {
            "request_id": request_id, "created_at": time.time(), "systems": {}
        }
        entry = manifest["systems"].setdefault(
            system, {"data": DATA_PENDING, "file": FILE_PENDING, "filename": None, "error": None}
        )
        entry.update({k: v for k, v in fields.items() if v is not None})
        _write_manifest(base_output_dir, request_id, manifest)
        return manifest


def set_data_status(base_output_dir, request_id: str, system: str, status: str,
                    error: Optional[str] = None) -> None:
    _update_system(base_output_dir, request_id, system, data=status, error=error)
    logger.info("request_id=%s event=data_status system=%s status=%s",
                request_id, system, status)


def mark_generating(base_output_dir, request_id: str, system: str) -> None:
    _update_system(base_output_dir, request_id, system, file=FILE_GENERATING)
    logger.info("request_id=%s event=rca_file_generation_started system=%s",
                request_id, system)


def write_system_file(base_output_dir, request_id: str, system: str,
                      filename: str, content) -> str:
    """Atomically write a system's RCA file and mark it READY. Returns filename."""
    fname = _safe_component(filename)
    path = request_dir(base_output_dir, request_id) / fname
    mark_generating(base_output_dir, request_id, system)
    _atomic_write_bytes(path, content)
    _update_system(base_output_dir, request_id, system, file=FILE_READY, filename=fname)
    logger.info("request_id=%s event=rca_file_ready system=%s file=%s",
                request_id, system, fname)
    return fname


def write_system_files(base_output_dir, request_id: str, system: str, files) -> List[str]:
    """Same as write_system_file for a system that produces SEVERAL RCA files
    (Splunk: one per core node, because the RCA tool detects the node type from
    the filename). ``files`` = [(filename, content), ...]. The entry keeps
    ``filename`` (first file) for compatibility and lists all in ``filenames``."""
    names = [_safe_component(f) for f, _ in files]
    mark_generating(base_output_dir, request_id, system)
    for fname, (_f, content) in zip(names, files):
        _atomic_write_bytes(request_dir(base_output_dir, request_id) / fname, content)
    _update_system(base_output_dir, request_id, system, file=FILE_READY,
                   filename=names[0], filenames=names)
    logger.info("request_id=%s event=rca_file_ready system=%s files=%s",
                request_id, system, ",".join(names))
    return names


def mark_file_failed(base_output_dir, request_id: str, system: str,
                     error: Optional[str] = None) -> None:
    _update_system(base_output_dir, request_id, system, file=FILE_FAILED, error=error)
    logger.info("request_id=%s event=rca_file_failed system=%s error=%s",
                request_id, system, error)


def mark_not_applicable(base_output_dir, request_id: str, system: str) -> None:
    _update_system(base_output_dir, request_id, system, file=FILE_NOT_APPLICABLE)


def provision_threshold(base_output_dir, request_id: str, threshold_src_path) -> dict:
    """Copy the server-side thresholds.xlsx into the request dir so the RCA tool
    gets it automatically (no manual browse). Returns diagnostics for logging.

    ``threshold_src_path`` should come from Django settings (portable), not a
    hardcoded path.
    """
    info = {"path": str(threshold_src_path), "exists": False, "copied": False, "size": 0}
    try:
        src = Path(threshold_src_path)
        if not src.is_file():
            _update_system(base_output_dir, request_id, THRESHOLD_SYSTEM,
                           file=FILE_FAILED, error=f"threshold file not found: {src}")
            logger.error("request_id=%s event=threshold_missing path=%s", request_id, src)
            return info
        info["exists"] = True
        data = src.read_bytes()
        info["size"] = len(data)
        write_system_file(base_output_dir, request_id, THRESHOLD_SYSTEM, "thresholds.xlsx", data)
        info["copied"] = True
        logger.info("request_id=%s event=threshold_provisioned path=%s size=%d",
                    request_id, src, len(data))
    except Exception as e:  # pragma: no cover - defensive
        _update_system(base_output_dir, request_id, THRESHOLD_SYSTEM, file=FILE_FAILED, error=str(e))
        logger.exception("request_id=%s event=threshold_copy_failed", request_id)
    return info


def compute_rca_status(manifest: Optional[dict]) -> str:
    """Overall readiness from the per-system FILE statuses (section 23).

    A system is 'expected' to produce a file when its data COMPLETED or its file
    lifecycle already started. NOT_APPLICABLE systems (no data) are ignored.
    """
    if not manifest:
        return RCA_NOT_STARTED
    systems = manifest.get("systems", {})
    expected = []
    for _s, e in systems.items():
        # Threshold is a shared/optional input (RCA has built-in defaults); it
        # must not gate KPI/filter readiness.
        if _s in THRESHOLD_SYSTEMS:
            continue
        file_status = e.get("file")
        data_status = e.get("data")
        if file_status == FILE_NOT_APPLICABLE:
            continue
        if data_status == DATA_COMPLETED or file_status in (
            FILE_GENERATING, FILE_READY, FILE_FAILED
        ):
            expected.append(file_status)

    if not expected:
        # Nothing to generate yet: NOT_STARTED if still pending, else nothing usable.
        any_started = any(
            e.get("data") in (DATA_PROCESSING, DATA_COMPLETED)
            for e in systems.values()
        )
        return RCA_GENERATING if any_started else RCA_NOT_STARTED

    ready = sum(1 for s in expected if s == FILE_READY)
    failed = sum(1 for s in expected if s == FILE_FAILED)
    generating = sum(1 for s in expected if s in (FILE_GENERATING, FILE_PENDING))

    if generating > 0:
        return RCA_GENERATING
    if ready == len(expected):
        return RCA_READY
    if ready > 0 and failed > 0:
        return RCA_PARTIAL
    if ready == 0 and failed == len(expected):
        return RCA_FAILED
    return RCA_GENERATING


def rca_status(base_output_dir, request_id: str) -> dict:
    """The readiness contract payload (section 16)."""
    manifest = _read_manifest_raw(base_output_dir, request_id)
    status = compute_rca_status(manifest)
    files = {}
    if manifest:
        for s, e in manifest.get("systems", {}).items():
            files[s] = e.get("file")
    payload = {
        "request_id": request_id,
        "status": status,
        "files": files,
    }
    if status in (RCA_READY, RCA_PARTIAL):
        payload["rca_files_url"] = f"/callflow/api/analysis/{request_id}/rca-files"
    return payload


def list_ready_files(base_output_dir, request_id: str) -> dict:
    """Return the {kpi, filter, threshold} shape the RCA HTML tool consumes,
    including only READY files, with request-scoped download URLs (section 19)."""
    manifest = _read_manifest_raw(base_output_dir, request_id)
    kpi, filt, threshold = [], [], []
    if manifest:
        for s, e in manifest.get("systems", {}).items():
            if e.get("file") != FILE_READY or not e.get("filename"):
                continue
            for fname in (e.get("filenames") or [e["filename"]]):
                item = {
                    "name": fname,
                    "url": f"/callflow/api/analysis/{request_id}/file/{fname}",
                }
                if s in THRESHOLD_SYSTEMS:
                    threshold.append(item)
                elif s in FILTER_SYSTEMS:
                    filt.append(item)
                else:
                    kpi.append(item)
    return {"request_id": request_id, "kpi": kpi, "filter": filt, "threshold": threshold}


def resolve_file(base_output_dir, request_id: str, filename: str) -> Path:
    """Path-safe resolution for the download route; raises on traversal/missing."""
    fname = _safe_component(filename)
    path = request_dir(base_output_dir, request_id) / fname
    resolved = path.resolve()
    root = request_dir(base_output_dir, request_id).resolve()
    if root not in resolved.parents and resolved != root:
        raise ValueError("resolved path escapes request directory")
    if not resolved.is_file():
        raise FileNotFoundError(fname)
    return resolved
