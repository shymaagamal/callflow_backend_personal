# callflow/core/utils/file_cache.py
"""
Simple file-based cache using pickle.
Cache is keyed by a SHA-256 hash of the request inputs.
"""

import hashlib
import pickle
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Where cache files will live ──────────────────────────────────────────────
# Change this path to wherever you want (must be writable by the gunicorn user)
CACHE_DIR = Path("/home/AutomationProjects/CallFlowTool/backend/callflow_portal/cache_store")

CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _cache_path(cache_key: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{cache_key}.pkl"


def cache_get(cache_key: str):
    """Return cached value or None if missing / expired."""
    path = _cache_path(cache_key)
    if not path.exists():
        logger.debug(f"[FILE CACHE MISS] {cache_key}")
        return None
    try:
        with open(path, "rb") as f:
            entry = pickle.load(f)

        # Check TTL
        if datetime.utcnow() > entry["expires_at"]:
            logger.info(f"[FILE CACHE EXPIRED] {cache_key}")
            path.unlink(missing_ok=True)
            return None

        logger.info(f"[FILE CACHE HIT] {cache_key}")
        return entry["data"]

    except Exception as e:
        logger.warning(f"[FILE CACHE READ ERROR] {cache_key}: {e}")
        return None


def cache_set(cache_key: str, data, ttl_seconds: int = 36000):
    """Persist data to disk with a TTL."""
    path = _cache_path(cache_key)
    try:
        entry = {
            "data": data,
            "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds),
        }
        with open(path, "wb") as f:
            pickle.dump(entry, f)
        logger.info(f"[FILE CACHE SET] {cache_key} (ttl={ttl_seconds}s)")
    except Exception as e:
        logger.warning(f"[FILE CACHE WRITE ERROR] {cache_key}: {e}")


def cache_delete(cache_key: str):
    _cache_path(cache_key).unlink(missing_ok=True)


def cache_clear_expired():
    """Optional housekeeping — call from a management command or cron."""
    removed = 0
    for path in CACHE_DIR.glob("*.pkl"):
        try:
            with open(path, "rb") as f:
                entry = pickle.load(f)
            if datetime.utcnow() > entry["expires_at"]:
                path.unlink(missing_ok=True)
                removed += 1
        except Exception:
            path.unlink(missing_ok=True)
            removed += 1
    logger.info(f"[FILE CACHE CLEANUP] Removed {removed} expired entries")


# ── Key builders ──────────────────────────────────────────────────────────────

def make_splunk_cache_key(node: str, query_start: str, query_end: str) -> str:
    """
    Unique key per (node, time window).
    Same window → same key → cache hit.
    """
    raw = f"splunk|{node}|{query_start}|{query_end}"
    return "splunk_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def make_npp_cache_key(cells: list, query_start, query_end, resolution: str,  entity_type: str = "cell") -> str:
    """
    Unique key per (sorted cell list, time window, resolution).
    """
    cells_str = ",".join(sorted(str(c) for c in cells))
    raw = f"npp|{cells_str}|{query_start}|{query_end}|{resolution}|{entity_type}"
    return "npp_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def make_viavi_cache_key(sites: list, start: str, end: str) -> str:
    sites_str = ",".join(sorted(str(s) for s in sites))
    raw = f"viavi|{sites_str}|{start}|{end}"
    return "viavi_" + hashlib.sha256(raw.encode()).hexdigest()[:16]

def make_remedy_cache_key(sites: list, start, end) -> str:
    sites_str = ",".join(sorted(str(s) for s in sites))
    raw = f"remedy|{sites_str}|{start}|{end}"
    return "remedy_" + hashlib.sha256(raw.encode()).hexdigest()[:16]

def make_smartcare_cache_key(msisdn_list, start_date, end_date, complain_type: str = "Data") -> str:
    """
    Unique key for full Smartcare nodes_data (used by NPP to load it later).
    """
    msisdn_str = ",".join(sorted(str(m) for m in msisdn_list))
    raw = f"smartcare|{msisdn_str}|{start_date}|{end_date}|{complain_type}"
    return "smartcare_" + hashlib.sha256(raw.encode()).hexdigest()[:16]