"""
Cross-process limits for the Prototype analysis.

Why: Gunicorn runs several *processes*. A threading.Semaphore only limits one
process, so N workers could still start N parallel Smartcare analyses. The
slots below live in Redis (the same Redis Django's cache already uses), so the
limit holds across every Gunicorn worker and the optional queue worker.

  * Slot semaphore  - at most PROTOTYPE_MAX_CONCURRENT analyses at once.
                      Leases expire (PROTOTYPE_SLOT_TTL_S) and are refreshed by
                      a heartbeat, so a killed worker cannot hold a slot forever.
  * In-flight dedup - an identical async request while one is pending/running
                      returns the existing job_id instead of starting another.
  * Pending bound   - queued + running background jobs are capped.
  * Job store       - Prototype-owned job records (longer TTL than core's
                      30 min, which is shorter than one long Smartcare query).

If Redis is not reachable the module degrades to process-local limits and
logs a warning (the request is never failed because of the limiter itself).
"""
import hashlib
import json
import logging
import threading
import time
import uuid

from django.conf import settings
from django.core.cache import cache

from . import conf

logger = logging.getLogger("callflow.prototype.concurrency")

SLOT_KEY = "prototype:slot:{}"
INFLIGHT_KEY = "prototype:inflight:{}"
PENDING_KEY = "prototype:pending"
JOB_KEY = "prototype_job:{}"
QUEUE_KEY = "prototype:queue"
WORKER_HEARTBEAT_KEY = "prototype:worker:heartbeat"

_RELEASE_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
_REFRESH_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"


# --------------------------------------------------------------------------
# Backends
# --------------------------------------------------------------------------
class _LocalBackend:
    """Process-local fallback (also used by tests)."""
    name = "local"

    def __init__(self):
        self._lock = threading.Lock()
        self._kv = {}          # key -> (value, expires_at)
        self._pending = {}     # job_id -> ts
        self._queue = []

    def _alive(self, key):
        item = self._kv.get(key)
        if item and item[1] > time.time():
            return item[0]
        self._kv.pop(key, None)
        return None

    def set_nx(self, key, value, ttl):
        with self._lock:
            if self._alive(key) is not None:
                return False
            self._kv[key] = (value, time.time() + ttl)
            return True

    def get(self, key):
        with self._lock:
            return self._alive(key)

    def delete_if(self, key, value):
        with self._lock:
            if self._alive(key) == value:
                self._kv.pop(key, None)
                return True
            return False

    def refresh_if(self, key, value, ttl):
        with self._lock:
            if self._alive(key) == value:
                self._kv[key] = (value, time.time() + ttl)
                return True
            return False

    def pending_add(self, job_id):
        with self._lock:
            self._pending[job_id] = time.time()

    def pending_remove(self, job_id):
        with self._lock:
            self._pending.pop(job_id, None)

    def pending_count(self, max_age):
        with self._lock:
            cutoff = time.time() - max_age
            for k in [k for k, ts in self._pending.items() if ts < cutoff]:
                self._pending.pop(k, None)
            return len(self._pending)

    def queue_push(self, payload):
        with self._lock:
            self._queue.insert(0, payload)

    def queue_pop(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self._queue:
                    return self._queue.pop()
            time.sleep(0.05)
        return None


class _RedisBackend:
    name = "redis"

    def __init__(self, client):
        self.r = client
        self._release = client.register_script(_RELEASE_LUA)
        self._refresh = client.register_script(_REFRESH_LUA)

    @staticmethod
    def _s(v):
        return v.decode() if isinstance(v, bytes) else v

    def set_nx(self, key, value, ttl):
        return bool(self.r.set(key, value, nx=True, ex=int(ttl)))

    def get(self, key):
        return self._s(self.r.get(key))

    def delete_if(self, key, value):
        return bool(self._release(keys=[key], args=[value]))

    def refresh_if(self, key, value, ttl):
        return bool(self._refresh(keys=[key], args=[value, int(ttl)]))

    def pending_add(self, job_id):
        self.r.zadd(PENDING_KEY, {job_id: time.time()})

    def pending_remove(self, job_id):
        self.r.zrem(PENDING_KEY, job_id)

    def pending_count(self, max_age):
        self.r.zremrangebyscore(PENDING_KEY, 0, time.time() - max_age)
        return int(self.r.zcard(PENDING_KEY))

    def queue_push(self, payload):
        self.r.lpush(QUEUE_KEY, payload)

    def queue_pop(self, timeout):
        item = self.r.brpop(QUEUE_KEY, timeout=max(1, int(timeout)))
        return self._s(item[1]) if item else None


_backend = None
_backend_lock = threading.Lock()


def backend():
    global _backend
    if _backend is not None:
        return _backend
    with _backend_lock:
        if _backend is not None:
            return _backend
        choice = str(getattr(settings, "PROTOTYPE_CONCURRENCY_BACKEND", "auto")).lower()
        if choice in ("auto", "redis"):
            try:
                from django_redis import get_redis_connection
                client = get_redis_connection("default")
                client.ping()
                _backend = _RedisBackend(client)
                return _backend
            except Exception as e:
                if choice == "redis":
                    raise
                logger.warning("prototype: Redis unavailable (%s); using process-local limits", e)
        _backend = _LocalBackend()
        return _backend


def reset_backend_for_tests():
    global _backend
    with _backend_lock:
        _backend = None


# --------------------------------------------------------------------------
# Slot semaphore
# --------------------------------------------------------------------------
class SlotBusy(Exception):
    pass


class Slot:
    """Context manager: holds one of PROTOTYPE_MAX_CONCURRENT slots."""

    def __init__(self, wait_s, on_wait=None):
        self.wait_s = wait_s
        self.on_wait = on_wait
        self.token = uuid.uuid4().hex
        self.key = None
        self.waited_s = 0.0
        self._stop = threading.Event()
        self._hb = None

    def __enter__(self):
        b = backend()
        ttl = conf.slot_ttl_s()
        start = time.monotonic()
        notified = False
        while True:
            for i in range(conf.max_concurrent()):
                key = SLOT_KEY.format(i)
                if b.set_nx(key, self.token, ttl):
                    self.key = key
                    self.waited_s = time.monotonic() - start
                    self._start_heartbeat(b, ttl)
                    return self
            if time.monotonic() - start >= self.wait_s:
                raise SlotBusy(f"Prototype analysis busy: {conf.max_concurrent()} running; "
                               f"waited {self.wait_s:.0f}s")
            if self.on_wait and not notified:
                self.on_wait()
                notified = True
            time.sleep(min(2.0, max(0.05, self.wait_s / 50)))

    def _start_heartbeat(self, b, ttl):
        interval = max(1.0, ttl / 3.0)

        def beat():
            while not self._stop.wait(interval):
                try:
                    if not b.refresh_if(self.key, self.token, ttl):
                        logger.warning("prototype: slot %s lease lost", self.key)
                        return
                except Exception:
                    logger.warning("prototype: slot heartbeat failed", exc_info=True)

        self._hb = threading.Thread(target=beat, name=f"prototype-slot-hb-{self.key}", daemon=True)
        self._hb.start()

    def __exit__(self, *exc):
        self._stop.set()
        try:
            if self.key:
                backend().delete_if(self.key, self.token)
        except Exception:
            logger.warning("prototype: slot release failed (lease will expire)", exc_info=True)
        return False


# --------------------------------------------------------------------------
# Dedup + pending bound
# --------------------------------------------------------------------------
def request_fingerprint(msisdn_list, start, end, complain_type, imsi):
    if isinstance(msisdn_list, (list, tuple)):
        msisdns = sorted(str(m).strip() for m in msisdn_list)
    else:
        msisdns = str(msisdn_list)
    raw = json.dumps([msisdns, str(start), str(end), str(complain_type), str(imsi)])
    return hashlib.sha1(raw.encode()).hexdigest()[:20]


def claim_inflight(fingerprint, job_id):
    """Returns None if claimed, or the job_id already in flight for this fingerprint."""
    b = backend()
    key = INFLIGHT_KEY.format(fingerprint)
    if b.set_nx(key, job_id, conf.job_ttl_s()):
        return None
    existing = b.get(key)
    job = get_job(existing) if existing else None
    if job and job.get("status") in ("PENDING", "RUNNING"):
        return existing
    # stale marker (job finished/expired): take it over
    if existing:
        b.delete_if(key, existing)
    return None if b.set_nx(key, job_id, conf.job_ttl_s()) else b.get(key)


def release_inflight(fingerprint, job_id):
    try:
        backend().delete_if(INFLIGHT_KEY.format(fingerprint), job_id)
    except Exception:
        logger.warning("prototype: inflight release failed", exc_info=True)


def pending_count():
    return backend().pending_count(conf.job_ttl_s())


def pending_add(job_id):
    backend().pending_add(job_id)


def pending_remove(job_id):
    try:
        backend().pending_remove(job_id)
    except Exception:
        logger.warning("prototype: pending remove failed", exc_info=True)


# --------------------------------------------------------------------------
# Job store (same field names as core job_manager, own key + TTL)
# --------------------------------------------------------------------------
def create_job(who, request_id):
    now = time.time()
    job = {
        "job_id": request_id,
        "request_id": request_id,
        "job_type": "prototype_smartcare",
        "who": who,
        "status": "PENDING",
        "step": "queued",
        "progress_percent": 0,
        "created_at": now,
        "updated_at": now,
        "result": None,
        "http_status": None,
        "error": None,
    }
    cache.set(JOB_KEY.format(request_id), job, timeout=conf.job_ttl_s())
    return job


def update_job(job_id, **fields):
    key = JOB_KEY.format(job_id)
    job = cache.get(key)
    if job is None:
        logger.warning("prototype: job %s not found on update", job_id)
        return None
    job.update(fields)
    job["updated_at"] = time.time()
    cache.set(key, job, timeout=conf.job_ttl_s())
    return job


def get_job(job_id):
    return cache.get(JOB_KEY.format(job_id))


# --------------------------------------------------------------------------
# Queue worker presence
# --------------------------------------------------------------------------
def worker_alive():
    try:
        return backend().get(WORKER_HEARTBEAT_KEY) is not None
    except Exception:
        return False
