"""
MI job management on the Redis that Django's cache already uses (no new
dependency):

  * job records    cache key "mi_job:<id>" (Django cache), TTL MI_JOB_TTL_S
  * queue          Redis list "mi:queue", consumed by callflow.mi.worker
  * slots          "mi:slot:<n>" leases -> at most MI_MAX_CONCURRENT jobs run
                   at once across every Gunicorn worker and MI worker process
  * dedup          "mi:inflight:<fingerprint>" -> identical submissions while a
                   job is queued/running return that job
  * queue bound    sorted set "mi:pending" -> at most MI_MAX_QUEUED_JOBS
  * heartbeats     worker: "mi:worker:heartbeat"; job: job["heartbeat_at"]

If Redis is unreachable, a process-local backend is used and a warning logged.
"""
import hashlib
import json
import logging
import threading
import time
import uuid

from django.core.cache import cache

from . import config

logger = logging.getLogger("callflow.mi.jobs")

SLOT_KEY = "mi:slot:{}"
INFLIGHT_KEY = "mi:inflight:{}"
PENDING_KEY = "mi:pending"
QUEUE_KEY = "mi:queue"
WORKER_HEARTBEAT_KEY = "mi:worker:heartbeat"
JOB_KEY = "mi_job:{}"

ACTIVE = ("PENDING", "RUNNING")

_RELEASE_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
_REFRESH_LUA = ("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) "
                "else return 0 end")


# --------------------------------------------------------------- backends
class LocalBackend:
    name = "local"

    def __init__(self):
        self._lock = threading.Lock()
        self._kv = {}
        self._pending = {}
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

    def set(self, key, value, ttl):
        with self._lock:
            self._kv[key] = (value, time.time() + ttl)

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

    def queue_length(self):
        with self._lock:
            return len(self._queue)


class RedisBackend:
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

    def set(self, key, value, ttl):
        self.r.set(key, value, ex=int(ttl))

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

    def queue_length(self):
        return int(self.r.llen(QUEUE_KEY))


_backend = None
_backend_lock = threading.Lock()


def backend():
    global _backend
    if _backend is not None:
        return _backend
    with _backend_lock:
        if _backend is None:
            choice = config.concurrency_backend()
            if choice in ("auto", "redis"):
                try:
                    from django_redis import get_redis_connection
                    client = get_redis_connection("default")
                    client.ping()
                    _backend = RedisBackend(client)
                except Exception as e:
                    if choice == "redis":
                        raise
                    logger.warning("Redis unavailable (%s); MI uses process-local limits", e)
            if _backend is None:
                _backend = LocalBackend()
    return _backend


def set_backend_for_tests(value=None):
    global _backend
    with _backend_lock:
        _backend = value


# ------------------------------------------------------------------ slots
class SlotBusy(Exception):
    pass


class Slot:
    """Holds one of MI_MAX_CONCURRENT slots; the lease is kept alive by a heartbeat."""

    def __init__(self, wait_s, on_wait=None):
        self.wait_s = wait_s
        self.on_wait = on_wait
        self.token = uuid.uuid4().hex
        self.key = None
        self.waited_s = 0.0
        self._stop = threading.Event()

    def __enter__(self):
        b = backend()
        ttl = config.slot_ttl_s()
        start = time.monotonic()
        notified = False
        while True:
            for i in range(config.max_concurrent()):
                if b.set_nx(SLOT_KEY.format(i), self.token, ttl):
                    self.key = SLOT_KEY.format(i)
                    self.waited_s = time.monotonic() - start
                    threading.Thread(target=self._beat, args=(b, ttl), daemon=True,
                                     name=f"mi-slot-{i}").start()
                    return self
            if time.monotonic() - start >= self.wait_s:
                raise SlotBusy(f"MI analysis busy: {config.max_concurrent()} running, waited {self.wait_s:.0f}s")
            if self.on_wait and not notified:
                self.on_wait()
                notified = True
            time.sleep(min(2.0, max(0.05, self.wait_s / 50)))

    def _beat(self, b, ttl):
        while not self._stop.wait(max(1.0, ttl / 3.0)):
            try:
                if not b.refresh_if(self.key, self.token, ttl):
                    logger.warning("slot %s lease lost", self.key)
                    return
            except Exception:
                logger.warning("slot heartbeat failed", exc_info=True)

    def __exit__(self, *exc):
        self._stop.set()
        try:
            if self.key:
                backend().delete_if(self.key, self.token)
        except Exception:
            logger.warning("slot release failed (lease will expire)", exc_info=True)
        return False


# ------------------------------------------------------------ job records
def new_job_id():
    return uuid.uuid4().hex[:16]


def create_job(job_id, who, params, endpoint, day):
    now = time.time()
    job = {
        "job_id": job_id,
        "job_type": "mi_smartcare",
        "who": who,
        "endpoint": endpoint,
        "status": "PENDING",          # PENDING -> RUNNING -> DONE | FAILED
        "step": "queued",
        "progress_percent": 0,
        "created_at": now,
        "updated_at": now,
        "heartbeat_at": None,
        "day": day,
        "complain_type": params.get("Complain_Type"),
        "msisdn_count": len(params.get("msisdn_list") or []),
        "result": None,
        "http_status": None,
        "error": None,
        "partial": False,
        "warnings": [],
        "timings": None,
    }
    cache.set(JOB_KEY.format(job_id), job, timeout=config.job_ttl_s())
    return job


def update_job(job_id, **fields):
    key = JOB_KEY.format(job_id)
    job = cache.get(key)
    if job is None:
        logger.warning("job %s not found on update (expired?)", job_id)
        return None
    job.update(fields)
    job["updated_at"] = time.time()
    cache.set(key, job, timeout=config.job_ttl_s())
    return job


def get_job(job_id):
    job = cache.get(JOB_KEY.format(job_id))
    if job and job.get("status") == "RUNNING":
        beat = job.get("heartbeat_at") or job.get("updated_at") or 0
        if time.time() - beat > config.job_stale_s():
            job = update_job(job_id, status="FAILED", step="worker_lost", http_status=500,
                             error="MI worker stopped while running this job",
                             result={'error': "MI worker stopped while running this job"}) or job
    return job


class JobHeartbeat:
    """Refreshes job["heartbeat_at"] while a job runs, so a dead worker is detectable."""

    def __init__(self, job_id, interval=30):
        self.job_id = job_id
        self.interval = interval
        self._stop = threading.Event()

    def __enter__(self):
        update_job(self.job_id, heartbeat_at=time.time())
        threading.Thread(target=self._run, daemon=True, name=f"mi-job-hb-{self.job_id}").start()
        return self

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                update_job(self.job_id, heartbeat_at=time.time())
            except Exception:
                logger.warning("job heartbeat failed", exc_info=True)

    def __exit__(self, *exc):
        self._stop.set()
        return False


# ------------------------------------------------------- dedup / queueing
def fingerprint(params):
    msisdns = params.get("msisdn_list")
    msisdns = sorted(str(m).strip() for m in msisdns) if isinstance(msisdns, (list, tuple)) else str(msisdns)
    raw = json.dumps([msisdns, str(params.get("start_date")), str(params.get("end_date")),
                      str(params.get("Complain_Type")), str(params.get("imsi"))])
    return hashlib.sha1(raw.encode()).hexdigest()[:20]


def claim_inflight(fp, job_id):
    """None when claimed for job_id; otherwise the id of the active identical job."""
    b = backend()
    key = INFLIGHT_KEY.format(fp)
    for _ in range(2):
        if b.set_nx(key, job_id, config.job_ttl_s()):
            return None
        existing = b.get(key)
        job = get_job(existing) if existing else None
        if job and job.get("status") in ACTIVE:
            return existing
        if existing:
            b.delete_if(key, existing)          # finished / expired / lost -> take over
    return b.get(key)


def release_inflight(fp, job_id):
    try:
        backend().delete_if(INFLIGHT_KEY.format(fp), job_id)
    except Exception:
        logger.warning("inflight release failed", exc_info=True)


def pending_count():
    return backend().pending_count(config.job_ttl_s())


def pending_add(job_id):
    backend().pending_add(job_id)


def pending_remove(job_id):
    try:
        backend().pending_remove(job_id)
    except Exception:
        logger.warning("pending remove failed", exc_info=True)


def enqueue(job_id, params, fp):
    backend().queue_push(json.dumps({"job_id": job_id, "params": params, "fingerprint": fp}))


def worker_alive():
    try:
        return backend().get(WORKER_HEARTBEAT_KEY) is not None
    except Exception:
        return False
