# Prototype Smartcare refactor: deployment

Scope: `callflow/prototype/` only. The main CallFlow app, NCI, `callflow_portal/settings.py`,
`callflow_gunicorn_start` and the Gunicorn systemd unit are **not** changed. Every new behaviour
has a default in `callflow/prototype/conf.py`, so no settings change is required.

Server paths below assume
`/home/AutomationProjects/CallFlowTool/backend/callflow_portal` (project) and `../portalenv` (venv).

```bash
cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
PY=../portalenv/bin/python
```

## 1. Pre-checks (5 minutes)

```bash
# a) Effective Gunicorn settings. callflow_gunicorn_start does not forward "$@",
#    so --workers 4 --timeout 10500 from the systemd unit are ignored.
ps -ef | grep '[g]unicorn' | grep -c 'callflow_portal.wsgi'          # expect 1 master + 3 workers
ps -o args= -p "$(systemctl show -p MainPID --value callflow_gunicorn)"

# b) Redis (used for the cross-worker limit and job records)
redis-cli -n 1 ping                                                   # PONG

# c) Is the Prototype route really what the 15-minute caller hits? (local repo has it commented out)
grep -h 'prototype/api/smartcare' /var/log/nginx/*access*.log | tail -3
cat callflow/prototype/urls.py

# d) Who reads the legacy fixed files? (they are still written, now atomically)
ls -l /home/Shorouk/CallFlow/sc_nodes*.csv /home/Shorouk/CallFlow/sc_data.csv
crontab -l; ls /etc/cron.d; grep -rl 'sc_nodes.csv' /home 2>/dev/null | head

# e) Capture a BEFORE response for a fixed past window (used in step 5)
curl -s -X POST https://<host>/callflow/prototype/api/smartcare \
  -H 'Content-Type: application/json' \
  -d '{"msisdn_list":["10XXXXXXXX"],"start_date":"2026-10-14T10:00","end_date":"2026-10-14T11:00","Complain_Type":"Data","imsi":""}' \
  > /tmp/prototype_before.json
```

## 2. Install

```bash
tar czf /root/prototype_backup_$(date +%F_%H%M).tgz callflow/prototype
# either apply the patch ...
git apply --check prototype_refactor.patch && git apply prototype_refactor.patch
# ... or copy the delivered callflow/prototype/ folder over the existing one.
find callflow/prototype -name '__pycache__' -exec rm -rf {} +
```

## 3. Test on the server (no database or Smartcare needed)

```bash
$PY manage.py check
$PY manage.py test callflow.prototype.tests -v 2      # 29 tests, uses only SimpleTestCase
```

## 4. Reload Gunicorn: pick a quiet moment

A reload kills any Prototype request still running in an old worker after Gunicorn's 30 s
graceful timeout. Reload right after a Prototype request has finished:

```bash
tail -1 callflow_output/logs/prototype_timings.jsonl 2>/dev/null   # empty before the first run
systemctl restart callflow_gunicorn
systemctl status callflow_gunicorn --no-pager | head -5
```

## 5. Verify

```bash
# same window as step 1e -> body must be identical
curl -s -D /tmp/h.txt -X POST https://<host>/callflow/prototype/api/smartcare \
  -H 'Content-Type: application/json' \
  -d '{"msisdn_list":["10XXXXXXXX"],"start_date":"2026-10-14T10:00","end_date":"2026-10-14T11:00","Complain_Type":"Data","imsi":""}' \
  > /tmp/prototype_after.json
diff <(python3 -m json.tool --sort-keys /tmp/prototype_before.json) \
     <(python3 -m json.tool --sort-keys /tmp/prototype_after.json) && echo IDENTICAL
grep -i -E 'x-prototype-request-id|server-timing' /tmp/h.txt

# where the time goes
tail -1 callflow_output/logs/prototype_timings.jsonl | python3 -m json.tool | head -40
$PY -m callflow.prototype.timings_report --last 50          # after a few hours of traffic
```

The very first request after the deploy is the one to watch: the Smartcare connection now passes
user/password/timeouts as JDBC properties (the same way `core/utils/connections.py` already does
with the same driver). If it fails with an authentication error, roll back (step 9) and send me the
`smartcare_connect_failed` line from `callflow_output/logs/prototype/<date>/<request_id>.log`.

### Files written per request

| What | Where |
|---|---|
| Per-request log (every stage, every query attempt with `execute_s`, `fetch_s`, `rows`) | `callflow_output/logs/prototype/<YYYY-MM-DD>/<request_id>.log` |
| One JSON line per request: totals, `top_stage`, `slowest_query`, executions, memo hits | `callflow_output/logs/prototype_timings.jsonl` |
| Request-specific CSVs (`sc_nodes.csv`, `sc_nodes_before_voice.csv`, `sc_data.csv`) | `callflow_output/prototype/runs/<YYYY-MM-DD>/<request_id>/` |
| Legacy fixed CSVs (unchanged names, now written via temp file + rename) | `/home/Shorouk/CallFlow/` |
| Legacy shared log (unchanged, plus a `request_id=` line) | `static/CallFlow/output/logs.txt` |

Retention (add to root's crontab):

```cron
30 3 * * * find /home/AutomationProjects/CallFlowTool/backend/callflow_portal/callflow_output/prototype/runs -mindepth 1 -maxdepth 1 -type d -mtime +7 -exec rm -rf {} +
35 3 * * * find /home/AutomationProjects/CallFlowTool/backend/callflow_portal/callflow_output/logs/prototype -mindepth 1 -maxdepth 1 -type d -mtime +14 -exec rm -rf {} +
```

## 6. Configuration (optional, add to settings.py only to change a default)

```python
# --- Prototype app (all optional) ---
PROTOTYPE_MAX_CONCURRENT = 1                  # analyses at once across ALL workers (= Smartcare sessions)
PROTOTYPE_SYNC_SLOT_WAIT_S = 120              # sync caller waits this long for a slot, then HTTP 429
PROTOTYPE_SMARTCARE_SOCKET_TIMEOUT_MS = 1_800_000   # per query; before: no timeout at all
PROTOTYPE_SMARTCARE_MAX_ATTEMPTS = 5          # unchanged default; 3 is a reasonable next step
PROTOTYPE_SMARTCARE_RETRY_BACKOFF_S = 5       # 5s, 10s, 20s ... capped at 60s (before: immediate)
PROTOTYPE_SMARTCARE_RETRY_BUDGET_S = 3600     # stop retrying after 1h of Smartcare time in one request
PROTOTYPE_MEMOIZE_QUERIES = True              # identical successful query reused within one request
PROTOTYPE_SKIP_UNUSED_DATA_QUERIES = False    # True = skip REG_Voice in the Data path (see below)
PROTOTYPE_EXECUTOR = "thread"                 # "thread" | "queue" (needs step 8)
PROTOTYPE_DEFAULT_ASYNC = False               # keep False until the caller polls (step 7)
PROTOTYPE_MAX_PENDING_JOBS = 3
PROTOTYPE_JOB_TTL_S = 6 * 3600
PROTOTYPE_LEGACY_OUTPUT_DIR = "/home/Shorouk/CallFlow"   # None = stop writing the fixed files
```

`PROTOTYPE_SKIP_UNUSED_DATA_QUERIES`: `smartcare_nodes_REG_Voice` returns only IMS columns
(`IMPU_TEL_URI1`, `p_cscf_Name`, …) that never reach the Data response, and it contains
`LEFT JOIN nethouse.cfg_ims_ne_ip F` with **no ON clause** (a cartesian join). Enabling it keeps the
Data response identical (covered by `test_skip_unused_data_queries_keeps_response`). Only the
`sc_nodes.csv` dump loses those IMS rows. Enable it after checking nobody reads those columns
from the CSV.

## 7. Free the Gunicorn workers: switch the caller to async (when you're ready)

The synchronous contract has to hold a sync worker for the whole analysis; that is inherent to sync
Gunicorn workers. To stop occupying a worker, the 15-minute caller sends `async=1` and polls:

```bash
curl -s -X POST 'https://<host>/callflow/prototype/api/smartcare?async=1' -H 'Content-Type: application/json' -d '{...same body...}'
# -> 202 {"job_id":"<id>","status":"PENDING"}
curl -s https://<host>/callflow/prototype/api/smartcare/jobs/<id>
# -> {"status":"RUNNING",...}  then  {"status":"DONE","http_status":200,"result":{ ...exact sync body... }}
```

An identical request while one is pending or running returns the same `job_id`
(`"deduplicated": true`). The sync path stays available, so the caller can switch whenever it suits.

## 8. Optional: dedicated worker process (full isolation from Gunicorn)

With `PROTOTYPE_EXECUTOR = "thread"` an async analysis runs in a background thread of the Gunicorn
worker that received the POST. That's the same pattern core already uses. The worker stays free to
serve requests, but the analysis shares that process's CPU, memory and JVM. To move it out of
Gunicorn entirely, add a separate unit (the Gunicorn unit stays untouched):

`/etc/systemd/system/callflow_prototype_worker.service`

```ini
[Unit]
Description=CallFlow Prototype analysis worker
After=network.target redis.service

[Service]
User=root
Group=root
WorkingDirectory=/home/AutomationProjects/CallFlowTool/backend/callflow_portal
Environment=DJANGO_SETTINGS_MODULE=callflow_portal.settings
Environment=PYTHONPATH=/home/AutomationProjects/CallFlowTool/backend/callflow_portal
ExecStart=/home/AutomationProjects/CallFlowTool/backend/portalenv/bin/python -m callflow.prototype.worker
Nice=10
CPUQuota=100%
MemoryMax=4G
Restart=always
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=600

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now callflow_prototype_worker
journalctl -u callflow_prototype_worker -f
# then in settings.py:  PROTOTYPE_EXECUTOR = "queue"   and restart callflow_gunicorn
```

If the worker is down, the API sees no heartbeat and falls back to the thread executor (logged as a
warning), so jobs never sit unprocessed.

## 9. Rollback

```bash
cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
rm -rf callflow/prototype && tar xzf /root/prototype_backup_<stamp>.tgz
systemctl restart callflow_gunicorn
systemctl disable --now callflow_prototype_worker 2>/dev/null
```

Redis keys used (safe to delete): `prototype:slot:*`, `prototype:inflight:*`, `prototype:pending`,
`prototype:queue`, `prototype:worker:heartbeat`, and cache keys `prototype_job:*`.
