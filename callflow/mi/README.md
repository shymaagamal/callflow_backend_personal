# MI (formerly Prototype): design, deployment, rollback

## 1. Root causes found (production `logs.txt`, 15 Oct 2026)

| Symptom in the log | Cause in the old code | MI behaviour |
|---|---|---|
| `KeyError: 'VPCRF_NAME'` (36 times, ~7 per request) | SCF returns rows whose `VPCRF_NAME` is all null, so the fallback **re-runs the same SCF query** and merges `VPCRF_NAME` again. pandas renames both columns to `VPCRF_NAME_x`/`_y`, and `distinct_count(..., 'VPCRF_NAME')` raises. Everything computed after it (RAT, region, subregion, TAC, SGSN, MME, sites, SGWs) comes back **empty**. The 6-attempt loop repeats the identical crash. | No fallback queries any more (removed 2026-10-23), so there is no re-merge. Every metric is computed independently. Result: full Data response, `PCRF={}`. |
| `KeyError: 'Voice_Data_Flag'` (Voice) | The Voice DRA/OCS/PCRF queries return `Service_flag` (`'VOLTE'`/`'Data'`), but the aggregation filters on `Voice_Data_Flag == 'Voice'`. That column only comes from the SCF fallback, which only yields `'Data'` (`WHERE left(MS_IP,3)='10.'`). The result is a crash when SCF is empty, and silently empty Voice DRA/OCS/PCRF otherwise. | DRA/OCS/PCRF are now counted as is from the rows `smartcare_DRA_Voice` / `_OCS_Voice` / `_PCRF_Voice` return (distinct MSISDN per name, no row filter). Since 2026-10-23 there is no flag. |
| `Table or view not found: ps.DETAIL_CDR_SGS_20463`, `cs.TDR_IMS_HSS_TRANS_20463` | 20463 = **2026-01-10**: a 9-month-old window. Missing tables were retried 5×. | Classified as `unavailable`, never retried, reported as `query_unavailable` (partial result). |
| `SocketTimeoutException: timeout`, "all 5 attempts" | The Prototype passed its timeouts in jaydebeapi's `libs` argument (ignored). Retries were immediate, plus 4 more full retry cycles from the fallbacks. | Socket/connect timeouts applied, 3 attempts with backoff (10 s, 20 s), reconnect after a failure, retry budget per job, no fallback re-runs. An SCF timeout costs 3 executions instead of 25. |
| Two "Smartcare Request" lines in the same second | No duplicate protection | Identical submissions while a job is queued or running return that job. |
| (not in the log) | A window crossing midnight never read the next day's table. One JDBC connection leaked per request. All rows printed to stdout. | Every calendar day read (`MI_FIX_MIDNIGHT_TABLES`). One connection per job, always closed. `logging` only. |

`golden_legacy_responses.json` (tests) is the original code's output on the same fixtures. The tests
check that MI returns the same body wherever the old code did not crash, and the complete body where
it did.

## 2. Structure (file by file)

```
callflow/mi/
  __init__.py       package doc (not a Django app: no models, migrations, INSTALLED_APPS entry)
  config.py         every MI_* setting with its default (settings.py > environment/.env > default)
  validation.py     request parsing + rules/messages of the old view; DataFrame column report
  Queries/          Smartcare SQL: data_queries.smartcare_Data_QUERIES, voice_queries.smartcare_Voice_QUERIES
                    (your package; MI only needs those two dicts)
  catalog.py        query sets per Complain_Type, CS subscriber-column mapping, columns each metric needs
  smartcare.py      JDBC connection, error classification, per-day tables, retries, memo
  pipeline.py       stages: validation -> connection -> queries -> data validation
                    -> aggregation -> hub metrics -> result generation
  aggregation.py    Data / Voice metrics (same pandas expressions), guarded per metric
  hub.py            HUB metrics (unchanged logic and return shapes), cached master sheet
  response.py       response bodies, exact keys and order
  jobs.py           Redis job records, queue, dedup, slots, heartbeats (local fallback)
  runner.py         runs one job: slot -> heartbeat -> pipeline -> job record
  worker.py         the MI worker process (systemd)
  views.py          submit / status / result endpoints
  urls.py           MI routes + legacy_urlpatterns for the old path
  mi_logging.py     daily app log, per-job log, stage timings, timings.jsonl
  outputs.py        CALLFLOW_OUTPUT/MI layout, atomic writes, legacy copies, retention
  selfcheck.py      `python -m callflow.mi.selfcheck [--smartcare]`
  report.py         `python -m callflow.mi.report` (where the time goes)
  deploy/           callflow_mi_worker.service, env.example
  tests/            45 tests, fake Smartcare, golden legacy responses, test URLconf

callflow/prototype/
  __init__.py       deprecation note
  urls.py           1-line shim -> callflow.mi.urls.legacy_urlpatterns
  (apis.py, views.py, constants.py, services/ removed: moved to mi/ or unused)

callflow/urls.py    +1 line: path("mi/api/", include("callflow.mi.urls"))
```

Nothing in CORE, NCI, `callflow/core`, `settings.py`, the Gunicorn script or its unit changes.
Before renaming I checked these: `INSTALLED_APPS` (Prototype not listed, so no models, tables or
migrations), Angular `src/` (no reference), templates and static assets (none), scripts (none), the
Gunicorn start script and unit (none). `Shourok_Code.py` still writes to `static/CallFlow/output/`,
but nothing imports it, so it was left alone. `prototype/services/kpi_analyzer.py` and
`kpi_thresholds.py` were imported by the old view but never used.

## 3. Endpoints

| Path | Default | Notes |
|---|---|---|
| `POST /callflow/prototype/api/smartcare` | **synchronous**, the old body and error codes | `?async=1` → 202 + `job_id`. The heavy work runs in the MI worker; the request only waits. |
| `POST /callflow/mi/api/smartcare` | **202** `{"job_id","status","status_url","result_url"}` | validation errors → 400 before a job exists |
| `GET …/smartcare/jobs/<id>` | job record: `status` (PENDING/RUNNING/DONE/FAILED), `step`, `partial`, `warnings`, `timings`, `output_dir`, `result` | works on both prefixes |
| `GET …/smartcare/jobs/<id>/result` | the exact analysis body with its original HTTP status; 202 while running | drop-in for consumers of the sync body |

Headers: `X-MI-Job-Id`, and on finished jobs `X-MI-Partial: true|false` and `X-MI-Warnings: <codes>`.
The response **body** is unchanged, so existing consumers are unaffected.

**Important:** a synchronous caller still holds one Gunicorn worker while it waits, although it
uses no CPU and no Smartcare connection, because the work happens in the MI worker. To stop holding
CORE/NCI workers, the 15-minute caller should POST with `?async=1` (or to `/callflow/mi/api/smartcare`)
and poll `result_url`.

## 4. Outputs and logs: `CALLFLOW_OUTPUT/MI/`

Default `…/callflow_portal/callflow_output/MI` (`settings.OUTPUT_DIR/MI`); override with an absolute
`MI_OUTPUT_DIR`.

```
MI/logs/<YYYY-MM-DD>/mi.log           all MI processes; every line has job_id=, event=
MI/logs/<YYYY-MM-DD>/timings.jsonl    one JSON line per job (stages, queries, warnings)
MI/jobs/<YYYY-MM-DD>/<job_id>/
    job.log              DEBUG: every stage, query, table, attempt, duration, full tracebacks
    request.json  result.json  summary.json
    sc_nodes.csv  sc_nodes_before_voice.csv  sc_data.csv  matched_master.csv  sitename_errors.log
MI/latest/<file>.csv                  newest copy (atomic) - new stable path for consumers
```

- **Concurrency:** each job writes only to its own folder. The shared app log uses append-only,
  date-named files, so there are no rotation races between Gunicorn processes.
- **Retention:** folders older than `MI_LOG_RETENTION_DAYS` (14) or `MI_JOB_RETENTION_DAYS` (7) are
  deleted by an hourly sweep (in the worker and after each job). No logrotate entry is needed.
- **Compatibility:**
  - `/home/Shorouk/CallFlow/sc_nodes.csv`, `sc_nodes_before_voice.csv` and `sc_data.csv` are still
    written, atomically. Set `MI_LEGACY_EXPORT_DIR=` empty once no one reads them.
  - The old `static/CallFlow/output/logs.txt` is **no longer written**. `MI_LEGACY_STATIC_LOG=True`
    restores it if something tails it.
  - `matched_master_Prototype_<ts>.csv` in `callflow_output/` now lives in the job folder as
    `matched_master.csv`.

Diagnose one job: `grep job_id=<id> MI/logs/*/mi.log`, then read `MI/jobs/*/<id>/job.log`.
Over many jobs: `python -m callflow.mi.report`.

## 5. Running the jobs: options

**Option A, recommended: a separate worker within the existing Django project.**
`callflow_mi_worker.service` runs `python -m callflow.mi.worker` from the same code, `.env` and
virtualenv, and consumes a Redis list on the Redis that Django's cache already uses. It needs no new
package, no nginx change and no Gunicorn change.
- It runs one job at a time (`MI_MAX_CONCURRENT=1`, enforced across all processes), so there is one
  Smartcare session.
- It runs at `Nice=10` with `CPUQuota=100%`, `MemoryMax=4G` and its own JVM capped by
  `MI_JVM_MAX_HEAP=1g`.
- If the worker is down, the API sees no heartbeat and runs the job locally (`MI_WORKER_FALLBACK=local`)
  so nothing stalls. Set `none` once you trust the worker.

**Option B: a fully separate MI service.** A second Gunicorn unit (for example 2 workers on
`run/callflow_mi_gunicorn.sock`) serving the same project, with nginx sending `/callflow/mi/` and
`/callflow/prototype/` to it, plus the Option A worker. It is justified when:
- the synchronous caller cannot move to async soon, because its waits would then occupy MI's own
  Gunicorn workers instead of CORE/NCI's;
- MI needs independent restarts or deploys;
- MI needs a different host (for example close to Smartcare);
- MI request volume grows enough to need its own scaling.

Its cost is shared nginx configuration and a second API process to operate. Start with A, and move
to B only if one of those applies.

### No fallback queries (since 2026-10-23)

Each Complain_Type runs only its own queries: Data runs 4 (S1MME, REG_Voice, GBIUPS, SCF) and Voice
runs 10. The Prototype re-ran SCF and S1MME for missing columns, including for Voice.
- **Voice:** that re-run never changed the response.
- **Data:** the re-run only happened when SCF or S1MME had failed, which is now reported as
  `query_failed` instead of being retried through a fallback.

### Java values from JDBC (since 2026-10-23)

jaydebeapi returns VARCHAR columns as `java.lang.String` objects when the calling thread isn't
attached to the JVM, and MI's query threads weren't. Those objects:
- break pandas `.str` methods;
- never match Python strings in `isin()` or dict lookups (server result:
  `HUB_Counts {"subs_per_hub": []}` because no `CELL_NAME` "matched");
- cannot be pickled into the Redis job record.

MI now does what `core/services/smartcare_query_runner.py` does: each thread attaches itself to the
JVM. Every fetched value and column name is also converted to plain Python (`smartcare.to_python`):
strings become `str`, Long/Integer become `int`, Double/BigDecimal become `float`, anything else
becomes `str`. The job result is converted again before it is stored (`to_python_deep`, also
available as `hub.to_native`).

Only `Data` and `Voice` are accepted as `Complain_Type`; anything else returns 400
`{"error": "Complain_Type must be Data or Voice"}` before a job is created. Each job also writes
`sc_nodes_raw.csv`, the combined query result before aggregation.

### Your Queries package (reviewed 2026-10-23)

- **Voice:** 7 queries. `smartcare_DRA_Voice` now returns DRA, OCS and PCRF together; MI counts its
  `PCRF_NAME` for `PCRF` (the old separate query called it `VPCRF_NAME`; either name works).
  `smartcare_nodes_REG_Voice` is no longer a Voice query, so `CSCF_Name` and `AS_Name` (from
  `p_cscf_Name` / `AS_NAME` per `IMPU_TEL_URI1`) are always `{}` and reported as `metric_unavailable`.
- **Data:** `smartcare_SGIDiameter_nodes` was added. Its `G`, `K1` and `G1` joins used `ON K.NE_IP = …`
  (cartesian joins, wrong names), and `VPCRF_NAME` took `K`/`G` (OCS tables). Fixed to
  `G.NE_IP` / `K1.NE_IP` / `G1.NE_IP` and `COALESCE(H, K1, G1)`, as in `smartcare_DRA_Voice`.
  Data DRA/OCS/PCRF still count only rows with `Voice_Data_Flag == 'Data'`, which only SCF returns, so
  this query does not change the Data response yet.

### Voice DRA / OCS / PCRF (since 2026-10-23)

These come straight from what `smartcare_DRA_Voice`, `smartcare_OCS_Voice` and `smartcare_PCRF_Voice`
return: the number of distinct MSISDNs per `DRA_NAME` / `OCS_NAME` / `VPCRF_NAME`, with no row filter.
That includes rows where the query's `Service_flag` is `'Data'`. Rows from other queries don't carry
these columns and are ignored (empty names are skipped). If one of the three queries fails, only its
metric is empty and the job reports `query_failed` / `query_unavailable`.

### CS subscriber column (since 2026-10-23)

`smartcare_network_CS_MOC` returns `callerno` and `smartcare_network_CS_MTC` returns `calledno`.
These are the requested MSISDNs, but neither query has a column named `MSISDN`. Every CS row was
therefore counted as the same empty subscriber: all Voice counts were 1, and `HUB_Counts` could count
no one. MI renames these columns to `MSISDN` (logged as `event=subscriber_column_mapped`).

When `HUB_Counts` is empty, the job log now says why:
- `hub_no_overlap`: no row has one of the affected cells;
- `hub_no_site_match`: the sites are not `SITE_ID`s in the master sheet (with sample sites and the
  sheet's path);
- `hub_no_subscribers`: the matched rows have no MSISDN or no Hub.

### Parallel queries

Within a job, the main queries run in parallel (`MI_QUERY_PARALLELISM`, default 4), each on its own
JDBC session. Results are put back in the original query order, so concatenation and every metric
are identical to sequential execution (`ParallelTests` compares 1 vs 5 on all scenarios: body,
`sc_nodes.csv` and the executions).

On the 15 Oct production job (Voice, 0 rows) the job took 653 s: 263 s of queries plus 388 s of
fallbacks. Fallbacks are now removed, and at parallelism 4 the job becomes roughly 101 s (the longest
single query, DRA). Two caveats:
- Smartcare may slow individual queries down when more run at once.
- Something in Smartcare or its driver ends queries at 120 s, and DRA already takes 101 s.

Start with 4, watch `python -m callflow.mi.report`, and lower the value if you see more `kind=timeout`.

## 6. Deployment (Option A)

```bash
cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
PY=../portalenv/bin/python

# 0. backup + capture a reference response for a fixed past window
tar czf /root/callflow_prototype_backup_$(date +%F_%H%M).tgz callflow/prototype callflow/urls.py
curl -s -X POST https://<host>/callflow/prototype/api/smartcare -H 'Content-Type: application/json' \
  -d '{"msisdn_list":["10XXXXXXXX"],"start_date":"2026-10-14T10:00","end_date":"2026-10-14T11:00","Complain_Type":"Voice","imsi":""}' \
  > /tmp/mi_before.json

# 1. credentials (previously hard-coded in callflow/prototype/constants.py)
grep -n "db_user\|db_password" callflow/prototype/constants.py      # copy the values
cat >> .env <<'EOF'
STCC_SMARTCARE_DB_USER=<Smartcare user>
STCC_SMARTCARE_DB_PASSWORD=<value>
EOF

# 2. code (patch or copy callflow/mi/, callflow/prototype/, callflow/urls.py)
git apply --check mi_refactor.patch && git apply mi_refactor.patch
find callflow -name __pycache__ -prune -exec rm -rf {} +

# 3. checks
$PY manage.py check
$PY manage.py test callflow.mi.tests            # 45 tests, no DB/Redis/Smartcare needed
$PY -m callflow.mi.selfcheck --smartcare        # dirs, credentials, Redis, real Smartcare SELECT 1

# 4. worker
cp callflow/mi/deploy/callflow_mi_worker.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now callflow_mi_worker
systemctl status callflow_mi_worker --no-pager; journalctl -u callflow_mi_worker -n 20 --no-pager

# 5. Gunicorn reload, right after a Prototype request has finished
#    (a reload kills in-flight requests after 30 s)
systemctl restart callflow_gunicorn
$PY -m callflow.mi.selfcheck                     # "MI worker heartbeat" should be OK now

# 6. verify
curl -s -D /tmp/h -X POST https://<host>/callflow/prototype/api/smartcare -H 'Content-Type: application/json' \
  -d '{...same body as step 0...}' > /tmp/mi_after.json
grep -i '^x-mi-' /tmp/h
diff <(python3 -m json.tool --sort-keys /tmp/mi_before.json) <(python3 -m json.tool --sort-keys /tmp/mi_after.json)
#   Voice/Data with no crash in the old code -> identical.
#   Data where the old log showed KeyError 'VPCRF_NAME' -> MI returns the metrics the old code lost.
ls callflow_output/MI/jobs/$(date +%F)/; tail -3 callflow_output/MI/logs/$(date +%F)/mi.log
$PY -m callflow.mi.report
```

After deployment, have the 15-minute caller switch to `?async=1` + polling `result_url`. Then set
`MI_LEGACY_ENDPOINT_MODE=async` only if every caller of the old path polls.

## 7. Rollback

```bash
cd /home/AutomationProjects/CallFlowTool/backend/callflow_portal
systemctl disable --now callflow_mi_worker && rm /etc/systemd/system/callflow_mi_worker.service && systemctl daemon-reload
rm -rf callflow/mi callflow/prototype
tar xzf /root/callflow_prototype_backup_<stamp>.tgz          # restores prototype/ and callflow/urls.py
find callflow -name __pycache__ -prune -exec rm -rf {} +
systemctl restart callflow_gunicorn
# optional cleanup: redis-cli -n 1 --scan --pattern 'mi:*' | xargs -r redis-cli -n 1 del
```

The `.env` lines and `callflow_output/MI/` are harmless to keep. Nothing in the database changed.

## 8. Settings reference (all optional)

| Setting | Default | Meaning |
|---|---|---|
| `MI_OUTPUT_DIR` | `OUTPUT_DIR/MI` | absolute base for all MI logs/outputs |
| `MI_LEGACY_EXPORT_DIR` | `/home/Shorouk/CallFlow` | fixed-name CSV copies; empty = off |
| `MI_WRITE_LATEST` | True | `MI/latest/*.csv` |
| `MI_LEGACY_STATIC_LOG` | False | old `static/CallFlow/output/logs.txt` |
| `MI_LOG_RETENTION_DAYS` / `MI_JOB_RETENTION_DAYS` | 14 / 7 | retention |
| `STCC_SMARTCARE_DB_USER` / `STCC_SMARTCARE_DB_PASSWORD` | – (required) | Smartcare account (.env) |
| `MI_SMARTCARE_SOCKET_TIMEOUT_MS` | 1 800 000 | per query (same as core) |
| `MI_SMARTCARE_MAX_ATTEMPTS` | 3 | was 5 with no backoff |
| `MI_SMARTCARE_RETRY_BACKOFF_S` / `_MAX_S` | 10 / 120 | exponential backoff |
| `MI_SMARTCARE_RETRY_BUDGET_S` | 2700 | no more retries after 45 min of Smartcare time |
| `MI_QUERY_PARALLELISM` | 4 | Smartcare queries of one job running at once, each on its own session; 1 = sequential. Sessions open at once = `MI_MAX_CONCURRENT` × this |
| `MI_JVM_MAX_HEAP` | 1g | `-Xmx` when MI starts the JVM (worker) |
| `MI_FIX_MIDNIGHT_TABLES` | True | read every day touched by the window |
| `MI_SKIP_INEFFECTIVE_QUERIES` | False | skip REG_Voice in the Data path; same response, smaller CSV dump |
| `MI_MAP_CS_SUBSCRIBER_TO_MSISDN` | True | CS_MOC `callerno` / CS_MTC `calledno` → `MSISDN` (False = old behaviour: every Voice count is 1) |
| `MI_EXECUTOR` | worker | worker / thread / inline |
| `MI_WORKER_FALLBACK` | local | local / none |
| `MI_ENDPOINT_MODE` / `MI_LEGACY_ENDPOINT_MODE` | async / sync | default per path |
| `MI_SYNC_WAIT_TIMEOUT_S` | 3600 | then 202 + job_id |
| `MI_MAX_CONCURRENT` | 1 | running jobs across all processes |
| `MI_MAX_QUEUED_JOBS` | 5 | queued + running; beyond → 429 |
| `MI_JOB_TTL_S` / `MI_JOB_STALE_S` | 86400 / 300 | job record lifetime / lost-worker detection |
