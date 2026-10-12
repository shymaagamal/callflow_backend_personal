# End-to-end integration: Angular → Job API → orchestration → RCA

This documents the full user flow wired on top of the existing data-availability
orchestration (see `ANALYSIS.md`). No second orchestration mechanism was added —
Angular consumes the existing scheduler/pipeline status; the backend just exposes
it and generates request-scoped RCA files.

## Current flow (before) — analyzed, then replaced

- Frontend `user-input-form.component.ts`: `runSmartcareJob` → read
  `suspected_cells_raw` from the result → `runNppMinutesJob` → navigate to
  `/summary`. Strictly Smartcare-first.
- `summary.component.ts`: fired three abnormal-KPI jobs, each of which is itself
  Smartcare-first on the backend.
- RCA: `rootcause.component.ts` loads `assets/external/rca_tool_V24.html` in an
  iframe and `postMessage`s a payload; the tool POSTs `/callflow/api/root-cause`,
  which scans `callflow_output/` for `kpi_{ts}`/`filter_{ts}` **at request time**.
  `ts = "{start}_{end}"` (window only) → not request-scoped.
- Two concrete bugs confirmed: (a) RCA can load files before the job finished
  writing them (needs manual refresh); (b) two requests with the same window
  collide, so RCA can load another request's files.

## New flow (after)

```
user-input-form: POST /callflow/api/analysis  ─▶ { job_id }   (job_id == request_id)
        │  navigate('/summary', { request_id, inputs })
        ▼
summary: pollJob(request_id) ── GET /analysis/{id} every 1.5s ──▶ per-system status
        │   (retrieval runs without Smartcare; analysis waits for Smartcare)
        ▼  job DONE/FAILED
summary: pollRcaStatus(request_id) ── GET /analysis/{id}/rca-status ──▶ READY?
        ▼  only when READY/PARTIAL
"Open RCA" enabled → app-rootcause mounts → RCA tool GETs
        /analysis/{id}/rca-files  (request-scoped, only READY files)
```

## API contract

| Method | URL | Purpose |
| --- | --- | --- |
| POST | `/callflow/api/analysis` | Start job. Body: `{ msisdn_list, start_date, end_date, Complain_Type }`. Returns `{ job_id, status: "PENDING" }`. `request_id == job_id`. |
| GET | `/callflow/api/analysis/<request_id>` | Job status: `{ status, step, progress_percent, systems, rca, result }`. |
| GET | `/callflow/api/analysis/<request_id>/rca-status` | Readiness: `{ request_id, status, files, rca_files_url? }`. |
| GET | `/callflow/api/analysis/<request_id>/rca-files` | RCA-tool file list `{ kpi[], filter[], threshold[] }` (only READY files, request-scoped URLs). |
| GET | `/callflow/api/analysis/<request_id>/file/<filename>` | Streams one request-scoped artifact (path-safe). |

`systems` example while running:
```json
{ "trino":"COMPLETED","smartcare":"PROCESSING","npp":"COMPLETED",
  "splunk":"COMPLETED","viavi":"PROCESSING","remedy":"PROCESSING" }
```
Per-system states: `PENDING | PROCESSING | COMPLETED | FAILED | PARTIAL`
(`PARTIAL` = data retrieved but analysis unavailable, e.g. Smartcare failed).

## Job lifecycle (section 22)

- Job status (polling contract): `PENDING → RUNNING → DONE|FAILED`.
- `result.overall_status`: `COMPLETED` (all analysis systems ok),
  `PARTIAL_SUCCESS` (some ok, some failed/partial), `FAILED` (none ok).
- Statuses come from the scheduler's `on_start`/`progress_cb` callbacks — they are
  truthful events, never time-based fake percentages (section 27).

## RCA readiness lifecycle (sections 15, 23) — separate from data

Each system tracks **data** status and **file** status independently:
`NOT_STARTED → GENERATING → READY | PARTIAL | FAILED`.

RCA files are the raw retrieval outputs, written when each retrieval completes:
1. `set_data_status(system, PROCESSING)` before retrieval,
2. retrieval runs (no Smartcare dependency for NPP/Splunk/Viavi/Remedy),
3. `set_data_status(system, COMPLETED)`,
4. `write_system_file(...)` → `GENERATING` → atomic write → `READY`.

`compute_rca_status` returns READY only when **all expected** files are READY,
`PARTIAL` when some READY & some FAILED, `GENERATING` while any is still writing.
The frontend never enables "Open RCA" until this is READY/PARTIAL (section 18).

## File association & concurrency isolation (sections 14, 19)

Every request writes into its own directory:
```
callflow_output/rca/<request_id>/
    manifest.json
    smartcare_filter.csv
    npp_kpi.csv
    splunk_kpi.csv
    viavi_kpi.csv
    remedy_kpi.csv
```
`request_id == job_id` (a uuid), so REQ-101 and REQ-102 can never mix — same
filenames live in different directories, and the file list returns request-scoped
URLs. Downloads are path-safe (`resolve_file` rejects traversal / escapes).

## Atomic generation (section 17)

`_atomic_write_bytes` writes `<name>.tmp`, `fsync`s, then `os.replace(tmp, name)`
(atomic within a filesystem). Readers only ever see a complete file, so RCA can
never open a half-written CSV.

## Progress source (section 8, 25)

The backend scheduler is the single source of truth. `AnalysisOrchestratorAPIView`
wires `on_start`/`progress_cb` → `rollup_all()` (task→system) →
`job_manager.update_job(systems=..., rca=...)`. The frontend only reads this. No
frontend scheduler, no second dependency engine.

## Logging (section 24)

Lifecycle events are logged with `request_id=...` and `system=...`, e.g.
`event=analysis_started`, `event=retrieval_success system=npp`,
`event=rca_file_ready system=npp`, `event=rca_ready`, `event=analysis_finished`.

## Files changed

Backend:
- `orchestration/rca_files.py` (new) — request-scoped artifacts, atomic writes,
  manifest, readiness computation, path-safe serving.
- `orchestration/scheduler.py` — additive `on_start` callback (no rewrite).
- `orchestration/pipeline.py` — task→system maps, `rollup_all`,
  `classify_overall`, `on_start` passthrough.
- `views/orchestrated_analysis.py` — per-system status + request-scoped RCA file
  generation + rca-status/rca-files/file endpoints.
- `core/urls.py` — RCA routes.
- `orchestration/tests/test_rca_files.py`, `test_status_rollup.py` (new).

Frontend:
- `services/orchestration.service.ts` (new) — submit/pollJob/pollRcaStatus.
- `user-input-form.component.ts` — submit orchestrated job, navigate with
  `request_id` (old Smartcare→NPP chain removed for this path).
- `summary.component.ts` / `.html` — poll job, per-system progress, partial
  success messaging, RCA readiness gating, lifecycle cleanup.
- `assets/external/rca_tool_V24.html` — load request-scoped `rca_files_url` when
  provided (fallback to legacy path preserved).
- `summary/summary.orchestration.spec.ts` (new), `auth.guard.spec.ts` (fixed
  stale compile error), `karma.conf.js` (headless).

## Tested vs not tested

- Backend: `scheduler` (8), `pipeline` DAG (4), `rca_files` readiness/atomic/
  isolation (8), `status_rollup` (10) — 30/30 pass here.
- Frontend: the orchestrated job→UI mapping (`SummaryComponent orchestration
  mapping`, 5 specs) pass headless; `ng build` type-checks the whole app.
- Not runnable here: the Django app itself (missing proprietary modules
  `general_apis`/`cs`/`ps`/`nethouse`, external infra, no `manage.py`/
  `requirements.txt`), so live end-to-end HTTP was not exercised. The contract,
  file lifecycle, isolation and status mapping are covered by unit tests; the
  real service adapters are additive and byte-compiled.
