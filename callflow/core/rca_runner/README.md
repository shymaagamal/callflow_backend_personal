# RCA run endpoint (`/callflow/api/rca/run`)

One request runs every CallFlow system, writes the RCA input files, runs the
existing RCA HTML tool (`rca_tool_V24.html`) in headless Chromium on those files,
and returns the tool's own results. No RCA logic is re-implemented in Python.

## How it works

1. `RCARunAPIView` (views/rca_run.py) is the existing `AnalysisOrchestratorAPIView`
   with its files and logs moved to `OUTPUT_DIR/rca_engine`. `POST` returns a job id at once;
   the job thread runs the unchanged orchestration (Trino, Smartcare, NPP, Splunk,
   Viavi, Remedy), which writes the request-scoped CSVs and `manifest.json` to
   `OUTPUT_DIR/rca_engine/rca/<request_id>/`.
2. When no file is still being written, `browser_runner.run_rca_tool` opens the tool
   in Chromium and calls the tool's own `callAPIWithPayload({rca_files_url, start_date,
   end_date})`, which is what the portal iframe calls. That runs
   `handleApiResponse` -> `loadFiles` -> `startAnalysis`.
3. Every request from the page is answered by Playwright interception: the file
   list and files of this request only, local copies of the CDN libraries (`vendor/`),
   everything else blocked. No internet access or HTTP call back to Django is needed.
4. The runner waits for `startAnalysis()` to settle and reads
   `AppState.correlationResults` plus `performRCA(...)` (the "Generate Analysis"
   button), then closes the browser.

## API

```
POST /callflow/api/rca/run          (one of two body formats, see below)
-> 202 {"job_id": "<request_id>", "status": "PENDING"}

GET /callflow/api/rca/run/<request_id>
   PENDING/RUNNING -> {"request_id", "status", "progress_percent"}
   DONE            -> the RCA report (below)
   FAILED          -> {"request_id", "status": "FAILED", "error": {"code", "message"}}
GET /callflow/api/rca/run/<request_id>/findings.csv   one row per finding
GET /callflow/api/rca/run/<request_id>/diagnostics    logs, file loading, browser and pipeline detail
```

### Two endpoints (`site_mode.py`)

`POST /callflow/api/rca/run` (MSISDN run, existing flow):
`{"msisdn": "01012345678,01098765432", "complain_type": "data", "start": "2026-10-21 10:00:00", "end": "2026-10-21 12:00:00"}`
(the earlier `msisdn_list` / `Complain_Type` / `imsi` / `start_date` / `end_date` keys still work;
`complain_type` is `data` or `voice`, default `data`).

`POST /callflow/api/rca/sites/run` (site run, `RCASiteRunAPIView`):
`{"sitelist": ["SITE_A", "SITE_B"], "start": "...", "end": "..."}`
GET `/callflow/api/rca/sites/run/<id>`, `.../findings.csv` and `.../diagnostics` work as for `/rca/run`.

Sending a site list to `/rca/run`, or MSISDN keys to `/rca/sites/run`, or invalid values gets HTTP 400
`{"error": "..."}` naming the right endpoint.

The site run uses the same job, pipeline, analyzers, RCA file writers, RCA tool and report. It does
not query Trino; the sites go straight to the systems:
- NPP: `nppIntegration(..., entity_type="site")` (the NPP site endpoint).
- Viavi, Remedy: the requested sites as given.
- Splunk: unchanged.
- Smartcare: `fetch_smartcare_site_kpis` (the `SmartcareSiteKpiAPIView` implementation) for the sites
  and the request's `start`..`end` (plus the same range 1 day and 1 week earlier), saved as
  `smartcare_site_kpis.json`. The RCA tool has no input for per-site Smartcare counters, so it does
  not use them.
- RCA filter file `smartcare_filter.csv`: the requested sites (`SITE_NAME`), so the tool scopes to them
  (without it the tool builds no site suspects and links no tickets).
- No subscribers: customer counts are unknown and the tool reports 0 subscribers.

The report (`report.py`) only re-shapes the tool's own objects (`performRCA(...)` and
`AppState.correlationResults`); nothing is recomputed:
`status` (COMPLETED | PARTIAL), `input` (mode, sites or MSISDN count), `analysis_window`, `data_coverage`, `summary`
(primary root cause, layers, confidence, scope, counts), `root_cause` (hypothesis, mechanism,
evidence chain, recommended actions, next steps, differentials), `findings` (one per anomaly,
ids F1..Fn), `suspects` (sites / core nodes / TACs, ranked, linked to findings by id),
`impact` (subscribers, sites, tickets), `warnings`, `links`.
A value the tool did not produce is `null`, never 0. A failed system makes the report
`PARTIAL` and is listed in `data_coverage.systems_missing` with a warning.

Error codes (`error.code`, also in `rca_error.json`):
`RCA_NO_INPUT_FILES`, `RCA_TOOL_NOT_FOUND`, `RCA_VENDOR_MISSING`, `BROWSER_START_FAILED`,
`RCA_TOOL_LOAD_FAILED`, `RCA_FILE_LOAD_FAILED`, `RCA_JS_ERROR`, `RCA_TIMEOUT`,
`RCA_INCOMPLETE`, `RCA_QUEUE_TIMEOUT`.

Files per request in `rca_engine/rca/<request_id>/`: `rca_result.json` (report), `rca_findings.csv`,
`rca_diagnostics.json`, `rca_tool_log.json`, `browser_console.log`, `rca_error.json` on failure.
They outlive the 30-minute job record, so `GET` keeps working.

## Settings (all optional)

| Setting | Default | Meaning |
|---|---|---|
| `RCA_TOOL_PATH` | `BASE_DIR/CallFlow-Frontend/src/assets/external/rca_tool_V24.html` | The tool file to run |
| `RCA_TOOL_TIMEOUT_S` | 300 | Limit for loading + analysis in the browser |
| `RCA_FILES_WAIT_S` | 60 | Wait for files still being written |
| `RCA_RUN_MAX_SYSTEM_JOBS` | 2 | rca/run jobs querying the systems at once (all workers) |
| `RCA_RUN_MAX_BROWSERS` | 1 | Headless browsers at once (all workers) |
| `RCA_RUN_SLOT_WAIT_S` | 1800 | Longest a job waits in the queue |
| `RCA_BROWSER_ARGS` | `["--no-sandbox", "--disable-dev-shm-usage"]` | Chromium flags (`--no-sandbox` is required when Gunicorn runs as root) |

## Deployment (server: Python 3.7, Gunicorn as root)

Playwright 1.35.0 is the last release that supports Python 3.7.

```bash
cd /home/AutomationProjects/CallFlowTool/backend
source portalenv/bin/activate
pip install playwright==1.35.0
python -m playwright install-deps chromium   # system libraries (apt/yum); needs root
python -m playwright install chromium        # browser into ~/.cache/ms-playwright
```

Without internet on the server, run the two `install` commands on a machine with the same OS,
then copy `~/.cache/ms-playwright` to the server, or set `PLAYWRIGHT_BROWSERS_PATH` to a shared
folder before both installing and running Gunicorn.

The tool HTML must be on the server. Either deploy the frontend repo next to the backend
(the default path) or copy the file and set `RCA_TOOL_PATH`.

Restart Gunicorn, then check:

```bash
python -c "from playwright.sync_api import sync_playwright as s; p=s().start(); b=p.chromium.launch(args=['--no-sandbox']); print(b.version); b.close(); p.stop()"
python manage.py test callflow.core.rca_runner     # fake systems + real tool in Chromium
```

Each RCA run uses one Chromium process for the run's duration (about 150-300 MB).
Raise `RCA_RUN_MAX_BROWSERS` only if the host has memory to spare.
