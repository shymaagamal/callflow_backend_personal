# RCA run endpoint (`/callflow/api/rca/run`)

One request runs every CallFlow system, writes the RCA input files, runs the
existing RCA HTML tool (`rca_tool_V24.html`) in headless Chromium on those files,
and returns the tool's own results. No RCA logic is re-implemented in Python.

## How it works

1. `RCARunAPIView` (views/rca_run.py) is the existing `AnalysisOrchestratorAPIView`
   with its files and logs moved to `OUTPUT_DIR/MI`. `POST` returns a job id at once;
   the job thread runs the unchanged orchestration (Trino, Smartcare, NPP, Splunk,
   Viavi, Remedy), which writes the request-scoped CSVs and `manifest.json` to
   `OUTPUT_DIR/MI/rca/<request_id>/`.
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
POST /callflow/api/rca/run
{"msisdn_list": ["1012345678"], "start_date": "2026-10-10T14:21",
 "end_date": "2026-10-10T15:21", "Complain_Type": "Data", "imsi": ""}
-> 202 {"job_id": "<request_id>", "status": "PENDING"}

GET /callflow/api/rca/run/<request_id>
-> RUNNING: step, systems, rca_run.status   DONE: result   FAILED: error + rca_run.code
```

Error codes in `rca_run.code`, also written to `rca_error.json`:
`RCA_NO_INPUT_FILES`, `RCA_TOOL_NOT_FOUND`, `RCA_VENDOR_MISSING`, `BROWSER_START_FAILED`,
`RCA_TOOL_LOAD_FAILED`, `RCA_FILE_LOAD_FAILED`, `RCA_JS_ERROR`, `RCA_TIMEOUT`,
`RCA_INCOMPLETE`, `RCA_QUEUE_TIMEOUT`.

The final result is also kept in `MI/rca/<request_id>/rca_result.json`, so `GET` still
answers after the 30-minute job record expires.

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
