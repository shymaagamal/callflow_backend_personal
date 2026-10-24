# callflow/core/rca_runner/browser_runner.py
"""
Run the EXISTING RCA HTML tool (rca_tool_V24.html) in headless Chromium and
return its real results.

Nothing in the RCA tool is re-implemented here. The page runs unmodified and
goes through the same path the Angular iframe uses:

    callAPIWithPayload({rca_files_url, start_date, end_date})
      -> fetch(rca_files_url)            -> {kpi, filter, threshold} file list
      -> handleApiResponse -> loadFiles  -> fetch each file URL into File objects
      -> startAnalysis()                 -> parse, filter, thresholds, detectAnomalies,
                                            correlateAnomalies, renderResults

Every network request the page makes is answered by Playwright request
interception (no HTTP round trip to Django, no internet):

    tool page            -> the HTML file on disk
    CDN libraries        -> pinned local copies in ./vendor (XLSX, JSZip, Chart.js)
    .../{id}/rca-files   -> this request's READY file list (rca_files.list_ready_files)
    .../{id}/file/{name} -> bytes of that request-scoped file (rca_files.resolve_file)
    anything else        -> blocked and logged

Completion: startAnalysis is async but callAPIWithPayload does not await it, so
the runner wraps window.startAnalysis (same function, awaited) and records when
it settles. Results are read from the tool's own structured state:
AppState.correlationResults (summary, chains, suspects, anomalies ...) and
performRCA(correlationResults), the function behind the tool's
"Generate Analysis" button (root-cause hypothesis, layers, confidence).
"""

import json
import logging
import re
import time
from pathlib import Path
import asyncio
import threading
logger = logging.getLogger(__name__)

TOOL_ORIGIN = "http://rca-runner.local"
TOOL_URL = TOOL_ORIGIN + "/rca_tool.html"
VENDOR_DIR = Path(__file__).resolve().parent / "vendor"

# CDN URL fragment -> local file (checked in order; the adapter contains "chart").
VENDOR_FILES = (
    ("chartjs-adapter-date-fns", "chartjs-adapter-date-fns.bundle.min.js"),
    ("npm/chart.js", "chart.umd.js"),
    ("jszip", "jszip.min.js"),
    ("xlsx", "xlsx.full.min.js"),
)
_FILE_URL = re.compile(r"/callflow/api/analysis/(?P<rid>[^/]+)/(?P<kind>rca-files|file/(?P<name>[^/?#]+))")


class RCARunError(RuntimeError):
    """A failure of the RCA tool run, with a stable machine-readable code."""

    def __init__(self, code, message, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details or {}


# JS helpers evaluated in the page -------------------------------------------

_JS_RUN = """
async (payload) => {
  if (typeof callAPIWithPayload !== 'function' || typeof startAnalysis !== 'function') {
    return {ok: false, reason: 'RCA tool functions not defined (callAPIWithPayload/startAnalysis)'};
  }
  if (typeof XLSX === 'undefined') {
    return {ok: false, reason: 'XLSX library not loaded'};
  }
  const run = window.__rcaRun = {started: false, done: false, error: null, t0: Date.now()};
  const original = window.startAnalysis;
  window.startAnalysis = async function () {
    run.started = true;
    try { return await original.apply(this, arguments); }
    catch (e) { run.error = String((e && (e.stack || e.message)) || e); }
    finally { run.done = true; run.ms = Date.now() - run.t0; }
  };
  await callAPIWithPayload(payload);
  return {
    ok: true,
    started: run.started,
    kpiFiles: (AppState.kpiFiles || []).map(f => f.name),
    filterFile: AppState.filterFile ? AppState.filterFile.name : null,
    thresholdFile: AppState.thresholdFile ? AppState.thresholdFile.name : null,
    fileListText: (document.getElementById('kpi-file-list') || {}).innerText || ''
  };
}
"""

_JS_COLLECT = """
(maxArr) => {
  const toSafe = (root) => {
    const path = new WeakSet();
    const walk = (v, d) => {
      if (v === null || v === undefined) return null;
      const t = typeof v;
      if (t === 'number') return Number.isFinite(v) ? v : null;
      if (t === 'string' || t === 'boolean') return v;
      if (t === 'bigint') return String(v);
      if (t === 'function' || t === 'symbol') return undefined;
      if (v instanceof Date) return isNaN(v.getTime()) ? null : v.toISOString();
      if (typeof Node !== 'undefined' && v instanceof Node) return undefined;
      if (typeof Blob !== 'undefined' && v instanceof Blob) return undefined;
      if (path.has(v)) return '[circular]';
      if (d > 14) return '[max-depth]';
      path.add(v);
      let out;
      if (v instanceof Map) {
        out = {};
        for (const [k, x] of v) { const w = walk(x, d + 1); if (w !== undefined) out[String(k)] = w; }
      } else if (v instanceof Set) {
        out = [...v].slice(0, maxArr).map(x => walk(x, d + 1));
      } else if (Array.isArray(v)) {
        out = v.slice(0, maxArr).map(x => { const w = walk(x, d + 1); return w === undefined ? null : w; });
      } else {
        out = {};
        for (const k of Object.keys(v)) { const w = walk(v[k], d + 1); if (w !== undefined) out[k] = w; }
      }
      path.delete(v);
      return out;
    };
    return walk(root, 0);
  };
  const r = AppState.correlationResults;
  const status = ((document.getElementById('hdr-status') || {}).textContent || '').trim();
  const log = (AppState.analysisLog || []).slice();
  const errors = log.filter(e => e.level === 'error').map(e => e.msg);
  let rootCause = null, rootCauseError = null;
  if (r && typeof performRCA === 'function') {
    try {
      const res = performRCA(r);           // same call as the "Generate Analysis" button
      AppState._lastRCAResult = res;
      const copy = Object.assign({}, res);
      delete copy.r;                       // the input object itself
      delete copy.chains;                  // already returned as results.chains
      rootCause = toSafe(copy);
    } catch (e) { rootCauseError = String((e && e.message) || e); }
  }
  const results = r ? toSafe({
    params: r.params,
    summary: r.summary,
    chains: r.chains,
    suspects: r.suspects,
    tacSuspects: r.tacSuspects,
    nodeSuspects: r.nodeSuspects,
    anomalies: r.allAnomalies,
    txAnomalies: r.txAnomalies,
    subscriberStats: r.subscriberStats,
    ticketsBySite: r.ticketsBySite,
    tickets: r.allTickets,
    detectedNodeTypes: r.detectedNodeTypes,
    bulkIntel: r.bulkIntel
  }) : null;
  const datasets = Object.entries(AppState.datasets || {}).map(([name, d]) => ({
    name, type: d && d.type, rows: d && d.data ? d.data.length : null
  }));
  return {
    status, errors, hasResults: !!r, results, rootCause, rootCauseError, datasets,
    run: window.__rcaRun || null, log
  };
}
"""


def _vendor_file_for(url):
    for fragment, fname in VENDOR_FILES:
        if fragment in url:
            return VENDOR_DIR / fname
    return None


def run_rca_tool(*, request_id, tool_path, files_listing, resolve_file, start_date, end_date,
                 timezone_id=None,
                 out_dir, timeout_seconds=300, launch_args=None, max_array=2000, log=None):
    """Run the RCA tool on one request's files; return the structured output.

    files_listing: the dict list_ready_files() returns ({kpi, filter, threshold}).
    resolve_file(name) -> Path of that request-scoped file (raises if missing/unsafe).
    out_dir: where the browser console log, tool log and result are written.
    Raises RCARunError(code, ...) on any failure; artifacts are written either way.
    """
    log = log or logger
    tool_path = Path(tool_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if not tool_path.is_file():
        raise RCARunError("RCA_TOOL_NOT_FOUND", f"RCA tool HTML not found at {tool_path}")
    missing_vendor = [f for _, f in VENDOR_FILES if not (VENDOR_DIR / f).is_file()]
    if missing_vendor:
        raise RCARunError("RCA_VENDOR_MISSING", f"missing local libraries in {VENDOR_DIR}: {missing_vendor}")
    
    try: 
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
       
    except Exception as e:
        raise RCARunError("BROWSER_START_FAILED", f"playwright is not installed: {e}")

    console, page_errors, blocked, served, failed_files = [], [], [], [], []
    collected = None
    t0 = time.monotonic()

    def handle(route):
        url = route.request.url
        try:
            if url.split("?")[0] == TOOL_URL:
                return route.fulfill(status=200, content_type="text/html; charset=utf-8",
                                     body=tool_path.read_bytes())
            m = _FILE_URL.search(url)
            if m and url.startswith(TOOL_ORIGIN):
                if m.group("rid") != str(request_id):   # never serve another request's files
                    blocked.append(url)
                    return route.fulfill(status=404, body="not this request")
                if m.group("kind") == "rca-files":
                    served.append("rca-files")
                    return route.fulfill(status=200, content_type="application/json",
                                         body=json.dumps(files_listing))
                name = m.group("name")
                try:
                    path = Path(resolve_file(name))
                    body = path.read_bytes()
                except Exception as e:
                    failed_files.append({"name": name, "error": str(e)})
                    return route.fulfill(status=404, body="file not found")
                served.append(name)
                ctype = "text/csv" if name.lower().endswith(".csv") else \
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                return route.fulfill(status=200, content_type=ctype, body=body)
            vendor = _vendor_file_for(url)
            if vendor is not None:
                return route.fulfill(status=200, content_type="application/javascript",
                                     body=vendor.read_bytes())
            blocked.append(url)
            return route.abort()
        except Exception as e:  # a routing bug must surface, not hang the page
            page_errors.append(f"route handler error for {url}: {e}")
            try:
                route.abort()
            except Exception:
                pass

    payload = {"request_id": request_id, "start_date": start_date, "end_date": end_date,
               "rca_files_url": f"/callflow/api/analysis/{request_id}/rca-files"}
    try:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True, args=list(launch_args or []))
            except Exception as e:
                raise RCARunError("BROWSER_START_FAILED", str(e))
            context = None
            try:
                # timezone_id: the tool parses times without an offset (start/end,
                # NPP DATETIME_ID) as browser-local time, like in the portal users'
                # browsers; the server's own zone (often UTC) would shift them.
                context = browser.new_context(viewport={"width": 1400, "height": 900},
                                              accept_downloads=False,
                                              **({"timezone_id": timezone_id} if timezone_id else {}))
                context.route("**/*", handle)
                page = context.new_page()
                page.on("console", lambda m: console.append(f"[{m.type}] {m.text}"))
                page.on("pageerror", lambda e: page_errors.append(str(e)))
                page.on("dialog", lambda d: (console.append(f"[dialog] {d.message}"), d.dismiss()))
                timeout_ms = int(timeout_seconds * 1000)
                page.set_default_timeout(timeout_ms)
                try:
                    page.goto(TOOL_URL, wait_until="load", timeout=60000)
                except Exception as e:
                    raise RCARunError("RCA_TOOL_LOAD_FAILED", f"page did not load: {e}")

                start = page.evaluate(_JS_RUN, payload)
                if not start.get("ok"):
                    raise RCARunError("RCA_TOOL_LOAD_FAILED", start.get("reason", "unknown"),
                                      {"page_errors": page_errors[:20]})
                expected = len(files_listing.get("kpi") or [])
                if not start.get("started"):
                    raise RCARunError(
                        "RCA_FILE_LOAD_FAILED",
                        f"the tool loaded {len(start.get('kpiFiles') or [])} of {expected} KPI files "
                        f"and did not start the analysis",
                        {"failed_files": failed_files, "tool_message": start.get("fileListText")})
                log.info("[RCA-RUN] request_id=%s tool started: kpi=%s filter=%s threshold=%s",
                         request_id, start.get("kpiFiles"), start.get("filterFile"), start.get("thresholdFile"))

                remaining = max(1.0, timeout_seconds - (time.monotonic() - t0))
                try:
                    page.wait_for_function("() => window.__rcaRun && window.__rcaRun.done",
                                           timeout=int(remaining * 1000), polling=500)
                except PWTimeout:
                    raise RCARunError("RCA_TIMEOUT",
                                      f"RCA analysis did not finish within {timeout_seconds:.0f}s")

                collected = page.evaluate(_JS_COLLECT, max_array)
                collected["inputs"] = start
            finally:
                for closer in (context, browser):
                    try:
                        if closer is not None:
                            closer.close()
                    except Exception:
                        pass
    finally:
        _write(out_dir / "browser_console.log", "\n".join(
            console + [f"[pageerror] {e}" for e in page_errors] + [f"[blocked] {u}" for u in blocked]))
        if collected is not None:
            _write(out_dir / "rca_tool_log.json", json.dumps(collected.get("log") or [], indent=1))

    run = collected.get("run") or {}
    js_errors = [e for e in (collected.get("errors") or []) if e]
    if run.get("error") or collected.get("status") == "ERROR" or \
            any(str(e).startswith("Analysis error") for e in js_errors):
        raise RCARunError("RCA_JS_ERROR", (run.get("error") or "; ".join(js_errors[:3]) or "analysis error"),
                          {"errors": js_errors[:20], "page_errors": page_errors[:20]})
    if not collected.get("hasResults"):
        raise RCARunError("RCA_INCOMPLETE", "analysis finished without correlation results",
                          {"errors": js_errors[:20], "page_errors": page_errors[:20]})

    inputs = collected["inputs"]
    warnings = []
    if failed_files:
        warnings.append({"code": "FILES_NOT_LOADED", "files": failed_files})
    if len(inputs.get("kpiFiles") or []) < len(files_listing.get("kpi") or []):
        warnings.append({"code": "FEWER_KPI_FILES_LOADED",
                         "offered": [f.get("name") for f in files_listing.get("kpi") or []],
                         "loaded": inputs.get("kpiFiles")})
    if collected.get("rootCauseError"):
        warnings.append({"code": "ROOT_CAUSE_FAILED", "message": collected["rootCauseError"]})
    if page_errors:
        warnings.append({"code": "PAGE_ERRORS", "messages": page_errors[:20]})

    return {
        "status": "SUCCESS",
        "tool": tool_path.name,
        "duration_seconds": round(time.monotonic() - t0, 2),
        "analysis_ms": run.get("ms"),
        "inputs": {
            "kpi_files": inputs.get("kpiFiles"),
            "filter_file": inputs.get("filterFile"),
            "threshold_file": inputs.get("thresholdFile"),
            "datasets": collected.get("datasets"),
        },
        "results": collected.get("results"),
        "root_cause": collected.get("rootCause"),
        "log_errors": js_errors,
        "warnings": warnings,
    }


def _write(path, text):
    try:
        tmp = Path(str(path) + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    except Exception:
        logger.warning("could not write %s", path, exc_info=True)
