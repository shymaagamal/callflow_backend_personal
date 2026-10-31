# NCI/service/rca_pipeline.py
"""
POST /NCI/analysis/ -- the sites go through the EXISTING RCA tool.

    requested sites
      -> Trino (S1-MME site KPIs), NPP, Viavi, Remedy     existing NCI fetchers / pools
      -> raw RCA input CSVs in this request's own folder  existing RCA file writers + manifest
      -> rca_tool_V24.html in headless Chromium           rca_runner.browser_runner (unchanged)
      -> the tool's per-site results -> NCI sites_summary

Nothing here judges a KPI. The RCA tool loads the files, splits the complaint
window and its baseline ("auto": 2 hours before, else the same window
yesterday), applies the threshold sheet, detects and de-duplicates anomalies,
links them and the Remedy tickets to sites, and scores the sites. This module
only retrieves, writes files, runs the tool and copies the tool's per-site
numbers into the NCI response.

Analysis window: the last 12 hours (NCI's current window, rounded down to 15
minutes). Retrieval covers what the tool's baseline needs:
    NPP, Viavi, Trino   [start - 2 h, end] and [start - 24 h, end - 24 h]
    Remedy              [start, end]

RCA input files (OUTPUT_DIR/nci_rca/rca/<request_id>/):
    npp_kpi.csv                      NPP rows (existing writer; "cell" -> "Cell")
    viavi_kpi.csv                    Viavi rows (existing writer; FLWID -> TX)
    remedy_kpi.csv                   one row per ticket with its Remedy site (existing writer)
    trino_s1mme_<kpi>.csv            one file per S1-MME KPI, see _trino_files
    nci_scope.csv                    filter file: NPP's cells for the sites, plus
                                     SITE_NAME for requested sites NPP returned no cell for
    thresholds.xlsx                  the server's threshold sheet (existing provisioning)

Trino in the RCA tool: the tool has no site-level S1-MME loader. Its core-node
CSV format with an NE_NAME column is used (file name contains "mme" -> MME
type; NE_NAME = the Trino site name -> the tool's instance). The tool then
evaluates the KPIs with the MME rows of the threshold sheet and links an
anomaly to a site only through its own numeric site-id rule. KPI columns are
named so the sheet's rules match by the tool's own KPI matcher:
    MME_Attach_Failure_4G_TRINO  -> sheet "Attach_Failure_4G_NW"
    MME_TAU_SR_TRINO             -> sheet "TAU SR"
    MME_Authentication_SR_TRINO  -> no sheet row today (the tool skips it; add
                                    an "Authentication SR" MME row to enable it)
One file per KPI, rows only where that KPI had requests, because the tool
averages a missing value as 0.

Response mapping (per requested site, from the tool's correlation results):
    critical_cases   chains[site].criticalIssues   (tool severity CRITICAL, KPI + TX)
    minor_cases      chains[site].anomalyIssues    (every other tool severity)
    trouble_tickets  unique incidents in chains[site].ticketList
A requested site is matched to the tool's site name exactly, else by the
tool's numeric site id (extractNumericSiteId); NPP cell names make the tool
use e.g. "LK2398" for the requested "K2398". A site the tool did not score,
or that matches more than one requested site, gets 0 and an entry in
errors["rca"] -- never another site's numbers.
"""

import datetime
import json
import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from django.conf import settings

from callflow.core.jobs import job_manager
from callflow.core.orchestration import rca_files as rf
from callflow.core.rca_runner import collect, limits
from callflow.core.rca_runner.browser_runner import RCARunError, run_rca_tool
from callflow.core.rca_runner.report import build_report
from callflow.core.services.smartcare_site_kpi_service import fetch_smartcare_site_kpis
from callflow.core.views.orchestrated_analysis import AnalysisOrchestratorAPIView as _Writers

from ..utils.helpers import NCI_CACHE_GRANULARITY_MINUTES, _add_formatted, _round_down

logger = logging.getLogger(__name__)

SYSTEMS = ["trino", "npp", "viavi", "remedy"]
OUTPUT_SUBDIR = "nci_rca"
WINDOW_HOURS = 12
BASELINE_SHIFT_HOURS = 2          # the tool's "auto" baseline: 2 hours before the window
NPP_RESOLUTION = "daily"          # same NPP export the NCI RCA endpoint feeds the tool
SCOPE_FILE = "nci_scope.csv"
RESULT_FILE = "nci_result.json"
REPORT_FILE = "rca_result.json"
DIAGNOSTICS_FILE = "rca_diagnostics.json"
PERIOD = "current"                # the analysed 12 hours, as in the NCI errors entries

# KPI file name -> (column, request column, failure column, kind)
TRINO_KPIS = {
    "trino_s1mme_attach_failure.csv": ("MME_Attach_Failure_4G_TRINO", "ATTACH_REQ_TIMES",
                                       "ATTACH_FAIL_TIMES", "failure_rate"),
    "trino_s1mme_tau_sr.csv": ("MME_TAU_SR_TRINO", "TAU_REQ_TIMES", "TAU_FAIL_TIMES", "success_rate"),
    "trino_s1mme_authentication_sr.csv": ("MME_Authentication_SR_TRINO", "S1MME_AUTHENTICATION_REQ_TIMES",
                                          "S1MME_AUTHENTICATION_FAIL_TIMES", "success_rate"),
}
_COUNT_COLS = ["ATTACH_REQ_TIMES", "ATTACH_FAIL_TIMES", "TAU_REQ_TIMES", "TAU_FAIL_TIMES",
               "S1MME_AUTHENTICATION_REQ_TIMES", "S1MME_AUTHENTICATION_FAIL_TIMES"]


def _setting(name, default):
    return getattr(settings, name, default)


def base_dir():
    return Path(settings.OUTPUT_DIR) / OUTPUT_SUBDIR


def lock_dir():
    # Same lock folder as the RCA run endpoints, so the browser limit
    # (RCA_RUN_MAX_BROWSERS) is shared by every headless RCA tool on the server.
    return Path(settings.OUTPUT_DIR) / "rca_engine" / "locks"


# ── the tool's site-id rule (used only to match names, never to judge) ────────

def numeric_site_id(s):
    """Port of the RCA tool's extractNumericSiteId: longest run of 3+ digits
    (the last one on a tie), leading zeros stripped."""
    if not s:
        return None
    matches = re.findall(r"\d{3,}", str(s))
    if not matches:
        return None
    longest = matches[0]
    for m in matches[1:]:
        if len(m) >= len(longest):
            longest = m
    return re.sub(r"^0+(\d)", r"\1", longest)


# ── windows ───────────────────────────────────────────────────────────────────

def analysis_windows(now=None):
    end = _round_down(now or datetime.datetime.now(), NCI_CACHE_GRANULARITY_MINUTES)
    start = end - datetime.timedelta(hours=WINDOW_HOURS)
    shift = datetime.timedelta(hours=BASELINE_SHIFT_HOURS)
    day = datetime.timedelta(days=1)
    return {
        "complaint": {"start": start, "end": end},
        "fetch": {
            "primary": _add_formatted({"start": start - shift, "end": end}),
            "yesterday": _add_formatted({"start": start - day, "end": end - day}),
        },
    }


# ── retrieval (existing NCI fetchers: caches, pools, NPP semaphore) ───────────

def _retrieve_npp(sites, windows, log_path, ts, notes, rlog):
    from .analysis_runner import fetch_npp_one_site
    frames, failures = [], []
    with ThreadPoolExecutor(max_workers=min(len(sites) * len(windows), 16)) as ex:
        futs = {ex.submit(fetch_npp_one_site, site, w, log_path, ts, NPP_RESOLUTION): (site, name)
                for name, w in windows.items() for site in sites}
        for fut in as_completed(futs):
            site, name = futs[fut]
            try:
                df = fut.result()
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as e:
                failures.append({"site": site, "window": name, "error": str(e)})
                rlog.error(f"[NCI] NPP {site}/{name} failed: {e}")
    if failures and len(failures) == len(futs):
        raise RuntimeError(f"NPP retrieval failed for every site and window: {failures[0]['error']}")
    notes["npp_failures"] = failures
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def _retrieve_viavi(sites, windows, log_path, ts, notes, rlog):
    from .analysis_runner import fetch_viavi
    frames, failures = [], []
    for name, w in windows.items():
        try:
            df = fetch_viavi(sites, w, log_path, ts)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception as e:
            failures.append({"window": name, "error": str(e)})
            rlog.error(f"[NCI] Viavi {name} failed: {e}")
    if failures and len(failures) == len(windows):
        raise RuntimeError(f"Viavi retrieval failed for every window: {failures[0]['error']}")
    notes["viavi_failures"] = failures
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def _retrieve_remedy(sites, complaint, log_path, ts):
    from .analysis_runner import fetch_remedy
    remedy_output, total = fetch_remedy(sites, {"start": complaint["start"], "end": complaint["end"]},
                                        log_path, ts)
    return {"remedy_tickets": remedy_output or {}, "status": total}


def _retrieve_trino(sites, windows, log_path, rlog):
    primary = windows["primary"]
    return fetch_smartcare_site_kpis(sites, log_path, start=primary["start"], end=primary["end"],
                                     now=primary["end"], request_logger=rlog,
                                     window_names=("current", "prev_1day"))


# ── RCA input files ───────────────────────────────────────────────────────────

def trino_frame(result):
    """Combined Trino rows of every retrieved window -> one row per (time, site)."""
    rows = [r for w in ((result or {}).get("s1mme_rows") or {}).values() for r in (w or [])]
    if not rows:
        return pd.DataFrame(columns=["timestamp", "NE_NAME"] + _COUNT_COLS)
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["Time", "Site_Logical_Name"])
    for col in _COUNT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    out = (df.groupby(["Time", "Site_Logical_Name"], sort=True)[_COUNT_COLS]
           .sum(min_count=1).reset_index()
           .rename(columns={"Time": "timestamp", "Site_Logical_Name": "NE_NAME"}))
    return out


def _trino_files(system, data, ctx):
    """One RCA file per S1-MME KPI: timestamp, NE_NAME (= site), the KPI in %.
    Only rows with requests for that KPI (no invented values)."""
    df = trino_frame(data)
    files = []
    for fname, (col, req, fail, kind) in TRINO_KPIS.items():
        part = df[(df[req].notna()) & (df[req] > 0) & (df[fail].notna())].copy()
        if part.empty:
            continue
        fail_pct = 100.0 * part[fail] / part[req]
        part[col] = (fail_pct if kind == "failure_rate" else 100.0 - fail_pct).round(4)
        files.append((fname, part[["timestamp", "NE_NAME", col]].to_csv(index=False)))
    return files


def _trino_rows(data):
    return len(trino_frame(data))


def scope_csv(sites, npp_cells):
    """Filter file: NPP's cells for the sites, plus SITE_NAME for every requested
    site none of whose cells NPP returned (matched by the tool's site-id rule),
    so the tool still scores that site from Trino / Viavi / Remedy."""
    cell_ids = {numeric_site_id(c) for c in npp_cells}
    rows = [{"cell_name": c, "SITE_NAME": ""} for c in npp_cells]
    rows += [{"cell_name": "", "SITE_NAME": s} for s in sites if numeric_site_id(s) not in cell_ids]
    return pd.DataFrame(rows, columns=["cell_name", "SITE_NAME"]).to_csv(index=False)


def _log_files(base, request_id, rlog):
    manifest = rf._read_manifest_raw(base, request_id) or {}
    rdir = rf.request_dir(base, request_id)
    for system, entry in (manifest.get("systems") or {}).items():
        for fname in entry.get("filenames") or ([entry["filename"]] if entry.get("filename") else []):
            path = rdir / fname
            try:
                records = max(sum(1 for _ in open(path, encoding="utf-8")) - 1, 0) \
                    if fname.endswith(".csv") else None
            except OSError:
                records = None
            rlog.info(f"[NCI] RCA file system={system} file={path} records={records}")


# ── mapping the tool's results ────────────────────────────────────────────────

def _incident_ids(tickets):
    ids, anonymous = set(), 0
    for t in tickets or []:
        tid = next((t.get(k) for k in ("incident_number", "ticket_number", "incident_id",
                                       "ticket_id", "Ticket_ID", "ID", "id") if t.get(k)), None)
        if tid:
            ids.add(str(tid))
        else:
            anonymous += 1
    return len(ids) + anonymous


def map_rca_to_sites(sites, rca_results):
    """{requested site: {critical_cases, minor_cases, trouble_tickets}}, plus
    errors["rca"] entries for sites the tool's results cannot be tied to."""
    chains = (rca_results or {}).get("chains") or []
    by_name = {}
    for c in chains:
        if c.get("site"):
            by_name.setdefault(str(c["site"]), []).append(c)
    # numeric id -> requested sites sharing it (ambiguous when > 1)
    req_by_id = {}
    for s in sites:
        req_by_id.setdefault(numeric_site_id(s), []).append(s)

    summary, problems = {}, []
    for site in sites:
        matched = list(by_name.get(site, []))
        how = "exact"
        if not matched:
            sid = numeric_site_id(site)
            if sid and len(req_by_id.get(sid, [])) > 1:
                problems.append({"period": PERIOD, "site": site,
                                 "error": f"site id {sid} is shared by requested sites "
                                          f"{req_by_id[sid]}; RCA results cannot be assigned"})
                matched, how = [], "ambiguous"
            else:
                matched = [c for c in chains if c.get("site") and sid
                           and numeric_site_id(c["site"]) == sid]
                how = "site id"
        if not matched and how != "ambiguous":
            problems.append({"period": PERIOD, "site": site,
                             "error": "the RCA tool linked no data to this site"})
        tickets = [t for c in matched for t in (c.get("ticketList") or [])]
        summary[site] = {
            "critical_cases": int(sum(c.get("criticalIssues") or 0 for c in matched)),
            "minor_cases": int(sum(c.get("anomalyIssues") or 0 for c in matched)),
            "trouble_tickets": _incident_ids(tickets),
        }
        summary[site]["_rca_sites"] = sorted({str(c["site"]) for c in matched})
        summary[site]["_match"] = how
    return summary, problems


def _coverage_errors(sites, collected_systems, data, notes):
    """errors[<system>] entries: failures, no data, and requested sites a system
    returned nothing for (so missing data never looks like a healthy site)."""
    errors = {s: [] for s in SYSTEMS}
    for system in SYSTEMS:
        st = collected_systems.get(system) or {}
        status = st.get("status")
        if status in collect.MISSING:
            errors[system].append({"period": PERIOD, "error": st.get("error") or status})
        elif status == collect.NO_DATA:
            errors[system].append({"period": PERIOD, "error": "no data returned for the requested sites"})
    for f in notes.get("npp_failures") or []:
        errors["npp"].append({"period": f["window"], "site": f["site"], "error": f["error"]})
    for f in notes.get("viavi_failures") or []:
        errors["viavi"].append({"period": f["window"], "error": f["error"]})
    for f_key, msg in ((data.get("trino") or {}).get("errors") or {}).items():
        errors["trino"].append({"period": f_key.split(".", 1)[0], "source": f_key.split(".")[-1],
                                "error": msg})

    def missing(system, have_ids):
        st = (collected_systems.get(system) or {}).get("status")
        if st != collect.COMPLETED:
            return
        for s in sites:
            if numeric_site_id(s) not in have_ids and s not in have_ids:
                errors[system].append({"period": PERIOD, "site": s,
                                       "error": f"no {system.upper()} data for this site"})

    npp = data.get("npp")
    if npp is not None and not npp.empty:
        missing("npp", {numeric_site_id(c) for c in collect._npp_cells(npp)})
    viavi = data.get("viavi")
    if viavi is not None and not viavi.empty and "FLWID" in viavi.columns:
        from .analysis_runner import _canonicalize
        missing("viavi", {_canonicalize(f, sites) for f in viavi["FLWID"].dropna().unique()})
    tr = trino_frame(data.get("trino"))
    # With a failed Trino query the per-site gaps are partly that failure; the
    # failure entry above already says so, so no per-site "no data" guesses.
    if not tr.empty and not ((data.get("trino") or {}).get("errors")):
        missing("trino", {numeric_site_id(s) for s in tr["NE_NAME"]} | set(tr["NE_NAME"]))
    return errors


def _write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


# ── orchestration ─────────────────────────────────────────────────────────────

def run_nci_rca(sites, request_logger, log_path, ts, job_id=None, now=None, tool_path=None):
    """The NCI analysis through the RCA tool. Returns {"sites_summary", "errors"}."""
    rlog = request_logger
    request_id = job_id or uuid.uuid4().hex
    base = base_dir()
    t0 = time.monotonic()
    win = analysis_windows(now)
    complaint, fetch_windows = win["complaint"], win["fetch"]
    rlog.info(f"[NCI] request_id={request_id} sites={sites} analysis window "
              f"{complaint['start']} -> {complaint['end']} (RCA tool, baseline auto)")

    def progress(step, pct):
        if job_id is not None:
            job_manager.update_job(job_id, status="RUNNING", step=step, progress_percent=pct)

    def on_change(states):
        done = sum(1 for s in states.values() if s != "RUNNING")
        if job_id is not None:
            job_manager.update_job(job_id, status="RUNNING", step="retrieving data",
                                   progress_percent=int(70 * done / max(len(states), 1)),
                                   systems=states)

    # 1) retrieval -> RCA files (written by the collecting thread only, after
    #    each retrieval returned in time)
    rf.init_request(base, request_id, SYSTEMS + ["scope"])
    notes, data = {}, {}
    ctx = {"request_id": request_id, "sitelist": list(sites)}
    c = collect._Collector(base, request_id, ctx, rlog, on_change)
    t_ret = float(_setting("RCA_RETRIEVAL_TIMEOUT_S", 600))
    t_trino = float(_setting("RCA_TRINO_TIMEOUT_S", 1800))
    try:
        for name in SYSTEMS:
            rlog.info(f"[NCI] request_id={request_id} {name} retrieval started")
        c.start("trino", _retrieve_trino, sites, fetch_windows, log_path, rlog, timeout=t_trino)
        c.start("npp", _retrieve_npp, sites, fetch_windows, log_path, ts, notes, rlog, timeout=t_ret)
        c.start("viavi", _retrieve_viavi, sites, fetch_windows, log_path, ts, notes, rlog, timeout=t_ret)
        c.start("remedy", _retrieve_remedy, sites, complaint, log_path, ts, timeout=t_ret)

        data["npp"] = c.finish("npp", collect._df_rows, _Writers._extract_df_csv("npp"))
        data["viavi"] = c.finish("viavi", collect._df_rows, _Writers._extract_df_csv("viavi"))
        data["remedy"] = c.finish("remedy", collect._remedy_rows, _Writers._extract_remedy)
        data["trino"] = c.finish("trino", _trino_rows, _trino_files)
    finally:
        c.close()
    for name in SYSTEMS:
        st = c.systems.get(name) or {}
        rlog.info(f"[NCI] request_id={request_id} {name} retrieval completed status={st.get('status')} "
                  f"rows={st.get('rows')} files={st.get('files')} error={st.get('error')}")

    npp_cells = collect._npp_cells(data.get("npp"))
    rf.write_system_file(base, request_id, "scope", SCOPE_FILE, scope_csv(sites, npp_cells))
    rf.set_data_status(base, request_id, "scope", rf.DATA_COMPLETED)
    threshold_path = getattr(settings, "THRESHOLDING_SHEET_PATH", None)
    tinfo = rf.provision_threshold(base, request_id, threshold_path) if threshold_path else {"copied": False}
    rlog.info(f"[NCI] request_id={request_id} threshold sheet copied={tinfo.get('copied')} "
              f"path={tinfo.get('path')}")
    _log_files(base, request_id, rlog)

    errors = _coverage_errors(sites, c.systems, data, notes)
    errors["rca"] = []
    listing = rf.list_ready_files(base, request_id)
    rdir = rf.request_dir(base, request_id)

    if not listing.get("kpi"):
        # Every system failed or returned nothing: there is nothing for the tool
        # to analyse. Reported per system, not as healthy sites.
        rlog.warning(f"[NCI] request_id={request_id} RCA tool not run: no KPI file is ready")
        errors["rca"].append({"period": PERIOD, "error": "RCA not run: no data from any system"})
        summary = {s: {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 0} for s in sites}
        result = {"sites_summary": summary, "errors": errors}
        _write_json(rdir / RESULT_FILE, {"request_id": request_id, **result})
        return result

    # 2) the existing RCA tool on exactly these files
    progress("rca_tool", 80)
    wait_s = float(_setting("RCA_RUN_SLOT_WAIT_S", 1800))
    rlog.info(f"[NCI] request_id={request_id} RCA execution started kpi={[f['name'] for f in listing['kpi']]} "
              f"filter={[f['name'] for f in listing['filter']]} threshold={[f['name'] for f in listing['threshold']]}")
    tool = Path(tool_path or _setting("RCA_TOOL_PATH", Path(settings.BASE_DIR) / "rca_tool_V24.html"))
    with limits.slot(lock_dir(), "rca_run_browser", _setting("RCA_RUN_MAX_BROWSERS", 1), wait_s,
                     on_wait=lambda waited: progress(f"queued for the RCA tool ({waited:.0f}s)", 80)):
        rca = run_rca_tool(
            request_id=request_id, tool_path=tool, files_listing=listing,
            resolve_file=lambda name: rf.resolve_file(base, request_id, name),
            start_date=complaint["start"].strftime("%Y-%m-%dT%H:%M"),
            end_date=complaint["end"].strftime("%Y-%m-%dT%H:%M"),
            timezone_id=_setting("RCA_BROWSER_TIMEZONE", "Africa/Cairo"),
            out_dir=rdir, timeout_seconds=float(_setting("RCA_TOOL_TIMEOUT_S", 300)),
            launch_args=_setting("RCA_BROWSER_ARGS", ["--no-sandbox", "--disable-dev-shm-usage"]),
            log=rlog,
        )
    results = rca.get("results") or {}
    rlog.info(f"[NCI] request_id={request_id} RCA execution completed in {rca.get('duration_seconds')}s "
              f"anomalies={len(results.get('anomalies') or [])} tx={len(results.get('txAnomalies') or [])} "
              f"sites_scored={len(results.get('chains') or [])} warnings={rca.get('warnings')}")

    # 3) the tool's per-site results -> the NCI response
    progress("mapping RCA results", 95)
    mapped, problems = map_rca_to_sites(sites, results)
    errors["rca"].extend(problems)
    for w in rca.get("warnings") or []:
        if w.get("code") in ("FILES_NOT_LOADED", "FEWER_KPI_FILES_LOADED"):
            errors["rca"].append({"period": PERIOD, "error": f"{w['code']}: {w.get('files') or w.get('loaded')}"})
    for site, row in mapped.items():
        rlog.info(f"[NCI] request_id={request_id} site={site} -> RCA site(s) {row['_rca_sites']} "
                  f"(match: {row['_match']}) critical={row['critical_cases']} minor={row['minor_cases']} "
                  f"tickets={row['trouble_tickets']}")
    summary = {s: {k: v for k, v in row.items() if not k.startswith("_")} for s, row in mapped.items()}
    result = {"sites_summary": summary, "errors": errors}

    # traceability: the tool's own report + what was sent back
    systems = {s: (c.systems.get(s) or {}).get("status") for s in SYSTEMS}
    failed = sorted(s for s, st in systems.items() if st in collect.MISSING)
    no_data = sorted(s for s, st in systems.items() if st == collect.NO_DATA)
    _write_json(rdir / REPORT_FILE, build_report(request_id, rca, systems, failed, no_data,
                                                 request_input={"mode": "nci", "sites": list(sites),
                                                                "msisdn_count": None, "complain_type": None},
                                                 link_base="/callflow/NCI/analysis"))
    _write_json(rdir / DIAGNOSTICS_FILE, {"request_id": request_id, "collection": c.systems,
                                          "rca_inputs": listing,
                                          "rca_tool": {k: v for k, v in rca.items()
                                                       if k not in ("results", "root_cause")},
                                          "mapping": mapped,
                                          "duration_seconds": round(time.monotonic() - t0, 2)})
    _write_json(rdir / RESULT_FILE, {"request_id": request_id, **result})
    rlog.info(f"[NCI] request_id={request_id} mapped {len(summary)} site(s) from the RCA results "
              f"in {round(time.monotonic() - t0, 1)}s; files in {rdir}")
    return result
