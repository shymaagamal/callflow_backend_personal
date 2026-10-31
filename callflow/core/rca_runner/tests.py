# callflow/core/rca_runner/tests.py
"""
End-to-end tests for POST /callflow/api/rca/run.

Real code under test: RCARunAPIView / RCASiteRunAPIView, rca_runner.collect
(retrieval -> RCA file writers, manifest), rca_runner.browser_runner, and the REAL
rca_tool_V24.html running in headless Chromium. Only the external systems are
replaced by in-memory fakes that return data in the same shapes the real
services return, so no Trino/Smartcare/NPP/Splunk/Viavi/Remedy is touched.
Every test also asserts that the portal pipeline (orchestrator plan, system
analyzers, summary cards) never runs for an RCA request.

Run:  python manage.py test callflow.core.rca_runner
(The browser tests are skipped when Playwright/Chromium is not installed.)
"""

import dataclasses
import json
import re
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
from django.conf import settings
from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory

from ..jobs import job_manager
from ..orchestration import pipeline as pl
from ..orchestration import summary_readiness
from ..orchestration.analyzers import NppAnalyzer, ViaviAnalyzer, RemedyAnalyzer, SplunkAnalyzer
from ..orchestration.customer_counts import CTX_KEY
from ..services import trino_site_kpi_service as site_kpi_service
from ..services.smartcare_service import process_smartcare_data
from ..services.trino_service import customer_counts_from_df
from ..views import orchestrated_analysis as oa
from ..views.rca_run import RCARunAPIView, RCASiteRunAPIView, RCARunFindingsCSVView, RCARunDiagnosticsView
from . import collect, site_mode

try:
    from playwright.sync_api import sync_playwright  # noqa: F401
    HAVE_PW = True
except Exception:  # pragma: no cover
    HAVE_PW = False

BASE_DIR = Path(settings.BASE_DIR)
TOOL = Path(getattr(settings, "RCA_TOOL_PATH", BASE_DIR / "CallFlow-Frontend" / "src" / "assets" /
                    "external" / "rca_tool_V24.html"))
THRESHOLDS = Path(getattr(settings, "THRESHOLDING_SHEET_PATH", BASE_DIR / "data" / "thresholds.xlsx"))

START, END = "2026-10-10T14:21", "2026-10-10T15:21"
MSISDNS = [f"10{i:08d}" for i in range(1, 7)]


# ---------------------------------------------------------------- fake data --
def _times(freq):
    """Rows over the query span the services use: complaint day and the day before."""
    return list(pd.date_range("2026-10-09 10:00", "2026-10-09 17:00", freq=freq)) + \
        list(pd.date_range("2026-10-10 10:00", "2026-10-10 17:00", freq=freq))


def _in_window(t):
    return pd.Timestamp("2026-10-10 12:21") <= t <= pd.Timestamp("2026-10-10 15:21")


def fake_sources(bad_cell="L18_LU4820_04", fail=(), empty=False, marker=0):
    """DataSources with the same output shapes as the real services."""
    cells = [bad_cell, "L18_LU4821_01"]

    def get_cells(ctx):
        if "trino" in fail:
            raise RuntimeError("Trino unavailable (test)")
        if empty:
            df = pd.DataFrame(columns=["MSISDN", "cell_ID", "lac_tac_dec", "cell_name"])
        else:
            ms = list(ctx["msisdn_list"])
            df = pd.DataFrame({"MSISDN": ms, "cell_ID": [str(1000 + marker + i % 2) for i in range(len(ms))],
                               "lac_tac_dec": ["4011"] * len(ms),
                               "cell_name": [cells[i % 2] for i in range(len(ms))]})
        ctx[pl.TRINO_FRAME_KEY] = df
        ctx[CTX_KEY] = customer_counts_from_df(df)
        return [] if empty else cells

    def fetch_smartcare(ctx):
        if "smartcare" in fail:
            raise RuntimeError("Smartcare unavailable (test)")
        ms = list(ctx["msisdn_list"])
        n = len(ms)
        raw = pd.DataFrame({
            "myday": ["2026/10/10 14:30"] * n, "MSISDN": ms, "cell_name": [cells[i % 2] for i in range(n)],
            "MME_Name": ["HQ_MME_6"] * n, "SGW_SGSN_NAME": ["ALX_SGW-PGW_10"] * n,
            "__source": ["smartcare_nodes_S1mme"] * n,
        })
        if empty:
            raw = raw.iloc[0:0]
        return process_smartcare_data(raw, "Data", "", "/dev/null")

    def retrieve_npp(cells_, ctx):
        if "npp" in fail:
            raise RuntimeError("NPP API 500 (test)")
        if empty:
            return pd.DataFrame()
        rows = []
        for t in _times("15min"):
            for c in cells_:
                bad = c == bad_cell and _in_window(t)
                rows.append({"DATETIME_ID": t, "cell": c, "Vendor": "Ericsson",
                             "sitePhysical": c.split("_")[1][1:],
                             "CFR": 2.5 if bad else 0.05, "ERAB_Setup_SR": 80.0 if bad else 99.6,
                             "AVG_DL_USER_THROUGHPUT": 3.0 if bad else 22.0})
        return pd.DataFrame(rows)

    def retrieve_viavi(sites, ctx):
        ctx["_viavi_sites"] = sites
        if "viavi" in fail:
            raise RuntimeError("Viavi connection failed (test)")
        if empty or not sites:
            return pd.DataFrame(columns=["PERIOD_TIMESTAMP", "FLWID", "AVG_FD_MS", "AVG_FDV_MS",
                                         "ONEW_FLR_AZ", "ONEW_FLR_ZA"])
        rows = [{"PERIOD_TIMESTAMP": t, "FLWID": f"MW_{sites[0]}_HUB1", "AVG_FD_MS": 4.0,
                 "AVG_FDV_MS": 0.5, "ONEW_FLR_AZ": 0.0, "ONEW_FLR_ZA": 0.0} for t in _times("15min")]
        return pd.DataFrame(rows)

    def retrieve_remedy(sites, ctx):
        if "remedy" in fail:
            raise RuntimeError("Remedy connection failed (test)")
        if empty or not sites:
            return {"remedy_tickets": {}, "status": 0}
        t = {"incident_number": f"INC{marker:04d}01", "site": sites[0], "status": "Assigned",
             "actual_fault_time": "2026-10-10 13:00:00", "summary": "Site down (test)"}
        return {"remedy_tickets": {sites[0]: {"total": 1, "status": {"Assigned": 1}, "tickets": [t]}},
                "status": 1}

    def retrieve_splunk(ctx):
        if "splunk" in fail:
            raise RuntimeError("Splunk unavailable (test)")
        if empty:
            return {}
        ts = _times("5min")
        mme = pd.DataFrame({"timestamp": ts,
                            "MME_CPULOAD__SGSN6": [95.0 if _in_window(t) else 40.0 for t in ts]})
        ggsn = pd.DataFrame({"timestamp": ts, "GGSN_PDPSR__ALX-GGSN_CI-10": [99.5] * len(ts)})
        return {"MME": mme, "GGSN": ggsn}

    return pl.DataSources(
        get_cells=get_cells, fetch_smartcare=fetch_smartcare,
        derive_sites=pl.default_sources().derive_sites,
        retrieve_npp=retrieve_npp, retrieve_viavi=retrieve_viavi,
        retrieve_remedy=retrieve_remedy, retrieve_splunk=retrieve_splunk,
        analyze_npp=lambda raw, sc, ctx: NppAnalyzer().analyze(raw, sc, ctx),   # no NPP chart fetch
        analyze_viavi=lambda raw, sc, ctx: ViaviAnalyzer().analyze(raw, sc, ctx),
        analyze_remedy=lambda raw, sc, ctx: RemedyAnalyzer().analyze(raw, sc, ctx),
        analyze_splunk=lambda raw, sc, ctx: SplunkAnalyzer().analyze(raw, sc, ctx),
    )


def dispatch_sources(by_first_msisdn):
    """One DataSources that routes each call to the fake chosen by the request's
    first MSISDN, so concurrent jobs get their own data without timing tricks."""
    def pick(ctx):
        return by_first_msisdn[str(ctx["msisdn_list"][0])]
    names = ("get_cells", "fetch_smartcare", "derive_sites", "retrieve_npp", "retrieve_viavi",
             "retrieve_remedy", "retrieve_splunk", "analyze_npp", "analyze_viavi",
             "analyze_remedy", "analyze_splunk")
    return pl.DataSources(**{n: (lambda n: lambda *a: getattr(pick(a[-1]), n)(*a))(n) for n in names})


def retrievers_for(sources, site_run=False):
    """collect.Retrievers over the fakes, wired exactly like collect.default_retrievers."""
    def trino(ctx):
        sources.get_cells(ctx)                  # the fake builds the Trino rows
        return ctx.pop(pl.TRINO_FRAME_KEY)
    return collect.Retrievers(
        trino=trino,
        smartcare=site_mode.fetch_trino if site_run else sources.fetch_smartcare,
        derive_sites=sources.derive_sites, npp=sources.retrieve_npp, viavi=sources.retrieve_viavi,
        remedy=sources.retrieve_remedy, splunk=sources.retrieve_splunk)


def recording(sources):
    """Copy of the fake sources that records what each system was asked for."""
    seen = {"trino": [], "smartcare": [], "npp": [], "viavi": [], "remedy": [], "splunk": 0}
    rec = dataclasses.replace(sources)

    def get_cells(ctx):
        seen["trino"].append(list(ctx.get("msisdn_list") or []))
        return sources.get_cells(ctx)

    def fetch_smartcare(ctx):
        seen["smartcare"].append({"msisdn_list": list(ctx.get("msisdn_list") or []),
                                  "complain_type": ctx.get("complain_type")})
        return sources.fetch_smartcare(ctx)

    def npp(entities, ctx):
        # NPP site endpoint: rows per cell of the requested site (columns cell + sitePhysical)
        seen["npp"].append({"entities": list(entities), "entity_type": ctx.get("npp_entity_type", "cell")})
        if ctx.get("npp_entity_type") == "site":
            entities = [f"L18_{s}_0{4 if i == 0 else 1}" for i, s in enumerate(entities)]
        return sources.retrieve_npp(entities, ctx)

    def by_sites(name, orig):
        def _f(sites, ctx):
            seen[name].append(list(sites))
            return orig(sites, ctx)
        return _f

    def splunk(ctx):
        seen["splunk"] += 1
        return sources.retrieve_splunk(ctx)
    rec.get_cells, rec.fetch_smartcare, rec.retrieve_npp, rec.retrieve_splunk = get_cells, fetch_smartcare, npp, splunk
    rec.retrieve_viavi = by_sites("viavi", sources.retrieve_viavi)
    rec.retrieve_remedy = by_sites("remedy", sources.retrieve_remedy)
    return rec, seen


# ---------------------------------------------------------------- helpers --
_ANALYSIS_CODE = [
    (pl, "run_plan"), (oa, "run_callflow_analysis"),
    (NppAnalyzer, "analyze"), (ViaviAnalyzer, "analyze"), (RemedyAnalyzer, "analyze"),
    (SplunkAnalyzer, "analyze"), (summary_readiness, "summary_cards"),
]


class _Base(SimpleTestCase):
    databases = []

    def setUp(self):
        self.out = Path(tempfile.mkdtemp(prefix="rca_run_test_"))
        self.addCleanup(shutil.rmtree, self.out, True)
        self._ovr = override_settings(
            OUTPUT_DIR=self.out, THRESHOLDING_SHEET_PATH=str(THRESHOLDS), RCA_TOOL_PATH=str(TOOL),
            RCA_TOOL_TIMEOUT_S=180, RCA_RUN_SLOT_WAIT_S=600,
            CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                "LOCATION": f"rca-run-{id(self)}"}})
        self._ovr.enable()
        self.addCleanup(self._ovr.disable)
        self.factory = APIRequestFactory()
        # The RCA endpoints must never run the portal pipeline: orchestrator plan,
        # system analyzers, summary cards. Any call fails the job and the test.
        self.analysis_calls = []
        for owner, attr in _ANALYSIS_CODE:
            p = mock.patch.object(owner, attr, side_effect=self._analysis_called(f"{owner.__name__}.{attr}"))
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(lambda: self.assertEqual(self.analysis_calls, [], "portal analysis code ran"))

    def _analysis_called(self, name):
        def _f(*a, **k):
            self.analysis_calls.append(name)
            raise AssertionError(f"{name} must not run for an RCA request")
        return _f

    def post(self, sources, msisdns=MSISDNS, body=None, view=RCARunAPIView, path="/callflow/api/rca/run"):
        req = self.factory.post(path, body or {
            "msisdn_list": msisdns, "start_date": START, "end_date": END, "Complain_Type": "Data"},
            format="json")
        with mock.patch.object(collect, "default_retrievers", lambda site_run: retrievers_for(sources, site_run)):
            resp = view.as_view()(req)
            self.assertEqual(resp.status_code, 202, resp.data)
            job_id = resp.data["job_id"]
            return job_id, self.wait(job_id)

    def post_site(self, sources, body):
        return self.post(sources, body=body, view=RCASiteRunAPIView, path="/callflow/api/rca/sites/run")

    def wait(self, job_id, timeout=400):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = job_manager.get_job(job_id)
            if job and job["status"] in ("DONE", "FAILED"):
                return job
            time.sleep(1)
        self.fail(f"job {job_id} did not finish in {timeout}s")

    def get(self, job_id, view=RCARunAPIView, suffix=""):
        return view.as_view()(self.factory.get(f"/callflow/api/rca/run/{job_id}{suffix}"), job_id=job_id)

    def rdir(self, job_id):
        return self.out / RCARunAPIView.output_subdir / "rca" / job_id

    def request_log(self, name, job_id):
        lines = (self.out / RCARunAPIView.output_subdir / "logs" / f"{name}.txt").read_text().splitlines()
        return [l for l in lines if job_id in l]

    def tool_copy(self, script):
        """The real tool plus one extra <script> at the end (test-only fault injection)."""
        p = self.out / "rca_tool_fault.html"
        html = TOOL.read_text(encoding="utf-8")
        p.write_text(html.replace("</body>", f"<script>{script}</script></body>"), encoding="utf-8")
        return str(p)


# ------------------------------------------------------------------ tests --
@unittest.skipUnless(HAVE_PW and TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class RCARunEndToEndTests(_Base):

    def test_success_returns_clean_report(self):
        job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id).data
        json.dumps(rep)                                   # valid JSON, no NaN/objects
        self.assertEqual(rep["request_id"], job_id)
        self.assertEqual(rep["status"], "COMPLETED")
        self.assertEqual(rep["input"], {"mode": "msisdn", "sites": None, "msisdn_count": len(MSISDNS),
                                        "complain_type": "Data"})
        self.assertEqual(set(rep), {"report_version", "request_id", "status", "input", "analysis_window",
                                    "data_coverage", "summary", "root_cause", "findings", "suspects",
                                    "impact", "warnings", "links"})
        self.assertEqual(rep["data_coverage"]["systems_available"],
                         ["npp", "remedy", "smartcare", "splunk", "trino", "viavi"])
        flat = json.dumps(rep)
        for leaked in ("analysisLog", "browser", "kpi_files", "manifest", "dashboard", "details",
                       "rca_tool_log", "duration_seconds", "subProfiles", "anomalyList"):
            self.assertNotIn(leaked, flat)
        self.assertTrue(rep["summary"]["primary_root_cause"])
        self.assertEqual(rep["summary"]["primary_root_cause"], rep["root_cause"]["hypothesis"])
        self.assertIn(rep["summary"]["confidence"], ("HIGH", "MEDIUM", "LOW"))
        self.assertTrue(rep["root_cause"]["recommended_actions"])
        self.assertTrue(rep["findings"])
        f = rep["findings"][0]
        self.assertEqual(set(f), {"id", "system", "kpi", "node", "site", "severity", "observed",
                                  "baseline", "deviation_pct", "direction", "threshold"})
        self.assertIsInstance(f["observed"], (int, float))
        self.assertTrue(any(x["node"] == "L18_LU4820_04" and x["kpi"] == "CFR" for x in rep["findings"]))
        ids = {x["id"] for x in rep["findings"]}
        top = rep["suspects"]["sites"][0]
        self.assertEqual(top["site"], "LU4820")
        self.assertTrue(set(top["finding_ids"]) <= ids and top["finding_ids"])
        csv_resp = self.get(job_id, RCARunFindingsCSVView, "/findings.csv")
        self.assertEqual(csv_resp.status_code, 200)
        lines = csv_resp.content.decode().strip().splitlines()
        self.assertEqual(lines[0], "request_id,id,system,kpi,node,site,severity,observed,baseline,"
                                   "deviation_pct,direction,threshold")
        self.assertEqual(len(lines) - 1, len(rep["findings"]))
        # the tool loaded exactly this request's files
        diag = self.get(job_id, RCARunDiagnosticsView, "/diagnostics").data
        loaded = diag["pipeline"]["rca_tool"]["inputs"]
        self.assertEqual(sorted(loaded["kpi_files"]), sorted(
            ["npp_kpi.csv", "remedy_kpi.csv", "splunk_ggsn_kpi.csv", "splunk_mme_kpi.csv",
             "trino_cells.csv", "viavi_kpi.csv"]))
        self.assertEqual(loaded["filter_file"], "smartcare_filter.csv")
        self.assertEqual(sorted(diag["pipeline"]["collection"]["systems"]),
                         ["npp", "remedy", "smartcare", "splunk", "trino", "viavi"])

    def test_result_survives_job_expiry(self):
        job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "DONE", job.get("error"))
        from django.core.cache import cache
        cache.delete(f"callflow_job:{job_id}")
        resp = self.get(job_id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["status"], "COMPLETED")
        self.assertTrue(resp.data["findings"])

    def test_failed_system_gives_partial_report(self):
        job_id, job = self.post(fake_sources(fail=("npp",)))
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id).data
        self.assertEqual(rep["status"], "PARTIAL")
        self.assertEqual(rep["data_coverage"]["systems_missing"], ["npp"])
        w = [w for w in rep["warnings"] if w["code"] == "SYSTEM_UNAVAILABLE"]
        self.assertEqual(len(w), 1)
        self.assertIn("NPP API 500 (test)", w[0]["message"])
        self.assertFalse(any(f["system"] == "NPP" for f in rep["findings"]))
        self.assertFalse((self.rdir(job_id) / "npp_kpi.csv").exists())

    def test_trino_failure_skips_cell_based_systems(self):
        job_id, job = self.post(fake_sources(fail=("trino",)))
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id).data
        self.assertEqual(rep["data_coverage"]["systems_missing"], ["npp", "remedy", "trino", "viavi"])
        self.assertEqual(rep["data_coverage"]["systems_available"], ["smartcare", "splunk"])

    def test_missing_csvs_fail_clearly(self):
        job_id, job = self.post(fake_sources(empty=True))
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_NO_INPUT_FILES", job["error"])
        err = json.loads((self.rdir(job_id) / "rca_error.json").read_text())
        self.assertEqual(err["code"], "RCA_NO_INPUT_FILES")
        resp = self.get(job_id)
        self.assertEqual(resp.data["status"], "FAILED")
        self.assertEqual(resp.data["error"]["code"], "RCA_NO_INPUT_FILES")
        self.assertNotIn("details", resp.data["error"])

    def test_rca_javascript_error_is_reported(self):
        tool = self.tool_copy("window.parseFiles = async function () { throw new Error('injected parse failure'); };")
        with override_settings(RCA_TOOL_PATH=tool):
            job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_JS_ERROR", job["error"])
        self.assertIn("injected parse failure", job["error"])
        self.assertTrue((self.rdir(job_id) / "browser_console.log").is_file())

    def test_rca_timeout_is_reported_and_browser_released(self):
        tool = self.tool_copy("window.parseFiles = function () { return new Promise(function () {}); };")
        with override_settings(RCA_TOOL_PATH=tool, RCA_TOOL_TIMEOUT_S=8):
            job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_TIMEOUT", job["error"])
        job_id2, job2 = self.post(fake_sources())
        self.assertEqual(job2["status"], "DONE", job2.get("error"))

    def test_slow_system_times_out_without_writing_late(self):
        slow = fake_sources()
        orig = slow.retrieve_viavi

        def stuck(sites, ctx):
            time.sleep(6)
            return orig(sites, ctx)
        slow.retrieve_viavi = stuck
        with override_settings(RCA_RETRIEVAL_TIMEOUT_S=2):
            job_id, job = self.post(slow)
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id).data
        self.assertIn("viavi", rep["data_coverage"]["systems_missing"])
        time.sleep(5)                                # the stuck retrieval finishes now
        self.assertFalse((self.rdir(job_id) / "viavi_kpi.csv").exists())

    def test_tool_not_found(self):
        with override_settings(RCA_TOOL_PATH=str(self.out / "missing.html")):
            _, job = self.post(fake_sources())
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_TOOL_NOT_FOUND", job["error"])

    def test_concurrent_requests_are_isolated(self):
        ms_a = [f"10{i:08d}" for i in range(1, 7)]
        ms_b = [f"11{i:08d}" for i in range(1, 5)]
        sources = dispatch_sources({ms_a[0]: fake_sources(bad_cell="L18_LU4820_04", marker=0),
                                    ms_b[0]: fake_sources(bad_cell="L18_LU4830_02", marker=50)})
        jobs = {}
        with override_settings(RCA_RUN_MAX_BROWSERS=1), \
                mock.patch.object(collect, "default_retrievers", lambda site_run: retrievers_for(sources)):
            for name, ms in (("A", ms_a), ("B", ms_b)):      # both start before either finishes
                resp = RCARunAPIView.as_view()(self.factory.post("/callflow/api/rca/run", {
                    "msisdn_list": ms, "start_date": START, "end_date": END}, format="json"))
                self.assertEqual(resp.status_code, 202)
                jobs[name] = resp.data["job_id"]
            done = {n: self.wait(j) for n, j in jobs.items()}
        self.assertNotEqual(jobs["A"], jobs["B"])
        for n, job in done.items():
            self.assertEqual(job["status"], "DONE", job.get("error"))
        npp_a = (self.rdir(jobs["A"]) / "npp_kpi.csv").read_text()
        npp_b = (self.rdir(jobs["B"]) / "npp_kpi.csv").read_text()
        self.assertIn("L18_LU4820_04", npp_a)
        self.assertNotIn("L18_LU4830_02", npp_a)
        self.assertIn("L18_LU4830_02", npp_b)
        self.assertNotIn("L18_LU4820_04", npp_b)
        an_a = json.dumps(done["A"]["result"]["findings"])
        an_b = json.dumps(done["B"]["result"]["findings"])
        self.assertIn("LU4820", an_a)
        self.assertNotIn("LU4830", an_a)
        self.assertIn("LU4830", an_b)
        self.assertNotIn("LU4820", an_b)


# ------------------------------------------------- MSISDN run vs site run --
SITES = ["LU4820", "LU4821"]


class FakeTrinoSites:
    """Stands in for run_trino_query in smartcare_site_kpi_service (the Trino
    call) and records every S1-MME query the real service sends. Returns
    lower-cased column names, as Trino does."""

    COLS = ["time", "site_logical_name", "attach_req_times", "attach_fail_times", "tau_req_times",
            "tau_fail_times", "s1mme_authentication_req_times", "s1mme_authentication_fail_times",
            "fail_cause_name"]

    def __init__(self, rows=True):
        self.calls = []
        self.rows = rows

    def __call__(self, sql):
        source = (site_kpi_service.RECENT_SOURCE if "clickhouse.e2e." in sql
                  else site_kpi_service.HISTORY_SOURCE)
        start = int(re.search(r">= (\d+)", sql).group(1))
        end = int(re.search(r"\s<\s(\d+)", sql).group(1))
        self.calls.append({"source": source, "sql": sql, "start_epoch": start, "end_epoch": end})
        if not self.rows:
            return pd.DataFrame(columns=self.COLS)
        auth = [7, 3] if source == site_kpi_service.RECENT_SOURCE else [None, None]
        return pd.DataFrame([[start, site, req, fail, 20, 2, a, 1 if a else None, "cause"]
                             for site, req, fail, a in zip(SITES, [100, 80], [5, 1], auth)],
                            columns=self.COLS)


class RequestFormatTests(SimpleTestCase):
    """Request validation of the two endpoints (no systems, no browser)."""
    databases = []

    def test_msisdn_request_new_format(self):
        r = site_mode.parse_msisdn_request({"msisdn": "01012345678, 01098765432", "complain_type": "data",
                                            "start": "2026-10-21 10:00:00", "end": "2026-10-21 12:00:00"})
        self.assertEqual(r["msisdn_list"], ["01012345678", "01098765432"])
        self.assertEqual(r["complain_type"], "Data")      # the value the Smartcare service compares

    def test_msisdn_request_previous_format_still_accepted(self):
        r = site_mode.parse_msisdn_request({"msisdn_list": ["1012345678"], "Complain_Type": "Voice",
                                            "imsi": "", "start_date": START, "end_date": END})
        self.assertEqual((r["msisdn_list"], r["complain_type"], r["start"]), (["1012345678"], "Voice", START))
        r = site_mode.parse_msisdn_request({"msisdn_list": ["1012345678"], "start_date": START, "end_date": END})
        self.assertEqual(r["complain_type"], "Data")      # previous default

    def test_site_request(self):
        r = site_mode.parse_site_request({"sitelist": ["SITE_A", "SITE_B", "SITE_A"],
                                          "start": "2026-10-21 10:00:00", "end": "2026-10-21 12:00:00"})
        self.assertEqual(r["sitelist"], ["SITE_A", "SITE_B"])
        self.assertEqual(site_mode.parse_site_request({"sitelist": "SITE_A,SITE_B", "start": START,
                                                       "end": END})["sitelist"], ["SITE_A", "SITE_B"])

    def test_invalid_requests(self):
        cases = [
            (site_mode.parse_msisdn_request, "send site requests",
             {"sitelist": ["SITE_A"], "msisdn": "01012345678", "start": START, "end": END}),
            (site_mode.parse_msisdn_request, "'msisdn' is required", {"start": START, "end": END}),
            (site_mode.parse_msisdn_request, "invalid MSISDN", {"msisdn": "0101234abc", "start": START, "end": END}),
            (site_mode.parse_msisdn_request, "must be one of",
             {"msisdn": "01012345678", "complain_type": "sms", "start": START, "end": END}),
            (site_mode.parse_site_request, "send MSISDN requests",
             {"sitelist": ["SITE_A"], "msisdn": "01012345678", "start": START, "end": END}),
            (site_mode.parse_site_request, "'complain_type' not accepted",
             {"sitelist": ["SITE_A"], "complain_type": "data", "start": START, "end": END}),
            (site_mode.parse_site_request, "'sitelist' is required", {"start": START, "end": END}),
            (site_mode.parse_site_request, "'start' and 'end' are required", {"sitelist": ["SITE_A"], "start": START}),
            (site_mode.parse_site_request, "invalid site id",
             {"sitelist": ["SITE_A') OR 1=1 --"], "start": START, "end": END}),
            (site_mode.parse_site_request, "'sitelist' is empty", {"sitelist": " , ", "start": START, "end": END}),
        ]
        for parse, msg, body in cases:
            with self.subTest(msg):
                with self.assertRaises(ValueError) as cm:
                    parse(body)
                self.assertIn(msg, str(cm.exception))

    def test_invalid_request_is_http_400(self):
        f = APIRequestFactory()
        resp = RCARunAPIView.as_view()(f.post("/callflow/api/rca/run", {
            "sitelist": ["SITE_A"], "start": START, "end": END}, format="json"))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("/callflow/api/rca/sites/run", resp.data["error"])
        resp = RCASiteRunAPIView.as_view()(f.post("/callflow/api/rca/sites/run", {
            "msisdn": "01012345678", "start": START, "end": END}, format="json"))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("/callflow/api/rca/run", resp.data["error"])
        resp = RCASiteRunAPIView.as_view()(f.post("/callflow/api/rca/sites/run", {
            "sitelist": ["SITE_A"], "start": "2026-10-21 12:00:00", "end": "2026-10-21 10:00:00"},
            format="json"))
        self.assertEqual(resp.status_code, 400)

    def test_site_kpi_window_defaults_unchanged(self):
        import datetime as dt
        now = dt.datetime(2026, 10, 21, 12, 7)
        w = site_kpi_service.build_nci_time_windows(now=now)      # SmartcareSiteKpiAPIView path
        self.assertEqual((w["current"]["start"], w["current"]["end"]),
                         (dt.datetime(2026, 10, 21, 0, 0), dt.datetime(2026, 10, 21, 12, 0)))
        a, b = dt.datetime(2026, 10, 21, 10), dt.datetime(2026, 10, 21, 12)
        w = site_kpi_service.build_nci_time_windows(start=a, end=b)
        self.assertEqual((w["current"]["start"], w["current"]["end"]), (a, b))
        self.assertEqual(w["prev_1day"]["start"], a - dt.timedelta(days=1))
        self.assertEqual(w["prev_1week"]["end"], b - dt.timedelta(days=7))

    def test_npp_retrieval_passes_entity_type(self):
        calls = []

        def fake_npp(entities, **kw):
            calls.append((list(entities), kw["entity_type"]))
            return pd.DataFrame()
        qtr = {"current_start": 1, "current_end": 2, "prev_start_1day": 3, "prev_end_1day": 4}
        with mock.patch.object(pl, "nppIntegration", fake_npp):
            src = pl.default_sources()
            src.retrieve_npp(["L18_LU4820_04"], {"query_time_range": qtr})
            src.retrieve_npp(SITES, {"query_time_range": qtr, "npp_entity_type": "site"})
        self.assertEqual({c[1] for c in calls[:2]}, {"cell"})     # MSISDN run: unchanged
        self.assertEqual(calls[2:], [(SITES, "site"), (SITES, "site")])

    def test_default_retrievers_use_existing_functions(self):
        with mock.patch.object(collect, "default_sources", return_value=fake_sources()) as ds:
            r_msisdn = collect.default_retrievers(False)
            r_site = collect.default_retrievers(True)
        src = ds.return_value
        self.assertIs(r_msisdn.smartcare, src.fetch_smartcare)            # MSISDN Smartcare query
        self.assertIs(r_site.smartcare, site_mode.fetch_trino)        # SmartcareSiteKpiAPIView code
        for r in (r_msisdn, r_site):
            self.assertEqual((r.npp, r.viavi, r.remedy, r.splunk),
                             (src.retrieve_npp, src.retrieve_viavi, src.retrieve_remedy, src.retrieve_splunk))


@unittest.skipUnless(HAVE_PW and TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class RCARunRequestModeTests(_Base):

    def test_msisdn_run_collects_raw_data_only(self):
        sources, seen = recording(fake_sources())
        job_id, job = self.post(sources, body={"msisdn": ",".join(MSISDNS), "complain_type": "data",
                                               "start": "2026-10-10 14:21:00", "end": "2026-10-10 15:21:00"})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        self.assertEqual(seen["trino"], [MSISDNS])                                         # Trino ran
        self.assertEqual(seen["smartcare"], [{"msisdn_list": MSISDNS, "complain_type": "Data"}])
        self.assertEqual(seen["npp"], [{"entities": ["L18_LU4820_04", "L18_LU4821_01"], "entity_type": "cell"}])
        derived = pl.default_sources().derive_sites(["L18_LU4820_04", "L18_LU4821_01"], {})
        self.assertEqual(seen["viavi"], [derived])           # sites derived from the Trino cells
        self.assertEqual(seen["remedy"], [derived])
        self.assertEqual(seen["splunk"], 1)
        rep = self.get(job_id).data
        self.assertEqual(rep["input"]["mode"], "msisdn")
        self.assertIn("trino", rep["data_coverage"]["systems_available"])
        self.assertEqual(rep["links"]["diagnostics"], f"/callflow/api/rca/run/{job_id}/diagnostics")
        log = "\n".join(self.request_log("rca_run", job_id))
        for system in ("trino", "smartcare", "npp", "viavi", "remedy", "splunk"):
            self.assertIn(f"system={system} status=COMPLETED", log)
        self.assertNotIn("[orchestration]", log)
        self.assertNotIn("_analysis", log)
        self.assertNotIn("card.", log)

    def test_site_run_skips_trino_and_feeds_sites_to_systems(self):
        sources, seen = recording(fake_sources())
        fake_db = FakeTrinoSites()
        with mock.patch.object(site_kpi_service, "run_trino_query", fake_db):
            job_id, job = self.post_site(sources, {"sitelist": SITES, "start": "2026-10-10 14:21:00",
                                                   "end": "2026-10-10 15:21:00"})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        # no Trino, no MSISDN Smartcare query; the sites go straight to NPP / Viavi / Remedy
        self.assertEqual((seen["trino"], seen["smartcare"]), ([], []))
        self.assertEqual(seen["npp"], [{"entities": SITES, "entity_type": "site"}])
        self.assertEqual(seen["viavi"], [SITES])
        self.assertEqual(seen["remedy"], [SITES])
        self.assertEqual(seen["splunk"], 1)

        # Site KPIs: SmartcareSiteKpiAPIView's Trino S1-MME queries for the sites and the
        # request's range. The range is older than 6 hours, so only the Hive query runs.
        self.assertEqual({c["source"] for c in fake_db.calls}, {site_kpi_service.HISTORY_SOURCE})
        for c in fake_db.calls:
            self.assertIn("IN ('LU4820','LU4821')", c["sql"])
        import datetime as dt
        epochs = lambda a, b: site_kpi_service._epochs(dt.datetime.fromisoformat(a), dt.datetime.fromisoformat(b))[:2]
        self.assertEqual({(c["start_epoch"], c["end_epoch"]) for c in fake_db.calls},
                         {epochs("2026-10-10 14:21:00", "2026-10-10 15:21:00"),     # request range
                          epochs("2026-10-09 14:21:00", "2026-10-09 15:21:00"),     # 1 day earlier
                          epochs("2026-10-03 14:21:00", "2026-10-03 15:21:00")})    # 1 week earlier

        rdir = self.rdir(job_id)
        kpis = json.loads((rdir / "smartcare_site_kpis.json").read_text())
        self.assertEqual(kpis["windows"]["current"]["s1mme"][0]["LOGICAL_SITE_ID"], "LU4820")
        self.assertFalse((rdir / "trino_cells.csv").exists())
        self.assertEqual((rdir / "smartcare_filter.csv").read_text().split(),
                         ["cell_name", "L18_LU4820_04", "L18_LU4821_01"])   # NPP's own cell names
        npp = (rdir / "npp_kpi.csv").read_text()
        self.assertIn("L18_LU4820_04", npp)
        self.assertNotIn("trino", (rdir / "manifest.json").read_text())

        rep = self.get(job_id, RCASiteRunAPIView).data
        self.assertEqual(rep["status"], "COMPLETED")
        self.assertEqual(rep["input"], {"mode": "site", "sites": SITES, "msisdn_count": None,
                                        "complain_type": None})
        self.assertEqual(rep["data_coverage"]["systems_available"],
                         ["npp", "remedy", "smartcare", "splunk", "viavi"])
        self.assertTrue(any(x["node"] == "L18_LU4820_04" and x["kpi"] == "CFR" for x in rep["findings"]))
        self.assertEqual(rep["suspects"]["sites"][0]["site"], "LU4820")
        self.assertTrue(rep["summary"]["primary_root_cause"])
        self.assertEqual(rep["links"]["findings_csv"], f"/callflow/api/rca/sites/run/{job_id}/findings.csv")
        diag = self.get(job_id, RCARunDiagnosticsView, "/diagnostics").data
        loaded = diag["pipeline"]["rca_tool"]["inputs"]
        self.assertEqual(sorted(loaded["kpi_files"]), sorted(
            ["npp_kpi.csv", "remedy_kpi.csv", "splunk_ggsn_kpi.csv", "splunk_mme_kpi.csv", "viavi_kpi.csv"]))
        self.assertEqual(loaded["filter_file"], "smartcare_filter.csv")

        log = "\n".join(self.request_log("rca_site_run", job_id))
        for system in ("smartcare", "npp", "viavi", "remedy", "splunk"):
            self.assertIn(f"system={system} status=COMPLETED", log)
        self.assertNotIn("system=trino", log)
        self.assertNotIn("[orchestration]", log)
        self.assertNotIn("cells:", log)                    # no cell resolution / site derivation

    def test_site_run_smartcare_without_cells_is_no_data_not_success(self):
        # Trino returned no S1-MME rows for the sites.
        with mock.patch.object(site_kpi_service, "run_trino_query", FakeTrinoSites(rows=False)):
            job_id, job = self.post_site(fake_sources(), {"sitelist": SITES, "start": START, "end": END})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id, RCASiteRunAPIView).data
        self.assertEqual(rep["data_coverage"]["systems_without_data"], ["smartcare"])
        self.assertNotIn("smartcare", rep["data_coverage"]["systems_available"])
        w = [w for w in rep["warnings"] if w.get("system") == "smartcare"][0]
        self.assertEqual(w["code"], "NO_DATA")
        self.assertIn("no S1-MME rows for these sites in Trino", w["message"])

    def _real_naming_site_run(self, bad_cell):
        """Production naming: site K2398's LTE cells are named L.._LK2398_.., and the
        tool extracts the site LK2398 from them, not the requested K2398."""
        sources = fake_sources(bad_cell=bad_cell)
        cells = ["L18_LK2398_01", "L18_LK6068_02"]
        orig = sources.retrieve_npp

        def npp_site_endpoint(entities, ctx):
            self.assertEqual((list(entities), ctx["npp_entity_type"]), (["K2398", "K6068"], "site"))
            return orig(cells, ctx)
        sources.retrieve_npp = npp_site_endpoint
        with mock.patch.object(site_kpi_service, "run_trino_query", FakeTrinoSites(rows=False)):
            job_id, job = self.post_site(sources, {"sitelist": ["K2398", "K6068"], "start": START, "end": END})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        return job_id, self.get(job_id, RCASiteRunAPIView).data

    def test_site_run_npp_findings_match_real_cell_naming(self):
        job_id, rep = self._real_naming_site_run("L18_LK2398_01")
        self.assertEqual((self.rdir(job_id) / "smartcare_filter.csv").read_text().split(),
                         ["cell_name", "L18_LK2398_01", "L18_LK6068_02"])
        npp = [f for f in rep["findings"] if f["system"] == "NPP"]
        self.assertTrue(npp, "NPP rows were dropped by the RCA tool's scope check")
        self.assertEqual({f["node"] for f in npp}, {"L18_LK2398_01"})
        top = rep["suspects"]["sites"][0]
        self.assertEqual(top["site"], "LK2398")
        self.assertTrue(top["critical_issues"] and top["finding_ids"])
        diag = self.get(job_id, RCARunDiagnosticsView, "/diagnostics").data["pipeline"]["collection"]
        self.assertEqual(diag["scope"]["cells"], 2)
        self.assertEqual(diag["systems"]["npp"]["data"]["instances"], 2)

        # different input data -> a different RCA result (the tool analyses the files)
        job_id2, rep2 = self._real_naming_site_run("L18_LK6068_02")
        self.assertEqual({f["node"] for f in rep2["findings"] if f["system"] == "NPP"}, {"L18_LK6068_02"})
        self.assertEqual(rep2["suspects"]["sites"][0]["site"], "LK6068")
        self.assertNotEqual(rep["summary"]["primary_root_cause"], rep2["summary"]["primary_root_cause"])

    def test_browser_runs_in_cairo_time(self):
        tool = self.tool_copy("console.log('TZ=' + Intl.DateTimeFormat().resolvedOptions().timeZone);")
        with override_settings(RCA_TOOL_PATH=tool):
            job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "DONE", job.get("error"))
        self.assertIn("TZ=Africa/Cairo", (self.rdir(job_id) / "browser_console.log").read_text())

    def test_site_run_with_failed_npp_is_partial(self):
        with mock.patch.object(site_kpi_service, "run_trino_query", FakeTrinoSites()):
            job_id, job = self.post_site(fake_sources(fail=("npp",)), {"sitelist": SITES, "start": START,
                                                                       "end": END})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        rep = self.get(job_id, RCASiteRunAPIView).data
        self.assertEqual(rep["status"], "PARTIAL")
        self.assertEqual(rep["data_coverage"]["systems_missing"], ["npp"])


class SlotLimiterTests(SimpleTestCase):
    databases = []

    def test_slots_are_exclusive_and_time_out(self):
        from . import limits
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        with limits.slot(d, "t", 1, 5):
            with self.assertRaises(limits.SlotTimeout):
                with limits.slot(d, "t", 1, 0.5, poll_seconds=0.1):
                    pass
        with limits.slot(d, "t", 1, 1):   # free again after release
            pass
