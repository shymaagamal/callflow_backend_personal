# callflow/core/rca_runner/tests.py
"""
End-to-end tests for POST /callflow/api/rca/run.

Real code under test: RCARunAPIView, the orchestration DAG + wrappers (RCA file
writers, manifest), the analyzers, rca_runner.browser_runner, and the REAL
rca_tool_V24.html running in headless Chromium. Only the six external systems
are replaced by in-memory fakes that return data in the same shapes the real
services return, so no Trino/Smartcare/NPP/Splunk/Viavi/Remedy is touched.

Run:  python manage.py test callflow.core.rca_runner
(The browser tests are skipped when Playwright/Chromium is not installed.)
"""

import dataclasses
import json
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
from ..orchestration.analyzers import NppAnalyzer, ViaviAnalyzer, RemedyAnalyzer, SplunkAnalyzer
from ..orchestration.customer_counts import CTX_KEY
from ..services.smartcare_service import process_smartcare_data
from ..services.trino_service import customer_counts_from_df
from ..views import orchestrated_analysis as oa
from ..views.rca_run import RCARunAPIView

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


# ---------------------------------------------------------------- helpers --
class _Base(SimpleTestCase):
    databases = []

    def setUp(self):
        self.out = Path(tempfile.mkdtemp(prefix="rca_run_test_"))
        self.addCleanup(shutil.rmtree, self.out, True)
        self._ovr = override_settings(
            OUTPUT_DIR=self.out, THRESHOLDING_SHEET_PATH=str(THRESHOLDS), RCA_TOOL_PATH=str(TOOL),
            RCA_TOOL_TIMEOUT_S=180, RCA_FILES_WAIT_S=5, RCA_RUN_SLOT_WAIT_S=600,
            CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                "LOCATION": f"rca-run-{id(self)}"}})
        self._ovr.enable()
        self.addCleanup(self._ovr.disable)
        self.factory = APIRequestFactory()

    def post(self, sources, msisdns=MSISDNS):
        req = self.factory.post("/callflow/api/rca/run", {
            "msisdn_list": msisdns, "start_date": START, "end_date": END, "Complain_Type": "Data"},
            format="json")
        # default_sources() returns a NEW DataSources per job in production and the
        # view wraps it in place, so hand out a fresh copy per call here too.
        with mock.patch.object(oa, "default_sources", lambda: dataclasses.replace(sources)):
            resp = RCARunAPIView.as_view()(req)
            self.assertEqual(resp.status_code, 202, resp.data)
            job_id = resp.data["job_id"]
            return job_id, self.wait(job_id)

    def wait(self, job_id, timeout=400):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = job_manager.get_job(job_id)
            if job and job["status"] in ("DONE", "FAILED"):
                return job
            time.sleep(1)
        self.fail(f"job {job_id} did not finish in {timeout}s")

    def get(self, job_id):
        return RCARunAPIView.as_view()(self.factory.get(f"/callflow/api/rca/run/{job_id}"), job_id=job_id)

    def rdir(self, job_id):
        return self.out / "MI" / "rca" / job_id

    def tool_copy(self, script):
        """The real tool plus one extra <script> at the end (test-only fault injection)."""
        p = self.out / "rca_tool_fault.html"
        html = TOOL.read_text(encoding="utf-8")
        p.write_text(html.replace("</body>", f"<script>{script}</script></body>"), encoding="utf-8")
        return str(p)


# ------------------------------------------------------------------ tests --
@unittest.skipUnless(HAVE_PW and TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class RCARunEndToEndTests(_Base):

    def test_success_returns_real_rca_results(self):
        job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "DONE", job.get("error"))
        res = job["result"]
        self.assertEqual(res["request_id"], job_id)
        self.assertEqual(res["status"], "COMPLETED")
        rca = res["rca"]
        self.assertEqual(rca["status"], "SUCCESS")
        # files went through the tool's own loader
        self.assertIn("npp_kpi.csv", rca["inputs"]["kpi_files"])
        self.assertEqual(rca["inputs"]["filter_file"], "smartcare_filter.csv")
        self.assertEqual(rca["inputs"]["threshold_file"], "thresholds.xlsx")
        # the tool's structured results, not just logs
        r = rca["results"]
        for key in ("summary", "chains", "suspects", "nodeSuspects", "anomalies", "detectedNodeTypes"):
            self.assertIn(key, r)
        self.assertGreater(r["summary"]["total"], 0, "the degraded cell must produce anomalies")
        self.assertTrue(any(a.get("kpiName") for a in r["anomalies"]))
        self.assertIsNotNone(rca["root_cause"])
        self.assertIn("hypothesis", rca["root_cause"])
        # request-scoped artifacts under MI
        d = self.rdir(job_id)
        for name in ("rca_result.json", "rca_tool_log.json", "browser_console.log", "manifest.json",
                     "npp_kpi.csv", "smartcare_filter.csv"):
            self.assertTrue((d / name).is_file(), name)
        self.assertTrue((self.out / "MI" / "logs" / "rca_run.txt").is_file())

    def test_result_survives_job_expiry(self):
        job_id, job = self.post(fake_sources())
        self.assertEqual(job["status"], "DONE", job.get("error"))
        from django.core.cache import cache
        cache.delete(f"callflow_job:{job_id}")
        resp = self.get(job_id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["status"], "DONE")
        self.assertEqual(resp.data["source"], "file")
        self.assertEqual(resp.data["result"]["rca"]["status"], "SUCCESS")

    def test_failed_system_still_runs_rca_on_the_rest(self):
        job_id, job = self.post(fake_sources(fail=("npp",)))
        self.assertEqual(job["status"], "DONE", job.get("error"))
        res = job["result"]
        self.assertEqual(res["status"], "PARTIAL")
        self.assertIn("npp", res["failed_systems"])
        self.assertEqual(res["systems"]["npp"], "FAILED")
        self.assertNotIn("npp_kpi.csv", res["rca"]["inputs"]["kpi_files"])
        self.assertEqual(res["rca"]["status"], "SUCCESS")

    def test_missing_csvs_fail_clearly(self):
        job_id, job = self.post(fake_sources(empty=True))
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_NO_INPUT_FILES", job["error"])
        self.assertEqual(job["rca_run"]["code"], "RCA_NO_INPUT_FILES")
        err = json.loads((self.rdir(job_id) / "rca_error.json").read_text())
        self.assertEqual(err["code"], "RCA_NO_INPUT_FILES")
        resp = self.get(job_id)
        self.assertEqual(resp.data["status"], "FAILED")

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
        # the browser slot was released: a normal run right after still completes
        job_id2, job2 = self.post(fake_sources())
        self.assertEqual(job2["status"], "DONE", job2.get("error"))

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
                mock.patch.object(oa, "default_sources", lambda: dataclasses.replace(sources)):
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
        an_a = json.dumps(done["A"]["result"]["rca"]["results"]["anomalies"])
        an_b = json.dumps(done["B"]["result"]["rca"]["results"]["anomalies"])
        self.assertIn("LU4820", an_a)
        self.assertNotIn("LU4830", an_a)
        self.assertIn("LU4830", an_b)
        self.assertNotIn("LU4820", an_b)


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
