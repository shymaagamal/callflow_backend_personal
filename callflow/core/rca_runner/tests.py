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
from ..services import smartcare_site_kpi_service as site_kpi_service
from ..services.smartcare_service import process_smartcare_data
from ..services.trino_service import customer_counts_from_df
from ..views import orchestrated_analysis as oa
from ..views.rca_run import RCARunAPIView, RCASiteRunAPIView, RCARunFindingsCSVView, RCARunDiagnosticsView
from . import site_mode

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

    def post(self, sources, msisdns=MSISDNS, body=None):
        req = self.factory.post("/callflow/api/rca/run", body or {
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

    def get(self, job_id, view=RCARunAPIView, suffix=""):
        return view.as_view()(self.factory.get(f"/callflow/api/rca/run/{job_id}{suffix}"), job_id=job_id)

    def rdir(self, job_id):
        return self.out / RCARunAPIView.output_subdir / "rca" / job_id

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
        # nothing from the pipeline / browser / logs leaks into the report
        flat = json.dumps(rep)
        for leaked in ("analysisLog", "browser", "kpi_files", "manifest", "dashboard", "details",
                       "rca_tool_log", "duration_seconds", "subProfiles", "anomalyList"):
            self.assertNotIn(leaked, flat)
        # values are the tool's, with predictable types
        self.assertTrue(rep["summary"]["primary_root_cause"])
        self.assertEqual(rep["summary"]["primary_root_cause"], rep["root_cause"]["hypothesis"])
        self.assertIn(rep["summary"]["confidence"], ("HIGH", "MEDIUM", "LOW"))
        self.assertTrue(rep["root_cause"]["recommended_actions"])
        self.assertTrue(rep["root_cause"]["evidence_chain"])
        self.assertTrue(rep["findings"])
        f = rep["findings"][0]
        self.assertEqual(set(f), {"id", "system", "kpi", "node", "site", "severity", "observed",
                                  "baseline", "deviation_pct", "direction", "threshold"})
        self.assertIsInstance(f["observed"], (int, float))
        self.assertTrue(any(x["node"] == "L18_LU4820_04" and x["kpi"] == "CFR" for x in rep["findings"]))
        ids = {x["id"] for x in rep["findings"]}
        self.assertEqual(len(ids), len(rep["findings"]))
        top = rep["suspects"]["sites"][0]
        self.assertEqual(top["site"], "LU4820")
        self.assertTrue(set(top["finding_ids"]) <= ids and top["finding_ids"])
        self.assertNotIn("anomalies", top)                # suspects reference findings, no copies
        self.assertEqual(rep["summary"]["counts"]["critical_findings"],
                         sum(1 for x in rep["findings"] if x["severity"] == "CRITICAL"))
        # CSV: one row per finding
        csv_resp = self.get(job_id, RCARunFindingsCSVView, "/findings.csv")
        self.assertEqual(csv_resp.status_code, 200)
        self.assertIn("text/csv", csv_resp["Content-Type"])
        lines = csv_resp.content.decode().strip().splitlines()
        self.assertEqual(lines[0], "request_id,id,system,kpi,node,site,severity,observed,baseline,"
                                   "deviation_pct,direction,threshold")
        self.assertEqual(len(lines) - 1, len(rep["findings"]))
        # diagnostics keep the detail the report leaves out
        diag = self.get(job_id, RCARunDiagnosticsView, "/diagnostics").data
        self.assertTrue(diag["rca_tool_log"])
        self.assertIn("npp_kpi.csv", diag["pipeline"]["rca_tool"]["inputs"]["kpi_files"])
        self.assertTrue((self.out / RCARunAPIView.output_subdir / "logs" / "rca_run.txt").is_file())

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
        self.assertNotIn("npp", rep["data_coverage"]["systems_available"])
        self.assertTrue(any(w["code"] == "SYSTEM_UNAVAILABLE" and w["system"] == "npp"
                            for w in rep["warnings"]))
        self.assertFalse(any(f["system"] == "NPP" for f in rep["findings"]))
        self.assertIsNotNone(rep["summary"])              # what the tool did produce is returned

    def test_missing_csvs_fail_clearly(self):
        job_id, job = self.post(fake_sources(empty=True))
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("RCA_NO_INPUT_FILES", job["error"])
        self.assertEqual(job["rca_run"]["code"], "RCA_NO_INPUT_FILES")
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
        an_a = json.dumps(done["A"]["result"]["findings"])
        an_b = json.dumps(done["B"]["result"]["findings"])
        self.assertIn("LU4820", an_a)
        self.assertNotIn("LU4830", an_a)
        self.assertIn("LU4830", an_b)
        self.assertNotIn("LU4820", an_b)


# ------------------------------------------------- MSISDN run vs site run --
SITES = ["LU4820", "LU4821"]


class FakeSmartcareSites:
    """Stands in for execute_smartcare_queries (the JDBC call) and records every
    query the real smartcare_site_kpi_service sends."""

    def __init__(self):
        self.calls = []

    def __call__(self, queries_dict, params, log_path=""):
        (key, sql), = queries_dict.items()
        self.calls.append({"key": key, "sql": sql, "params": dict(params)})
        if key == "cells":
            return pd.DataFrame({"CGISAI": ["602020A1B2C01", "602020A1B2C02"],
                                 "LOGICAL_SITE_ID": SITES})
        if key == "s1mme":
            return pd.DataFrame({"LOGICAL_SITE_ID": SITES, "ATTACH_REQ_TIMES": [100, 80],
                                 "ATTACH_Fail_TIMES": [5, 1]})
        return pd.DataFrame({"LOGICAL_SITE_ID": SITES, "S6A_UPDATE_LOC_REQ_TIMES": [50, 40],
                             "S6A_UPDATE_LOC_FAIL_TIMES": [0, 1]})


def recording(sources):
    """Copy of the fake sources that records what each system was asked for."""
    seen = {"get_cells": [], "fetch_smartcare": [], "npp": [], "viavi": [], "remedy": []}
    rec = dataclasses.replace(sources)

    def wrap_ctx(name):
        orig = getattr(sources, name)

        def _f(ctx):
            seen[name].append({"msisdn_list": list(ctx.get("msisdn_list") or []),
                               "complain_type": ctx.get("complain_type")})
            return orig(ctx)
        return _f

    def site_npp(entities, ctx):
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
    rec.get_cells, rec.fetch_smartcare = wrap_ctx("get_cells"), wrap_ctx("fetch_smartcare")
    rec.retrieve_npp = site_npp
    rec.retrieve_viavi = by_sites("viavi", sources.retrieve_viavi)
    rec.retrieve_remedy = by_sites("remedy", sources.retrieve_remedy)
    return rec, seen


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


@unittest.skipUnless(HAVE_PW and TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class RCARunRequestModeTests(_Base):

    def post_site(self, sources, body):
        req = self.factory.post("/callflow/api/rca/sites/run", body, format="json")
        with mock.patch.object(oa, "default_sources", lambda: dataclasses.replace(sources)):
            resp = RCASiteRunAPIView.as_view()(req)
            self.assertEqual(resp.status_code, 202, resp.data)
            return resp.data["job_id"], self.wait(resp.data["job_id"])

    def test_msisdn_run_new_format_runs_existing_flow(self):
        sources, seen = recording(fake_sources())
        job_id, job = self.post(sources, body={"msisdn": ",".join(MSISDNS), "complain_type": "data",
                                               "start": "2026-10-10 14:21:00", "end": "2026-10-10 15:21:00"})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        self.assertEqual(seen["get_cells"], [{"msisdn_list": MSISDNS, "complain_type": "Data"}])
        self.assertEqual(seen["fetch_smartcare"], [{"msisdn_list": MSISDNS, "complain_type": "Data"}])
        self.assertEqual(seen["npp"], [{"entities": ["L18_LU4820_04", "L18_LU4821_01"], "entity_type": "cell"}])
        rep = self.get(job_id).data
        self.assertEqual(rep["input"]["mode"], "msisdn")
        self.assertIn("trino", rep["data_coverage"]["systems_available"])
        self.assertEqual(rep["links"]["diagnostics"], f"/callflow/api/rca/run/{job_id}/diagnostics")

    def test_site_run_skips_trino_and_feeds_sites_to_systems(self):
        sources, seen = recording(fake_sources())
        fake_db = FakeSmartcareSites()
        with mock.patch.object(site_kpi_service, "execute_smartcare_queries", fake_db):
            job_id, job = self.post_site(sources, {"sitelist": SITES, "start": "2026-10-10 14:21:00",
                                                   "end": "2026-10-10 15:21:00"})
        self.assertEqual(job["status"], "DONE", job.get("error"))
        # no Trino, no MSISDN Smartcare query; the sites go straight to NPP / Viavi / Remedy
        self.assertEqual((seen["get_cells"], seen["fetch_smartcare"]), ([], []))
        self.assertEqual(seen["npp"], [{"entities": SITES, "entity_type": "site"}])
        self.assertEqual(seen["viavi"], [SITES])
        self.assertEqual(seen["remedy"], [SITES])

        # Smartcare: SmartcareSiteKpiAPIView's queries for the sites and the request's range
        self.assertEqual(sorted({c["key"] for c in fake_db.calls}), ["cells", "s1mme", "s6a"])
        for c in fake_db.calls:
            self.assertEqual(c["params"]["site_tuple"], "('LU4820','LU4821')")
        kpi_calls = [c for c in fake_db.calls if c["key"] != "cells"]
        self.assertEqual({c["sql"] for c in kpi_calls}, set(site_kpi_service.smartcare_site_kpi_QUERIES.values()))
        self.assertEqual({(c["params"]["start_date"], c["params"]["end_date"]) for c in kpi_calls},
                         {("2026-10-10 14:21:00", "2026-10-10 15:21:00"),     # request range
                          ("2026-10-09 14:21:00", "2026-10-09 15:21:00"),     # 1 day earlier
                          ("2026-10-03 14:21:00", "2026-10-03 15:21:00")})    # 1 week earlier

        rdir = self.rdir(job_id)
        self.assertTrue((rdir / "smartcare_site_kpis.json").is_file())
        self.assertFalse((rdir / "trino_cells.csv").exists())
        self.assertEqual((rdir / "smartcare_filter.csv").read_text().split(), ["SITE_NAME"] + SITES)
        manifest = json.loads((rdir / "manifest.json").read_text())
        self.assertNotIn("trino", json.dumps(manifest))

        # same RCA tool, same report format
        rep = self.get(job_id, RCASiteRunAPIView).data
        self.assertEqual(rep["status"], "COMPLETED")
        self.assertEqual(rep["input"], {"mode": "site", "sites": SITES, "msisdn_count": None,
                                        "complain_type": None})
        self.assertNotIn("trino", json.dumps(rep["data_coverage"]))
        self.assertTrue(any(x["node"] == "L18_LU4820_04" and x["kpi"] == "CFR" for x in rep["findings"]))
        self.assertEqual(rep["suspects"]["sites"][0]["site"], "LU4820")
        self.assertTrue(rep["summary"]["primary_root_cause"])
        self.assertEqual(rep["links"]["findings_csv"], f"/callflow/api/rca/sites/run/{job_id}/findings.csv")
        csv_resp = self.get(job_id, RCARunFindingsCSVView, "/findings.csv")
        self.assertEqual(len(csv_resp.content.decode().strip().splitlines()) - 1, len(rep["findings"]))

    def test_site_run_with_failed_npp_is_partial(self):
        with mock.patch.object(site_kpi_service, "execute_smartcare_queries", FakeSmartcareSites()):
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
