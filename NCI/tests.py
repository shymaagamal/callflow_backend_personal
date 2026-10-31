# NCI/tests.py
"""
NCI request flow after the webhook (callback_url) removal.

run_nci_analysis / run_rca_site_analysis are replaced by stubs; the views,
job handling and logging are the real code. requests.post is patched so a
leftover webhook call would be caught.

Run:  python manage.py test NCI
"""

import time
from unittest import mock

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from . import views

SITES = [{"SITE": "LU4820", "FLAG": "red"}, {"SITE": "LU4821", "FLAG": "green"}]
RESULT = {"sites_summary": {"LU4820": {"critical_cases": 1, "minor_cases": 0, "trouble_tickets": 0},
                            "LU4821": {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 2}},
          "errors": {"npp": [], "viavi": [], "remedy": []}}


def _analysis(*a, **k):
    return {"sites_summary": {s: dict(v) for s, v in RESULT["sites_summary"].items()},
            "errors": RESULT["errors"]}


def _wait(view, path, job_id):
    f = APIRequestFactory()
    for _ in range(100):
        job = view(f.get(f"{path}{job_id}/"), job_id=job_id).data
        if job["status"] in ("DONE", "FAILED"):
            return job
        time.sleep(0.05)
    raise AssertionError("job did not finish")


class NCIWebhookRemovedTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.f = APIRequestFactory()
        p = mock.patch("requests.post", side_effect=AssertionError("webhook POST must not happen"))
        self.post_spy = p.start()
        self.addCleanup(p.stop)

    def test_no_webhook_code_left(self):
        import inspect
        src = inspect.getsource(views)
        self.assertNotIn("requests.post", src)
        self.assertNotIn("_run_and_callback", src)
        self.assertFalse(hasattr(views.NCIAnalysisAPIView, "_run_and_callback"))
        self.assertFalse(hasattr(views.NCIRcaSitesAPIView, "_run_and_callback"))

    def test_analysis_sync_unchanged(self):
        with mock.patch.object(views, "run_nci_analysis", side_effect=_analysis) as run:
            resp = views.NCIAnalysisAPIView.as_view()(
                self.f.post("/callflow/NCI/analysis/", {"site_names": SITES}, format="json"))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["sites_summary"]["LU4820"]["FLAG"], "red")
        self.assertEqual(run.call_args.args[0], ["LU4820", "LU4821"])

    def test_analysis_callback_url_is_ignored(self):
        with mock.patch.object(views, "run_nci_analysis", side_effect=_analysis), \
                self.assertLogs(level="WARNING") as logs:
            resp = views.NCIAnalysisAPIView.as_view()(self.f.post(
                "/callflow/NCI/analysis/", {"site_names": SITES, "callback_url": "http://example.invalid/hook"},
                format="json"))
        # synchronous result, no job, no POST to the callback URL
        self.assertEqual(resp.status_code, 200)
        self.assertIn("sites_summary", resp.data)
        self.assertNotIn("job_id", resp.data)
        self.post_spy.assert_not_called()
        self.assertTrue(any("callback_url is no longer supported" in m for m in logs.output))

    def test_analysis_async_job_still_works_without_webhook(self):
        view = views.NCIAnalysisAPIView.as_view()
        with mock.patch.object(views, "run_nci_analysis", side_effect=_analysis):
            resp = view(self.f.post("/callflow/NCI/analysis/?async=1",
                                    {"site_names": SITES, "callback_url": "http://example.invalid/hook"},
                                    format="json"))
            self.assertEqual(resp.status_code, 202)
            job = _wait(view, "/callflow/NCI/analysis/", resp.data["job_id"])
        self.assertEqual(job["status"], "DONE")
        self.assertEqual(job["result"]["sites_summary"]["LU4821"]["FLAG"], "green")
        self.assertEqual(resp.data["poll_url"], f"/callflow/NCI/analysis/{resp.data['job_id']}/")
        self.post_spy.assert_not_called()

    def test_analysis_async_failure_is_reported_by_polling(self):
        view = views.NCIAnalysisAPIView.as_view()
        with mock.patch.object(views, "run_nci_analysis", side_effect=RuntimeError("boom")):
            resp = view(self.f.post("/callflow/NCI/analysis/", {"site_names": SITES, "async": True},
                                    format="json"))
            job = _wait(view, "/callflow/NCI/analysis/", resp.data["job_id"])
        self.assertEqual((job["status"], job["error"]), ("FAILED", "boom"))
        self.post_spy.assert_not_called()

    def test_rca_callback_url_is_ignored_and_async_works(self):
        view = views.NCIRcaSitesAPIView.as_view()
        rca = {"sites_summary": {"LU4820": {"rca_score": 1}}, "ranking": ["LU4820"], "errors": {}}
        with mock.patch.object(views, "run_rca_site_analysis", return_value=rca):
            resp = view(self.f.post("/callflow/NCI/rca/", {
                "site_names": SITES, "start_date": "2026-10-29 08:00", "end_date": "2026-10-29 11:00",
                "callback_url": "http://example.invalid/hook"}, format="json"))
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.data["sites_summary"]["LU4820"]["FLAG"], "red")
            resp = view(self.f.post("/callflow/NCI/rca/?async=1", {
                "site_names": SITES, "start_date": "2026-10-29 08:00", "end_date": "2026-10-29 11:00"},
                format="json"))
            self.assertEqual(resp.status_code, 202)
            job = _wait(view, "/callflow/NCI/rca/", resp.data["job_id"])
        self.assertEqual(job["status"], "DONE")
        self.post_spy.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# POST /NCI/analysis/ through the EXISTING RCA tool (service/rca_pipeline.py)
#
# Only the four systems are faked (same output shapes as the real fetchers);
# file writing, the manifest, the REAL rca_tool_V24.html in headless Chromium
# and the mapping are the real code.
# ─────────────────────────────────────────────────────────────────────────────
import datetime as _dt
import json as _json
import re as _re
import shutil as _shutil
import tempfile as _tempfile
import threading as _threading
import unittest as _unittest
from pathlib import Path as _Path

import pandas as _pd
from django.conf import settings as _settings
from django.test import override_settings as _override_settings

from callflow.core.services import trino_site_kpi_service as _svc
from .service import analysis_runner as _runner
from .service import rca_pipeline as _pipe

_ORIG_WINDOWS = _pipe.analysis_windows

try:
    from playwright.sync_api import sync_playwright  # noqa: F401
    _HAVE_PW = True
except Exception:  # pragma: no cover
    _HAVE_PW = False

_TOOL = _Path(_settings.BASE_DIR) / "CallFlow-Frontend" / "src" / "assets" / "external" / "rca_tool_V24.html"
_NOW = _dt.datetime(2026, 10, 31, 12, 7)          # window 2026-10-31 00:00 -> 12:00
_START, _END = _dt.datetime(2026, 10, 31, 0, 0), _dt.datetime(2026, 10, 31, 12, 0)
_SITES = ["LU4820", "LU4821", "K2398"]
_CELLS = {"LU4820": ["L18_LU4820_04"], "LU4821": ["L18_LU4821_01"], "K2398": []}   # NPP has no K2398 cell
_TCOLS = ["time", "site_logical_name", "attach_req_times", "attach_fail_times", "tau_req_times",
          "tau_fail_times", "s1mme_authentication_req_times", "s1mme_authentication_fail_times",
          "fail_cause_name"]


def _in_window(t):
    return _START <= t <= _END


class _Systems:
    """Fakes of the four NCI fetchers. LU4820: NPP CFR / ERAB degraded in the
    window. K2398: Trino attach failures 2 % -> 30 % in the last 6 hours; one
    Remedy incident listed twice. LU4821 and everything else healthy."""

    def __init__(self, fail=(), empty=(), marker=""):
        self.fail, self.empty, self.marker = set(fail), set(empty), marker
        self.calls = {"npp": [], "viavi": [], "remedy": [], "trino": []}
        self.lock = _threading.Lock()

    def npp(self, site, window, log_path, ts, resolution="hourly"):
        with self.lock:
            self.calls["npp"].append((site, window["start"], window["end"], resolution))
        if "npp" in self.fail:
            raise RuntimeError("NPP API 500 (test)")
        if "npp" in self.empty:
            return _pd.DataFrame()
        rows = []
        for t in _pd.date_range(window["start"], window["end"], freq="15min"):
            for c in _CELLS[site]:
                bad = c == "L18_LU4820_04" and _in_window(t)
                rows.append({"DATETIME_ID": t, "cell": c, "Vendor": "Ericsson", "sitePhysical": site,
                             "CFR": 2.5 if bad else 0.05, "ERAB_Setup_SR": 80.0 if bad else 99.6,
                             "AVG_DL_USER_THROUGHPUT": 3.0 if bad else 22.0})
        return _pd.DataFrame(rows)

    def viavi(self, sites, window, log_path, ts):
        with self.lock:
            self.calls["viavi"].append((list(sites), window["start"], window["end"]))
        if "viavi" in self.fail:
            raise RuntimeError("Viavi connection failed (test)")
        if "viavi" in self.empty:
            return _pd.DataFrame()
        return _pd.DataFrame([{"PERIOD_TIMESTAMP": t, "FLWID": "MW_LU4821_HUB1", "AVG_FD_MS": 4.0,
                               "AVG_FDV_MS": 0.5, "ONEW_FLR_AZ": 0.0, "ONEW_FLR_ZA": 0.0}
                              for t in _pd.date_range(window["start"], window["end"], freq="15min")])

    def remedy(self, sites, window, log_path, ts):
        with self.lock:
            self.calls["remedy"].append((list(sites), window["start"], window["end"]))
        if "remedy" in self.fail:
            raise RuntimeError("Remedy connection failed (test)")
        if "remedy" in self.empty:
            return {}, 0
        t = {"incident_number": f"INC{self.marker}0001", "status": "Assigned",
             "actual_fault_time": "2026-10-31 09:00:00", "summary": "Site K2398 S1 issue (test)"}
        return {"K2398": {"total": 2, "status": {"Assigned": 2}, "tickets": [t, dict(t)]}}, 2

    def trino(self, sql):
        recent = "clickhouse.e2e." in sql
        start = int(_re.search(r"PROC_STARTTIME >= (\d+)", sql).group(1))
        end = int(_re.search(r"PROC_STARTTIME < (\d+)", sql).group(1))
        with self.lock:
            self.calls["trino"].append(("recent" if recent else "history", start, end))
        if "trino" in self.fail:
            raise RuntimeError("Trino unavailable (test)")
        if "trino" in self.empty:
            return _pd.DataFrame(columns=_TCOLS)
        rows = []
        for t in range(start, end, 900):
            when = _pd.Timestamp(t, unit="s", tz="UTC")
            local = when.tz_convert("Africa/Cairo").tz_localize(None)
            for site in _SITES:
                bad = site == "K2398" and recent
                fail = 300 if bad else 20
                auth = (200, 4) if recent else (None, None)
                rows.append([when if recent else local.strftime("%Y-%m-%d %H:%M:%S"), site,
                             1000, fail, 500, 5, auth[0], auth[1], "cause"])
        return _pd.DataFrame(rows, columns=_TCOLS)


@_unittest.skipUnless(_HAVE_PW and _TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class NCIAnalysisThroughRCAToolTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.tmp = _Path(_tempfile.mkdtemp())
        self.addCleanup(_shutil.rmtree, self.tmp, ignore_errors=True)
        o = _override_settings(OUTPUT_DIR=self.tmp, RCA_TOOL_PATH=_TOOL)
        o.enable()
        self.addCleanup(o.disable)

    def run_pipeline(self, systems, sites=_SITES, job_id=None):
        import logging
        with mock.patch.object(_runner, "fetch_npp_one_site", systems.npp), \
                mock.patch.object(_runner, "fetch_viavi", systems.viavi), \
                mock.patch.object(_runner, "fetch_remedy", systems.remedy), \
                mock.patch.object(_svc, "run_trino_query", systems.trino), \
                mock.patch.object(_pipe, "analysis_windows", lambda now=None: _ORIG_WINDOWS(_NOW)):
            with self.assertLogs(level="INFO") as logs:
                out = _runner.run_nci_analysis(list(sites), logging.getLogger("nci-test"), "", "ts",
                                               job_id=job_id)
        self.logs = "\n".join(logs.output)
        return out

    def rdir(self):
        rid = _re.search(r"request_id=([0-9a-f]+) sites=", self.logs).group(1)
        return self.tmp / "nci_rca" / "rca" / rid

    def test_results_come_from_the_rca_tool(self):
        out = self.run_pipeline(_Systems())
        s = out["sites_summary"]
        self.assertEqual(set(s), set(_SITES))
        for row in s.values():
            self.assertEqual(set(row), {"critical_cases", "minor_cases", "trouble_tickets"})

        # the numbers are the RCA tool's own per-site results (its report in the request folder)
        report = _json.loads((self.rdir() / "rca_result.json").read_text())
        by_site = {x["site"]: x for x in report["suspects"]["sites"]}
        self.assertGreater(s["LU4820"]["critical_cases"], 0)                 # NPP cell degradation
        self.assertEqual(s["LU4820"]["critical_cases"], by_site["LU4820"]["critical_issues"])
        self.assertGreater(s["K2398"]["critical_cases"], 0)                  # Trino attach failures
        self.assertEqual(s["K2398"]["critical_cases"], by_site["K2398"]["critical_issues"])
        self.assertEqual(s["LU4821"], {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 0})
        # the Remedy incident is listed twice; the tool links both rows to K2398, counted once
        self.assertEqual(by_site["K2398"]["tickets"], 2)
        self.assertEqual(s["K2398"]["trouble_tickets"], 1)

        findings = report["findings"]
        trino = [f for f in findings if f["node"] == "K2398"]
        self.assertEqual({(f["system"], f["kpi"]) for f in trino}, {("MME", "Attach_Failure_4G")})
        self.assertTrue(all(f["site"] == "K2398" for f in trino))
        # no network-level or other-site finding reaches a healthy site
        self.assertFalse([f for f in findings if f["site"] == "LU4821"])

    def test_raw_rca_files_and_logs(self):
        self.run_pipeline(_Systems())
        rdir = self.rdir()
        files = sorted(p.name for p in rdir.iterdir() if p.suffix in (".csv", ".xlsx"))
        self.assertEqual(files, ["nci_scope.csv", "npp_kpi.csv", "remedy_kpi.csv", "thresholds.xlsx",
                                 "trino_s1mme_attach_failure.csv", "trino_s1mme_authentication_sr.csv",
                                 "trino_s1mme_tau_sr.csv", "viavi_kpi.csv"])
        npp = _pd.read_csv(rdir / "npp_kpi.csv")
        self.assertIn("Cell", npp.columns)                                  # RCA tool's column name
        self.assertEqual(set(npp["Cell"]), {"L18_LU4820_04", "L18_LU4821_01"})
        att = _pd.read_csv(rdir / "trino_s1mme_attach_failure.csv")
        self.assertEqual(list(att.columns), ["timestamp", "NE_NAME", "MME_Attach_Failure_4G_TRINO"])
        k = att[att["NE_NAME"] == "K2398"].set_index("timestamp")["MME_Attach_Failure_4G_TRINO"]
        self.assertEqual(k.loc["2026-10-31 11:45:00"], 30.0)                # ClickHouse half
        self.assertEqual(k.loc["2026-10-31 01:00:00"], 2.0)                 # Hive half
        auth = _pd.read_csv(rdir / "trino_s1mme_authentication_sr.csv")
        self.assertGreaterEqual(auth["timestamp"].min(), "2026-10-31 06:00:00")   # ClickHouse rows only
        scope = _pd.read_csv(rdir / "nci_scope.csv", keep_default_na=False)
        self.assertEqual(set(scope["cell_name"]) - {""}, {"L18_LU4820_04", "L18_LU4821_01"})
        self.assertEqual(set(scope["SITE_NAME"]) - {""}, {"K2398"})        # no NPP cell for K2398
        remedy = _pd.read_csv(rdir / "remedy_kpi.csv")
        self.assertEqual(list(remedy["site"]), ["K2398", "K2398"])          # raw rows, not summaries

        for stage in ("trino retrieval started", "npp retrieval completed status=COMPLETED",
                      "RCA file system=trino file=", "records=", "RCA execution started",
                      "RCA execution completed", "site=K2398 -> RCA site(s) ['K2398']",
                      "mapped 3 site(s) from the RCA results"):
            self.assertIn(stage, self.logs)

    def test_retrieval_windows_cover_the_tool_baseline(self):
        sysm = _Systems()
        self.run_pipeline(sysm)
        two_h, day = _dt.timedelta(hours=2), _dt.timedelta(days=1)
        self.assertEqual({(a, b) for _, a, b, _ in sysm.calls["npp"]},
                         {(_START - two_h, _END), (_START - day, _END - day)})
        self.assertEqual({r for *_, r in sysm.calls["npp"]}, {_pipe.NPP_RESOLUTION})
        self.assertEqual({(a, b) for _, a, b in sysm.calls["viavi"]},
                         {(_START - two_h, _END), (_START - day, _END - day)})
        self.assertEqual(sysm.calls["remedy"], [(_SITES, _START, _END)])
        # Trino: current (+2 h) split at now - 6 h, and the day before; no 1-week query
        self.assertEqual(len(sysm.calls["trino"]), 3)
        self.assertEqual(sum(1 for s, *_ in sysm.calls["trino"] if s == "recent"), 1)

    def test_missing_data_is_reported_not_healthy(self):
        out = self.run_pipeline(_Systems(fail=("viavi",), empty=("remedy",)))
        e = out["errors"]
        self.assertEqual(set(e), {"npp", "viavi", "remedy", "trino", "rca"})
        self.assertEqual(e["viavi"], [{"period": "current", "error": "Viavi retrieval failed for every window: "
                                                                    "Viavi connection failed (test)"}])
        self.assertEqual(e["remedy"], [{"period": "current", "error": "no data returned for the requested sites"}])
        self.assertIn({"period": "current", "site": "K2398", "error": "no NPP data for this site"}, e["npp"])
        self.assertEqual(out["sites_summary"]["K2398"]["trouble_tickets"], 0)
        self.assertGreater(out["sites_summary"]["K2398"]["critical_cases"], 0)   # Trino still analysed

    def test_no_data_anywhere_skips_the_tool(self):
        out = self.run_pipeline(_Systems(empty=("npp", "viavi", "remedy", "trino")))
        self.assertEqual(out["errors"]["rca"], [{"period": "current", "error": "RCA not run: no data from any system"}])
        self.assertTrue(all(out["errors"][s] for s in ("npp", "viavi", "remedy", "trino")))
        self.assertEqual(out["sites_summary"]["LU4820"], {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 0})
        self.assertNotIn("RCA execution started", self.logs)

    def test_concurrent_requests_use_their_own_files(self):
        results, errs = {}, []

        def one(name, sites, systems):
            try:
                import logging
                results[name] = _pipe.run_nci_rca(sites, logging.getLogger(name), "", "ts",
                                                  job_id=f"{name}0000000000000000"[:16], now=_NOW)
            except Exception as e:  # pragma: no cover
                errs.append(e)
        a, b = _Systems(marker="A"), _Systems(marker="B")
        with mock.patch.object(_runner, "fetch_npp_one_site", lambda s, *x: (a if s == "LU4820" else b).npp(s, *x)), \
                mock.patch.object(_runner, "fetch_viavi", lambda s, *x: (a if s == ["LU4820"] else b).viavi(s, *x)), \
                mock.patch.object(_runner, "fetch_remedy", lambda s, *x: (a if s == ["LU4820"] else b).remedy(s, *x)), \
                mock.patch.object(_svc, "run_trino_query", a.trino):
            ts = [_threading.Thread(target=one, args=("a", ["LU4820"], a)),
                  _threading.Thread(target=one, args=("b", ["K2398"], b))]
            [t.start() for t in ts]
            [t.join(600) for t in ts]
        self.assertEqual(errs, [])
        self.assertEqual(set(results["a"]["sites_summary"]), {"LU4820"})
        self.assertEqual(set(results["b"]["sites_summary"]), {"K2398"})
        da = self.tmp / "nci_rca" / "rca" / "a000000000000000"
        db = self.tmp / "nci_rca" / "rca" / "b000000000000000"
        self.assertIn("INCA0001", (da / "remedy_kpi.csv").read_text())
        self.assertIn("INCB0001", (db / "remedy_kpi.csv").read_text())
        self.assertNotIn("K2398", (da / "nci_scope.csv").read_text())
        self.assertNotIn("LU4820", (db / "nci_scope.csv").read_text())

    def test_endpoint_contract_unchanged(self):
        view = views.NCIAnalysisAPIView.as_view()
        f = APIRequestFactory()
        body = {"site_names": [{"SITE": s, "FLAG": f"flag-{s}"} for s in _SITES]}
        sysm = _Systems()
        with mock.patch.object(_runner, "fetch_npp_one_site", sysm.npp), \
                mock.patch.object(_runner, "fetch_viavi", sysm.viavi), \
                mock.patch.object(_runner, "fetch_remedy", sysm.remedy), \
                mock.patch.object(_svc, "run_trino_query", sysm.trino):
            resp = view(f.post("/callflow/NCI/analysis/", body, format="json"))
            resp.render()
            # async job: same result through polling, progress reflects the stages
            resp2 = view(f.post("/callflow/NCI/analysis/?async=1", body, format="json"))
            job = None
            for _ in range(1200):
                job = view(f.get(f"/callflow/NCI/analysis/{resp2.data['job_id']}/"),
                           job_id=resp2.data["job_id"]).data
                if job["status"] in ("DONE", "FAILED"):
                    break
                time.sleep(0.25)
        self.assertEqual(resp.status_code, 200)
        data = _json.loads(resp.content)
        self.assertEqual(set(data), {"sites_summary", "errors"})
        for s in _SITES:
            self.assertEqual(set(data["sites_summary"][s]),
                             {"critical_cases", "minor_cases", "trouble_tickets", "FLAG"})
            self.assertEqual(data["sites_summary"][s]["FLAG"], f"flag-{s}")
        self.assertEqual(job["status"], "DONE", job.get("error"))
        self.assertEqual({k: {kk: vv for kk, vv in v.items()} for k, v in job["result"]["sites_summary"].items()},
                         data["sites_summary"])


class NCIMappingTests(SimpleTestCase):
    databases = []

    def test_numeric_site_id_matches_the_tool(self):
        for raw, want in (("K2398", "2398"), ("L18_LK2398_01", "2398"), ("0160", "160"),
                          ("CU0160", "160"), ("AB12", None), ("X123_45678", "45678"), ("", None)):
            self.assertEqual(_pipe.numeric_site_id(raw), want, raw)

    def test_mapping_exact_then_site_id_never_other_sites(self):
        results = {"chains": [
            {"site": "LK2398", "criticalIssues": 3, "anomalyIssues": 1,
             "ticketList": [{"incident_number": "I1"}, {"incident_number": "I1"}, {"incident_number": "I2"}]},
            {"site": "LU4820", "criticalIssues": 2, "anomalyIssues": 0, "ticketList": []},
            {"site": "HUB9999", "criticalIssues": 9, "anomalyIssues": 9, "ticketList": []},
        ]}
        out, problems = _pipe.map_rca_to_sites(["K2398", "LU4820", "Q5555"], results)
        self.assertEqual({k: v["critical_cases"] for k, v in out.items()}, {"K2398": 3, "LU4820": 2, "Q5555": 0})
        self.assertEqual(out["K2398"]["trouble_tickets"], 2)
        self.assertEqual(out["K2398"]["_rca_sites"], ["LK2398"])
        self.assertEqual(problems, [{"period": "current", "site": "Q5555",
                                     "error": "the RCA tool linked no data to this site"}])

    def test_ambiguous_site_ids_are_not_assigned(self):
        results = {"chains": [{"site": "LK2398", "criticalIssues": 3, "anomalyIssues": 0, "ticketList": []}]}
        out, problems = _pipe.map_rca_to_sites(["K2398", "G2398"], results)
        self.assertEqual(out["K2398"]["critical_cases"], 0)
        self.assertEqual(out["G2398"]["critical_cases"], 0)
        self.assertEqual(len(problems), 2)
        self.assertIn("shared by requested sites", problems[0]["error"])


class NCIRcaEndpointStillWorksTests(SimpleTestCase):
    """/NCI/rca/ keeps its own analysis (rca_site_runner) on the shared fetchers."""
    databases = []

    def test_rca_site_runner_runs(self):
        import logging
        from .service.rca_site_runner import run_rca_site_analysis
        sysm = _Systems()
        with mock.patch("NCI.service.rca_site_runner.fetch_npp_one_site", sysm.npp), \
                mock.patch("NCI.service.rca_site_runner.fetch_viavi", sysm.viavi), \
                mock.patch("NCI.service.rca_site_runner.fetch_remedy", sysm.remedy):
            out = run_rca_site_analysis(["LU4820", "LU4821"], _dt.datetime(2026, 10, 31, 9),
                                        _dt.datetime(2026, 10, 31, 12), logging.getLogger("rca"), "", "ts")
        self.assertEqual(set(out["sites_summary"]), {"LU4820", "LU4821"})
        self.assertIn("rca_score", out["sites_summary"]["LU4820"])
