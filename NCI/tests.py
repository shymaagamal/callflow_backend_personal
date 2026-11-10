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
# file writing, the manifest, the REAL rca_tool_V25.html in headless Chromium
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

_TOOL = _Path(_settings.BASE_DIR) / "CallFlow-Frontend" / "src" / "assets" / "external" / "rca_tool_V25.html"
_NOW = _dt.datetime(2026, 10, 31, 12, 7)          # window 2026-10-31 00:00 -> 12:00
_START, _END = _dt.datetime(2026, 10, 31, 0, 0), _dt.datetime(2026, 10, 31, 12, 0)
_SITES = ["LU4820", "LU4821", "K2398"]
_PORTAL = "https://10.30.30.177/callflow-portal/"


def _sheet_with_trino_rows(folder):
    """data/thresholds.xlsx + two MME rows named exactly like the Trino query
    columns, which is how the team enables RCA rules for the raw counters."""
    sheets = _pd.read_excel(_Path(_settings.BASE_DIR) / "data" / "thresholds.xlsx", sheet_name=None)
    sheets["Packet"] = _pd.concat([sheets["Packet"], _pd.DataFrame([
        {"KPI": "ATTACH_FAIL_TIMES", "Node": "MME", "Alarming Threshold": "Deviation Rate > 20%"},
        {"KPI": "TAU_FAIL_TIMES", "Node": "MME", "Alarming Threshold": "Deviation Rate > 20%"}])],
        ignore_index=True)
    path = _Path(folder) / "thresholds_with_trino.xlsx"
    with _pd.ExcelWriter(path) as w:
        for name, frame in sheets.items():
            frame.to_excel(w, sheet_name=name, index=False)
    return path
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
        start = int(_re.search(r">= (\d+)", sql).group(1))
        end = int(_re.search(r"\s<\s(\d+)", sql).group(1))
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
                # ClickHouse: epoch seconds (CAST(STARTTIME AS BIGINT)); Hive: time_t text
                rows.append([t if recent else local.strftime("%Y/%m/%d %H:%M"), site,
                             1000, fail, 500, 5, auth[0], auth[1], "cause"])
        return _pd.DataFrame(rows, columns=_TCOLS)


@_unittest.skipUnless(_HAVE_PW and _TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class NCIAnalysisThroughRCAToolTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.tmp = _Path(_tempfile.mkdtemp())
        self.addCleanup(_shutil.rmtree, self.tmp, ignore_errors=True)
        o = _override_settings(OUTPUT_DIR=self.tmp, RCA_TOOL_PATH=_TOOL, CALLFLOW_PORTAL_URL=_PORTAL,
                               THRESHOLDING_SHEET_PATH=_sheet_with_trino_rows(self.tmp))
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
        return self.tmp / "rca" / rid

    def test_results_come_from_the_rca_tool(self):
        out = self.run_pipeline(_Systems())
        s = out["sites_summary"]
        self.assertEqual(set(s), set(_SITES))
        for row in s.values():
            self.assertEqual(set(row), {"critical_cases", "minor_cases", "trouble_tickets", "score"})

        # the numbers are the RCA tool's own per-site results (its report in the request folder)
        report = _json.loads((self.rdir() / "rca_result.json").read_text())
        by_site = {x["site"]: x for x in report["suspects"]["sites"]}
        self.assertGreater(s["LU4820"]["critical_cases"], 0)                 # NPP cell degradation
        self.assertEqual(s["LU4820"]["critical_cases"], by_site["LU4820"]["critical_issues"])
        self.assertGreater(s["K2398"]["critical_cases"], 0)                  # Trino attach failures
        self.assertEqual(s["K2398"]["critical_cases"], by_site["K2398"]["critical_issues"])
        # score: the tool's own site score, as shown in its site ranking
        for site in ("LU4820", "K2398"):
            self.assertGreater(s[site]["score"], 0)
            self.assertEqual(s[site]["score"], by_site[site]["score"])
        self.assertEqual(s["LU4821"], {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 0, "score": 0})
        # the Remedy incident is listed twice; the tool links both rows to K2398, counted once
        self.assertEqual(by_site["K2398"]["tickets"], 2)
        self.assertEqual(s["K2398"]["trouble_tickets"], 1)

        findings = report["findings"]
        trino = [f for f in findings if f["node"] == "K2398"]
        self.assertEqual({(f["system"], f["kpi"]) for f in trino}, {("MME", "ATTACH_FAIL_TIMES")})
        self.assertTrue(all(f["site"] == "K2398" for f in trino))
        # no network-level or other-site finding reaches a healthy site
        self.assertFalse([f for f in findings if f["site"] == "LU4821"])

    def test_real_sheet_has_no_rules_for_the_raw_trino_counters(self):
        """With data/thresholds.xlsx as it is, the tool loads the Trino counters but
        skips them (no sheet row has their names): Trino adds no critical case."""
        with _override_settings(THRESHOLDING_SHEET_PATH=_Path(_settings.BASE_DIR) / "data" / "thresholds.xlsx"):
            out = self.run_pipeline(_Systems())
        log = _json.loads((self.rdir() / "rca_tool_log.json").read_text())
        msgs = [e.get("msg", "") for e in log]
        self.assertTrue(any("MME/K2398 | KPI: ATTACH_FAIL_TIMES" in m and "NO THRESHOLD DEFINED" in m for m in msgs))
        self.assertEqual(out["sites_summary"]["K2398"]["critical_cases"], 0)
        self.assertEqual(out["sites_summary"]["LU4820"]["critical_cases"], 2)      # NPP unaffected

    def test_raw_rca_files_and_logs(self):
        self.run_pipeline(_Systems())
        rdir = self.rdir()
        files = sorted(p.name for p in rdir.iterdir() if p.suffix in (".csv", ".xlsx"))
        self.assertEqual(files, ["nci_scope.xlsx", "npp_kpi.csv", "remedy_kpi.csv", "thresholds.xlsx",
                                 "trino_s1mme_auth_kpi.csv", "trino_s1mme_kpi.csv", "viavi_kpi.csv"])
        npp = _pd.read_csv(rdir / "npp_kpi.csv")
        self.assertIn("Cell", npp.columns)                                  # RCA tool's column name
        self.assertEqual(set(npp["Cell"]), {"L18_LU4820_04", "L18_LU4821_01"})
        # the Trino query columns as returned: counters, no rates
        tr = _pd.read_csv(rdir / "trino_s1mme_kpi.csv")
        self.assertEqual(list(tr.columns), ["timestamp", "NE_NAME", "ATTACH_REQ_TIMES_TRINO",
                                            "ATTACH_FAIL_TIMES_TRINO", "TAU_REQ_TIMES_TRINO", "TAU_FAIL_TIMES_TRINO"])
        k = tr[tr["NE_NAME"] == "K2398"].set_index("timestamp")
        self.assertEqual(k.loc["2026-10-31 11:45:00", "ATTACH_FAIL_TIMES_TRINO"], 300)   # ClickHouse
        self.assertEqual(k.loc["2026-10-31 11:45:00", "ATTACH_REQ_TIMES_TRINO"], 1000)
        self.assertEqual(k.loc["2026-10-31 01:00:00", "ATTACH_FAIL_TIMES_TRINO"], 20)    # Hive
        auth = _pd.read_csv(rdir / "trino_s1mme_auth_kpi.csv")
        self.assertEqual(list(auth.columns), ["timestamp", "NE_NAME", "S1MME_AUTHENTICATION_REQ_TIMES_TRINO",
                                              "S1MME_AUTHENTICATION_FAIL_TIMES_TRINO"])
        self.assertGreaterEqual(auth["timestamp"].min(), "2026-10-31 06:00:00")   # ClickHouse rows only
        scope = _pd.read_excel(rdir / "nci_scope.xlsx", dtype=str, keep_default_na=False)
        self.assertEqual(list(scope.columns), ["myday", "cell_name", "SITE_NAME"])
        self.assertEqual(set(scope["cell_name"]) - {""}, {"L18_LU4820_04", "L18_LU4821_01", "K2398"})
        self.assertEqual(set(scope["SITE_NAME"]) - {""}, {"K2398"})        # no NPP cell for K2398
        # myday is the NCI window in 15-min steps (00:00 .. 12:00 -> 49 rows)
        self.assertEqual(list(scope["myday"]),
                         list(_pd.date_range("2026-10-31 00:00", "2026-10-31 12:00", freq="15min")
                              .strftime("%Y-%m-%d %H:%M")))
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

    def test_a_site_sent_several_times_is_analysed_once(self):
        once = self.run_pipeline(_Systems())
        twice = self.run_pipeline(_Systems(), sites=["LU4820", "LU4820", "LU4821", "K2398", "K2398"])
        self.assertEqual(twice["sites_summary"], once["sites_summary"])
        self.assertFalse([e for e in twice["errors"]["rca"] if "shared by requested sites" in e["error"]])
        self.assertIn("duplicate site(s) removed", self.logs)

    def test_no_data_anywhere_skips_the_tool(self):
        out = self.run_pipeline(_Systems(empty=("npp", "viavi", "remedy", "trino")))
        self.assertEqual(out["errors"]["rca"], [{"period": "current", "error": "RCA not run: no data from any system"}])
        self.assertTrue(all(out["errors"][s] for s in ("npp", "viavi", "remedy", "trino")))
        self.assertEqual(out["sites_summary"]["LU4820"], {"critical_cases": 0, "minor_cases": 0, "trouble_tickets": 0, "score": 0})
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
        da = self.tmp / "rca" / "a000000000000000"
        db = self.tmp / "rca" / "b000000000000000"
        self.assertIn("INCA0001", (da / "remedy_kpi.csv").read_text())
        self.assertIn("INCB0001", (db / "remedy_kpi.csv").read_text())
        scope = lambda d: _pd.read_excel(d / "nci_scope.xlsx", dtype=str).to_csv(index=False)
        self.assertNotIn("K2398", scope(da))
        self.assertNotIn("LU4820", scope(db))

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
        self.assertEqual(set(data), {"sites_summary", "errors", "message", "portal_url"})
        for s in _SITES:
            self.assertEqual(set(data["sites_summary"][s]),
                             {"critical_cases", "minor_cases", "trouble_tickets", "score", "FLAG"})
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
            {"site": "LK2398", "criticalIssues": 3, "anomalyIssues": 1, "score": 40,
             "ticketList": [{"incident_number": "I1"}, {"incident_number": "I1"}, {"incident_number": "I2"}]},
            {"site": "LU4820", "criticalIssues": 2, "anomalyIssues": 0, "score": 25, "ticketList": []},
            {"site": "HUB9999", "criticalIssues": 9, "anomalyIssues": 9, "ticketList": []},
        ]}
        out, problems = _pipe.map_rca_to_sites(["K2398", "LU4820", "Q5555"], results)
        self.assertEqual({k: v["critical_cases"] for k, v in out.items()}, {"K2398": 3, "LU4820": 2, "Q5555": 0})
        self.assertEqual(out["K2398"]["trouble_tickets"], 2)
        self.assertEqual(out["K2398"]["_rca_sites"], ["LK2398"])
        self.assertEqual({k: v["score"] for k, v in out.items()}, {"K2398": 40, "LU4820": 25, "Q5555": 0})
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


class _NoOffsetTZ(_dt.tzinfo):
    """A zone object that reports no UTC offset, as the server's Trino client
    attached to 'timestamp with time zone' values (production error:
    "'NoneType' object has no attribute 'total_seconds'")."""
    def utcoffset(self, d):
        return None

    def dst(self, d):
        return None

    def tzname(self, d):
        return "?"


class TrinoTimeRegressionTests(SimpleTestCase):
    databases = []

    def test_clickhouse_query_returns_epoch_not_zoned_timestamp(self):
        sql = _svc.trino_s1mme_QUERIES[_svc.RECENT_SOURCE]
        self.assertIn("CAST(a.STARTTIME AS BIGINT) AS Time", sql)
        self.assertNotIn("from_unixtime", sql)

    def test_time_values_from_both_queries(self):
        epoch = int(_pd.Timestamp("2026-10-31 10:15", tz="Africa/Cairo").timestamp())
        bad = _dt.datetime(2026, 10, 31, 10, 15, tzinfo=_NoOffsetTZ())
        with self.assertRaises(AttributeError):          # what pandas did on the server
            _pd.to_datetime(_pd.Series([bad]), utc=True)
        self.assertEqual(_svc._to_local_time(_pd.Series([epoch])).iloc[0], _pd.Timestamp("2026-10-31 10:15"))
        t = _svc._to_local_time(_pd.Series([bad, "2026/10/31 04:15", None, "junk"], dtype=object))
        self.assertEqual(list(t.iloc[:2]), [_pd.Timestamp("2026-10-31 10:15"), _pd.Timestamp("2026-10-31 04:15")])
        self.assertTrue(t.iloc[2:].isna().all())


# ─────────────────────────────────────────────────────────────────────────────
# Portal link: the NCI response points at the Summary page, which loads the
# stored result and opens the RCA tool on the same request's files.
# ─────────────────────────────────────────────────────────────────────────────
@_unittest.skipUnless(_HAVE_PW and _TOOL.is_file(), "Playwright/Chromium or the RCA tool is not available")
class NCIPortalLinkTests(NCIAnalysisThroughRCAToolTests):
    """Reuses the end-to-end setup above (fake systems, the real RCA tool)."""

    def _link(self, out):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(out["portal_url"])
        return u, {k: v[0] for k, v in parse_qs(u.query).items()}

    def test_response_carries_the_portal_link(self):
        out = self.run_pipeline(_Systems())
        self.assertEqual(out["message"], "Want to see more details? View this analysis on the portal.")
        u, q = self._link(out)
        self.assertEqual(f"{u.scheme}://{u.netloc}{u.path}", "https://10.30.30.177/callflow-portal/summary")
        self.assertEqual(q["mode"], "nci")
        self.assertEqual(q["sites"], ",".join(_SITES))
        self.assertRegex(q["nci_id"], r"^[0-9a-f]{16}$")
        self.assertEqual(self.rdir().name, q["nci_id"])          # link id = the request's RCA folder

    def test_portal_get_returns_the_stored_result_with_its_rca_files(self):
        out = self.run_pipeline(_Systems())
        _, q = self._link(out)
        nci_id = q["nci_id"]
        f = APIRequestFactory()
        job = views.NCIPortalAnalysisAPIView.as_view()(
            f.get(f"/callflow/NCI/portal/analysis/{nci_id}"), nci_id=nci_id).data
        self.assertEqual((job["status"], job["job_id"]), ("DONE", nci_id))
        self.assertEqual(job["rca"]["status"], "READY")
        self.assertEqual(job["rca"]["rca_files_url"], f"/callflow/api/analysis/{nci_id}/rca-files")
        self.assertEqual(job["result"]["window"], {"start_date": "2026-10-31T00:00", "end_date": "2026-10-31T12:00"})
        self.assertEqual(job["result"]["sites_summary"], out["sites_summary"])
        self.assertEqual(job["systems"], {"trino": "COMPLETED", "npp": "COMPLETED", "viavi": "COMPLETED",
                                          "remedy": "COMPLETED"})
        # Summary cards and popups come from the RCA tool's own findings and tickets
        d = job["details"]["npp_data"]["affected_cells_details"]
        self.assertEqual(set(job["details"]["npp_data"]["cells_affected_list"]), {"LU4820", "K2398"})
        self.assertIn("ATTACH_FAIL_TIMES", d["K2398"]["kpis"])
        self.assertEqual(d["K2398"]["kpis"]["ATTACH_FAIL_TIMES"]["severity"], "CRITICAL")
        self.assertEqual(job["dashboard"], {"cellsAffected": 2, "totalCells": 3, "remedyAffected": 1})
        tickets = job["details"]["smartcare_data_metrics"]["remedy"]["remedy_output"]["K2398"]["tickets"]
        self.assertEqual([t["incident_number"] for t in tickets], ["INC0001"])

    def test_rca_tool_files_are_served_by_the_existing_routes(self):
        from callflow.core.views.orchestrated_analysis import RCAFilesAPIView, RCAFileDownloadAPIView
        out = self.run_pipeline(_Systems())
        _, q = self._link(out)
        nci_id = q["nci_id"]
        f = APIRequestFactory()
        listing = RCAFilesAPIView.as_view()(f.get("/x"), request_id=nci_id).data
        names = {i["name"] for i in listing["kpi"]}
        self.assertTrue({"npp_kpi.csv", "viavi_kpi.csv", "remedy_kpi.csv",
                         "trino_s1mme_kpi.csv"} <= names)
        self.assertEqual([i["name"] for i in listing["filter"]], ["nci_scope.xlsx"])
        self.assertEqual([i["name"] for i in listing["threshold"]], ["thresholds.xlsx"])
        url = next(i["url"] for i in listing["kpi"] if i["name"] == "trino_s1mme_kpi.csv")
        self.assertEqual(url, f"/callflow/api/analysis/{nci_id}/file/trino_s1mme_kpi.csv")
        resp = RCAFileDownloadAPIView.as_view()(f.get(url), request_id=nci_id,
                                                filename="trino_s1mme_kpi.csv")
        self.assertIn(b"ATTACH_FAIL_TIMES_TRINO", b"".join(resp.streaming_content))

    def test_the_portal_rca_tool_shows_the_same_analysis(self):
        """The portal iframe runs the same tool on the same files and window:
        its per-site result equals the one behind the NCI response."""
        from callflow.core.rca_runner.browser_runner import run_rca_tool
        from callflow.core.orchestration import rca_files as rf
        out = self.run_pipeline(_Systems())
        _, q = self._link(out)
        job = views.NCIPortalAnalysisAPIView.as_view()(
            APIRequestFactory().get("/x"), nci_id=q["nci_id"]).data
        base = self.tmp
        again = run_rca_tool(
            request_id=q["nci_id"], tool_path=_TOOL, files_listing=rf.list_ready_files(base, q["nci_id"]),
            resolve_file=lambda n: rf.resolve_file(base, q["nci_id"], n),
            start_date=job["result"]["window"]["start_date"], end_date=job["result"]["window"]["end_date"],
            timezone_id="Africa/Cairo", out_dir=self.tmp / "replay")
        mapped, _ = _pipe.map_rca_to_sites(_SITES, again["results"])
        self.assertEqual({s: {k: v for k, v in r.items() if not k.startswith("_")} for s, r in mapped.items()},
                         out["sites_summary"])

    def test_link_keeps_working_after_the_job_cache_expires(self):
        from callflow.core.jobs import job_manager
        sysm = _Systems()
        with mock.patch.object(_runner, "fetch_npp_one_site", sysm.npp), \
                mock.patch.object(_runner, "fetch_viavi", sysm.viavi), \
                mock.patch.object(_runner, "fetch_remedy", sysm.remedy), \
                mock.patch.object(_svc, "run_trino_query", sysm.trino):
            view = views.NCIAnalysisAPIView.as_view()
            resp = view(APIRequestFactory().post("/callflow/NCI/analysis/?async=1",
                                                 {"site_names": [{"SITE": "K2398", "FLAG": "1"}]}, format="json"))
            job_id = resp.data["job_id"]
            for _ in range(1200):
                job = view(APIRequestFactory().get("/x"), job_id=job_id).data
                if job["status"] in ("DONE", "FAILED"):
                    break
                time.sleep(0.25)
        self.assertEqual(job["status"], "DONE", job.get("error"))
        self.assertIn(f"nci_id={job_id}", job["result"]["portal_url"])   # async: link id = job id
        from django.core.cache import cache
        cache.clear()                                                     # job record gone
        self.assertIsNone(job_manager.get_job(job_id))
        stored = views.NCIPortalAnalysisAPIView.as_view()(APIRequestFactory().get("/x"), nci_id=job_id)
        self.assertEqual(stored.status_code, 200)
        self.assertEqual(stored.data["result"]["sites_summary"]["K2398"]["critical_cases"], 1)

    def test_portal_post_runs_a_new_analysis_for_sites(self):
        sysm = _Systems()
        view = views.NCIPortalAnalysisAPIView.as_view()
        with mock.patch.object(_runner, "fetch_npp_one_site", sysm.npp), \
                mock.patch.object(_runner, "fetch_viavi", sysm.viavi), \
                mock.patch.object(_runner, "fetch_remedy", sysm.remedy), \
                mock.patch.object(_svc, "run_trino_query", sysm.trino):
            resp = view(APIRequestFactory().post("/callflow/NCI/portal/analysis", {"sites": ["LU4820"]},
                                                 format="json"))
            self.assertEqual(resp.status_code, 202)
            job_id = resp.data["job_id"]
            for _ in range(1200):
                job = view(APIRequestFactory().get("/x"), nci_id=job_id).data
                if job["status"] in ("DONE", "FAILED"):
                    break
                self.assertIn(job["status"], ("PENDING", "RUNNING"))
                self.assertTrue(set((job.get("systems") or {}).values()) <= {"PROCESSING", "COMPLETED", "FAILED"})
                time.sleep(0.25)
        self.assertEqual(job["status"], "DONE")
        self.assertEqual(job["rca"]["rca_files_url"], f"/callflow/api/analysis/{job_id}/rca-files")

    def test_no_data_anywhere_still_links_without_rca(self):
        out = self.run_pipeline(_Systems(empty=("npp", "viavi", "remedy", "trino")))
        _, q = self._link(out)
        job = views.NCIPortalAnalysisAPIView.as_view()(APIRequestFactory().get("/x"), nci_id=q["nci_id"]).data
        self.assertIsNone(job["rca"])                  # the portal shows the RCA tab as Not Available
        self.assertEqual(job["result"]["errors"]["rca"],
                         [{"period": "current", "error": "RCA not run: no data from any system"}])


class NCIPortalRouteTests(SimpleTestCase):
    databases = []

    def test_routes_the_portal_calls(self):
        from django.urls import resolve
        for path in ("/callflow/NCI/portal/analysis", "/callflow/NCI/portal/analysis/",
                     "/callflow/NCI/portal/analysis/abc123abc123abcd",
                     "/callflow/NCI/portal/analysis/abc123abc123abcd/"):
            self.assertIs(resolve(path).func.view_class, views.NCIPortalAnalysisAPIView, path)

    def test_unknown_or_bad_id_is_404(self):
        f = APIRequestFactory()
        for nci_id in ("0000000000000000", "../../etc/passwd", "NOT-HEX"):
            resp = views.NCIPortalAnalysisAPIView.as_view()(f.get("/x"), nci_id=nci_id)
            self.assertEqual(resp.status_code, 404, nci_id)

    def test_portal_post_validates_sites(self):
        resp = views.NCIPortalAnalysisAPIView.as_view()(
            APIRequestFactory().post("/callflow/NCI/portal/analysis", {"sites": []}, format="json"))
        self.assertEqual(resp.status_code, 400)


class NCIScopeFileTests(SimpleTestCase):
    databases = []

    def _rows(self, sites, cells, win):
        xlsx = _pipe.scope_csv(sites, cells, win)
        return _pd.read_excel(__import__("io").BytesIO(xlsx), dtype=str,
                              keep_default_na=False).to_dict("records")

    def test_myday_is_the_nci_window_in_15_min_steps(self):
        win = {"start": _dt.datetime(2026, 11, 1, 5, 45), "end": _dt.datetime(2026, 11, 1, 6, 30)}
        rows = self._rows(["D2431", "K4369", "Q7777"], ["L09_LD2431_01", "L18_LK4369_01"], win)
        self.assertEqual(rows, [
            {"myday": "2026-11-01 05:45", "cell_name": "L09_LD2431_01", "SITE_NAME": ""},
            {"myday": "2026-11-01 06:00", "cell_name": "L18_LK4369_01", "SITE_NAME": ""},
            {"myday": "2026-11-01 06:15", "cell_name": "Q7777", "SITE_NAME": "Q7777"},
            {"myday": "2026-11-01 06:30", "cell_name": "", "SITE_NAME": ""},
        ])

    def test_more_cells_than_steps_repeat_the_window_times(self):
        win = {"start": _dt.datetime(2026, 11, 1, 0, 0), "end": _dt.datetime(2026, 11, 1, 0, 15)}
        rows = self._rows([], ["L09_LA0001_01", "L09_LA0002_01", "L09_LA0003_01"], win)
        self.assertEqual([r["myday"] for r in rows],
                         ["2026-11-01 00:00", "2026-11-01 00:15", "2026-11-01 00:00"])
        self.assertEqual([r["cell_name"] for r in rows],
                         ["L09_LA0001_01", "L09_LA0002_01", "L09_LA0003_01"])


class ParallelRetrievalTests(SimpleTestCase):
    databases = []

    def test_trino_queries_run_at_the_same_time(self):
        # current = ClickHouse + Hive, prev_1day = Hive: 3 queries. The barrier
        # only opens when all 3 are running together; sequential runs would time out.
        barrier = _threading.Barrier(3, timeout=10)

        def query(sql):
            barrier.wait()
            return _pd.DataFrame()

        now = _dt.datetime(2026, 11, 1, 12, 0)
        with mock.patch.object(_svc, "run_trino_query", query):
            res = _svc.fetch_trino_site_kpis(["K2398"], start=now - _dt.timedelta(hours=14), end=now,
                                             now=now, window_names=("current", "prev_1day"))
        self.assertEqual(res["errors"], {})
        # reported in the planned order, not the order they finished
        self.assertEqual([(q["window"], q["source"]) for q in res["queries"]],
                         [("current", "hive_s1mme_kpi"), ("current", "clickhouse_15min"),
                          ("prev_1day", "hive_s1mme_kpi")])

    def test_viavi_windows_run_at_the_same_time(self):
        barrier = _threading.Barrier(2, timeout=10)

        def viavi(sites, w, log_path, ts):
            barrier.wait()
            return _pd.DataFrame({"PERIOD_TIMESTAMP": [w["start"]]})

        wins = {"primary": {"start": 1}, "yesterday": {"start": 2}}
        notes = {}
        with mock.patch.object(_runner, "fetch_viavi", viavi):
            df = _pipe._retrieve_viavi(["K2398"], wins, "", "ts", notes, mock.Mock())
        self.assertEqual(sorted(df["PERIOD_TIMESTAMP"]), [1, 2])
        self.assertEqual(notes["viavi_failures"], [])


class NCIScoreTests(SimpleTestCase):
    databases = []

    def test_site_matched_to_several_rca_sites_gets_the_highest_score(self):
        results = {"chains": [
            {"site": "LD2431", "criticalIssues": 1, "anomalyIssues": 0, "score": 120, "ticketList": []},
            {"site": "D2431", "criticalIssues": 2, "anomalyIssues": 1, "score": 300, "ticketList": []},
        ]}
        out, _ = _pipe.map_rca_to_sites(["X2431"], results)
        self.assertEqual(out["X2431"]["critical_cases"], 3)      # counts add up
        self.assertEqual(out["X2431"]["score"], 300)             # score: the highest, not 420


class NCIDuplicateSiteTests(SimpleTestCase):
    databases = []

    # Same shape as request e2a4c48e32ab4a64: D3426 sent 3 times; the tool names
    # the site LD3426 (4G cells) and also LMD3426 / ND3426 (2G/3G cells).
    CHAINS = {"chains": [
        {"site": "LD3426", "criticalIssues": 15, "anomalyIssues": 14, "score": 5863,
         "ticketList": [{"incident_number": "INC000082618500"}, {"incident_number": "INC000082526534"}]},
        {"site": "LMD3426", "criticalIssues": 0, "anomalyIssues": 0, "score": 0, "ticketList": []},
        {"site": "ND3426", "criticalIssues": 0, "anomalyIssues": 0, "score": 0, "ticketList": []},
    ]}

    def test_repeated_site_gets_the_tool_results(self):
        out, problems = _pipe.map_rca_to_sites(["D3426", "D3426", "D3426"], self.CHAINS)
        self.assertEqual(problems, [])
        row = out["D3426"]
        self.assertEqual((row["critical_cases"], row["minor_cases"], row["trouble_tickets"], row["score"]),
                         (15, 14, 2, 5863))
        self.assertEqual(row["_rca_sites"], ["LD3426", "LMD3426", "ND3426"])

    def test_two_different_sites_with_one_id_are_still_refused(self):
        out, problems = _pipe.map_rca_to_sites(["D3426", "K3426"], self.CHAINS)
        self.assertEqual(out["D3426"]["critical_cases"], 0)
        self.assertEqual(len(problems), 2)
        self.assertIn("shared by requested sites ['D3426', 'K3426']", problems[0]["error"])


class HiveDayPartitionTests(SimpleTestCase):
    databases = []

    def test_day_list_covers_the_window_plus_the_day_before(self):
        days = _svc.daypartition_list(_dt.datetime(2026, 11, 3, 1, 30), _dt.datetime(2026, 11, 3, 7, 30))
        self.assertEqual(days, "('20261102', '2026-11-02', '20261103', '2026-11-03')")

    def test_end_at_midnight_does_not_add_the_next_day(self):
        days = _svc.daypartition_list(_dt.datetime(2026, 11, 2, 18, 0), _dt.datetime(2026, 11, 3, 0, 0))
        self.assertNotIn("20261103", days)
        self.assertIn("20261102", days)

    def test_hive_sql_filters_the_partition_first_clickhouse_unchanged(self):
        seen = {}

        def query(sql):
            seen["hive" if "s1mme_kpi a" in sql else "clickhouse"] = sql
            return _pd.DataFrame()

        now = _dt.datetime(2026, 11, 3, 13, 30)
        with mock.patch.object(_svc, "run_trino_query", query):
            _svc.fetch_trino_site_kpis(["D3426"], start=now - _dt.timedelta(hours=12), end=now,
                                       now=now, window_names=("current",))
        hive = seen["hive"]
        self.assertIn("WHERE a.daypartition IN ('20261102', '2026-11-02', '20261103', '2026-11-03')", hive)
        self.assertLess(hive.index("a.daypartition IN"), hive.index("date_parse(a.time_t"))
        self.assertNotIn("daypartition", seen["clickhouse"])
