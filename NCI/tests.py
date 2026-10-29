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
# Trino S1-MME site KPIs inside run_nci_analysis
# ─────────────────────────────────────────────────────────────────────────────
import re as _re

import pandas as _pd

from callflow.core.services import smartcare_site_kpi_service as _svc
from .service import analysis_runner as _runner
from .service import trino_site_analysis as _tsa

_COLS = ["time", "site_logical_name", "attach_req_times", "attach_fail_times", "tau_req_times",
         "tau_fail_times", "s1mme_authentication_req_times", "s1mme_authentication_fail_times",
         "fail_cause_name"]


class _FakeTrino:
    """LU4820: attach failures jump in the last 6 hours (2 % -> 30 %), TAU fine,
    authentication 80 % SR (ClickHouse only). LU4821: too few attempts to judge."""

    def __init__(self, fail_source=None):
        self.calls, self.fail_source = [], fail_source

    def __call__(self, sql):
        recent = "clickhouse.e2e." in sql
        self.calls.append({"recent": recent, "start": int(_re.search(r">= (\d+)", sql).group(1)),
                           "end": int(_re.search(r"PROC_STARTTIME < (\d+)", sql).group(1))})
        if self.fail_source == ("recent" if recent else "history"):
            raise RuntimeError("trino down")
        t = self.calls[-1]["start"]
        if recent:
            rows = [[t, "LU4820", 1000, 300, 500, 10, 200, 40, "x"],
                    [t, "LU4821", 20, 5, 10, 1, 5, 1, "x"]]
        else:
            when = _pd.Timestamp(t, unit="s", tz="UTC").tz_convert("Africa/Cairo").strftime("%Y-%m-%d %H:%M:%S")
            rows = [[when, "LU4820", 1000, 20, 500, 10, None, None, "x"],
                    [when, "LU4821", 20, 5, 10, 1, None, None, "x"]]
        return _pd.DataFrame(rows, columns=_COLS)


def _run_nci(fake):
    empty = _pd.DataFrame()
    with mock.patch.object(_svc, "run_trino_query", fake), \
            mock.patch.object(_tsa, "cache_get", return_value=None), \
            mock.patch.object(_tsa, "cache_set") as cache_set, \
            mock.patch.object(_runner, "fetch_npp_one_site", return_value=empty), \
            mock.patch.object(_runner, "fetch_viavi", return_value=empty), \
            mock.patch.object(_runner, "fetch_remedy", return_value=({}, 0)):
        import logging
        out = _runner.run_nci_analysis(["LU4820", "LU4821"], logging.getLogger("nci-test"), "", "ts")
    return out, cache_set


class NCITrinoSourceTests(SimpleTestCase):
    databases = []

    def test_trino_queries_cover_the_nci_windows(self):
        fake = _FakeTrino()
        _run_nci(fake)
        self.assertEqual(len(fake.calls), 4)                       # current: 2 halves; 1 day / 1 week: Hive
        self.assertEqual(sum(c["recent"] for c in fake.calls), 1)
        recent = [c for c in fake.calls if c["recent"]][0]
        self.assertEqual(recent["end"] - recent["start"], 6 * 3600)
        cur_hist = max((c for c in fake.calls if not c["recent"]), key=lambda c: c["end"])
        self.assertEqual((cur_hist["end"], cur_hist["end"] - cur_hist["start"]), (recent["start"], 6 * 3600))
        # 1 day / 1 week before: the same 12 hours shifted
        starts = sorted(c["start"] for c in fake.calls if not c["recent"])
        self.assertEqual(cur_hist["start"] - starts[1], 86400)
        self.assertEqual(cur_hist["start"] - starts[0], 7 * 86400)

    def test_trino_kpis_count_in_sites_summary(self):
        out, cache_set = _run_nci(_FakeTrino())
        s = out["sites_summary"]
        # attach failure 16 % (> 15 %, and +700 % vs baseline) and auth SR 80 % (< 90 %, no baseline)
        self.assertEqual((s["LU4820"]["critical_cases"], s["LU4820"]["minor_cases"]), (2, 0))
        # LU4821: fewer than MIN_ATTEMPTS requests -> not judged
        self.assertEqual((s["LU4821"]["critical_cases"], s["LU4821"]["minor_cases"]), (0, 0))
        self.assertEqual(out["errors"]["trino"], [])
        cache_set.assert_called_once()

    def test_trino_comparison_details(self):
        fake = _FakeTrino()
        with mock.patch.object(_svc, "run_trino_query", fake):
            windows = _runner.build_nci_time_windows()
            result = _svc.fetch_smartcare_site_kpis(["LU4820", "LU4821"], start=windows["current"]["start"],
                                                    end=windows["current"]["end"], now=windows["current"]["end"])
        raw = _tsa.run_trino_comparisons(_runner._analyzer, result, ["LU4820", "LU4821"], _runner._COMPARISONS)
        kpis = raw["current_vs_1daybefore"]["affected_sites_details"]["LU4820"]["kpis"]
        self.assertEqual(set(kpis), {"S1MME_ATTACH_FAILURE_RATE", "S1MME_AUTH_SR"})
        self.assertEqual(kpis["S1MME_ATTACH_FAILURE_RATE"]["current_value"], 16.0)
        self.assertEqual(kpis["S1MME_ATTACH_FAILURE_RATE"]["previous_value"], 2.0)
        self.assertIsNone(kpis["S1MME_AUTH_SR"]["previous_value"])
        self.assertIn("no baseline", kpis["S1MME_AUTH_SR"]["message"])
        # 1 day vs 1 week: both 2 % attach failure, no auth data -> nothing abnormal
        self.assertEqual(raw["1daybefore_vs_1week"]["affected_sites_list"], [])

    def test_trino_failure_is_reported_and_other_windows_still_used(self):
        out, cache_set = _run_nci(_FakeTrino(fail_source="recent"))
        self.assertEqual([(e["period"], e["source"]) for e in out["errors"]["trino"]],
                         [("current", "clickhouse_15min")])
        # current window has no totals -> no Trino issues (no partial data judged)
        self.assertEqual(out["sites_summary"]["LU4820"]["critical_cases"], 0)
        cache_set.assert_not_called()                           # failed results are not cached

    def test_trino_exception_does_not_break_nci(self):
        with mock.patch.object(_runner, "fetch_trino_site_kpis", side_effect=RuntimeError("no route")):
            out, _ = _run_nci(_FakeTrino())
        self.assertEqual(out["errors"]["trino"], [{"period": "all", "error": "no route"}])
        self.assertEqual(set(out["sites_summary"]), {"LU4820", "LU4821"})
