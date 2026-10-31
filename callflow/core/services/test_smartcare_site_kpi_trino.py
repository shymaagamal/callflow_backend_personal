# callflow/core/services/test_smartcare_site_kpi_trino.py
"""
Tests for the Trino S1-MME site KPIs (smartcare_site_kpi_service).

Only run_trino_query (the network call) is replaced; the window split, SQL
building, site escaping, normalisation, combining and the per-site roll-up are
the real code.

Run:  python manage.py test callflow.core.services.test_smartcare_site_kpi_trino
"""

import datetime as dt
import json
import re
from unittest import mock

import pandas as pd
import pytz
from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from . import smartcare_site_kpi_service as svc

NOW = dt.datetime(2026, 10, 29, 12, 7)          # rounds down to 12:00 (Cairo)
NOW_R = dt.datetime(2026, 10, 29, 12, 0)
SITES = ["LU4820", "LU4821"]
CAIRO = pytz.timezone("Africa/Cairo")
COLS = ["time", "site_logical_name", "attach_req_times", "attach_fail_times", "tau_req_times",
        "tau_fail_times", "s1mme_authentication_req_times", "s1mme_authentication_fail_times",
        "fail_cause_name"]


def _epoch(local_naive):
    return int(CAIRO.localize(local_naive).timestamp())


class FakeTrino:
    """run_trino_query stand-in. ClickHouse rows carry tz-aware UTC timestamps
    (what the Trino client returns for from_unixtime), Hive rows naive local
    strings (time_t), both with lower-cased column names like Trino."""

    def __init__(self, empty=(), fail=(), drop_col=None):
        self.calls, self.empty, self.fail, self.drop_col = [], set(empty), set(fail), drop_col

    def __call__(self, sql):
        source = svc.RECENT_SOURCE if "clickhouse.e2e." in sql else svc.HISTORY_SOURCE
        start = int(re.search(r"PROC_STARTTIME >= (\d+)", sql).group(1))
        end = int(re.search(r"PROC_STARTTIME < (\d+)", sql).group(1))
        self.calls.append({"source": source, "sql": sql, "start": start, "end": end})
        key = (source, start)
        if source in self.fail or key in self.fail:
            raise RuntimeError(f"{source} unavailable")
        if source in self.empty or key in self.empty:
            return pd.DataFrame(columns=COLS)
        t = dt.datetime.fromtimestamp(start, tz=dt.timezone.utc)
        rows = []
        for site, base in (("LU4820", 100), ("LU4821", 10)):
            if source == svc.RECENT_SOURCE:
                when = t
                auth, auth_fail = 7, 1
            else:
                when = t.astimezone(CAIRO).strftime("%Y-%m-%d %H:%M:%S")
                auth, auth_fail = None, None
            rows.append([when, site, base, 3, 20, 2, auth, auth_fail, "cause A"])
            rows.append([when, site, base // 2, 0, 5, 0, auth, auth_fail, None])
        df = pd.DataFrame(rows, columns=COLS)
        return df.drop(columns=[self.drop_col]) if self.drop_col else df


def run(fake, **kw):
    with mock.patch.object(svc, "run_trino_query", fake):
        return svc.fetch_trino_site_kpis(SITES, now=NOW, **kw)


class WindowSplitTests(SimpleTestCase):
    databases = []

    def test_12h_current_window_is_6h_hive_plus_6h_clickhouse(self):
        cutoff = NOW_R - dt.timedelta(hours=6)
        w = svc.build_nci_time_windows(now=NOW)
        parts = svc.split_window(w["current"]["start"], w["current"]["end"], cutoff)
        self.assertEqual(parts, [
            (svc.HISTORY_SOURCE, NOW_R - dt.timedelta(hours=12), cutoff),
            (svc.RECENT_SOURCE, cutoff, NOW_R),
        ])
        # half-open and contiguous: no overlap, no gap
        self.assertEqual(parts[0][2], parts[1][1])

    def test_comparison_windows_are_hive_only(self):
        cutoff = NOW_R - dt.timedelta(hours=6)
        w = svc.build_nci_time_windows(now=NOW)
        for name in ("prev_1day", "prev_1week"):
            parts = svc.split_window(w[name]["start"], w[name]["end"], cutoff)
            self.assertEqual(parts, [(svc.HISTORY_SOURCE, w[name]["start"], w[name]["end"])])

    def test_recent_explicit_range_is_clickhouse_only(self):
        cutoff = NOW_R - dt.timedelta(hours=6)
        a, b = NOW_R - dt.timedelta(hours=2), NOW_R
        self.assertEqual(svc.split_window(a, b, cutoff), [(svc.RECENT_SOURCE, a, b)])


class SiteListTests(SimpleTestCase):
    databases = []

    def test_site_list(self):
        self.assertEqual(svc.sql_site_list(["LU4820", "K-2398.1"]), "('LU4820','K-2398.1')")

    def test_injection_is_rejected_before_any_query(self):
        fake = FakeTrino()
        for bad in ["LU4820') OR 1=1 --", "A'B", "A B", ""]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                with mock.patch.object(svc, "run_trino_query", fake):
                    svc.fetch_trino_site_kpis([bad] if bad else [" "], now=NOW)
        self.assertEqual(fake.calls, [])


class FetchTests(SimpleTestCase):
    databases = []

    def test_queries_sent(self):
        fake = FakeTrino()
        run(fake)
        cutoff = NOW_R - dt.timedelta(hours=6)
        expected = {
            (svc.HISTORY_SOURCE, _epoch(NOW_R - dt.timedelta(hours=12)), _epoch(cutoff)),
            (svc.RECENT_SOURCE, _epoch(cutoff), _epoch(NOW_R)),
            (svc.HISTORY_SOURCE, _epoch(NOW_R - dt.timedelta(days=1, hours=12)),
             _epoch(NOW_R - dt.timedelta(days=1))),
            (svc.HISTORY_SOURCE, _epoch(NOW_R - dt.timedelta(days=7, hours=12)),
             _epoch(NOW_R - dt.timedelta(days=7))),
        }
        self.assertEqual({(c["source"], c["start"], c["end"]) for c in fake.calls}, expected)
        self.assertEqual(len(fake.calls), 4)
        for c in fake.calls:
            self.assertIn("IN ('LU4820','LU4821')", c["sql"])
            self.assertNotIn("unix_timestamp", c["sql"])
            self.assertNotIn("{", c["sql"])
            self.assertNotIn("BETWEEN", c["sql"])

    def test_combined_rows_have_one_schema(self):
        out = run(FakeTrino())
        rows = out["s1mme_rows"]["current"]
        self.assertEqual(len(rows), 8)                   # 2 sites x 2 fail causes x 2 sources
        self.assertEqual({tuple(r) for r in rows}, {tuple(svc.QUERY_COLS + ["SOURCE"])})
        self.assertEqual({r["SOURCE"] for r in rows}, {svc.HISTORY_SOURCE, svc.RECENT_SOURCE})
        # both sources' times are on the same (Cairo, naive) clock: each part
        # starts at its own window boundary
        times = {r["SOURCE"]: r["Time"] for r in rows}
        self.assertEqual(times[svc.HISTORY_SOURCE], "2026-10-29 00:00:00")
        self.assertEqual(times[svc.RECENT_SOURCE], "2026-10-29 06:00:00")
        for r in rows:
            for col in svc.COUNT_COLS:
                self.assertTrue(r[col] is None or type(r[col]) is int, (col, r[col]))
        # Hive has no auth counters: null, not 0
        self.assertTrue(all(r["S1MME_AUTHENTICATION_REQ_TIMES"] is None
                            for r in rows if r["SOURCE"] == svc.HISTORY_SOURCE))
        json.dumps(out, allow_nan=False)                # DRF strict JSON

    def test_frames_normalise_to_same_dtypes(self):
        fake = FakeTrino()
        recent = svc.normalize_frame(fake(svc.trino_s1mme_QUERIES[svc.RECENT_SOURCE].format(
            site_list="('X')", start_epoch=1793253600, end_epoch=1793275200)), svc.RECENT_SOURCE)
        hist = svc.normalize_frame(fake(svc.trino_s1mme_QUERIES[svc.HISTORY_SOURCE].format(
            site_list="('X')", start_epoch=1793253600, end_epoch=1793275200)), svc.HISTORY_SOURCE)
        self.assertEqual(list(recent.columns), list(hist.columns))
        self.assertEqual(recent.dtypes.to_dict(), hist.dtypes.to_dict())
        self.assertEqual(str(recent["Time"].dtype), "datetime64[ns]")

    def test_per_site_records_keep_the_previous_shape(self):
        out = run(FakeTrino())
        cur = out["windows"]["current"]
        self.assertEqual(set(cur), {"start", "end", "s1mme", "s6a"})
        self.assertEqual(cur["s6a"], [])
        self.assertEqual([r["LOGICAL_SITE_ID"] for r in cur["s1mme"]], SITES)
        lu20 = cur["s1mme"][0]
        self.assertEqual(set(lu20), {"LOGICAL_SITE_ID", "ATTACH_REQ_TIMES", "ATTACH_Fail_TIMES", "TAU_REQ_TIMES",
                                     "TAU_FAIL_TIMES", "S1MME_AUTHENTICATION_REQ_TIMES",
                                     "S1MME_AUTHENTICATION_FAIL_TIMES"})
        # (100 + 50) per source, 2 sources
        self.assertEqual((lu20["ATTACH_REQ_TIMES"], lu20["ATTACH_Fail_TIMES"]), (300, 6))
        self.assertEqual((lu20["TAU_REQ_TIMES"], lu20["TAU_FAIL_TIMES"]), (50, 4))
        # auth only from the ClickHouse half (2 rows x 7)
        self.assertEqual(lu20["S1MME_AUTHENTICATION_REQ_TIMES"], 14)

    def test_hive_only_window_has_null_auth_totals(self):
        out = run(FakeTrino())
        for r in out["windows"]["prev_1day"]["s1mme"]:
            self.assertIsNone(r["S1MME_AUTHENTICATION_REQ_TIMES"])
            self.assertIsNone(r["S1MME_AUTHENTICATION_FAIL_TIMES"])
            self.assertEqual(r["ATTACH_REQ_TIMES"], 150 if r["LOGICAL_SITE_ID"] == "LU4820" else 15)

    def test_one_empty_part_keeps_the_other(self):
        out = run(FakeTrino(empty=[svc.RECENT_SOURCE]))
        cur = out["windows"]["current"]["s1mme"]
        self.assertEqual(cur[0]["ATTACH_REQ_TIMES"], 150)       # Hive half only
        self.assertIsNone(cur[0]["S1MME_AUTHENTICATION_REQ_TIMES"])
        self.assertNotIn("error", out)
        self.assertEqual(out["errors"], {})
        rows = [q["rows"] for q in out["queries"] if q["source"] == svc.RECENT_SOURCE]
        self.assertEqual(rows, [0])

    def test_all_empty_is_reported_not_filled(self):
        out = run(FakeTrino(empty=[svc.RECENT_SOURCE, svc.HISTORY_SOURCE]))
        self.assertEqual(out["error"], "no S1-MME rows for these sites in Trino")
        for w in out["windows"].values():
            self.assertEqual((w["s1mme"], w["s6a"]), ([], []))
        self.assertEqual(out["s1mme_rows"], {"current": [], "prev_1day": [], "prev_1week": []})

    def test_failed_part_gives_no_partial_window(self):
        out = run(FakeTrino(fail=[svc.RECENT_SOURCE]))
        self.assertEqual(out["windows"]["current"]["s1mme"], [])
        self.assertEqual(out["s1mme_rows"]["current"], [])
        self.assertEqual(list(out["errors"]), ["current.s1mme.clickhouse_15min"])
        self.assertIn("unavailable", out["errors"]["current.s1mme.clickhouse_15min"])
        self.assertTrue(out["windows"]["prev_1day"]["s1mme"])         # other windows unaffected
        failed = [q for q in out["queries"] if q.get("error")]
        self.assertEqual([(q["window"], q["source"]) for q in failed], [("current", svc.RECENT_SOURCE)])
        self.assertNotIn("error", out)

    def test_unexpected_result_columns_fail_loudly(self):
        out = run(FakeTrino(drop_col="fail_cause_name"))
        self.assertEqual(len(out["errors"]), 4)
        self.assertIn("missing column", next(iter(out["errors"].values())))
        self.assertTrue(all(w["s1mme"] == [] for w in out["windows"].values()))

    def test_logging(self):
        with self.assertLogs(svc.logger, level="INFO") as cm:
            run(FakeTrino(empty=[svc.RECENT_SOURCE], fail=[(svc.HISTORY_SOURCE,
                _epoch(NOW_R - dt.timedelta(days=7, hours=12)))]))
        text = "\n".join(cm.output)
        self.assertIn("FROM clickhouse.e2e.BKPI_SIGNAL_S1MME_CISAI_15MIN", text)   # SQL
        self.assertIn("FROM hive.smartcare.s1mme_kpi", text)
        self.assertIn("current hive_s1mme_kpi range [2026-10-29 00:00:00, 2026-10-29 06:00:00)", text)
        self.assertIn("current hive_s1mme_kpi returned 4 row(s)", text)
        self.assertIn("current clickhouse_15min returned no rows", text)
        self.assertIn("prev_1week hive_s1mme_kpi failed", text)

    def test_explicit_range_rca_site_run(self):
        fake = FakeTrino()
        a, b = dt.datetime(2026, 10, 10, 14, 21), dt.datetime(2026, 10, 10, 15, 21)
        out = run(fake, start=a, end=b)
        self.assertEqual({c["source"] for c in fake.calls}, {svc.HISTORY_SOURCE})
        self.assertEqual(out["windows"]["current"]["start"], "2026-10-10 14:21:00")
        self.assertEqual(out["windows"]["prev_1week"]["end"], "2026-10-03 15:21:00")


class SiteKpiViewTests(SimpleTestCase):
    databases = []

    def test_view_returns_strict_json(self):
        from ..views.smartcare_site_kpi import SmartcareSiteKpiAPIView
        f = APIRequestFactory()
        with mock.patch.object(svc, "run_trino_query", FakeTrino()):
            resp = SmartcareSiteKpiAPIView.as_view()(
                f.post("/callflow/api/smartcare/site-kpis", {"site_names": SITES}, format="json"))
            resp.render()
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertEqual(body["sites"], SITES)
        self.assertIn("s1mme", body["windows"]["current"])

    def test_view_rejects_bad_site_ids(self):
        from ..views.smartcare_site_kpi import SmartcareSiteKpiAPIView
        f = APIRequestFactory()
        fake = FakeTrino()
        with mock.patch.object(svc, "run_trino_query", fake):
            resp = SmartcareSiteKpiAPIView.as_view()(
                f.post("/callflow/api/smartcare/site-kpis", {"site_names": ["X') OR 1=1 --"]}, format="json"))
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(fake.calls, [])


class _NoOffsetTZ(dt.tzinfo):
    """A zone object that reports no UTC offset, as some Trino client versions
    attach to 'timestamp with time zone' values (production error:
    "'NoneType' object has no attribute 'total_seconds'")."""
    def utcoffset(self, d):
        return None

    def dst(self, d):
        return None

    def tzname(self, d):
        return "?"


class TimeColumnRegressionTests(SimpleTestCase):
    databases = []

    def test_clickhouse_query_returns_epoch_not_zoned_timestamp(self):
        sql = svc.trino_s1mme_QUERIES[svc.RECENT_SOURCE]
        self.assertIn("CAST(a.STARTTIME AS BIGINT) AS Time", sql)
        self.assertNotIn("from_unixtime", sql)

    def test_epoch_seconds_become_cairo_local_time(self):
        t = svc._to_local_time(pd.Series([_epoch(dt.datetime(2026, 10, 29, 6, 0))]))
        self.assertEqual(t.iloc[0], pd.Timestamp("2026-10-29 06:00:00"))
        self.assertEqual(str(t.dtype), "datetime64[ns]")

    def test_zone_without_offset_does_not_crash(self):
        bad = dt.datetime(2026, 10, 29, 6, 0, tzinfo=_NoOffsetTZ())
        with self.assertRaises(AttributeError):          # what pandas did on the server
            pd.to_datetime(pd.Series([bad]), utc=True)
        t = svc._to_local_time(pd.Series([bad, None], dtype=object))
        self.assertEqual(t.iloc[0], pd.Timestamp("2026-10-29 06:00:00"))   # taken as local time
        self.assertTrue(pd.isna(t.iloc[1]))

    def test_mixed_values(self):
        utc = dt.datetime(2026, 10, 29, 3, 0, tzinfo=dt.timezone.utc)   # Cairo was UTC+3 that day
        t = svc._to_local_time(pd.Series([utc, "2026-10-29 07:00:00", "not a time"], dtype=object))
        self.assertEqual(list(t.iloc[:2]), [pd.Timestamp("2026-10-29 06:00:00"),
                                            pd.Timestamp("2026-10-29 07:00:00")])
        self.assertTrue(pd.isna(t.iloc[2]))

    def test_full_fetch_with_server_like_trino_values(self):
        """ClickHouse rows as epoch numbers (the new query), Hive rows with an
        offset-less zone -- the whole fetch works and nothing is dropped."""
        class ServerLike(FakeTrino):
            def __call__(self, sql):
                df = super().__call__(sql)
                if "clickhouse.e2e." in sql:
                    df["time"] = [int(v.timestamp()) for v in df["time"]]
                else:
                    df["time"] = [dt.datetime.fromisoformat(v).replace(tzinfo=_NoOffsetTZ())
                                  for v in df["time"]]
                return df
        out = run(ServerLike())
        self.assertEqual(out["errors"], {})
        times = {r["SOURCE"]: r["Time"] for r in out["s1mme_rows"]["current"]}
        self.assertEqual(times, {svc.HISTORY_SOURCE: "2026-10-29 00:00:00",
                                 svc.RECENT_SOURCE: "2026-10-29 06:00:00"})
