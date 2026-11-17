"""
DataCube query limits: shared Data/Voice execution, the 12-query cap per
request (retries included) and the global DataCube session ledger.

    python manage.py test callflow.mi.tests.test_mi_datacube
"""
import json
import time
from unittest import mock

from django.test import override_settings

from callflow.mi import catalog, jobs, query_plan
from callflow.mi.tests import fake_smartcare as fs
from callflow.mi.tests.test_mi import MITestBase
from callflow.mi.tests.test_mi_batch import MI_PATH, both_fixture, subs

A, B, C, D = fs.MSISDNS            # 1012345678, 1112345678, 1212345678, 1512345678
TIMEOUT = fs.SOCKET_TIMEOUT
DATA_KEYS = ("sgid_data", "s1mme", "gbiups", "scf")
VOICE_KEYS = ("dra_voice", "sgs", "hss", "cs_moc", "cs_mtc", "volte_mo", "volte_mt", "reg")


def request(*groups, start="2026-10-15T10:00", end="2026-10-15T11:00"):
    return {"start_date": start, "end_date": end,
            "complaints": [{"category": c, "subcategory": s, "msisdn_list": list(m)} for c, s, m in groups]}


# the user's real sheet shape: 11 complaint groups, overlapping MSISDNs
ELEVEN_GROUPS = (
    ("Home Wireless", "N/A", (A, B)),
    ("MI", "MI Connection", (B, C)),
    ("MI", "MI Connection not solved", (A,)),
    ("MI", "MI Connection Solved", (D,)),
    ("MI", "N/A", (A, B, C, D)),
    ("Network", "Call Dropped / Short call", (A,)),
    ("Network", "Can't receive call", (B, C)),
    ("Network", "Can't make/receive call", (C,)),
    ("Network", "Network coverage", (D,)),
    ("Network", "Network voice", (A, D)),
    ("Network", "VOLTE", (A, B, C, D)),
)


class SharedExecutionTests(MITestBase):

    def run_batch(self, body, fake):
        resp = self.post(body, fake, path=MI_PATH)
        self.assertEqual(resp.status_code, 202, resp.content)
        job_id = json.loads(resp.content)["job_id"]
        return job_id, json.loads(self.get(f"{MI_PATH}/jobs/{job_id}/result").content)

    def single(self, complain_type, msisdns):
        """What the old per-group run returned: the group's MSISDNs queried alone."""
        resp = self.post({"msisdn_list": list(msisdns), "start_date": "2026-10-15T10:00",
                          "end_date": "2026-10-15T11:00", "Complain_Type": complain_type},
                         fs.FakeSmartcare(both_fixture(), filter_msisdns=True))
        return resp.status_code, json.loads(resp.content)

    def test_eleven_groups_run_twelve_queries_and_match_per_group_results(self):
        fake = fs.FakeSmartcare(both_fixture(), filter_msisdns=True)
        job_id, body = self.run_batch(request(*ELEVEN_GROUPS), fake)
        # 4 Data + 8 Voice statements, whatever the number of groups (old design: 4*5 + 8*6 = 68)
        self.assertEqual(len(fake.executions), 12)
        self.assertEqual(sorted(set(fake.executions)), sorted(DATA_KEYS + VOICE_KEYS))
        self.assertEqual(body["datacube_queries"]["planned"], 12)
        self.assertEqual(body["datacube_queries"]["executed"], 12)
        # one IN list per Complain_Type with the pooled unique MSISDNs
        s1mme_sql = next(q for q in fake.sqls if "S1MME" in q.upper())
        for m in (A, B, C, D):
            self.assertIn(f"'{m}'", s1mme_sql)
        # every group's result == the result of querying its MSISDNs alone
        by_name = {(c, s): m for c, s, m in ELEVEN_GROUPS}
        checked = 0
        for category, ctype, sub in subs(body):
            msisdns = by_name[(category, sub["subcategory"])]
            status, expected = self.single("Data" if ctype == "DATA" else "Voice", msisdns)
            self.assertEqual(status, 200)
            self.assertEqual(sub["analysis_result"], expected, (category, sub["subcategory"]))
            self.assertEqual(sub["requested_msisdns"], len(msisdns))
            checked += 1
        self.assertEqual(checked, 11)

    def test_overlapping_msisdn_is_kept_in_every_group(self):
        fake = fs.FakeSmartcare(both_fixture(), filter_msisdns=True)
        job_id, body = self.run_batch(request(("MI", "x", (A,)), ("Home Wireless", "y", (A, B)),
                                              ("Network", "z", (A,))), fake)
        mapping = json.loads((self.job_dir(job_id) / "shared" / "msisdn_groups.json").read_text())
        self.assertEqual([(g["category"], g["subcategory"], g["complain_type"]) for g in mapping[A]],
                         [("MI", "x", "Data"), ("Home Wireless", "y", "Data"), ("Network", "z", "Voice")])
        results = {s["subcategory"]: s for _c, _t, s in subs(body)}
        # A's rows are counted in both Data groups and in the Voice group
        self.assertEqual(results["x"]["analysis_result"], self.single("Data", (A,))[1])
        self.assertEqual(results["y"]["analysis_result"], self.single("Data", (A, B))[1])
        self.assertEqual(results["z"]["analysis_result"], self.single("Voice", (A,))[1])
        self.assertNotEqual(results["x"]["analysis_result"], results["y"]["analysis_result"])
        self.assertEqual(len(fake.executions), 12)
        self.assertEqual(fake.sqls[0].count(f"'{A}'"), 1)          # deduplicated inside the IN list

    def test_group_without_rows_is_no_data_while_others_have_rows(self):
        fake = fs.FakeSmartcare(both_fixture(), filter_msisdns=True)
        _job, body = self.run_batch(request(("MI", "has rows", (A,)), ("MI", "no rows", ("1099999999",))), fake)
        statuses = {s["subcategory"]: s["status"] for _c, _t, s in subs(body)}
        self.assertEqual(statuses, {"has rows": "COMPLETED", "no rows": "NO_DATA"})
        self.assertEqual(len(fake.executions), 4)

    def test_only_needed_complain_types_are_queried(self):
        fake = fs.FakeSmartcare(both_fixture(), filter_msisdns=True)
        _job, body = self.run_batch(request(("MI", "a", (A,)), ("Home Wireless", "b", (B,))), fake)
        self.assertEqual(sorted(fake.executions), sorted(DATA_KEYS))
        self.assertEqual(body["datacube_queries"]["by_complain_type"], {"DATA": {"msisdns": 2, "queries": 4,
                                                                                "statements": 4}})

    def test_shared_query_failure_is_reported_in_every_group_of_its_type(self):
        fake = fs.FakeSmartcare(both_fixture(), filter_msisdns=True, fail={"cs_mtc": (9, TIMEOUT)})
        _job, body = self.run_batch(request(("MI", "data", (A,)), ("Network", "v1", (A,)),
                                            ("Network", "v2", (C,))), fake)
        st = {s["subcategory"]: (s["status"], s["failed_queries"]) for _c, _t, s in subs(body)}
        self.assertEqual(st["data"], ("COMPLETED", []))
        self.assertEqual(st["v1"], ("PARTIALLY_COMPLETED", ["smartcare_network_CS_MTC"]))
        self.assertEqual(st["v2"], ("PARTIALLY_COMPLETED", ["smartcare_network_CS_MTC"]))
        # Data + Voice = 12 planned -> no retry was possible
        self.assertEqual(fake.count("cs_mtc"), 1)
        self.assertEqual(body["datacube_queries"]["executed"], 12)
        self.assertEqual(body["datacube_queries"]["retries_denied"], 1)


class QueryCapTests(MITestBase):

    def test_plan_counts(self):
        from datetime import datetime
        s, e = datetime(2026, 10, 15, 10), datetime(2026, 10, 15, 11)
        self.assertEqual(query_plan.build_plan({"Data": [A]}, s, e).total, 4)
        self.assertEqual(query_plan.build_plan({"Voice": [A]}, s, e).total, 8)
        both = query_plan.build_plan({"Voice": [A], "Data": [B]}, s, e)
        self.assertEqual((both.total, both.retry_allowance), (12, 0))
        self.assertEqual([q.complain_type for q in both.queries], ["Data"] * 4 + ["Voice"] * 8)
        # midnight: still 12 with UNION ALL, 24 (refused) without
        night = (datetime(2026, 10, 15, 23, 30), datetime(2026, 10, 16, 0, 30))
        self.assertEqual(query_plan.build_plan({"Voice": [A], "Data": [B]}, *night).total, 12)
        with override_settings(MI_UNION_DAY_TABLES=False):
            with self.assertRaises(query_plan.QueryPlanError) as cm:
                query_plan.build_plan({"Voice": [A], "Data": [B]}, *night)
            self.assertIn("needs 24 DataCube queries (DATA 8, VOICE 16)", str(cm.exception))
            self.assertIn("2 day tables and MI_UNION_DAY_TABLES is off", str(cm.exception))

    @override_settings(MI_MAX_QUERIES_PER_REQUEST=10)
    def test_over_the_cap_is_refused_before_anything(self):
        fake = fs.FakeSmartcare(both_fixture())
        resp = self.post(request(("MI", "a", (A,)), ("Network", "b", (B,))), fake, path=MI_PATH)
        self.assertEqual(resp.status_code, 400)
        data = json.loads(resp.content)
        self.assertIn("this request needs 12 DataCube queries (DATA 4, VOICE 8); the limit is 10", data["error"])
        self.assertEqual(data["max_queries_per_request"], 10)
        self.assertEqual(fake.executions, [])
        self.assertFalse((self.mi_root / "jobs").exists())
        self.assertEqual(jobs.quota_status()["used"], 0)
        # Data alone fits
        self.assertEqual(self.post(request(("MI", "a", (A,))), fake, path=MI_PATH).status_code, 202)

    @override_settings(MI_MAX_MSISDNS_PER_QUERY=3)
    def test_too_many_msisdns_refused_never_truncated(self):
        resp = self.post(request(("MI", "a", (A, B)), ("Home Wireless", "b", (C, D))),
                         fs.FakeSmartcare(both_fixture()), path=MI_PATH)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("DATA complaints have 4 unique MSISDNs; at most 3", json.loads(resp.content)["error"])

    def test_query_without_subscriber_column_cannot_be_planned(self):
        from datetime import datetime
        mapping = dict(catalog.QUERY_SUBSCRIBER_COLUMN)
        del mapping["smartcare_SGS_Voice"]
        with mock.patch.object(catalog, "QUERY_SUBSCRIBER_COLUMN", mapping):
            with self.assertRaises(query_plan.QueryPlanError) as cm:
                query_plan.build_plan({"Voice": [A]}, datetime(2026, 10, 15, 10), datetime(2026, 10, 15, 11))
        self.assertIn("smartcare_SGS_Voice has no subscriber column", str(cm.exception))

    @override_settings(MI_SMARTCARE_MAX_ATTEMPTS=5)
    def test_retries_use_only_the_room_left_and_never_pass_12(self):
        # Data only: 4 planned, 8 retries possible; every query keeps timing out
        fake = fs.FakeSmartcare(both_fixture(), fail={k: (99, TIMEOUT) for k in DATA_KEYS})
        resp = self.post(request(("MI", "a", (A,))), fake, path=MI_PATH)
        body = json.loads(self.get(f"{MI_PATH}/jobs/{json.loads(resp.content)['job_id']}/result").content)
        self.assertEqual(len(fake.executions), 12)                  # 4 x 5 attempts wanted, capped at 12
        dc = body["datacube_queries"]
        self.assertEqual((dc["planned"], dc["executed"], dc["retries"]), (4, 12, 8))
        self.assertGreater(dc["retries_denied"], 0)
        self.assertEqual(body["status"], "FAILED")

    def test_retry_allowed_when_room_left(self):
        fake = fs.FakeSmartcare(both_fixture(), fail={"s1mme": (1, TIMEOUT)})
        resp = self.post(request(("MI", "a", (A,))), fake, path=MI_PATH)
        body = json.loads(self.get(f"{MI_PATH}/jobs/{json.loads(resp.content)['job_id']}/result").content)
        self.assertEqual(fake.count("s1mme"), 2)
        self.assertEqual((body["datacube_queries"]["executed"], body["status"]), (5, "COMPLETED"))

    def test_single_requests_are_capped_too(self):
        with override_settings(MI_MAX_QUERIES_PER_REQUEST=6):
            resp = self.post({"msisdn_list": [A], "start_date": "2026-10-15T10:00", "end_date": "2026-10-15T11:00",
                              "Complain_Type": "Voice"}, fs.FakeSmartcare(both_fixture()))
            self.assertEqual(resp.status_code, 400)
            self.assertIn("needs 8 DataCube queries", json.loads(resp.content)["error"])

    def test_every_execution_is_logged_with_its_number(self):
        fake = fs.FakeSmartcare(both_fixture())
        resp = self.post(request(("MI", "a", (A,)), ("Network", "b", (A,))), fake, path=MI_PATH)
        log = (self.job_dir(json.loads(resp.content)["job_id"]) / "job.log").read_text()
        self.assertIn("event=query_plan planned=12 limit=12 retry_allowance=0", log)
        self.assertEqual(log.count("event=datacube_query "), 12)
        for n in range(1, 13):
            self.assertIn(f"seq={n}/12 ", log)
        self.assertIn("event=datacube_queries_done limit=12 planned=12", log)
        self.assertIn("executed=12", log)

    def test_budget_counter_is_hard(self):
        b = query_plan.RequestBudget(limit=3, planned=2)
        self.assertEqual([b.acquire(("q1", "t")), b.acquire(("q2", "t")), b.acquire(("q1", "t"))], [1, 2, 3])
        with self.assertRaises(query_plan.BudgetDenied):
            b.acquire(("q2", "t"))
        self.assertEqual(b.executed, 3)
        with self.assertRaises(query_plan.QueryPlanError):
            query_plan.RequestBudget(limit=3, planned=4)

    def test_union_statement(self):
        stmts = query_plan.statements("SELECT x FROM t_{tablename} WHERE m IN {msisdn_tuple} ORDER BY x;",
                                      [20741, 20742], "('1')", "s", "e", union=True)
        self.assertEqual(len(stmts), 1)
        label, sql = stmts[0]
        self.assertEqual(label, "20741+20742")
        self.assertEqual(sql, "SELECT * FROM (\nSELECT x FROM t_20741 WHERE m IN ('1') ORDER BY x\n) mi_day_1"
                              "\nUNION ALL\nSELECT * FROM (\nSELECT x FROM t_20742 WHERE m IN ('1') ORDER BY x\n) "
                              "mi_day_2")
        self.assertEqual(len(query_plan.statements("q {tablename} {msisdn_tuple}", [1, 2], "()", "s", "e",
                                                   union=False)), 2)

    def test_union_rows_equal_per_table_rows(self):
        """Midnight window: one UNION statement gives the same analysis as two statements."""
        night = {"start_date": "2026-10-15T23:30", "end_date": "2026-10-16T00:30"}
        out = []
        for union in (True, False):
            with override_settings(MI_UNION_DAY_TABLES=union, MI_MAX_QUERIES_PER_REQUEST=24):
                fake = fs.FakeSmartcare(fs.data_fixture())
                resp = self.post(dict(request(("MI", "a", (A, B))), **night), fake, path=MI_PATH)
                body = json.loads(self.get(f"{MI_PATH}/jobs/{json.loads(resp.content)['job_id']}/result").content)
                out.append((len(fake.executions), subs(body)[0][2]["analysis_result"]))
        self.assertEqual((out[0][0], out[1][0]), (4, 8))
        # the fake returns its rows for each statement, so 2 statements = rows twice;
        # the analysis counts distinct MSISDNs, so the result is the same
        self.assertEqual(out[0][1], out[1][1])


SPARK_TREE = ("java.sql.SQLException: java.sql.SQLException: [2F000-1][SPARK-SPARKSQL] Failed to execute SQL statements "
              "in the database. -> org.apache.hive.service.cli.HiveSQLException: Error running query: "
              "org.apache.spark.sql.catalyst.errors.package$TreeNodeException: execute, tree:")


class DataCubeLedgerTests(MITestBase):
    """A request = 4 DataCube sessions (its connections), however many queries. MI's share here: 6."""

    def setUp(self):
        super().setUp()                      # MITestBase sets a huge budget; restore a real one
        self.ledger = override_settings(MI_DATACUBE_SESSION_BUDGET=6, MI_DATACUBE_WAIT_S=0)
        self.ledger.enable()

    def tearDown(self):
        self.ledger.disable()
        super().tearDown()

    def submit(self, *groups, fake=None):
        fake = fake or fs.FakeSmartcare(both_fixture())
        resp = self.post(request(*groups), fake, path=MI_PATH)
        job_id = json.loads(resp.content)["job_id"]
        return job_id, self.get(f"{MI_PATH}/jobs/{job_id}/result"), fake

    def test_full_request_reserves_4_sessions_for_12_queries(self):
        job_id, resp, fake = self.submit(("MI", "a", (A,)), ("Network", "b", (B,)))
        dc = json.loads(resp.content)["datacube_queries"]
        self.assertEqual((len(fake.executions), dc["executed"], dc["sessions_reserved"]), (12, 12, 4))
        # connections open lazily (real queries take minutes, so all 4 open; the
        # fake answers at once, so fewer may) - never more than 4
        self.assertLessEqual(dc["sessions_opened"], 4)
        self.assertEqual(fake.opened, dc["sessions_opened"])
        self.assertEqual(jobs.datacube_status()["in_use"], dc["sessions_opened"])
        log = (self.job_dir(job_id) / "job.log").read_text()
        self.assertIn("stage=datacube_reservation sessions=4", log)
        self.assertIn("parallelism=4", log)

    def test_second_request_waits_then_fails_without_any_query(self):
        jobs.DataCubeReservation("other").reserve(4)                 # another request holds 4 of 6
        job_id, second, fake = self.submit(("MI", "c", (C,)), ("Network", "d", (D,)))
        self.assertEqual(second.status_code, 503)
        body = json.loads(second.content)
        self.assertEqual(fake.executions, [])                       # nothing sent
        for _c, _t, s in subs(body):
            self.assertIn("DataCube session budget for MI is in use (4 of 6", s["error"])
            self.assertIn("this request needs 4 sessions", s["error"])
        self.assertEqual(jobs.datacube_status()["in_use"], 4)     # the failed job holds nothing

    def test_sessions_expire_after_the_session_lifetime(self):
        self.submit(("MI", "a", (A,)), ("Network", "b", (B,)))
        later = time.time() + 3 * 3600 + 15 * 60 + 1
        with mock.patch.object(jobs.time, "time", return_value=later):
            self.assertEqual(jobs.datacube_status()["in_use"], 0)

    def test_unused_reservations_are_given_back(self):
        def refused():
            raise RuntimeError("java.net.ConnectException: Connection refused")
        fake = fs.FakeSmartcare({})
        fake.connect = lambda *a, **k: refused()
        with override_settings(MI_SMARTCARE_MAX_ATTEMPTS=1):
            _j, resp, _f = self.submit(("MI", "a", (A,)), ("Network", "b", (B,)), fake=fake)
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(jobs.datacube_status()["in_use"], 0)      # no connection opened -> no session held

    def test_budget_smaller_than_one_request_fails_clearly(self):
        with override_settings(MI_DATACUBE_SESSION_BUDGET=3):
            _j, resp, fake = self.submit(("MI", "a", (A,)), ("Network", "b", (B,)))
        self.assertEqual(resp.status_code, 503)
        self.assertIn("needs 4 DataCube sessions but MI_DATACUBE_SESSION_BUDGET is 3",
                      subs(json.loads(resp.content))[0][2]["error"])
        self.assertEqual(fake.executions, [])

    def test_waiting_job_shows_it_and_starts_when_capacity_frees(self):
        with override_settings(MI_DATACUBE_WAIT_S=5, MI_DATACUBE_POLL_S=0.05):
            r = jobs.DataCubeReservation("other-job")
            r.reserve(4)
            steps = []
            real = jobs.update_job

            def spy(job_id, **fields):
                if fields.get("step") == "waiting_for_datacube_capacity":
                    steps.append(fields["datacube_wait"])
                    r.finish(0)                                  # the other request's sessions end
                return real(job_id, **fields)
            with mock.patch.object(jobs, "update_job", spy):
                _j, resp, fake = self.submit(("MI", "a", (A,)), ("Network", "b", (B,)))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual((steps[0]["in_use"], steps[0]["needed"]), (4, 4))
        self.assertEqual(len(fake.executions), 12)

    def test_single_request_uses_the_ledger(self):
        resp = self.post({"msisdn_list": [A], "start_date": "2026-10-15T10:00", "end_date": "2026-10-15T11:00",
                          "Complain_Type": "Voice"}, fs.FakeSmartcare(both_fixture()))
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(0 < jobs.datacube_status()["in_use"] <= 4)

    @override_settings(MI_DATACUBE_SESSIONS_PER_REQUEST=1)
    def test_reconnect_after_timeout_is_recorded_as_one_more_session(self):
        fake = fs.FakeSmartcare(both_fixture(), fail={"s1mme": (1, TIMEOUT)})
        _j, resp, _f = self.submit(("MI", "a", (A,)), fake=fake)
        dc = json.loads(resp.content)["datacube_queries"]
        self.assertEqual((dc["sessions_reserved"], dc["sessions_opened"]), (1, 2))
        self.assertEqual(jobs.datacube_status()["in_use"], 2)


@override_settings(MI_MAX_QUERIES_PER_REQUEST=15)
class RetryTests(MITestBase):
    """Job 3da4de8105e64c66: 12 planned, limit 15, three Voice queries failing with a Spark error."""

    FAILING = ("volte_mo", "volte_mt", "reg")

    def run_job(self, fake, *groups):
        groups = groups or (("MI", "data", (A, B)), ("Network", "v1", (A,)), ("Network", "v2", (C,)))
        resp = self.post(request(*groups), fake, path=MI_PATH)
        job_id = json.loads(resp.content)["job_id"]
        return job_id, json.loads(self.get(f"{MI_PATH}/jobs/{job_id}/result").content)

    def test_only_failed_queries_are_retried_and_recover(self):
        fake = fs.FakeSmartcare(both_fixture(), fail={k: (1, SPARK_TREE) for k in self.FAILING})
        job_id, body = self.run_job(fake)
        for key in self.FAILING:
            self.assertEqual(fake.count(key), 2, key)                 # first run + one retry
        for key in set(fake.executions) - set(self.FAILING):
            self.assertEqual(fake.count(key), 1, key)                 # successful ones never re-run
        dc = body["datacube_queries"]
        self.assertEqual((dc["planned"], dc["executed"], dc["retries"], dc["retry_succeeded"], dc["retry_failed"]),
                         (12, 15, 3, 3, 0))
        self.assertEqual(body["status"], "COMPLETED")
        log = (self.job_dir(job_id) / "job.log").read_text()
        self.assertEqual(log.count("event=query_retry "), 3)
        self.assertIn("after_kind=error", log)
        self.assertIn("fresh_connection=False", log)

    def test_still_failing_groups_are_partial_and_data_untouched(self):
        fake = fs.FakeSmartcare(both_fixture(), fail={k: (99, SPARK_TREE) for k in self.FAILING})
        _job, body = self.run_job(fake)
        self.assertEqual(len(fake.executions), 15)                     # never more than the limit
        dc = body["datacube_queries"]
        self.assertEqual((dc["retries"], dc["retry_failed"]), (3, 3))
        self.assertGreater(dc["retries_denied"], 0)                   # 3 more wanted, no room left
        statuses = {s["subcategory"]: s["status"] for _c, _t, s in subs(body)}
        self.assertEqual(statuses, {"data": "COMPLETED", "v1": "PARTIALLY_COMPLETED", "v2": "PARTIALLY_COMPLETED"})

    def test_no_retry_room_at_limit_12(self):
        with override_settings(MI_MAX_QUERIES_PER_REQUEST=12):
            fake = fs.FakeSmartcare(both_fixture(), fail={k: (1, SPARK_TREE) for k in self.FAILING})
            _job, body = self.run_job(fake)
        self.assertEqual(len(fake.executions), 12)
        self.assertEqual(body["datacube_queries"]["retries_denied"], 3)

    def test_sql_errors_are_not_retried(self):
        fake = fs.FakeSmartcare(both_fixture(),
                                fail={"reg": (9, "org.apache.spark.sql.AnalysisException: cannot resolve 'x'")})
        _job, body = self.run_job(fake)
        self.assertEqual(fake.count("reg"), 1)
        self.assertEqual(body["datacube_queries"]["retries"], 0)

    @override_settings(MI_DATACUBE_SESSIONS_PER_REQUEST=1)
    def test_reconnect_only_after_timeout(self):
        fake = fs.FakeSmartcare(both_fixture(), fail={"reg": (1, TIMEOUT)})
        self.run_job(fake, ("Network", "v", (A,)))
        self.assertEqual(fake.opened, 2)                               # new connection for the retry
        fake = fs.FakeSmartcare(both_fixture(), fail={"reg": (1, SPARK_TREE)})
        self.run_job(fake, ("Network", "v", (A,)))
        self.assertEqual(fake.opened, 1)                               # same connection

    @override_settings(MI_SMARTCARE_RETRY_BACKOFF_S=10, MI_SMARTCARE_RETRY_BACKOFF_MAX_S=15,
                       MI_SMARTCARE_RETRY_JITTER=0.25, MI_SMARTCARE_MAX_ATTEMPTS=3)
    def test_backoff_exponential_with_jitter_and_cap(self):
        from callflow.mi import smartcare
        sleeps = []
        fake = fs.FakeSmartcare(both_fixture(), fail={"reg": (2, SPARK_TREE)})
        with mock.patch.object(smartcare.time, "sleep", side_effect=sleeps.append), \
                mock.patch.object(smartcare.random, "uniform", side_effect=[0.2, -0.2]):
            _job, body = self.run_job(fake, ("Network", "v", (A,)))
        self.assertEqual([round(x, 2) for x in sleeps], [12.0, 12.0])   # 10*1.2 ; min(15, 20)*0.8
        self.assertEqual(fake.count("reg"), 3)
        self.assertEqual(body["status"], "COMPLETED")
