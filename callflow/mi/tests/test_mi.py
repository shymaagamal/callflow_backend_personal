"""
MI tests.   python manage.py test callflow.mi.tests

No database, Redis or Smartcare needed (SimpleTestCase + fake JDBC cursor +
process-local job backend). golden_legacy_responses.json holds what the
ORIGINAL Prototype code returned on the same fixtures.
"""
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import pandas as pd
from django.test import SimpleTestCase, override_settings
from django.urls import resolve
from rest_framework.test import APIRequestFactory

from callflow.mi import aggregation, catalog, config, hub, jobs, mi_logging, outputs, smartcare, views, worker
from callflow.mi.tests import fake_smartcare as fs

GOLDEN = json.loads((Path(__file__).parent / "golden_legacy_responses.json").read_text())

# Voice counts once CS_MOC.callerno / CS_MTC.calledno are mapped to MSISDN.
# The original code counted every CS row as the same empty MSISDN -> all 1s, and
# its HUB_Counts was empty because no CS row had a subscriber.
VOICE_CS_FIX = {
    "Suspected_Sites": {"ALX0045": 3, "CAI0123": 1},
    "Suspected_Cells": {"ALX_LALX45_1": 2, "ALX_LALX45_2": 1, "CAI_LCAI0123_1": 1},
    "Cluster_Counts": {"CL1": 1, "CL2": 3},
    "Region_Counts": {"Cairo": 1, "Delta": 3},
    "Sub_Region_Counts": {"Alex": 3, "Giza": 1},
    "HUB_Counts": {"HUB_Counts": {"HUB_A": 3, "HUB_C": 1}},
}


# Voice DRA/OCS/PCRF, as returned by smartcare_DRA_Voice / _OCS_Voice / _PCRF_Voice
# (the original code filtered on Voice_Data_Flag == 'Voice', matched nothing -> {}).
VOICE_IMS = {
    "DRA_NAME": {"DRA_NAME_1": 1, "DRA_NAME_2": 1, "DRA_NAME_3": 1},
    "OCS_NAME": {"OCS_NAME_1": 1, "OCS_NAME_2": 1, "OCS_NAME_3": 1},
    "PCRF": {"PCRF_NAME_1": 1, "PCRF_NAME_2": 1},            # smartcare_DRA_Voice.PCRF_NAME
    # smartcare_nodes_REG_Voice is no longer a Voice query -> no p_cscf_Name / AS_NAME
    "CSCF_Name": {},
    "AS_Name": {},
}


def voice_expected(name, cs_mapping=True):
    expected = dict(GOLDEN[name]["body"], **VOICE_IMS)
    if cs_mapping:
        expected.update(VOICE_CS_FIX)
    return expected


def body(complain_type="Data", **extra):
    b = {"msisdn_list": fs.MSISDNS, "start_date": "2026-10-15T10:00", "end_date": "2026-10-15T11:00",
         "Complain_Type": complain_type, "imsi": ""}
    b.update(extra)
    return b


@override_settings(ROOT_URLCONF="callflow.mi.tests.urls")
class MITestBase(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mi_test_")
        self.static_root = os.path.join(self.tmp, "static")
        os.makedirs(os.path.join(self.static_root, "CallFlow", "output"))
        self.legacy_dir = os.path.join(self.tmp, "legacy")
        os.makedirs(self.legacy_dir)
        self.master = os.path.join(self.tmp, "master.xlsx")
        pd.DataFrame({"SITE_ID": ["CAI0123", "ALX0045", "Ke a0007"],
                      "Hub": ["HUB_C", "HUB_A", "HUB_K"]}).to_excel(self.master, index=False)
        self.override = override_settings(
            OUTPUT_DIR=Path(self.tmp) / "callflow_output",
            STATIC_ROOT=self.static_root,
            MASTER_SHEET_PATH=self.master,
            CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                "LOCATION": f"mi-tests-{id(self)}"}},
            MI_OUTPUT_DIR=None,
            MI_LEGACY_EXPORT_DIR=self.legacy_dir,
            MI_EXECUTOR="inline",
            MI_CONCURRENCY_BACKEND="local",
            MI_SMARTCARE_RETRY_BACKOFF_S=0,
            STCC_SMARTCARE_DB_USER="test", STCC_SMARTCARE_DB_PASSWORD="test",
        )
        self.override.enable()
        jobs.set_backend_for_tests(None)
        hub.clear_cache()
        mi_logging.reset_app_logging_for_tests()

    def tearDown(self):
        self.override.disable()
        jobs.set_backend_for_tests(None)
        hub.clear_cache()
        mi_logging.reset_app_logging_for_tests()
        shutil.rmtree(self.tmp, ignore_errors=True)

    @property
    def mi_root(self):
        return config.output_dir()

    def post(self, data, fake, path="/callflow/prototype/api/smartcare", query=""):
        view = resolve(path).func
        with mock.patch.object(smartcare, "connect", fake.connect):
            req = APIRequestFactory().post(path + query, data, format="json")
            resp = view(req)
            resp.render()
        return resp

    def get(self, path):
        match = resolve(path)
        resp = match.func(APIRequestFactory().get(path), **match.kwargs)
        resp.render()
        return resp

    def job_dir(self, job_id):
        return next(self.mi_root.glob(f"jobs/*/{job_id}"))

    def job_record(self, job_id):
        return jobs.get_job(job_id)


# --------------------------------------------------------------------------
# 1. Existing API contract (old path, synchronous body)
# --------------------------------------------------------------------------
class LegacyContractTests(MITestBase):

    def assert_golden(self, name, fixture, complain_type, fail=None):
        resp = self.post(body(complain_type), fs.FakeSmartcare(fixture, fail=fail))
        self.assertEqual(resp.status_code, GOLDEN[name]["status"])
        self.assertEqual(json.loads(resp.content), GOLDEN[name]["body"])
        return resp

    def test_data_identical_to_original(self):
        self.assert_golden("data_full", fs.data_fixture(), "Data")

    def test_data_without_scf_rows_identical(self):
        self.assert_golden("data_no_scf", fs.data_fixture(scf_rows=False), "Data")

    def test_data_without_scf_and_s1mme_rows_identical(self):
        self.assert_golden("data_none", fs.data_fixture(False, False), "Data")

    @override_settings(MI_MAP_CS_SUBSCRIBER_TO_MSISDN=False)
    def test_voice_matches_original_except_dra_ocs_pcrf_without_cs_mapping(self):
        cases = [("voice", fs.voice_fixture(), None),
                 ("voice_with_scf", fs.voice_fixture(with_scf=True), None),
                 ("voice_tables_missing", fs.voice_fixture(),
                  {"sgs": (99, fs.TABLE_NOT_FOUND), "hss": (99, fs.TABLE_NOT_FOUND)})]
        for name, fixture, fail in cases:
            resp = self.post(body("Voice"), fs.FakeSmartcare(fixture, fail=fail))
            self.assertEqual(json.loads(resp.content), voice_expected(name, cs_mapping=False), name)
            for key in ("DRA_NAME", "OCS_NAME", "PCRF"):
                self.assertEqual(GOLDEN[name]["body"][key], {})          # original: always empty

    def test_java_string_values_from_jdbc(self):
        # an unattached JPype thread returns java.lang.String / java.lang.Long objects;
        # before, the hub's isin() matched nothing -> {"subs_per_hub": []} (server, 23 Oct)
        for complain_type, expected in (("Voice", voice_expected("voice")), ("Data", GOLDEN["data_full"]["body"])):
            fixture = fs.voice_fixture() if complain_type == "Voice" else fs.data_fixture()
            resp = self.post(body(complain_type), fs.FakeSmartcare(fixture, java_values=True))
            self.assertEqual(resp.status_code, 200, complain_type)
            got = json.loads(resp.content)
            self.assertEqual(got, json.loads(json.dumps(expected)), complain_type)
            job = self.job_record(resp["X-MI-Job-Id"])          # stored (pickled) without error
            self.assertEqual(job["status"], "DONE")

    def test_to_python(self):
        self.assertEqual(smartcare.to_python(fs.JavaString("L21_LU0742_01")), "L21_LU0742_01")
        self.assertIs(type(smartcare.to_python(fs.JavaString("x"))), str)
        self.assertEqual(smartcare.to_python(fs.JavaLong("2101")), 2101)
        self.assertEqual(smartcare.to_python_deep({fs.JavaString("k"): [fs.JavaLong("1")]}), {"k": [1]})
        for v in (None, "a", 1, 1.5, True):
            self.assertEqual(smartcare.to_python(v), v)

    def test_voice_counts_real_subscribers_with_cs_mapping(self):
        # original: every count 1 and HUB_Counts {} (the symptom reported on 23 Oct)
        self.assertEqual(GOLDEN["voice"]["body"]["Cluster_Counts"], {"CL1": 1, "CL2": 1})
        self.assertEqual(GOLDEN["voice"]["body"]["HUB_Counts"], {"HUB_Counts": {}})
        resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        self.assertEqual(json.loads(resp.content), voice_expected("voice"))
        text = (self.job_dir(resp["X-MI-Job-Id"]) / "job.log").read_text()
        self.assertIn("event=subscriber_column_mapped query=smartcare_network_CS_MOC column=callerno", text)
        self.assertIn("event=subscriber_column_mapped query=smartcare_network_CS_MTC column=calledno", text)
        resp = self.post(body("Voice"), fs.FakeSmartcare(
            fs.voice_fixture(), fail={"sgs": (99, fs.TABLE_NOT_FOUND), "hss": (99, fs.TABLE_NOT_FOUND)}))
        self.assertEqual(json.loads(resp.content), voice_expected("voice_tables_missing"))

    def test_error_shapes_on_old_path(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        cases = [
            ({"msisdn_list": fs.MSISDNS}, 400, "Missing start_date or end_date"),
            (body(msisdn_list=[]), 400, "msisdn_list is required"),
            (body(msisdn_list=["99"]), 500, "Invalid MSISDN: 99"),
            (body(start_date="2026/10/15 10:00"), 500, "does not match format"),
            (body(end_date="2026-10-15T23:00"), 500, "Time_Stamp should not exceed 30 min"),
            (body(end_date="2026-10-15T09:00"), 500, "end_date must be after start_date"),
        ]
        for data, code, text in cases:
            resp = self.post(data, fake)
            self.assertEqual(resp.status_code, code, data)
            self.assertIn(text, json.loads(resp.content)["error"])
        self.assertEqual(fake.executions, [])

    def test_complain_type_must_be_data_or_voice(self):
        for path in ("/callflow/prototype/api/smartcare", "/callflow/mi/api/smartcare"):
            for value in ("Both", None):
                fake = fs.FakeSmartcare(fs.data_fixture())
                resp = self.post(body(value), fake, path=path)
                self.assertEqual((resp.status_code, json.loads(resp.content)),
                                 (400, {"error": "Complain_Type must be Data or Voice"}), (path, value))
                self.assertEqual(fake.executions, [])

    def test_validation_errors_are_400_on_mi_path(self):
        resp = self.post(body(msisdn_list=["99"]), fs.FakeSmartcare({}), path="/callflow/mi/api/smartcare")
        self.assertEqual(resp.status_code, 400)

    def test_connection_failure_shape(self):
        def broken():
            raise RuntimeError(fs.SOCKET_TIMEOUT)
        with mock.patch.object(smartcare, "connect", broken):
            req = APIRequestFactory().post("/x", body(), format="json")
            resp = views.LegacySmartcareView.as_view()(req)
            resp.render()
        self.assertEqual((resp.status_code, json.loads(resp.content)), (500, {"error": "Database connection failed"}))
        self.assertIn("smartcare_unreachable", resp["X-MI-Warnings"])

    def test_missing_credentials_reported(self):
        with override_settings(STCC_SMARTCARE_DB_USER=None, STCC_SMARTCARE_DB_PASSWORD=None):
            req = APIRequestFactory().post("/x", body(), format="json")
            resp = views.LegacySmartcareView.as_view()(req)
            resp.render()
        self.assertEqual(json.loads(resp.content), {"error": "Database connection failed"})
        self.assertIn("smartcare_not_configured", resp["X-MI-Warnings"])

    def test_urls(self):
        self.assertIs(resolve("/callflow/prototype/api/smartcare").func.view_class, views.LegacySmartcareView)
        self.assertIs(resolve("/callflow/mi/api/smartcare").func.view_class, views.MISmartcareView)
        self.assertIs(resolve("/callflow/mi/api/smartcare/jobs/abc").func.view_class, views.JobStatusView)
        self.assertIs(resolve("/callflow/prototype/api/smartcare/jobs/abc/result").func.view_class,
                      views.JobResultView)


# --------------------------------------------------------------------------
# 2. The production errors (attached logs.txt) and their fixes
# --------------------------------------------------------------------------
class RootCauseTests(MITestBase):

    def test_vpcrf_all_null_no_longer_breaks_data_response(self):
        # original: KeyError 'VPCRF_NAME' -> rat/region/subregion/tac/sgsn/mme/sites/sgws all empty
        self.assertEqual(GOLDEN["data_pcrf_null"]["legacy_keyerrors"], ["VPCRF_NAME"])
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture(pcrf_null=True)))
        expected = dict(GOLDEN["data_full"]["body"], PCRF={})
        self.assertEqual(json.loads(resp.content), expected)
        cols = pd.read_csv(self.job_dir(resp["X-MI-Job-Id"]) / "sc_nodes.csv", nrows=0).columns
        self.assertFalse([c for c in cols if c.endswith(("_x", "_y"))])

    def test_scf_timeout_gives_partial_result_not_crash(self):
        self.assertEqual(GOLDEN["data_scf_timeout"]["legacy_keyerrors"], ["Voice_Data_Flag"])
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"scf": (999, fs.SOCKET_TIMEOUT)})
        resp = self.post(body(), fake)
        self.assertEqual(json.loads(resp.content), GOLDEN["data_no_scf"]["body"])
        self.assertEqual(resp["X-MI-Partial"], "true")
        self.assertIn("query_failed", resp["X-MI-Warnings"])
        # 3 attempts, no fallback re-runs (original: 5 + 4x5 = 25 SCF executions)
        self.assertEqual(fake.count("scf"), 3)
        job = self.job_record(resp["X-MI-Job-Id"])
        failed = [w for w in job["warnings"] if w["code"] == "query_failed"][0]
        self.assertEqual((failed["query"], failed["kind"]), ("SCF_CCI_U_T_Data", "timeout"))

    def test_table_not_found_is_not_retried(self):
        fake = fs.FakeSmartcare(fs.voice_fixture(), fail={"sgs": (99, fs.TABLE_NOT_FOUND)})
        resp = self.post(body("Voice"), fake)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(fake.count("sgs"), 1)             # original: 5
        w = [w for w in self.job_record(resp["X-MI-Job-Id"])["warnings"] if w["code"] == "query_unavailable"]
        self.assertEqual(w[0]["query"], "smartcare_SGS_Voice")
        self.assertIn("Table or view not found: ps.DETAIL_CDR_SGS_20741", w[0]["error"])

    def test_voice_dra_ocs_pcrf_come_from_voice_queries_as_is(self):
        resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        got = json.loads(resp.content)
        for key, value in VOICE_IMS.items():
            self.assertEqual(got[key], value, key)          # every row the query returned, no filter
        unavailable = {w["metric"] for w in self.job_record(resp["X-MI-Job-Id"])["warnings"]
                       if w["code"] == "metric_unavailable"}
        # only the two metrics that need smartcare_nodes_REG_Voice (no longer a Voice query)
        self.assertEqual(unavailable, {"CSCF_Name", "AS_Name"})

    def test_voice_dra_query_failure_only_empties_its_metrics(self):
        resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture(),
                                                          fail={"dra_voice": (99, fs.TABLE_NOT_FOUND)}))
        got = json.loads(resp.content)
        self.assertEqual((got["DRA_NAME"], got["OCS_NAME"], got["PCRF"]), ({}, {}, {}))   # all from that query
        self.assertEqual(got["Suspected_Cells"], VOICE_CS_FIX["Suspected_Cells"])         # others unaffected
        warnings = self.job_record(resp["X-MI-Job-Id"])["warnings"]
        self.assertIn("smartcare_DRA_Voice", [w.get("query") for w in warnings if w["code"] == "query_unavailable"])

    def test_window_crossing_midnight_reads_both_day_tables(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        self.post(body(start_date="2026-10-15T23:00", end_date="2026-10-16T01:00"), fake)
        self.assertEqual(fake.count("s1mme"), 2)
        with override_settings(MI_FIX_MIDNIGHT_TABLES=False):
            fake = fs.FakeSmartcare(fs.data_fixture())
            self.post(body(start_date="2026-10-15T23:00", end_date="2026-10-16T01:00"), fake)
            self.assertEqual(fake.count("s1mme"), 1)

    def test_each_complain_type_runs_only_its_own_queries(self):
        fake = fs.FakeSmartcare(fs.data_fixture(False, False))
        resp = self.post(body(), fake)
        self.assertEqual(json.loads(resp.content), GOLDEN["data_none"]["body"])
        self.assertEqual(sorted(fake.executions), ["gbiups", "s1mme", "scf", "sgid_data"])   # original: 10
        self.assertEqual(GOLDEN["data_none"]["legacy_executions"], 10)
        fake = fs.FakeSmartcare(fs.voice_fixture(with_scf=True))
        resp = self.post(body("Voice"), fake)
        self.assertEqual(len(fake.executions), 7)           # the 7 Voice queries, no SCF/S1MME
        self.assertEqual(fake.count("scf") + fake.count("s1mme"), 0)
        self.assertNotIn("fallback", (self.job_dir(resp["X-MI-Job-Id"]) / "job.log").read_text())

    def test_aggregation_runs_once(self):
        with mock.patch.object(aggregation, "aggregate_data", wraps=aggregation.aggregate_data) as spy:
            self.post(body(), fs.FakeSmartcare(fs.data_fixture(pcrf_null=True)))
        self.assertEqual(spy.call_count, 1)

    def test_one_missing_column_only_empties_its_metric(self):
        fx = fs.data_fixture()
        for key in ("s1mme", "gbiups"):
            cols, rows = fx[key]
            i = cols.index("REGION")
            fx[key] = (cols[:i] + cols[i + 1:], [r[:i] + r[i + 1:] for r in rows])
        resp = self.post(body(), fs.FakeSmartcare(fx))
        expected = dict(GOLDEN["data_full"]["body"], region_counts={})
        self.assertEqual(json.loads(resp.content), expected)
        self.assertIn("metric_unavailable", resp["X-MI-Warnings"])

    @override_settings(MI_SKIP_INEFFECTIVE_QUERIES=True)
    def test_skip_ineffective_queries_keeps_responses(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        self.assertEqual(json.loads(self.post(body(), fake).content), GOLDEN["data_full"]["body"])
        self.assertEqual(fake.count("reg"), 0)

    def test_hub_explains_missing_site_match(self):
        pd.DataFrame({"SITE_ID": ["X0001"], "Hub": ["H"]}).to_excel(self.master, index=False)
        resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        self.assertEqual(json.loads(resp.content)["HUB_Counts"], {"subs_per_hub": []})
        w = [w for w in self.job_record(resp["X-MI-Job-Id"])["warnings"] if w["code"] == "hub_no_site_match"]
        self.assertEqual(w[0]["sample_sites"], "ALX0045,CAI0123")
        self.assertIn("event=hub_sites cell_column=CELL_NAME",
                      (self.job_dir(resp["X-MI-Job-Id"]) / "job.log").read_text())


# --------------------------------------------------------------------------
# 3. Smartcare session, retries
# --------------------------------------------------------------------------
class SmartcareTests(MITestBase):

    def test_sessions_bounded_by_parallelism_and_all_closed(self):
        fake = fs.FakeSmartcare(fs.voice_fixture())
        self.post(body("Voice"), fake)
        self.assertLessEqual(fake.opened, config.query_parallelism())
        self.assertEqual(fake.opened, fake.closed)
        with override_settings(MI_QUERY_PARALLELISM=1):
            fake = fs.FakeSmartcare(fs.voice_fixture())
            self.post(body("Voice"), fake)
            self.assertEqual((fake.opened, fake.closed), (1, 1))

    @override_settings(MI_QUERY_PARALLELISM=1)
    def test_reconnect_after_timeout_then_recover(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"scf": (2, fs.SOCKET_TIMEOUT)})
        resp = self.post(body(), fake)
        self.assertEqual(json.loads(resp.content), GOLDEN["data_full"]["body"])
        self.assertEqual(fake.count("scf"), 3)
        self.assertEqual(fake.opened, 3)
        self.assertEqual(fake.opened, fake.closed)
        self.assertEqual(resp["X-MI-Partial"], "false")

    def test_classification(self):
        self.assertEqual(smartcare.classify_error(RuntimeError(fs.TABLE_NOT_FOUND)), "unavailable")
        self.assertEqual(smartcare.classify_error(RuntimeError(fs.SOCKET_TIMEOUT)), "timeout")
        self.assertEqual(smartcare.classify_error(RuntimeError("Connection reset by peer")), "connection")
        self.assertEqual(smartcare.classify_error(RuntimeError("AnalysisException: cannot resolve 'x'")),
                         "query_error")
        self.assertEqual(smartcare.classify_error(RuntimeError("boom")), "error")

    @override_settings(MI_SMARTCARE_MAX_ATTEMPTS=1)
    def test_max_attempts(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"s1mme": (99, fs.SOCKET_TIMEOUT)})
        self.post(body(), fake)
        self.assertEqual(fake.count("s1mme"), 1)           # no fallback re-runs

    @override_settings(MI_SMARTCARE_RETRY_BUDGET_S=0)
    def test_retry_budget(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"s1mme": (99, fs.SOCKET_TIMEOUT)})
        resp = self.post(body(), fake)
        self.assertEqual(fake.count("s1mme"), 1)           # no retries once the budget is spent
        self.assertIn("event=retry_budget_exhausted",
                      (self.job_dir(resp["X-MI-Job-Id"]) / "job.log").read_text())
        self.assertEqual(resp.status_code, 200)


# --------------------------------------------------------------------------
# 3b. Parallel query execution
# --------------------------------------------------------------------------
class ParallelTests(MITestBase):

    SCENARIOS = (
        ("data_full", lambda: fs.data_fixture(), "Data", None),
        ("data_pcrf_null", lambda: fs.data_fixture(pcrf_null=True), "Data", None),
        ("data_no_scf", lambda: fs.data_fixture(scf_rows=False), "Data", None),
        ("data_none", lambda: fs.data_fixture(False, False), "Data", None),
        ("data_scf_timeout", lambda: fs.data_fixture(), "Data", {"scf": (999, fs.SOCKET_TIMEOUT)}),
        ("voice", lambda: fs.voice_fixture(), "Voice", None),
        ("voice_with_scf", lambda: fs.voice_fixture(with_scf=True), "Voice", None),
        ("voice_tables_missing", lambda: fs.voice_fixture(), "Voice",
         {"sgs": (99, fs.TABLE_NOT_FOUND), "hss": (99, fs.TABLE_NOT_FOUND)}),
    )

    def _run(self, parallelism, fixture, complain_type, fail):
        with override_settings(MI_QUERY_PARALLELISM=parallelism):
            fake = fs.FakeSmartcare(fixture, fail=dict(fail or {}))
            resp = self.post(body(complain_type), fake)
        csv = (self.job_dir(resp["X-MI-Job-Id"]) / "sc_nodes.csv").read_text()
        return resp.status_code, json.loads(resp.content), csv, sorted(fake.executions)

    def test_parallel_results_identical_to_sequential(self):
        for name, fixture, ct, fail in self.SCENARIOS:
            seq = self._run(1, fixture(), ct, fail)
            par = self._run(5, fixture(), ct, fail)
            self.assertEqual(seq[:2], par[:2], name)      # status + body
            self.assertEqual(seq[2], par[2], name)        # sc_nodes.csv: same rows, same order
            self.assertEqual(seq[3], par[3], name)        # same Smartcare executions

    def test_queries_overlap_but_never_exceed_the_limit(self):
        active, peak = [0], [0]
        lock = threading.Lock()
        real = fs.FakeCursor.execute

        def slow(cursor, sql):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.15)
            try:
                return real(cursor, sql)
            finally:
                with lock:
                    active[0] -= 1

        with mock.patch.object(fs.FakeCursor, "execute", slow):
            with override_settings(MI_QUERY_PARALLELISM=3):
                t0 = time.monotonic()
                resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
                elapsed = time.monotonic() - t0
        self.assertEqual(json.loads(resp.content), voice_expected("voice"))
        self.assertEqual(peak[0], 3)
        self.assertLess(elapsed, 7 * 0.15 * 0.75)          # 7 executions; sequential would be ~1.05 s

    def test_empty_results_run_only_the_main_queries(self):
        empty = {k: (cols, []) for k, (cols, _) in fs.voice_fixture().items()}
        fake = fs.FakeSmartcare(empty)
        resp = self.post(body("Voice"), fake)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(fake.executions), 7)           # the production log showed 14 (with fallbacks)
        self.assertEqual(fake.count("scf") + fake.count("s1mme"), 0)


# --------------------------------------------------------------------------
# 4. Logs and outputs under CALLFLOW_OUTPUT/MI
# --------------------------------------------------------------------------
class OutputAndLoggingTests(MITestBase):

    def test_everything_under_mi_and_nothing_in_static(self):
        resp = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        d = self.job_dir(resp["X-MI-Job-Id"])
        self.assertEqual(d.parent.parent.parent, self.mi_root)
        for name in ("job.log", "request.json", "result.json", "summary.json", "sc_nodes.csv", "sc_nodes_raw.csv",
                     "sc_nodes_before_voice.csv", "sc_data.csv", "matched_master.csv"):
            self.assertTrue((d / name).exists(), name)
        for name in ("sc_nodes.csv", "sc_nodes_before_voice.csv", "sc_data.csv"):
            self.assertEqual((Path(self.legacy_dir) / name).read_bytes(), (d / name).read_bytes())
            self.assertEqual((self.mi_root / "latest" / name).read_bytes(), (d / name).read_bytes())
        self.assertFalse(os.path.exists(os.path.join(self.static_root, "CallFlow", "output", "logs.txt")))
        self.assertEqual(self.mi_root, Path(self.tmp) / "callflow_output" / "MI")

    def test_concurrent_jobs_do_not_share_files(self):
        r1 = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        r2 = self.post(body("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        self.assertNotEqual(self.job_dir(r1["X-MI-Job-Id"]), self.job_dir(r2["X-MI-Job-Id"]))

    def test_job_log_has_every_stage_query_and_attempt(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"scf": (1, fs.SOCKET_TIMEOUT)})
        resp = self.post(body(), fake)
        jid = resp["X-MI-Job-Id"]
        text = (self.job_dir(jid) / "job.log").read_text()
        for stage in ("wait_for_slot", "input_validation", "database_connection", "query_execution",
                      "data_validation", "aggregation", "hub_metrics", "result_generation"):
            self.assertIn(f"event=stage_done stage={stage}", text)
        self.assertIn("name=SCF_CCI_U_T_Data table=20741 attempt=1 ok=False kind=timeout", text)
        self.assertIn("name=SCF_CCI_U_T_Data table=20741 attempt=2 ok=True", text)
        self.assertIn("event=job_done status=DONE", text)
        self.assertTrue(all(f"job_id={jid}" in line for line in text.splitlines() if "event=" in line))
        app = (self.mi_root / "logs" / outputs.today_str() / "mi.log").read_text()
        self.assertIn(f"job_id={jid} event=job_done", app)
        summary = json.loads((self.mi_root / "logs" / outputs.today_str() / "timings.jsonl")
                             .read_text().splitlines()[-1])
        self.assertEqual(summary["job_id"], jid)
        self.assertIn("aggregation", summary["stages"])

    def test_exception_traceback_in_job_log(self):
        with mock.patch.object(aggregation, "aggregate_data", side_effect=ZeroDivisionError("boom")):
            resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual((resp.status_code, json.loads(resp.content)), (500, {"error": "boom"}))
        text = (self.job_dir(resp["X-MI-Job-Id"]) / "job.log").read_text()
        self.assertIn("Traceback (most recent call last)", text)
        self.assertIn("ZeroDivisionError: boom", text)

    def test_handlers_released(self):
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertNotIn(f"callflow.mi.jobs.{resp['X-MI-Job-Id']}", logging.Logger.manager.loggerDict)

    def test_configurable_absolute_output_dir(self):
        target = os.path.join(self.tmp, "elsewhere", "MI")
        with override_settings(MI_OUTPUT_DIR=target):
            resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
            self.assertTrue(list(Path(target).glob(f"jobs/*/{resp['X-MI-Job-Id']}/job.log")))

    def test_retention_sweep(self):
        root = self.mi_root
        old = (date.today() - timedelta(days=30)).strftime(outputs.DATE_FMT)
        new = outputs.today_str()
        for sub in ("logs", "jobs"):
            (root / sub / old).mkdir(parents=True)
            (root / sub / new).mkdir(parents=True, exist_ok=True)
        (root / "jobs" / "not-a-date").mkdir(parents=True)
        removed = outputs.sweep_retention()
        self.assertEqual(len(removed), 2)
        self.assertTrue((root / "logs" / new).exists() and (root / "jobs" / "not-a-date").exists())

    def test_daily_handler_switches_file_at_midnight(self):
        base = Path(self.tmp) / "dh"
        h = mi_logging.DailyFileHandler(lambda: base, "x.log")
        h.setFormatter(logging.Formatter("%(message)s"))
        lg = logging.getLogger("mi.test.daily")
        lg.addHandler(h)
        lg.propagate = False

        class Fake:
            value = "2026-10-15"

            @classmethod
            def now(cls):
                class D:
                    def strftime(self, fmt):
                        return Fake.value
                return D()
        with mock.patch.object(mi_logging, "datetime", Fake):
            lg.warning("one")
            Fake.value = "2026-10-16"
            lg.warning("two")
        lg.removeHandler(h)
        h.close()
        self.assertEqual((base / "2026-10-15" / "x.log").read_text(), "one\n")
        self.assertEqual((base / "2026-10-16" / "x.log").read_text(), "two\n")

    @override_settings(MI_LEGACY_STATIC_LOG=True)
    def test_optional_legacy_static_log(self):
        self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertIn("MI Smartcare Request",
                      Path(self.static_root, "CallFlow", "output", "logs.txt").read_text())


# --------------------------------------------------------------------------
# 5. Jobs, worker, dedup, limits
# --------------------------------------------------------------------------
class JobTests(MITestBase):

    def test_mi_path_returns_job_id_and_result_endpoint_matches_sync_body(self):
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()), path="/callflow/mi/api/smartcare")
        self.assertEqual(resp.status_code, 202)
        data = json.loads(resp.content)
        self.assertEqual(data["status_url"], f"/callflow/mi/api/smartcare/jobs/{data['job_id']}")
        result = self.get(data["result_url"])
        self.assertEqual((result.status_code, json.loads(result.content)), (200, GOLDEN["data_full"]["body"]))
        status = json.loads(self.get(data["status_url"]).content)
        self.assertEqual((status["status"], status["http_status"], status["partial"]), ("DONE", 200, False))
        self.assertIn("timings", status)

    def test_old_path_async_opt_in(self):
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()), query="?async=1")
        self.assertEqual(resp.status_code, 202)

    def test_duplicate_submission_returns_existing_job(self):
        fp = jobs.fingerprint(body())
        jobs.create_job("runningjob000001", "x", body(), "mi", outputs.today_str())
        self.assertIsNone(jobs.claim_inflight(fp, "runningjob000001"))
        fake = fs.FakeSmartcare(fs.data_fixture())
        resp = self.post(body(), fake, path="/callflow/mi/api/smartcare")
        data = json.loads(resp.content)
        self.assertEqual((data["job_id"], data.get("deduplicated")), ("runningjob000001", True))
        self.assertEqual(fake.executions, [])

    @override_settings(MI_MAX_QUEUED_JOBS=1)
    def test_queue_is_bounded(self):
        jobs.pending_add("other")
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()), path="/callflow/mi/api/smartcare")
        self.assertEqual(resp.status_code, 429)

    @override_settings(MI_SLOT_WAIT_S=0.2)
    def test_busy_slot(self):
        with jobs.Slot(1):
            resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual(resp.status_code, 429)

    @override_settings(MI_JOB_STALE_S=1)
    def test_stale_running_job_reported_lost(self):
        jobs.create_job("stalejob00000001", "x", body(), "mi", outputs.today_str())
        jobs.update_job("stalejob00000001", status="RUNNING", heartbeat_at=time.time() - 5)
        job = jobs.get_job("stalejob00000001")
        self.assertEqual((job["status"], job["step"]), ("FAILED", "worker_lost"))

    @override_settings(MI_EXECUTOR="worker")
    def test_worker_process_runs_queued_job(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        stop = threading.Event()
        with mock.patch.object(smartcare, "connect", fake.connect), \
                mock.patch.object(worker, "_setup_django", lambda: None):
            t = threading.Thread(target=worker.main, kwargs={"stop_event": stop, "install_signals": False,
                                                             "allow_local": True}, daemon=True)
            t.start()
            for _ in range(50):
                if jobs.worker_alive():
                    break
                time.sleep(0.05)
            self.assertTrue(jobs.worker_alive())
            req = APIRequestFactory().post("/callflow/mi/api/smartcare", body(), format="json")
            resp = views.MISmartcareView.as_view()(req)
            resp.render()
            job_id = json.loads(resp.content)["job_id"]
            job = views.wait_for(job_id, 10)
            stop.set()
            t.join(10)
        self.assertEqual((job["status"], job["started_by"]), ("DONE", "worker"))
        self.assertEqual(job["result"], GOLDEN["data_full"]["body"])

    @override_settings(MI_EXECUTOR="worker", MI_WORKER_FALLBACK="local")
    def test_no_worker_falls_back_locally(self):
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual(json.loads(resp.content), GOLDEN["data_full"]["body"])
        self.assertEqual(self.job_record(resp["X-MI-Job-Id"])["started_by"], "inline")

    @override_settings(MI_EXECUTOR="worker", MI_WORKER_FALLBACK="none", MI_SYNC_WAIT_TIMEOUT_S=0.3)
    def test_no_worker_and_no_fallback_returns_202_after_wait(self):
        resp = self.post(body(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(json.loads(resp.content)["status"], "PENDING")
        self.assertEqual(jobs.backend().queue_length(), 1)

    def test_validation_happens_before_a_job_exists(self):
        resp = self.post(body(msisdn_list=["1"]), fs.FakeSmartcare({}), path="/callflow/mi/api/smartcare")
        self.assertEqual(resp.status_code, 400)
        self.assertFalse((self.mi_root / "jobs").exists())

    def test_catalog_unchanged(self):
        self.assertEqual(list(catalog.query_set("Data")),
                         ["smartcare_SGIDiameter_nodes", "smartcare_S1mme_nodes", "smartcare_GBIUPS_nodes",
                          "SCF_CCI_U_T_Data"])
        self.assertEqual(list(catalog.query_set("Voice")),
                         ["smartcare_DRA_Voice", "smartcare_SGS_Voice", "smartcare_HSS_Voice",
                          "smartcare_network_CS_MOC", "smartcare_network_CS_MTC",
                          "smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL",
                          "smartcare_network_VoLTE_CFR_CSR_MT_CALL_voice"])
