"""
Focused tests for the Prototype Smartcare refactor.

    python manage.py test callflow.prototype.tests

No database, no Redis, no Smartcare: a fake JDBC cursor (fake_smartcare.py)
answers each query by table name. `golden_responses.json` holds the response
bodies produced by the ORIGINAL (pre-refactor) code on the same fixtures, so
the response-compatibility tests compare against real previous output.
"""
import json
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import pandas as pd
from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory

from callflow.prototype import concurrency
from callflow.prototype import apis, runtime, views
from callflow.prototype.tests import fake_smartcare as fs

GOLDEN = json.loads((Path(__file__).parent / "golden_responses.json").read_text())


def payload(complain_type="Data", **extra):
    body = {"msisdn_list": fs.MSISDNS, "start_date": "2026-10-15T10:00",
            "end_date": "2026-10-15T11:00", "Complain_Type": complain_type, "imsi": ""}
    body.update(extra)
    return body


class PrototypeTestBase(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="prototype_test_")
        static_root = os.path.join(self.tmp, "static")
        os.makedirs(os.path.join(static_root, "CallFlow", "output"))
        self.legacy_dir = os.path.join(self.tmp, "legacy")
        os.makedirs(self.legacy_dir)
        self.out_dir = Path(self.tmp) / "out"
        (self.out_dir / "logs").mkdir(parents=True)
        self.master = os.path.join(self.tmp, "master.xlsx")
        pd.DataFrame({"SITE_ID": ["CAI0123", "ALX0045", "Ke a0007"],
                      "Hub": ["HUB_C", "HUB_A", "HUB_K"]}).to_excel(self.master, index=False)
        self.settings_override = override_settings(
            STATIC_ROOT=static_root,
            OUTPUT_DIR=self.out_dir,
            MASTER_SHEET_PATH=self.master,
            CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                "LOCATION": f"prototype-tests-{id(self)}"}},
            PROTOTYPE_LEGACY_OUTPUT_DIR=self.legacy_dir,
            PROTOTYPE_CONCURRENCY_BACKEND="local",
            PROTOTYPE_SMARTCARE_RETRY_BACKOFF_S=0,
            PROTOTYPE_EXECUTOR="inline",
        )
        self.settings_override.enable()
        concurrency.reset_backend_for_tests()
        runtime.clear_master_cache()

    def tearDown(self):
        self.settings_override.disable()
        concurrency.reset_backend_for_tests()
        runtime.clear_master_cache()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def post(self, body, fake, query=""):
        with mock.patch.object(apis, "smartcareConnection", fake.connect):
            req = APIRequestFactory().post("/callflow/prototype/api/smartcare" + query, body, format="json")
            resp = views.PrototypeSmartcareIntegrationAPIView.as_view()(req)
            resp.render()
        return resp

    def get_job(self, job_id):
        req = APIRequestFactory().get(f"/callflow/prototype/api/smartcare/jobs/{job_id}")
        resp = views.PrototypeSmartcareJobStatusAPIView.as_view()(req, job_id=job_id)
        resp.render()
        return resp


# --------------------------------------------------------------------------
# 1. Response compatibility (vs. output of the original code)
# --------------------------------------------------------------------------
class ResponseCompatibilityTests(PrototypeTestBase):

    def test_data_response_identical_to_original(self):
        resp = self.post(payload("Data"), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["data_full"]))

    def test_data_response_identical_when_scf_and_s1mme_empty(self):
        resp = self.post(payload("Data"), fs.FakeSmartcare(fs.data_fixture(False, False)))
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["data_none"]))

    def test_voice_response_identical_to_original(self):
        resp = self.post(payload("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["voice"]))

    def test_error_shapes_preserved(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        resp = self.post({"msisdn_list": fs.MSISDNS}, fake)
        self.assertEqual((resp.status_code, json.loads(resp.content)),
                         (400, {"error": "Missing start_date or end_date"}))
        resp = self.post(payload(msisdn_list=[]), fake)
        self.assertEqual((resp.status_code, json.loads(resp.content)),
                         (400, {"error": "msisdn_list is required"}))
        # invalid MSISDN: ValueError from validation -> 500 {'error': ...} as before
        resp = self.post(payload(msisdn_list=["99"]), fake)
        self.assertEqual(resp.status_code, 500)
        self.assertIn("Invalid MSISDN", json.loads(resp.content)["error"])
        self.assertEqual(fake.executions, [], "validation must fail before any Smartcare query")

    def test_connection_failure_shape_preserved(self):
        with mock.patch.object(apis, "smartcareConnection", lambda logs_file: None):
            req = APIRequestFactory().post("/x", payload(), format="json")
            resp = views.PrototypeSmartcareIntegrationAPIView.as_view()(req)
            resp.render()
        self.assertEqual((resp.status_code, json.loads(resp.content)),
                         (500, {"error": "Database connection failed"}))

    def test_request_id_header(self):
        resp = self.post(payload(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertRegex(resp["X-Prototype-Request-Id"], r"^[0-9a-f]{16}$")


# --------------------------------------------------------------------------
# 2. Smartcare load: repeated work, retries, connections
# --------------------------------------------------------------------------
class SmartcareLoadTests(PrototypeTestBase):

    def test_fallback_reuses_identical_successful_queries(self):
        # SCF and S1MME return no rows -> all six fallback columns are all-NaN.
        # Old code: 4 main + 4x SCF + 2x S1MME = 10 executions. New: 4.
        fake = fs.FakeSmartcare(fs.data_fixture(scf_rows=False, s1mme_rows=False))
        self.post(payload(), fake)
        self.assertEqual(len(fake.executions), 4)
        self.assertEqual(fake.count("scf"), 1)
        self.assertEqual(fake.count("s1mme"), 1)

    def test_one_connection_per_request_and_closed(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        self.post(payload(), fake)
        self.assertEqual(fake.opened, 1)       # old code: 5 (one of them leaked)
        self.assertEqual(fake.closed, 1)

    def test_transient_failure_is_retried_and_recovers(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"scf": 2})
        resp = self.post(payload(), fake)
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["data_full"]))
        self.assertEqual(fake.count("scf"), 3)
        self.assertEqual(fake.opened, fake.closed)

    def test_permanently_failing_query_not_hammered_by_fallbacks(self):
        # 5 attempts for the main query, then ONE attempt per fallback re-run
        # (old code: 5 attempts for each of the 4 fallback re-runs = 25).
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"scf": 10_000})
        resp = self.post(payload(), fake)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(fake.count("scf"), 5 + 4)
        self.assertEqual(fake.opened, fake.closed)

    @override_settings(PROTOTYPE_SMARTCARE_MAX_ATTEMPTS=2)
    def test_max_attempts_setting(self):
        fake = fs.FakeSmartcare(fs.data_fixture(), fail={"s1mme": 10_000})
        self.post(payload(), fake)
        self.assertEqual(fake.count("s1mme"), 2 + 2)   # main (2) + 2 fallback columns x 1

    @override_settings(PROTOTYPE_SKIP_UNUSED_DATA_QUERIES=True)
    def test_skip_unused_data_queries_keeps_response(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        resp = self.post(payload(), fake)
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["data_full"]))
        self.assertEqual(fake.count("reg"), 0)

    def test_queries_never_run_in_parallel_within_a_request(self):
        active, peak = [0], [0]
        lock = threading.Lock()
        real_execute = fs.FakeCursor.execute

        def slow_execute(cursor, sql):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.01)
            try:
                return real_execute(cursor, sql)
            finally:
                with lock:
                    active[0] -= 1

        with mock.patch.object(fs.FakeCursor, "execute", slow_execute):
            self.post(payload(), fs.FakeSmartcare(fs.data_fixture()))
        self.assertEqual(peak[0], 1)


# --------------------------------------------------------------------------
# 3. Aggregation: the 6-attempt loop was redundant
# --------------------------------------------------------------------------
class AggregationTests(PrototypeTestBase):

    def _nodes(self):
        frames = []
        for key in ("s1mme", "reg", "gbiups", "scf"):
            cols, rows = fs.data_fixture()[key]
            frames.append(pd.DataFrame(rows, columns=cols))
        return pd.concat(frames, ignore_index=True)

    def test_aggregation_is_pure_and_repeatable(self):
        nodes = self._nodes()
        before = nodes.copy(deep=True)
        log = os.path.join(self.tmp, "agg.log")
        first = apis.smartcareOutput_Aggregated_Data(nodes, log, "ts")
        pd.testing.assert_frame_equal(nodes, before)          # input not mutated
        for _ in range(5):                                      # what the old loop did
            again = apis.smartcareOutput_Aggregated_Data(nodes, log, "ts")
            pd.testing.assert_frame_equal(first[0], again[0])
            self.assertEqual(first[1:], again[1:])

    def test_site_letters_match_old_row_apply(self):
        df = pd.DataFrame({"site_name": ["CAI0123", "Kenana7", None, "ABC", "", "nan12"],
                           "site_name_number": ["0123", "7", "nan", "nan", "nan", "12"]})
        old = df.apply(lambda r: str(r["site_name"]).replace(str(r["site_name_number"]), ""), axis=1)
        new = apis._site_letters(df["site_name"], df["site_name_number"])
        self.assertEqual(old.tolist(), new.tolist())
        self.assertTrue(old.index.equals(new.index))

    def test_master_sheet_read_once_and_reloaded_on_change(self):
        with mock.patch.object(runtime.pd, "read_excel", wraps=pd.read_excel) as spy:
            a = runtime.get_master_sheet(self.master)
            b = runtime.get_master_sheet(self.master)
            self.assertIs(a, b)
            self.assertEqual(spy.call_count, 1)
            time.sleep(0.01)
            pd.DataFrame({"SITE_ID": [" X1 "], "Hub": ["H"]}).to_excel(self.master, index=False)
            os.utime(self.master, None)
            c = runtime.get_master_sheet(self.master)
            self.assertEqual(spy.call_count, 2)
            self.assertEqual(c["SITE_ID"].tolist(), ["X1"])


# --------------------------------------------------------------------------
# 4. Output files: request-specific + atomic legacy files
# --------------------------------------------------------------------------
class OutputFileTests(PrototypeTestBase):

    def test_each_request_gets_its_own_folder_and_legacy_file_is_kept(self):
        r1 = self.post(payload(), fs.FakeSmartcare(fs.data_fixture()))
        r2 = self.post(payload("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        ids = [r1["X-Prototype-Request-Id"], r2["X-Prototype-Request-Id"]]
        runs = list((self.out_dir / "prototype" / "runs").glob("*/*"))
        self.assertEqual(sorted(p.name for p in runs), sorted(ids))
        voice_run = next(p for p in runs if p.name == ids[1])
        self.assertTrue((voice_run / "sc_nodes_before_voice.csv").exists())
        self.assertTrue((voice_run / "sc_data.csv").exists())
        self.assertTrue((voice_run / "sc_nodes.csv").exists())
        # legacy consumer path still populated, with the latest request's data
        legacy = Path(self.legacy_dir) / "sc_nodes.csv"
        self.assertEqual(legacy.read_bytes(), (voice_run / "sc_nodes.csv").read_bytes())
        self.assertEqual([p for p in os.listdir(self.legacy_dir) if p.endswith(".tmp")], [])

    def test_legacy_file_never_seen_half_written(self):
        target = Path(self.legacy_dir) / "sc_nodes.csv"
        small = pd.DataFrame({"a": range(10)})
        big = pd.DataFrame({"a": range(200_000)})
        runtime.atomic_write_csv(small, target)
        stop = threading.Event()
        bad = []

        def writer(df):
            while not stop.is_set():
                runtime.atomic_write_csv(df, target)

        def reader():
            for _ in range(60):
                n = len(pd.read_csv(target))
                if n not in (10, 200_000):
                    bad.append(n)

        threads = [threading.Thread(target=writer, args=(small,)),
                   threading.Thread(target=writer, args=(big,))]
        for t in threads:
            t.start()
        reader()
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(bad, [])

    def test_unwritable_legacy_dir_does_not_change_response(self):
        with override_settings(PROTOTYPE_LEGACY_OUTPUT_DIR=os.path.join(self.tmp, "missing", "dir")):
            resp = self.post(payload("Voice"), fs.FakeSmartcare(fs.voice_fixture()))
        # old code: sc_data.csv failure inside aggregation silently emptied the Voice result
        self.assertEqual(json.loads(resp.content), json.loads(GOLDEN["voice"]))


# --------------------------------------------------------------------------
# 5. Structured logging / timings
# --------------------------------------------------------------------------
class TimingLogTests(PrototypeTestBase):

    def test_summary_line_and_request_log(self):
        resp = self.post(payload(), fs.FakeSmartcare(fs.data_fixture(scf_rows=False)))
        rid = resp["X-Prototype-Request-Id"]
        lines = (self.out_dir / "logs" / "prototype_timings.jsonl").read_text().splitlines()
        summary = json.loads(lines[-1])
        self.assertEqual(summary["request_id"], rid)
        self.assertEqual(summary["status"], "DONE")
        self.assertEqual(summary["http_status"], 200)
        for stage in ("wait_for_slot", "smartcare_total", "smartcare_query", "aggregate_data", "hub_metrics"):
            self.assertIn(stage, summary["stages"])
        self.assertTrue(summary["top_stage"])
        executed = [q for q in summary["queries"] if not q.get("memo_hit")]
        self.assertEqual(len(executed), 4)
        self.assertTrue(all("execute_s" in q and "fetch_s" in q and "rows" in q for q in executed))
        self.assertTrue(any(q.get("memo_hit") for q in summary["queries"]))
        log_file = next((self.out_dir / "logs" / "prototype").glob(f"*/{rid}.log"))
        text = log_file.read_text()
        self.assertIn(f"rid={rid} event=request_start", text)
        self.assertIn("event=request_done", text)
        self.assertIn("Server-Timing", resp)

    def test_logger_handler_released(self):
        import logging
        resp = self.post(payload(), fs.FakeSmartcare(fs.data_fixture()))
        name = f"callflow.prototype.run.{resp['X-Prototype-Request-Id']}"
        self.assertNotIn(name, logging.Logger.manager.loggerDict)


# --------------------------------------------------------------------------
# 6. Concurrency limits and background jobs
# --------------------------------------------------------------------------
class ConcurrencyTests(PrototypeTestBase):

    @override_settings(PROTOTYPE_SYNC_SLOT_WAIT_S=0.2, PROTOTYPE_MAX_CONCURRENT=1)
    def test_sync_request_gets_429_when_slot_busy(self):
        fake = fs.FakeSmartcare(fs.data_fixture())
        with concurrency.Slot(1):
            resp = self.post(payload(), fake)
        self.assertEqual(resp.status_code, 429)
        self.assertIn("error", json.loads(resp.content))
        self.assertEqual(fake.executions, [])
        # slot released -> next request runs
        self.assertEqual(self.post(payload(), fake).status_code, 200)

    @override_settings(PROTOTYPE_MAX_CONCURRENT=1, PROTOTYPE_SYNC_SLOT_WAIT_S=30)
    def test_concurrent_requests_are_serialised(self):
        active, peak = [0], [0]
        lock = threading.Lock()
        real = apis.smartcareNodesQueries

        def tracked(*a, **k):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.05)
            try:
                return real(*a, **k)
            finally:
                with lock:
                    active[0] -= 1

        results = []
        fake = fs.FakeSmartcare(fs.data_fixture())

        def call():
            req = APIRequestFactory().post("/x", payload(), format="json")
            results.append(views.PrototypeSmartcareIntegrationAPIView.as_view()(req).status_code)

        # patch once, outside the threads (mock.patch itself is not thread-safe)
        with mock.patch.object(apis, "smartcareNodesQueries", tracked), \
                mock.patch.object(apis, "smartcareConnection", fake.connect):
            threads = [threading.Thread(target=call) for _ in range(3)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        self.assertEqual(results, [200, 200, 200])
        self.assertEqual(peak[0], 1)

    def test_async_job_returns_same_body_as_sync(self):
        resp = self.post(payload(), fs.FakeSmartcare(fs.data_fixture()), query="?async=1")
        self.assertEqual(resp.status_code, 202)
        job_id = json.loads(resp.content)["job_id"]
        job = json.loads(self.get_job(job_id).content)
        self.assertEqual(job["status"], "DONE")
        self.assertEqual(job["http_status"], 200)
        self.assertEqual(job["result"], json.loads(GOLDEN["data_full"]))

    def test_async_validation_errors_stay_synchronous(self):
        resp = self.post({"msisdn_list": fs.MSISDNS, "async": True}, fs.FakeSmartcare({}))
        self.assertEqual(resp.status_code, 400)

    def test_async_duplicate_returns_existing_job(self):
        fp = concurrency.request_fingerprint(fs.MSISDNS, "2026-10-15T10:00", "2026-10-15T11:00", "Data", "")
        concurrency.create_job("someone", "existingjob00001")
        self.assertIsNone(concurrency.claim_inflight(fp, "existingjob00001"))
        fake = fs.FakeSmartcare(fs.data_fixture())
        resp = self.post(payload(), fake, query="?async=1")
        body = json.loads(resp.content)
        self.assertEqual((resp.status_code, body["job_id"], body.get("deduplicated")),
                         (202, "existingjob00001", True))
        self.assertEqual(fake.executions, [])

    @override_settings(PROTOTYPE_MAX_PENDING_JOBS=1)
    def test_async_queue_is_bounded(self):
        concurrency.pending_add("somebody-else")
        resp = self.post(payload(), fs.FakeSmartcare(fs.data_fixture()), query="?async=1")
        self.assertEqual(resp.status_code, 429)

    def test_unknown_job_404(self):
        self.assertEqual(self.get_job("nope").status_code, 404)

    def test_slot_lease_expires_if_holder_dies(self):
        with override_settings(PROTOTYPE_SLOT_TTL_S=1, PROTOTYPE_MAX_CONCURRENT=1):
            b = concurrency.backend()
            self.assertTrue(b.set_nx(concurrency.SLOT_KEY.format(0), "dead-worker", 1))
            time.sleep(1.1)
            with concurrency.Slot(0.5) as slot:
                self.assertEqual(slot.key, concurrency.SLOT_KEY.format(0))
