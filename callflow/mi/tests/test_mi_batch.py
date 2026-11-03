"""
Multi-complaint requests, daily submission limit and 24h job retention.

    python manage.py test callflow.mi.tests.test_mi_batch
"""
import json
import threading
import time
from datetime import datetime
from unittest import mock

from django.test import override_settings

from callflow.mi import batch, catalog, config, jobs, outputs, validation
from callflow.mi.tests import fake_smartcare as fs
from callflow.mi.tests.test_mi import MITestBase

MI_PATH = "/callflow/mi/api/smartcare"
LEGACY_PATH = "/callflow/prototype/api/smartcare"
DATA_KEYS = {"suspected_sites", "suspected_cells_raw", "suspected_sgws", "HUB_Counts", "rat_counts",
             "DRA_NAME", "OCS_NAME", "PCRF", "region_counts", "subregion_counts", "sgsn_counts",
             "tac_counts", "mme_counts"}
VOICE_KEYS = {"Suspected_Sites", "Suspected_Cells", "Cluster_Counts", "Region_Counts", "Sub_Region_Counts",
              "HUB_Counts", "CSCF_Name", "AS_Name", "MSC_Name", "DRA_NAME", "OCS_NAME", "PCRF"}
VOICE_TABLE_KEYS = ("dra_voice", "sgs", "hss", "cs_moc", "cs_mtc", "volte_mo", "volte_mt", "reg")
SYNTAX_ERROR = "java.sql.SQLException: org.apache.spark.sql.catalyst.parser.ParseException: syntax error"


def complaints_body(**extra):
    b = {
        "start_date": "2026-10-15T10:00",
        "end_date": "2026-10-15T11:00",
        "complaints": [
            {"category": "Home Wireless", "subcategory": "HG", "msisdn_list": ["1012345678", "1112345678"]},
            {"category": "MI", "subcategory": "MI Connection", "msisdn_list": ["1212345678", "1512345678"]},
            {"category": "MI", "subcategory": "MI Disconnection Not Solved", "msisdn_list": ["1012345678"]},
            {"category": "Network", "subcategory": "Call Forward / Third Call", "msisdn_list": ["1012345678"]},
        ],
    }
    b.update(extra)
    return b


def both_fixture():
    fx = fs.data_fixture()
    fx.update(fs.voice_fixture())
    return fx


def empty_fixture():
    return {k: (cols, []) for k, (cols, _rows) in both_fixture().items()}


def subs(body):
    return [(c["category"], c["complain_type"], s) for c in body["results"] for s in c["subcategories"]]


class BatchValidationTests(MITestBase):

    def test_category_mapping_is_central(self):
        self.assertEqual(dict(catalog.CATEGORY_COMPLAIN_TYPE),
                         {"Home Wireless": "Data", "MI": "Data", "Network": "Voice"})
        self.assertEqual(catalog.resolve_category("  home   WIRELESS "), "Home Wireless")
        self.assertIsNone(catalog.resolve_category("Roaming"))

    def test_parse_batch_maps_and_keeps_groups(self):
        req = validation.parse_batch(complaints_body())
        self.assertEqual([(g.key, g.category, g.subcategory, g.complain_type, g.msisdns) for g in req.groups], [
            ("g01", "Home Wireless", "HG", "Data", ["1012345678", "1112345678"]),
            ("g02", "MI", "MI Connection", "Data", ["1212345678", "1512345678"]),
            ("g03", "MI", "MI Disconnection Not Solved", "Data", ["1012345678"]),
            ("g04", "Network", "Call Forward / Third Call", "Voice", ["1012345678"]),
        ])
        # the same MSISDN may appear in several groups; duplicates inside one group are dropped
        dup = complaints_body(complaints=[{"category": "MI", "subcategory": "x",
                                           "msisdn_list": ["1012345678", "1012345678 "]}])
        self.assertEqual(validation.parse_batch(dup).groups[0].msisdns, ["1012345678"])
        # each group becomes exactly the single-complaint params pipeline.run already takes
        self.assertEqual(req.group_params(req.groups[3]),
                         {"msisdn_list": ["1012345678"], "start_date": "2026-10-15T10:00",
                          "end_date": "2026-10-15T11:00", "Complain_Type": "Voice", "imsi": None,
                          "user": "unknown"})

    def test_errors_name_the_group_and_are_400_on_both_paths(self):
        bad = complaints_body(complaints=[
            {"category": "Roaming", "subcategory": "x", "msisdn_list": ["1012345678"]},
            {"category": "MI", "subcategory": "", "msisdn_list": ["1012345678"]},
            {"category": "Network", "subcategory": "CF", "msisdn_list": ["999"]},
            {"category": "Network", "subcategory": "cf", "msisdn_list": ["1012345678"]},
        ])
        for path in (MI_PATH, LEGACY_PATH):
            resp = self.post(bad, fs.FakeSmartcare({}), path=path)
            self.assertEqual(resp.status_code, 400)
        error = json.loads(resp.content)["error"]
        self.assertIn("complaints[0].category 'Roaming' is not supported", error)
        self.assertIn("complaints[1].subcategory is required", error)
        self.assertIn("complaints[2].msisdn_list: Invalid MSISDN: 999", error)
        self.assertIn("complaints[3] repeats Network / cf", error)
        self.assertFalse((self.mi_root / "jobs").exists())

    def test_window_and_mixing_rules(self):
        for b, text in [
            (complaints_body(end_date="2026-10-15T09:00"), "end_date must be after start_date"),
            (complaints_body(complaints=[]), "complaints must be a non-empty list"),
            (complaints_body(Complain_Type="Data"), "not both"),
            (complaints_body(start_date=None), "Missing start_date or end_date"),
        ]:
            with self.assertRaises(validation.RequestError) as cm:
                validation.parse_batch(b)
            self.assertIn(text, cm.exception.message)

    @override_settings(MI_MAX_COMPLAINT_GROUPS=2)
    def test_group_count_is_bounded(self):
        with self.assertRaises(validation.RequestError) as cm:
            validation.parse_batch(complaints_body())
        self.assertIn("at most 2", cm.exception.message)

    def test_subcategory_allow_list(self):
        with mock.patch.dict(catalog.SUBCATEGORIES, {"MI": ("MI Connection",)}):
            ok = complaints_body(complaints=[{"category": "MI", "subcategory": "mi connection",
                                              "msisdn_list": ["1012345678"]}])
            self.assertEqual(validation.parse_batch(ok).groups[0].subcategory, "MI Connection")
            with self.assertRaises(validation.RequestError) as cm:
                validation.parse_batch(complaints_body(complaints=[
                    {"category": "MI", "subcategory": "Other", "msisdn_list": ["1012345678"]}]))
            self.assertIn("unknown subcategory 'Other' for MI", cm.exception.message)


class BatchExecutionTests(MITestBase):

    def run_batch(self, fixture, fail=None, b=None):
        fake = fs.FakeSmartcare(fixture, fail=fail)
        resp = self.post(b or complaints_body(), fake, path=MI_PATH)
        self.assertEqual(resp.status_code, 202, resp.content)
        job_id = json.loads(resp.content)["job_id"]
        return job_id, self.get(f"{MI_PATH}/jobs/{job_id}/result"), fake

    def test_each_group_runs_the_existing_pipeline_with_its_complain_type(self):
        job_id, resp, fake = self.run_batch(both_fixture())
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertEqual((body["job_id"], body["status"]), (job_id, "COMPLETED"))
        self.assertEqual([(c["category"], c["complain_type"], [s["subcategory"] for s in c["subcategories"]])
                          for c in body["results"]],
                         [("Home Wireless", "DATA", ["HG"]),
                          ("MI", "DATA", ["MI Connection", "MI Disconnection Not Solved"]),
                          ("Network", "VOICE", ["Call Forward / Third Call"])])
        for _cat, ctype, sub in subs(body):
            self.assertEqual(sub["status"], "COMPLETED")
            self.assertEqual(set(sub["analysis_result"]), DATA_KEYS if ctype == "DATA" else VOICE_KEYS)
            self.assertEqual(sub["requested_msisdns"], sub["analyzed_msisdns"])
        # real values, identical to what the single-complaint endpoint returns for the same input
        single = self.post({"msisdn_list": ["1012345678", "1112345678"], "start_date": "2026-10-15T10:00",
                            "end_date": "2026-10-15T11:00", "Complain_Type": "Data"},
                           fs.FakeSmartcare(both_fixture()))
        self.assertEqual(subs(body)[0][2]["analysis_result"], json.loads(single.content))
        # Data groups ran only Data tables, the Voice group only Voice tables
        self.assertEqual(fake.count("dra_voice"), 1)
        self.assertEqual(fake.count("s1mme"), 3)

    def test_groups_have_their_own_folders_and_tagged_logs(self):
        job_id, _resp, _fake = self.run_batch(both_fixture())
        root = self.job_dir(job_id)
        for key in ("g01", "g02", "g03", "g04"):
            folder = root / "groups" / key
            self.assertTrue((folder / "sc_nodes.csv").exists(), key)
            request = json.loads((folder / "request.json").read_text())
            self.assertEqual(request["Complain_Type"], "Voice" if key == "g04" else "Data")
        log = (root / "groups" / "g04" / "job.log").read_text()
        self.assertIn(f"job_id={job_id}-g04 parent_job_id={job_id} group=g04 category=Network "
                      f'subcategory="Call Forward / Third Call" complain_type=Voice', log)
        parent = (root / "job.log").read_text()
        self.assertEqual(parent.count("event=group_done"), 4)
        self.assertRegex(parent, r"event=group_done group=g02 category=MI subcategory=\"MI Connection\" "
                                 r"complain_type=Data requested_msisdns=2 analyzed_msisdns=2 status=COMPLETED "
                                 r"http_status=200 duration_s=\d")
        summary = json.loads((root / "groups" / "g02" / "summary.json").read_text())
        self.assertEqual((summary["parent_job_id"], summary["status"]), (job_id, "COMPLETED"))

    def test_one_failing_group_does_not_stop_the_others(self):
        fail = {k: (99, SYNTAX_ERROR) for k in VOICE_TABLE_KEYS}
        job_id, resp, _fake = self.run_batch(both_fixture(), fail=fail)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertEqual(body["status"], "PARTIALLY_COMPLETED")
        statuses = {s["subcategory"]: s["status"] for _c, _t, s in subs(body)}
        self.assertEqual(statuses, {"HG": "COMPLETED", "MI Connection": "COMPLETED",
                                    "MI Disconnection Not Solved": "COMPLETED",
                                    "Call Forward / Third Call": "FAILED"})
        voice = subs(body)[3][2]
        self.assertIsNone(voice["analysis_result"])
        self.assertIn("Smartcare queries failed", voice["error"])
        self.assertIn("smartcare_DRA_Voice", voice["failed_queries"])
        self.assertEqual(body["summary"]["completed"], 3)
        self.assertEqual(body["summary"]["failed"], 1)
        record = self.job_record(job_id)
        self.assertEqual((record["status"], record["partial"]), ("PARTIALLY_COMPLETED", True))
        self.assertIn("group_failed", resp["X-MI-Warnings"])

    def test_some_queries_failing_is_partially_completed_with_real_values(self):
        b = complaints_body(complaints=[complaints_body()["complaints"][3]])
        _job, resp, _fake = self.run_batch(both_fixture(), fail={"cs_mtc": (99, SYNTAX_ERROR)}, b=b)
        body = json.loads(resp.content)
        sub = subs(body)[0][2]
        self.assertEqual((body["status"], sub["status"]), ("PARTIALLY_COMPLETED", "PARTIALLY_COMPLETED"))
        self.assertEqual(sub["failed_queries"], ["smartcare_network_CS_MTC"])
        self.assertTrue(sub["analysis_result"]["Suspected_Cells"])

    def test_no_rows_is_no_data_not_failure(self):
        _job, resp, _fake = self.run_batch(empty_fixture())
        body = json.loads(resp.content)
        self.assertEqual((resp.status_code, body["status"]), (200, "COMPLETED"))
        for _c, _t, sub in subs(body):
            self.assertEqual((sub["status"], sub["analysis_result"], sub["analyzed_msisdns"]), ("NO_DATA", None, 0))

    def test_all_groups_failing_is_failed(self):
        def broken():
            raise RuntimeError("java.net.ConnectException: Connection refused")
        fake = fs.FakeSmartcare({})
        fake.connect = lambda *a, **k: broken()
        with override_settings(MI_SMARTCARE_MAX_ATTEMPTS=1):
            resp = self.post(complaints_body(), fake, path=MI_PATH)
        job_id = json.loads(resp.content)["job_id"]
        result = self.get(f"{MI_PATH}/jobs/{job_id}/result")
        body = json.loads(result.content)
        self.assertEqual((result.status_code, body["status"]), (500, "FAILED"))
        self.assertEqual({s["error"] for _c, _t, s in subs(body)}, {"Database connection failed"})
        self.assertEqual(self.job_record(job_id)["status"], "FAILED")

    def test_sync_legacy_path_returns_categorised_body(self):
        resp = self.post(complaints_body(), fs.FakeSmartcare(both_fixture()), path=LEGACY_PATH)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)["status"], "COMPLETED")

    def test_progress_is_published_per_group(self):
        seen = []
        real = jobs.update_job

        def spy(job_id, **fields):
            if "groups_done" in fields:
                seen.append((fields["groups_done"], fields.get("result", {}).get("summary", {}).get("running")))
            return real(job_id, **fields)
        with mock.patch.object(jobs, "update_job", spy):
            self.run_batch(both_fixture())
        self.assertEqual(seen, [(0, 1), (1, 1), (2, 1), (3, 1), (4, None)])

    def test_single_complaint_requests_unchanged(self):
        resp = self.post({"msisdn_list": fs.MSISDNS, "start_date": "2026-10-15T10:00",
                          "end_date": "2026-10-15T11:00", "Complain_Type": "Data"},
                         fs.FakeSmartcare(fs.data_fixture()), path=MI_PATH)
        job = self.job_record(json.loads(resp.content)["job_id"])
        self.assertEqual((job["status"], job["request_type"]), ("DONE", "single"))
        self.assertIn("suspected_sites", job["result"])

    @override_settings(MI_JOB_STALE_S=1)
    def test_lost_worker_keeps_finished_groups(self):
        req = validation.parse_batch(complaints_body())
        entries = [batch._entry(g) for g in req.groups]
        entries[0].update(status="COMPLETED", analysis_result={"x": 1})
        entries[1]["status"] = "RUNNING"
        jobs.create_job("lostbatch0000001", "x", req.to_params(), "mi", outputs.today_str())
        jobs.update_job("lostbatch0000001", status="RUNNING", heartbeat_at=time.time() - 5,
                        result=batch.build_body("lostbatch0000001", "RUNNING", req.raw_start, req.raw_end, entries))
        job = jobs.get_job("lostbatch0000001")
        self.assertEqual((job["status"], job["http_status"]), ("PARTIALLY_COMPLETED", 200))
        statuses = [s["status"] for _c, _t, s in subs(job["result"])]
        self.assertEqual(statuses, ["COMPLETED", "FAILED", "FAILED", "FAILED"])
        self.assertEqual(subs(job["result"])[0][2]["analysis_result"], {"x": 1})


@override_settings(MI_DAILY_SUBMISSION_LIMIT=2, TIME_ZONE="Africa/Cairo")
class DailyLimitTests(MITestBase):

    def submit(self, b=None, fixture=None):
        return self.post(b or complaints_body(), fs.FakeSmartcare(fixture or both_fixture()), path=MI_PATH)

    def test_limit_is_global_and_counts_requests_not_groups_or_msisdns(self):
        first = self.submit()
        self.assertEqual(json.loads(first.content)["daily_quota"]["used"], 1)
        self.assertEqual(first["X-MI-Daily-Remaining"], "1")
        self.submit(complaints_body(end_date="2026-10-15T10:30"))          # different request
        third = self.submit(complaints_body(end_date="2026-10-15T10:45"))
        self.assertEqual(third.status_code, 429)
        data = json.loads(third.content)
        self.assertIn("Daily MI analysis limit reached: 2 submissions per day", data["error"])
        self.assertEqual((data["daily_limit"], data["used_today"]), (2, 2))
        reset = datetime.fromisoformat(data["resets_at"])
        self.assertEqual((reset.hour, reset.minute, reset.utcoffset() is not None), (0, 0, True))
        self.assertGreater(int(third["Retry-After"]), 0)
        self.assertEqual(jobs.quota_status()["used"], 2)
        # single-complaint requests share the same counter
        single = self.post({"msisdn_list": fs.MSISDNS, "start_date": "2026-10-15T10:00",
                            "end_date": "2026-10-15T11:00", "Complain_Type": "Data"},
                           fs.FakeSmartcare(fs.data_fixture()), path=LEGACY_PATH)
        self.assertEqual(single.status_code, 429)

    def test_invalid_polling_and_results_do_not_consume(self):
        bad = self.submit(complaints_body(complaints=[{"category": "X", "subcategory": "y", "msisdn_list": []}]))
        self.assertEqual(bad.status_code, 400)
        job_id = json.loads(self.submit().content)["job_id"]
        for _ in range(3):
            self.get(f"{MI_PATH}/jobs/{job_id}")
            self.get(f"{MI_PATH}/jobs/{job_id}/result")
        self.assertEqual(jobs.quota_status()["used"], 1)

    def test_duplicate_of_an_active_job_does_not_consume(self):
        params = validation.parse_batch(complaints_body()).to_params()
        jobs.create_job("activebatch00001", "x", params, "mi", outputs.today_str())
        self.assertIsNone(jobs.claim_inflight(jobs.fingerprint(params), "activebatch00001"))
        reordered = complaints_body()
        reordered["complaints"] = list(reversed(reordered["complaints"]))
        resp = self.submit(reordered)
        self.assertEqual((json.loads(resp.content)["job_id"], json.loads(resp.content)["deduplicated"]),
                         ("activebatch00001", True))
        self.assertEqual(jobs.quota_status()["used"], 0)

    @override_settings(MI_MAX_QUEUED_JOBS=1)
    def test_queue_full_does_not_consume(self):
        jobs.pending_add("other")
        self.assertEqual(self.submit().status_code, 429)
        self.assertEqual(jobs.quota_status()["used"], 0)

    def test_failed_job_creation_gives_the_submission_back(self):
        with mock.patch.object(jobs, "create_job", side_effect=RuntimeError("cache down")):
            with self.assertRaises(RuntimeError):
                self.submit()
        self.assertEqual(jobs.quota_status()["used"], 0)

    @override_settings(MI_DAILY_SUBMISSION_LIMIT=10)
    def test_concurrent_reservations_never_exceed_the_limit(self):
        results = []

        def take():
            try:
                jobs.reserve_submission()
                results.append(True)
            except jobs.QuotaExceeded:
                results.append(False)
        threads = [threading.Thread(target=take) for _ in range(40)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual((results.count(True), jobs.quota_status()["used"]), (10, 10))

    def test_counter_is_per_calendar_day_in_the_configured_timezone(self):
        from django.utils import timezone
        tz = timezone.get_default_timezone()
        late = datetime(2026, 11, 3, 23, 59, 30)
        late = tz.localize(late) if hasattr(tz, "localize") else late.replace(tzinfo=tz)
        jobs.reserve_submission(now=late)
        jobs.reserve_submission(now=late)
        with self.assertRaises(jobs.QuotaExceeded) as cm:
            jobs.reserve_submission(now=late)
        self.assertTrue(cm.exception.resets_at.startswith("2026-11-04T00:00:00"))
        next_day = late + (datetime(2026, 11, 4, 0, 0, 30) - datetime(2026, 11, 3, 23, 59, 30))
        self.assertEqual(jobs.reserve_submission(now=next_day)["used"], 1)

    @override_settings(TIME_ZONE="UTC", MI_DAILY_LIMIT_TIMEZONE="Africa/Cairo")
    def test_limit_timezone_override(self):
        reset = datetime.fromisoformat(jobs.quota_status()["resets_at"])
        self.assertEqual((reset.hour, reset.utcoffset().total_seconds() / 3600 in (2.0, 3.0)), (0, True))
        with override_settings(MI_DAILY_LIMIT_TIMEZONE=""):
            self.assertEqual(datetime.fromisoformat(jobs.quota_status()["resets_at"]).utcoffset().total_seconds(), 0)

    @override_settings(MI_DAILY_SUBMISSION_LIMIT=0)
    def test_zero_disables(self):
        for _ in range(3):
            self.assertEqual(self.submit(complaints_body(end_date=f"2026-10-15T10:{_ + 10}")).status_code, 202)


class RetentionTests(MITestBase):

    def finished_job(self):
        resp = self.post(complaints_body(), fs.FakeSmartcare(both_fixture()), path=MI_PATH)
        return json.loads(resp.content)["job_id"]

    def test_finished_job_expires_24h_after_completion_and_polling_does_not_extend(self):
        self.assertEqual(config.job_ttl_s(), 24 * 3600)
        job = self.job_record(self.finished_job())
        self.assertAlmostEqual(job["expires_at"] - job["completed_at"], 24 * 3600, delta=1)
        before = job["expires_at"]
        for _ in range(3):
            self.get(f"{MI_PATH}/jobs/{job['job_id']}")
            self.get(f"{MI_PATH}/jobs/{job['job_id']}/result")
        self.assertEqual(self.job_record(job["job_id"])["expires_at"], before)

    @override_settings(MI_JOB_TTL_S=1)
    def test_expired_job_is_404(self):
        job_id = self.finished_job()
        self.assertEqual(self.get(f"{MI_PATH}/jobs/{job_id}/result").status_code, 200)
        time.sleep(1.3)
        self.assertEqual(self.get(f"{MI_PATH}/jobs/{job_id}").status_code, 404)
        self.assertEqual(self.get(f"{MI_PATH}/jobs/{job_id}/result").status_code, 404)

    @override_settings(MI_JOB_TTL_S=2)
    def test_running_job_kept_alive_by_heartbeat(self):
        jobs.create_job("runningjob000009", "x", {"complaints": []}, "mi", outputs.today_str())
        jobs.update_job("runningjob000009", status="RUNNING")
        with jobs.JobHeartbeat("runningjob000009", interval=0.5):
            time.sleep(3)
            self.assertIsNotNone(jobs.get_job("runningjob000009"))

    def test_heartbeat_cannot_drop_a_group_result(self):
        jobs.create_job("racejob000000001", "x", {"complaints": []}, "mi", outputs.today_str())
        stop = threading.Event()

        def beat():
            while not stop.is_set():
                jobs.update_job("racejob000000001", heartbeat_at=time.time())
        t = threading.Thread(target=beat)
        t.start()
        for i in range(200):
            jobs.update_job("racejob000000001", **{f"g{i}": i})
        stop.set()
        t.join()
        job = jobs.get_job("racejob000000001")
        self.assertEqual([job.get(f"g{i}") for i in range(200)], list(range(200)))

    @override_settings(MI_JOB_RETENTION_DAYS=1)
    def test_job_files_never_deleted_before_the_record_expires(self):
        self.assertEqual(config.min_job_retention_days(), 2)
        self.assertEqual(config.job_retention_days(), 2)

    def test_retention_default_and_disable(self):
        self.assertEqual(config.job_retention_days(), 7)
        with override_settings(MI_JOB_RETENTION_DAYS=0):
            self.assertEqual(config.job_retention_days(), 0)
