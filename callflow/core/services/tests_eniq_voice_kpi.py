"""
Tests for the Voice KPI ENIQ calculation.

    python manage.py test callflow.core.services.tests_eniq_voice_kpi

1. Every KPI's counters, node and time columns resolve against the column
   labels of the queries in constants/voice_kpis_Eniq_Queries.py (the source
   of truth) and of the query files in Eniq/Eniq_Queries/.
2. Per-row values (level=object) match the Excel formulas written in the
   sample workbooks in Eniq/eniq_Samples/. The expected values below are
   written from the sample sheets' own formulas and column letters, not from
   the KPI spec table, so they check the counter mapping too.
3. Node aggregation, null and division-by-zero rules on small frames.
4. The view end to end with a fake ENIQ connection (no Sybase needed).

Tests that need Eniq/ are skipped when that folder is not under BASE_DIR.
"""

import math
import os
import re
import tempfile
from pathlib import Path
from unittest import mock, skipUnless

import numpy as np
import pandas as pd
from django.conf import settings
from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory

from ..constants.voice_kpis_Eniq_Formulas import (
    DATETIME_COLUMNS, QUERY_OUTPUTS, VOICE_KPI_SPECS, VOICE_KPI_SPECS_BY_NAME,
)
from ..constants.voice_kpis_Eniq_Queries import VOICE_KPI_ENIQ_QUERIES
from . import eniq_voice_kpi_service as svc

ENIQ_DIR = Path(settings.BASE_DIR) / "Eniq"
QUERY_DIR = ENIQ_DIR / "Eniq_Queries"
SAMPLE_DIR = ENIQ_DIR / "eniq_Samples"
HAVE_QUERIES = QUERY_DIR.is_dir()
HAVE_SAMPLES = SAMPLE_DIR.is_dir()

_LABEL = re.compile(r'D1\."([^"]+)"(?:\s+AS\s+"([^"]+)")?')


def select_labels(sql):
    """Column labels a SELECT returns: the alias when given, else the column name."""
    select = sql.split("FROM")[0]
    return [alias or col for col, alias in _LABEL.findall(select)]


# ------------------------------------------------------------------ 1. mapping
class CounterMappingTests(SimpleTestCase):

    def _assert_resolves(self, labels_by_query, source):
        for spec in VOICE_KPI_SPECS:
            labels = labels_by_query.get(spec.query)
            self.assertIsNotNone(labels, f"{source}: no query {spec.query} for {spec.name}")
            df = pd.DataFrame(columns=labels)
            with self.subTest(kpi=spec.name, source=source):
                # raises MissingColumns if any counter / node / time label is absent
                svc._prepare(spec, df)

    def test_every_kpi_has_a_query(self):
        self.assertEqual(len(VOICE_KPI_SPECS), 23)
        for spec in VOICE_KPI_SPECS:
            self.assertIn(spec.query, VOICE_KPI_ENIQ_QUERIES, spec.name)

    def test_shipped_queries_return_every_counter(self):
        labels = {q: select_labels(sql) for q, sql in VOICE_KPI_ENIQ_QUERIES.items()}
        self._assert_resolves(labels, "voice_kpis_Eniq_Queries.py")

    @skipUnless(HAVE_QUERIES, "Eniq/Eniq_Queries not present")
    def test_updated_query_files_return_every_counter(self):
        labels = {p.stem: select_labels(p.read_text(encoding="utf-8"))
                  for p in QUERY_DIR.glob("*.txt")}
        self._assert_resolves(labels, "Eniq/Eniq_Queries")

    def test_queries_are_well_formed(self):
        for name, sql in VOICE_KPI_ENIQ_QUERIES.items():
            text = sql.format(hours=3)
            with self.subTest(query=name):
                self.assertEqual(text.count("("), text.count(")"))
                self.assertIn("dateadd(hour, -3, getdate())", text)
        ipstp = VOICE_KPI_ENIQ_QUERIES["IPSTP_PRC_THROTTLING_IL"]
        self.assertNotIn(" OR ", ipstp)
        self.assertIn("IN ('PRC600-', 'PRC601-', 'PRC575-', 'PRC578-')", ipstp)


# ------------------------------------------------------- 2. sample workbooks
def _f(df, col):
    return pd.to_numeric(df[col], errors="coerce")


def _z(df, col):  # Excel: a blank cell counts as 0
    return _f(df, col).fillna(0)


def _ratio(num, den):
    return (num / den.where(den > 0)).astype(float)


# KPI -> (sample file, node column in sample, counter columns, expected(df) -> Series,
#         row mask or None). Formulas copied from the sample sheets' formula columns.
SAMPLES = {
    "BGF PKT LOSS": ("BGF_Packerloss_IL", "NODENAME", ["RTPLOSTPACKETS", "RTPRECEIVEDPACKETS"],
                     lambda d: 100 * _ratio(_z(d, "RTPLOSTPACKETS"),
                                            _z(d, "RTPLOSTPACKETS") + _z(d, "RTPRECEIVEDPACKETS")), None),
    # =100 * (1 - (M2 / (X2- I2)))
    "SBG CFR": ("CSSR_CDR_THROTTLING_IL", "NEID",
                ["sbgSipNetIncSessionEstabNetworkSuccess", "SBGSIPTOTALINCSESSSETUPS",
                 "sbgSipChallengedIncInvSessions"],
                lambda d: 100 * (1 - _ratio(_z(d, "sbgSipNetIncSessionEstabNetworkSuccess"),
                                            _z(d, "SBGSIPTOTALINCSESSSETUPS")
                                            - _z(d, "sbgSipChallengedIncInvSessions"))), None),
    # =100 - (100 * ((U2 + V2) / (U2 + V2 + K2)))
    "SBG CDR": ("CSSR_CDR_THROTTLING_IL", "NEID",
                ["sbgSipSessionIncNormalRelease", "SBGSIPSESSIONOUTNORMALRELEAS",
                 "sbgSipInitiatedSessionRelease"],
                lambda d: 100 - 100 * _ratio(
                    _z(d, "sbgSipSessionIncNormalRelease") + _z(d, "SBGSIPSESSIONOUTNORMALRELEAS"),
                    _z(d, "sbgSipSessionIncNormalRelease") + _z(d, "SBGSIPSESSIONOUTNORMALRELEAS")
                    + _z(d, "sbgSipInitiatedSessionRelease")), None),
    "ThrottlingRej": ("CSSR_CDR_THROTTLING_IL", "NEID", ["sbgSipIngressInitRegThrottlingRej"],
                      lambda d: _z(d, "sbgSipIngressInitRegThrottlingRej"), None),
    "ThrottlingRejCNT": ("CSSR_CDR_THROTTLING_IL", "NEID", ["sbgSipInitRegThrottlingRejectedCnt"],
                         lambda d: _z(d, "sbgSipInitRegThrottlingRejectedCnt"), None),
    "DSC Throttling": ("DSC_Throttling_IL", "DSC", ["CCOUTREQPEERGROUPTHROTTLINGR"],
                       lambda d: _z(d, "CCOUTREQPEERGROUPTHROTTLINGR"), None),
    "DSC Error": ("DSC_ERROR_IL", "DSC", ["CCINANSNUMOFMSGSEBITSET"],
                  lambda d: _z(d, "CCINANSNUMOFMSGSEBITSET"), None),
    "IPSTP PRC MESG": ("IPSTP_PRC_THROTTLING_IL", "ELEM", ["MSGPERPRC"],
                       lambda d: _z(d, "MSGPERPRC"), None),
    "MGW_CPU": ("MGW_CPU_IL", "MGW", ["PMPROCESSORLOAD"], lambda d: _f(d, "PMPROCESSORLOAD"), None),
    # =I2/ H2
    "MGW_NB_ReJ": ("MGW_NBREJ_IL", "MGW", ["PMNROFORIGNBINITFAULTS", "PMNROFORIGNBINIT"],
                   lambda d: _ratio(_z(d, "PMNROFORIGNBINITFAULTS"), _z(d, "PMNROFORIGNBINIT")), None),
    # =100 * (E2 / (E2+ (F2* (2 ^ 31)) + G2))
    "MGW_PKTLOSS": ("MGW_PKTLOSS_IL", "MGW", ["PMRTPLOSTPKTS", "PMRTPRECEIVEDPKTSHI", "PMRTPRECEIVEDPKTSLO"],
                    lambda d: 100 * _ratio(_z(d, "PMRTPLOSTPKTS"),
                                           _z(d, "PMRTPLOSTPKTS") + _z(d, "PMRTPRECEIVEDPKTSHI") * 2 ** 31
                                           + _z(d, "PMRTPRECEIVEDPKTSLO")), None),
    # =J2/K2
    "MSC_Total_LU": ("MSC_LU_UPD_IL", "SN", ["NLALOCSUCC", "NLALOCTOT"],
                     lambda d: _ratio(_z(d, "NLALOCSUCC"), _z(d, "NLALOCTOT")), None),
    "MSC_Users": ("MSC_USERS_IL", "SN", ["NMMESATSGS"], lambda d: _z(d, "NMMESATSGS"), None),
    # =C2 / D2
    "MSC_CPLOAD": ("MSCBC_CPU_IL", "ELEM", ["LOASACCLOAD", "LOASNSCAN"],
                   lambda d: _ratio(_z(d, "LOASACCLOAD"), _z(d, "LOASNSCAN")), None),
    # =R2 / (N2 * P2)
    "MSC_SAE": ("SAE_MSC_IL", "SN", ["NTRALACC", "NIND", "NSCAN"],
                lambda d: _ratio(_z(d, "NTRALACC"), _z(d, "NIND") * _z(d, "NSCAN")), None),
    "SAPC GX CCR FR": ("SAPC_RX_GX_IL", "G3MANAGEDELEMENT",
                       ["GXRESPONSESSENT", "GXPDPACTIVATES", "GXPDPMODIFIES", "GXPDPTERMINATES"],
                       lambda d: 1 - _ratio(_z(d, "GXRESPONSESSENT"),
                                            _z(d, "GXPDPACTIVATES") + _z(d, "GXPDPMODIFIES")
                                            + _z(d, "GXPDPTERMINATES")), None),
    # =1-((IF(ISBLANK(P2),0,P2)+Z2)/(V2+T2+IF(ISBLANK(Y2),0,Y2)))
    "SAPC RX FR": ("SAPC_RX_GX_IL", "G3MANAGEDELEMENT",
                   ["RXAAASAFSIGNALLINGSUCCESS", "RXTERMINATERESPONSES", "RXAUTHREQCREATES",
                    "RXAARSUPDATE", "RXSTRSAFSIGNALLING"],
                   lambda d: 1 - _ratio(_z(d, "RXAAASAFSIGNALLINGSUCCESS") + _z(d, "RXTERMINATERESPONSES"),
                                        _z(d, "RXAUTHREQCREATES") + _z(d, "RXAARSUPDATE")
                                        + _z(d, "RXSTRSAFSIGNALLING")), None),
    "SBG CPU LOAD": ("SBG_CPU_IL", "NEID", ["CPULOADTOTAL"], lambda d: _f(d, "CPULOADTOTAL"),
                     lambda d: ~d["MOID"].astype(str).str.contains("OSDevice")),
    "SBG CPU Usage": ("SBG_CPUUsage_IL", "NEID", ["SBGAPPLCPUUSAGEMAX"],
                      lambda d: _f(d, "SBGAPPLCPUUSAGEMAX"), None),
    "SBG Total Users": ("SBG_Users_IL", "NEID", ["SBGSGCREGRESCURRENTTOTALREG"],
                        lambda d: _z(d, "SBGSGCREGRESCURRENTTOTALREG"), None),
    # =(F2+H2+ I2 + J2) / G2
    "SBG Intial REG SR": ("SBG_IntialReG_IL", "NEID",
                          ["SBGSIPREGSTATINITIALACCCNT", "sbgSipRegStatRejInitialRegCnt400",
                           "sbgSipRegStatRejInitialRegCnt403", "sbgSipRegStatRejInitialRegCnt404",
                           "SBGSIPREGSTATINITIALATTCNT"],
                          lambda d: _ratio(_z(d, "SBGSIPREGSTATINITIALACCCNT")
                                           + _z(d, "sbgSipRegStatRejInitialRegCnt400")
                                           + _z(d, "sbgSipRegStatRejInitialRegCnt403")
                                           + _z(d, "sbgSipRegStatRejInitialRegCnt404"),
                                           _z(d, "SBGSIPREGSTATINITIALATTCNT")), None),
    # =(W2 + S2 + T2) /X2
    "SBG SRVCC SR": ("SBG_SIP_SRVCC_IL", "NEID",
                     ["SBGSIPSUCCESSINCSESSIONS", "sbgSipRejectedIncAlertingSessions488",
                      "sbgSipRejectedIncPreAlertingSessions488", "SBGSIPTOTALINCSESSSETUPS"],
                     lambda d: _ratio(_z(d, "SBGSIPSUCCESSINCSESSIONS")
                                      + _z(d, "sbgSipRejectedIncAlertingSessions488")
                                      + _z(d, "sbgSipRejectedIncPreAlertingSessions488"),
                                      _z(d, "SBGSIPTOTALINCSESSSETUPS")), None),
}

_SAMPLE_CACHE = {}


def load_sample(name):
    if name not in _SAMPLE_CACHE:
        df = pd.read_excel(SAMPLE_DIR / f"{name}.xlsx")
        # drop the calculated columns added to the samples (formulas / blanks)
        df = df[[c for c in df.columns if isinstance(c, str) and not c.startswith("Unnamed")]]
        _SAMPLE_CACHE[name] = df
    return _SAMPLE_CACHE[name]


def _key(values):
    return sorted(("" if v is None else str(v)) for v in values)


def _close(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6)


@skipUnless(HAVE_SAMPLES, "Eniq/eniq_Samples not present")
class SampleFormulaTests(SimpleTestCase):

    def test_samples_cover_every_kpi_but_traffic(self):
        self.assertEqual(set(SAMPLES) | {"SBG Traffic"}, set(VOICE_KPI_SPECS_BY_NAME))

    def test_object_level_matches_sample_formulas(self):
        for kpi, (sample, node_col, counters, expected_fn, mask_fn) in SAMPLES.items():
            with self.subTest(kpi=kpi):
                df = load_sample(sample)
                block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME[kpi], df, level="object")
                self.assertEqual(block["status"], "SUCCESS")

                d = df[mask_fn(df)] if mask_fn else df
                d = d[d[counters].apply(pd.to_numeric, errors="coerce").notna().any(axis=1)]
                exp = expected_fn(d).replace([np.inf, -np.inf], np.nan)
                exp_rows = sorted(
                    zip(d["DATETIMEID"].astype(str), d[node_col].astype(str),
                        [None if pd.isna(v) else round(float(v), 6) for v in exp]),
                    key=lambda r: (r[0], r[1], -1e300 if r[2] is None else r[2]))
                got_rows = sorted(
                    ((str(pd.Timestamp(r["datetime"])), str(r["node"]), r["value"])
                     for r in block["results"]),
                    key=lambda r: (r[0], r[1], -1e300 if r[2] is None else r[2]))
                self.assertEqual(len(got_rows), len(exp_rows))
                self.assertGreater(len(got_rows), 0)
                for g, e in zip(got_rows, exp_rows):
                    self.assertEqual(g[:2], e[:2])
                    self.assertTrue(_close(g[2], e[2]), f"{kpi}: got {g} expected {e}")

    def test_node_level_is_weighted_by_counters(self):
        df = load_sample("MGW_NBREJ_IL")
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_NB_ReJ"], df, level="node")
        first = block["results"][0]
        t, node = pd.Timestamp(first["datetime"]), first["node"]
        grp = df[(pd.to_datetime(df["DATETIMEID"]) == t) & (df["MGW"] == node)]
        expected = grp["PMNROFORIGNBINITFAULTS"].sum() / grp["PMNROFORIGNBINIT"].sum()
        self.assertTrue(_close(first["value"], round(expected, 6)))
        # one row per node and period
        keys = [(r["datetime"], r["node"]) for r in block["results"]]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(keys), df.groupby(["DATETIMEID", "MGW"]).ngroups)

    def test_sapc_rx_fr_sample_row(self):
        df = load_sample("SAPC_RX_GX_IL").head(1)
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["SAPC RX FR"], df, level="object")
        self.assertEqual(block["results"][0]["value"], 0.75)  # 1 - (1+2)/(3+4+5)

    def test_output_has_no_raw_counters(self):
        df = load_sample("CSSR_CDR_THROTTLING_IL")
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["SBG CFR"], df, level="node")
        self.assertEqual(set(block["results"][0]), {"datetime", "node", "value"})

    def test_sae_keeps_utc_time(self):
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MSC_SAE"], load_sample("SAE_MSC_IL"))
        self.assertEqual(set(block["results"][0]), {"datetime", "node", "UTCDATETIMEID", "value"})
        self.assertEqual(block["results"][0]["UTCDATETIMEID"], "2026-10-27 14:30:00")


    def _green(self, sample):
        import openpyxl
        ws = openpyxl.load_workbook(SAMPLE_DIR / f"{sample}.xlsx").active
        return {ws.cell(1, c).value for c in range(1, ws.max_column + 1)
                if ws.cell(1, c).fill.fill_type and ws.cell(1, c).fill.fgColor.rgb == "FF92D050"}

    def test_output_columns_are_the_green_columns(self):
        for query, layout in QUERY_OUTPUTS.items():
            sample = {"MGW_LU_UPD_IL": "MSC_LU_UPD_IL"}.get(query, query)
            if not (SAMPLE_DIR / f"{sample}.xlsx").exists():
                continue
            with self.subTest(query=query):
                headers = {h for h, _ in layout}
                added = {"MGW"} if query == "MGW_CPU_IL" else set()
                self.assertEqual(headers - added, self._green(sample))

    def test_cssr_output_table(self):
        df = load_sample("CSSR_CDR_THROTTLING_IL")
        specs = [s for s in VOICE_KPI_SPECS if s.query == "CSSR_CDR_THROTTLING_IL"]
        blocks = svc.calculate_voice_kpis({"CSSR_CDR_THROTTLING_IL": df}, specs)
        tables, frames = svc.build_query_outputs(blocks)
        t = tables[0]
        self.assertEqual(t["columns"], ["DATETIMEID", "NEID", "sbgSipIngressInitRegThrottlingRej",
                                        "sbgSipInitRegThrottlingRejectedCnt", "SBG CFR", "SBG CDR"])
        self.assertEqual(t["count"], df.groupby(["DATETIMEID", "NEID"]).ngroups)
        row = next(r for r in t["results"] if r["NEID"] == "BSW-SBG-01"
                   and r["DATETIMEID"] == "2026-10-27 14:00:00")
        g = df[(df.NEID == "BSW-SBG-01") & (pd.to_datetime(df.DATETIMEID) == "2026-10-27 14:00:00")]
        cfr = 100 * (1 - g.sbgSipNetIncSessionEstabNetworkSuccess.sum()
                     / (g.SBGSIPTOTALINCSESSSETUPS.sum() - g.sbgSipChallengedIncInvSessions.fillna(0).sum()))
        self.assertTrue(_close(row["SBG CFR"], round(cfr, 6)))
        self.assertEqual(row["sbgSipIngressInitRegThrottlingRej"],
                         float(g.sbgSipIngressInitRegThrottlingRej.fillna(0).sum()))
        self.assertEqual(list(frames["CSSR_CDR_THROTTLING_IL"].columns), t["columns"])
        self.assertNotIn("_frame", blocks[0])

    def test_every_sample_builds_its_table(self):
        for query in QUERY_OUTPUTS:
            sample = {"MGW_LU_UPD_IL": "MSC_LU_UPD_IL"}.get(query, query)
            if not (SAMPLE_DIR / f"{sample}.xlsx").exists():
                continue
            with self.subTest(query=query):
                specs = [s for s in VOICE_KPI_SPECS if s.query == query]
                blocks = svc.calculate_voice_kpis({query: load_sample(sample)}, specs)
                t = svc.build_query_outputs(blocks)[0][0]
                self.assertEqual(t["status"], "SUCCESS", t.get("errors"))
                self.assertGreater(t["count"], 0)
                self.assertEqual(set(t["results"][0]), {h for h, _ in QUERY_OUTPUTS[query]})

# ----------------------------------------------------- 3. rules on small frames
class CalculationRuleTests(SimpleTestCase):
    T1, T2 = "2026-10-27 14:00:00.0", "2026-10-27 14:15:00.0"

    def nbrej(self, rows):
        return pd.DataFrame(rows, columns=["DATETIMEID", "MGW", "VMGW",
                                           "PMNROFORIGNBINIT", "PMNROFORIGNBINITFAULTS"])

    def test_ratio_sums_counters_before_dividing(self):
        df = self.nbrej([[self.T1, "MGW1", "a", 100, 1], [self.T1, "MGW1", "b", 900, 99]])
        res = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_NB_ReJ"], df)["results"]
        self.assertEqual(res, [{"datetime": "2026-10-27 14:00:00", "node": "MGW1", "value": 0.1}])

    def test_zero_denominator_gives_none(self):
        df = self.nbrej([[self.T1, "MGW1", "a", 0, 0]])
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_NB_ReJ"], df)
        self.assertIsNone(block["results"][0]["value"])
        self.assertEqual(block["null_values"], 1)

    def test_all_null_row_is_skipped_and_partial_null_counts_as_zero(self):
        df = self.nbrej([[self.T1, "MGW1", "a", None, None],
                         [self.T2, "MGW1", "a", 50, None]])
        block = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_NB_ReJ"], df)
        self.assertEqual(block["rows_without_counters"], 1)
        self.assertEqual([r["value"] for r in block["results"]], [0.0])

    def test_gauge_average_ignores_null_rows(self):
        df = pd.DataFrame([[self.T1, "MGW1", "u1", 10], [self.T1, "MGW1", "u2", 30],
                           [self.T1, "MGW1", "u3", None]],
                          columns=["DATETIMEID", "MGW", "MOID", "PMPROCESSORLOAD"])
        res = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_CPU"], df)["results"]
        self.assertEqual(res[0]["value"], 20.0)

    def test_numbers_as_strings_and_case_insensitive_labels(self):
        df = pd.DataFrame([[self.T1, "MGW1", "100", "5"]],
                          columns=["datetimeid", "mgw", "pmNrOfOrigNbInit", "pmNrOfOrigNbInitFaults"])
        res = svc.calculate_kpi(VOICE_KPI_SPECS_BY_NAME["MGW_NB_ReJ"], df)["results"]
        self.assertEqual(res[0]["value"], 0.05)

    def test_missing_counter_fails_only_that_kpi(self):
        cssr = pd.DataFrame([[self.T1, "SBG1", 10, 5, 1, 0, 0]],
                            columns=["DATETIMEID", "NEID", "sbgSipSessionIncNormalRelease",
                                     "SBGSIPSESSIONOUTNORMALRELEAS", "sbgSipInitiatedSessionRelease",
                                     "sbgSipIngressInitRegThrottlingRej",
                                     "sbgSipInitRegThrottlingRejectedCnt"])
        specs = [s for s in VOICE_KPI_SPECS if s.query == "CSSR_CDR_THROTTLING_IL"]
        blocks = {b["kpi"]: b for b in svc.calculate_voice_kpis({"CSSR_CDR_THROTTLING_IL": cssr}, specs)}
        self.assertEqual(blocks["SBG CFR"]["status"], "FAILED")
        self.assertIn("sbgSipNetIncSessionEstabNetworkSuccess", blocks["SBG CFR"]["error"])
        self.assertEqual(blocks["SBG CDR"]["status"], "SUCCESS")
        self.assertEqual(blocks["SBG CDR"]["results"][0]["value"], round(100 - 100 * 15 / 16, 6))

    def test_failed_query_fails_its_kpis(self):
        spec = VOICE_KPI_SPECS_BY_NAME["MGW_CPU"]
        block = svc.calculate_voice_kpis({}, [spec], query_errors={"MGW_CPU_IL": "timeout"})[0]
        self.assertEqual(block["status"], "FAILED")
        self.assertIn("timeout", block["error"])

    def test_select_specs(self):
        specs, unknown = svc.select_specs(["sbg cfr", "SAPC_RX_GX_IL", "nope"])
        self.assertEqual([s.name for s in specs], ["SBG CFR", "SAPC GX CCR FR", "SAPC RX FR"])
        self.assertEqual(unknown, ["nope"])
        self.assertEqual(svc.required_queries(specs), ["CSSR_CDR_THROTTLING_IL", "SAPC_RX_GX_IL"])


# ------------------------------------------------------------------- 4. view
class _FakeCursor:
    def __init__(self, tables):
        self.tables, self.description, self._rows = tables, None, []

    def execute(self, sql):
        table = re.search(r"FROM\s+dcpublic\.(\w+)", sql).group(1)
        labels = select_labels(sql)
        df = self.tables.get(table, pd.DataFrame(columns=labels))
        df = df.reindex(columns=labels)
        self.description = [(c,) for c in labels]
        self._rows = [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False)]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, tables):
        self.tables = tables

    def cursor(self):
        return _FakeCursor(self.tables)

    def close(self):
        pass


class VoiceKpiViewTests(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        t = "2026-10-27 14:00:00.0"
        self.tables = {
            "DC_E_MGW_VMGW_RAW": pd.DataFrame(
                [[t, "MGW1", "a", 100, 1], [t, "MGW1", "b", 900, 99]],
                columns=["DATETIMEID", "MGW", "VMGW", "PMNROFORIGNBINIT", "PMNROFORIGNBINITFAULTS"]),
            "DC_E_MGW_PLUGINUNIT_RAW": pd.DataFrame(
                [[t, "MGW1", "u1", 10], [t, "MGW1", "u2", 30]],
                columns=["DATETIMEID", "MGW", "MOID", "PMPROCESSORLOAD"]),
        }

    def _get(self, params):
        from ..views.Voice_KPI_Eniq import SybaseQueryView
        request = APIRequestFactory().get("/callflow/api/voice-kpi-eniq", params)
        with override_settings(OUTPUT_DIR=self.tmp), \
                mock.patch("callflow.core.views.Voice_KPI_Eniq.EniqConnection",
                           side_effect=lambda: _FakeConn(self.tables)):
            return SybaseQueryView.as_view()(request)

    def test_returns_calculated_kpis(self):
        resp = self._get({"kpis": "MGW_NB_ReJ,MGW_CPU", "hours": "2"})
        self.assertEqual(resp.status_code, 200, resp.data)
        out = {t["query"]: t for t in resp.data["outputs"]}
        self.assertEqual(out["MGW_NBREJ_IL"]["results"],
                         [{"DATETIMEID": "2026-10-27 14:00:00", "MGW": "MGW1", "MGW Rejections": 0.1}])
        self.assertEqual(out["MGW_CPU_IL"]["results"],
                         [{"DATETIMEID": "2026-10-27 14:00:00", "MGW": "MGW1", "PMPROCESSORLOAD": 20.0}])
        self.assertEqual([q["query"] for q in resp.data["queries"]], ["MGW_NBREJ_IL", "MGW_CPU_IL"])
        self.assertNotIn("results", resp.data["queries"][0])  # raw rows only with include_data
        self.assertNotIn("results", resp.data["kpis"][0])     # values live in outputs
        csv = pd.read_csv(out["MGW_NBREJ_IL"]["csv"])
        self.assertEqual(list(csv.columns), ["DATETIMEID", "MGW", "MGW Rejections"])
        self.assertEqual(csv.iloc[0].tolist(), ["2026-10-27 14:00:00", "MGW1", 0.1])
        self.assertTrue(out["MGW_NBREJ_IL"]["csv"].endswith("MGW_NBREJ_IL_KPI.csv"))
        self.assertTrue(os.path.exists(os.path.join(resp.data["output_dir"], "MGW_NBREJ_IL.csv")))

    def test_object_level(self):
        resp = self._get({"kpis": "MGW_NB_ReJ", "level": "object"})
        table = resp.data["outputs"][0]
        self.assertEqual(table["columns"], ["DATETIMEID", "MGW", "VMGW", "MGW Rejections"])
        self.assertEqual([(r["VMGW"], r["MGW Rejections"]) for r in table["results"]],
                         [("a", 0.01), ("b", 0.11)])

    def test_empty_result_is_success_with_no_rows(self):
        resp = self._get({"kpis": "SBG CFR"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["kpis"][0]["status"], "SUCCESS")
        table = resp.data["outputs"][0]
        full = ["DATETIMEID", "NEID", "sbgSipIngressInitRegThrottlingRej",
                "sbgSipInitRegThrottlingRejectedCnt", "SBG CFR", "SBG CDR"]
        self.assertEqual(table["columns"], full)
        self.assertEqual(table["results"], [])
        self.assertEqual(list(pd.read_csv(table["csv"]).columns), full)

    def test_one_kpi_returns_the_whole_query_table(self):
        t = "2026-10-27 14:00:00.0"
        self.tables["DC_E_IMSSBG_SIP_RAW"] = pd.DataFrame(
            [[t, "SBG1", 2, 90, 1, 80, 15, 3, 1, 100],
             [t, "SBG1", 0, 98, 0, 85, 10, 2, 0, 100]],
            columns=["DATETIMEID", "NEID", "sbgSipChallengedIncInvSessions",
                     "sbgSipNetIncSessionEstabNetworkSuccess", "sbgSipInitiatedSessionRelease",
                     "sbgSipSessionIncNormalRelease", "sbgSipSessionOutNormalRelease",
                     "sbgSipIngressInitRegThrottlingRej", "sbgSipInitRegThrottlingRejectedCnt",
                     "SBGSIPTOTALINCSESSSETUPS"])
        resp = self._get({"kpis": "SBG CFR", "hours": "1"})
        self.assertEqual(resp.status_code, 200, resp.data)
        table = resp.data["outputs"][0]
        cfr = round(100 * (1 - 188 / (200 - 2)), 6)
        cdr = round(100 - 100 * (165 + 25) / (165 + 25 + 1), 6)
        self.assertEqual(table["results"], [{
            "DATETIMEID": "2026-10-27 14:00:00", "NEID": "SBG1",
            "sbgSipIngressInitRegThrottlingRej": 5.0, "sbgSipInitRegThrottlingRejectedCnt": 1.0,
            "SBG CFR": cfr, "SBG CDR": cdr}])
        csv = pd.read_csv(table["csv"])
        self.assertEqual(list(csv.columns), list(table["results"][0]))

    def test_bad_params(self):
        self.assertEqual(self._get({"kpis": "NOT_A_KPI"}).status_code, 400)
        self.assertEqual(self._get({"level": "cell"}).status_code, 400)

    def test_query_without_kpi_still_runs(self):
        resp = self._get({"kpis": "MGW_LU_IL"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["kpis"], [])
        self.assertEqual(resp.data["outputs"], [])
        self.assertEqual(resp.data["queries"][0]["query"], "MGW_LU_IL")

    def test_connection_failure_fails_kpis(self):
        from ..views import Voice_KPI_Eniq as v
        request = APIRequestFactory().get("/callflow/api/voice-kpi-eniq", {"kpis": "MGW_CPU"})
        with override_settings(OUTPUT_DIR=self.tmp), \
                mock.patch.object(v, "EniqConnection", side_effect=RuntimeError("refused")), \
                mock.patch.object(v, "CONNECT_BACKOFF_S", (0, 0, 0)):
            resp = v.SybaseQueryView.as_view()(request)
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.data["kpis"][0]["status"], "FAILED")
        self.assertIn("refused", resp.data["kpis"][0]["error"])
        self.assertEqual(resp.data["outputs"][0]["status"], "FAILED")
        self.assertIsNone(resp.data["outputs"][0]["csv"])
