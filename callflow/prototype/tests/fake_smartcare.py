"""
In-memory stand-in for the Smartcare JDBC cursor. Each query is recognised by
the table it reads from; rows come from a fixture dict. Counts executions and
connection open/close so tests can assert on Smartcare load.
"""
import re
import threading

# (regex on the SQL text, fixture key) - first match wins, order matters
ROUTES = [
    (r"DETAIL_CDR_S1MME_", "s1mme"),
    (r"detail_cdr_gbiups_", "gbiups"),
    (r"DETAIL_CDR_SGiDiameter_.*Credit_control|Credit_control.*DETAIL_CDR_SGiDiameter_", "scf"),
    (r"tdr_ims_register_sip_comm_", "reg"),
    (r"(?i)DRA_NAME.*detail_cdr_sgidiameter_", "dra_voice"),
    (r"(?i)OCS_NAME.*detail_cdr_sgidiameter_", "ocs_voice"),
    (r"(?i)VPCRF_NAME.*detail_cdr_sgidiameter_", "pcrf_voice"),
    (r"CDR_AIU_MOC_", "cs_moc"),
    (r"CDR_AIU_MTC_", "cs_mtc"),
    (r"CDR_IMS_MO_CALL_LEG_SIP_", "volte_mo"),
    (r"CDR_IMS_MT_CALL_LEG_SIP_", "volte_mt"),
    (r"DETAIL_CDR_SGS_", "sgs"),
    (r"TDR_IMS_HSS_TRANS_", "hss"),
]

MSISDNS = ["1012345678", "1112345678", "1212345678", "1512345678"]


def data_fixture(scf_rows=True, s1mme_rows=True):
    s1mme_cols = ["MSISDN", "TAC_DECIMAL", "cell_name", "CLUSTER", "SUBREGION", "REGION",
                  "MME_ID", "MME_Name", "SGW_DATA_IP", "SGW_SGSN_NAME", "PROC_STARTTIME", "RAT", "TAC"]
    s1mme = [
        ["1012345678", "2101", "CAI_LCAI0123_1", "CL1", "Giza", "Cairo", 1, "MME01", "10.0.0.1", "SGW_vSGW-vPGW_01", 1700000000, "4G", 2101],
        ["1012345678", "2101", "CAI_LCAI0123_2", "CL1", "Giza", "Cairo", 1, "MME01", "10.0.0.1", "SGW_vSGW-vPGW_01", 1700000010, "4G", 2101],
        ["1112345678", "2102", "ALX_LALX45_1", "CL2", "Alex", "Delta", 2, "MME02", "10.0.0.2", "SGW_SGW-PGW_02", 1700000020, "4G", 2102],
        ["1212345678", "2103", "KEN_LKenana7_1", "CL3", "Upper", "South", 2, "MME02", "10.0.0.2", "SGW_SGW-PGW_02", 1700000030, "4G", 2103],
        ["1512345678", "2104", "NOSITE", "CL4", "Upper", "South", 3, "MME03", "10.0.0.3", "SGW_X", 1700000040, "4G", 2104],
        ["1512345678", None, "", "CL4", "Upper", "South", 3, None, "10.0.0.3", "SGW_X", 1700000050, "4G", None],
    ] if s1mme_rows else []
    gbiups_cols = ["MSISDN", "CELL", "cell_name", "CLUSTER", "SUBREGION", "REGION", "SGSN_ID",
                   "SGW_SGSN_NAME", "PROC_STARTTIME", "RAT", "MS_IP"]
    gbiups = [
        ["1112345678", "c1", "ALX_LALX45_3", "CL2", "Alex", "Delta", 9, "SGSN_9", 1700000100, "3G", "10.1.1.1"],
    ]
    scf_cols = ["myday", "MSISDN", "AAA_CLIENT_IP", "AAA_SERVER_IP", "DRA_ID", "VPCRF_OCS_ID",
                "GGSN_PGW_ID", "VPCRF_NAME", "OCS_NAME", "DRA_NAME", "GGSN_PGW_NAME", "Voice_Data_Flag"]
    scf = [
        ["2026/10/15 10:00", "1012345678", "1.1.1.1", "2.2.2.2", 6400001, 400028, 5, "PCRF_A", "OCS_A", "DRA_A", "PGW_A", "Data"],
        ["2026/10/15 10:05", "1112345678", "1.1.1.1", "2.2.2.2", 6400001, 400027, 5, "PCRF_B", "OCS_A", "DRA_A", "PGW_A", "Data"],
    ] if scf_rows else []
    reg_cols = ["myday", "IMPU_TEL_URI1", "p_cscf_Name", "I_cscf_Name", "S_cscf_Name", "AS_NAME"]
    reg = [["2026/10/15 10:00", "1012345678", "PCSCF1", "ICSCF1", "SCSCF1", "AS1"]]
    return {
        "s1mme": (s1mme_cols, s1mme),
        "gbiups": (gbiups_cols, gbiups),
        "scf": (scf_cols, scf),
        "reg": (reg_cols, reg),
    }


def voice_fixture():
    sgid = lambda name: (["myday", "MSISDN", "Service_flag", name, "RAT", "Voice_Data_Flag"], [
        ["2026/10/15 10:00", "1012345678", "x", f"{name}_1", "4G", "Voice"],
        ["2026/10/15 10:00", "1112345678", "x", f"{name}_2", "4G", "Voice"],
        ["2026/10/15 10:00", "1212345678", "x", f"{name}_2", "4G", "Data"],
    ])
    cs_cols = ["myday", "MSISDN", "MSC_NAME", "CELL_NAME", "CLUSTER", "SUBREGION", "REGION"]
    return {
        "dra_voice": sgid("DRA_NAME"),
        "ocs_voice": sgid("OCS_NAME"),
        "pcrf_voice": sgid("VPCRF_NAME"),
        "cs_moc": (cs_cols, [["2026/10/15 10:00", "1012345678", "MSC1", "CAI_LCAI0123_1", "CL1", "Giza", "Cairo"],
                             ["2026/10/15 10:00", "1112345678", "MSC2", "ALX_LALX45_1", "CL2", "Alex", "Delta"]]),
        "cs_mtc": (cs_cols, [["2026/10/15 10:00", "1212345678", "MSC2", "ALX_LALX45_2", "CL2", "Alex", "Delta"]]),
        "reg": (["myday", "IMPU_TEL_URI1", "p_cscf_Name", "I_cscf_Name", "S_cscf_Name", "AS_NAME"],
                [["2026/10/15 10:00", "1012345678", "PCSCF1", "ICSCF1", "SCSCF1", "AS1"],
                 ["2026/10/15 10:00", "1112345678", "PCSCF1", "ICSCF1", "SCSCF1", "AS2"]]),
        "volte_mo": (["myday", "MSISDN", "NODE", "CLUSTER", "SUBREGION", "REGION"], []),
        "volte_mt": (["myday", "MSISDN", "NODE", "CLUSTER", "SUBREGION", "REGION"], []),
        "sgs": (["myday", "MSISDN"], []),
        "hss": (["myday", "MSISDN"], []),
    }


class FakeSmartcare:
    """Factory: call .connect(logs_file) where the code calls smartcareConnection."""

    def __init__(self, fixture, fail=None):
        self.fixture = fixture
        self.fail = dict(fail or {})        # fixture key -> number of failures to inject
        self.executions = []                # fixture keys, in order
        self.opened = 0
        self.closed = 0
        self.lock = threading.Lock()

    def route(self, sql):
        for pattern, key in ROUTES:
            if re.search(pattern, sql, re.S):
                return key
        raise AssertionError("unrouted query: " + sql[:120])

    def connect(self, logs_file=None):
        with self.lock:
            self.opened += 1
        return FakeCursor(self)

    def count(self, key):
        return self.executions.count(key)


class _Conn:
    def __init__(self, owner):
        self.owner = owner
        self.is_closed = False

    def close(self):
        if not self.is_closed:
            self.is_closed = True
            with self.owner.lock:
                self.owner.closed += 1


class FakeCursor:
    def __init__(self, owner):
        self.owner = owner
        self._connection = _Conn(owner)
        self.description = None
        self._rows = []

    def execute(self, sql):
        if self._connection.is_closed:
            raise RuntimeError("connection closed")
        key = self.owner.route(sql)
        with self.owner.lock:
            self.owner.executions.append(key)
            if self.owner.fail.get(key, 0) > 0:
                self.owner.fail[key] -= 1
                raise RuntimeError(f"injected failure for {key}")
        cols, rows = self.owner.fixture.get(key, (["MSISDN"], []))
        self.description = [(c,) for c in cols]
        self._rows = [tuple(r) for r in rows]

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass
