"""
Which Smartcare queries MI runs for each Complain_Type (the Prototype's query
sets, unchanged), subscriber-column mapping, and which columns each response
metric needs. There are no fallback queries.
"""
from collections import OrderedDict

from . import config
from .Queries.data_queries import smartcare_Data_QUERIES
from .Queries.voice_queries import smartcare_Voice_QUERIES

DATA = "Data"
VOICE = "Voice"


def query_set(complain_type):
    """OrderedDict name -> SQL template, in the original execution order."""
    if complain_type == VOICE:
        queries = smartcare_Voice_QUERIES
    elif complain_type == DATA:
        queries = smartcare_Data_QUERIES
    else:
        raise ValueError(f"complain_type must be {VOICE} or {DATA}, not {complain_type!r}")
    queries = OrderedDict(queries)
    if complain_type == DATA and config.skip_ineffective_queries():
        for name in INEFFECTIVE_DATA_QUERIES:
            queries.pop(name, None)
    return queries


# Columns of smartcare_nodes_REG_Voice (IMPU_TEL_URI1, p_cscf_Name, ...) are
# not used by any Data metric.
INEFFECTIVE_DATA_QUERIES = ("smartcare_nodes_REG_Voice",)

# No fallback queries: each Complain_Type runs only its own query set above.
# (The Prototype re-ran SCF_CCI_U_T_Data / smartcare_nodes_S1mme for missing
#  columns, also for Voice; removed on request, 2026-10-23.)

# The subscriber number under another name. CS_MOC filters on `callerno` and
# CS_MTC on `calledno` (both are the requested MSISDNs) but neither returns a
# column called MSISDN, so before this mapping every CS row had an empty
# MSISDN: all Voice counts collapsed to 1 and HUB_Counts could not count
# anyone. VoLTE queries already alias IMPU_TEL_URI AS MSISDN.
SUBSCRIBER_COLUMNS = {
    "smartcare_network_CS_MOC": "callerno",
    "smartcare_network_CS_MTC": "calledno",
}


def normalise_subscriber_column(name, frame):
    """Return (frame, renamed_from) with the query's subscriber column named MSISDN."""
    source = SUBSCRIBER_COLUMNS.get(name)
    if (not config.map_cs_subscriber_to_msisdn() or source is None
            or source not in frame.columns or "MSISDN" in frame.columns):
        return frame, None
    return frame.rename(columns={source: "MSISDN"}), source


# Columns each response metric depends on (diagnostics + guards).
DATA_BASE = ("MSISDN", "cell_name", "SGW_SGSN_NAME")
DATA_METRICS = OrderedDict([
    ("suspected_sites", DATA_BASE),
    ("suspected_cells_raw", DATA_BASE),
    ("suspected_sgws", DATA_BASE),
    ("rat_counts", DATA_BASE + ("RAT",)),
    ("DRA_NAME", ("MSISDN", "Voice_Data_Flag", "DRA_NAME")),
    ("OCS_NAME", ("MSISDN", "Voice_Data_Flag", "OCS_NAME")),
    ("PCRF", ("MSISDN", "Voice_Data_Flag", "VPCRF_NAME")),
    ("region_counts", DATA_BASE + ("REGION",)),
    ("subregion_counts", DATA_BASE + ("SUBREGION",)),
    ("sgsn_counts", DATA_BASE),
    ("tac_counts", DATA_BASE + ("TAC_DECIMAL",)),
    ("mme_counts", DATA_BASE + ("MME_Name",)),
])

VOICE_BASE = ("MSISDN", "CELL_NAME")


def voice_metrics():
    return OrderedDict([
        ("Suspected_Sites", VOICE_BASE),
        ("Suspected_Cells", VOICE_BASE),
        ("Cluster_Counts", VOICE_BASE + ("CLUSTER",)),
        ("Region_Counts", VOICE_BASE + ("REGION",)),
        ("Sub_Region_Counts", VOICE_BASE + ("SUBREGION",)),
        ("CSCF_Name", ("p_cscf_Name", "IMPU_TEL_URI1")),
        ("AS_Name", ("AS_NAME", "IMPU_TEL_URI1")),
        ("MSC_Name", ("MSC_NAME", "MSISDN")),
        ("DRA_NAME", ("MSISDN", "DRA_NAME")),           # from smartcare_DRA_Voice, as returned
        ("OCS_NAME", ("MSISDN", "OCS_NAME")),           # from smartcare_OCS_Voice
        ("PCRF", ("MSISDN", "VPCRF_NAME")),             # from smartcare_PCRF_Voice
    ])
