"""
Which Smartcare queries MI runs, the fallback rules, and which columns each
response metric needs. Query sets and fallback order are the ones the
Prototype code used.
"""
from collections import OrderedDict

from . import config
from .Queries import data_queries, voice_queries

DATA = "Data"
VOICE = "Voice"


def query_set(complain_type):
    """OrderedDict name -> SQL template, in the original execution order."""
    if complain_type == VOICE:
        queries = voice_queries.smartcare_Voice_QUERIES
    elif complain_type == DATA:
        queries = data_queries.smartcare_Data_QUERIES
    else:
        raise ValueError(f"Unknown complain_type {complain_type!r}")
    queries = OrderedDict(queries)
    if complain_type == DATA and config.skip_ineffective_queries():
        for name in INEFFECTIVE_DATA_QUERIES:
            queries.pop(name, None)
    return queries


# Columns of smartcare_nodes_REG_Voice (IMPU_TEL_URI1, p_cscf_Name, ...) are
# not used by any Data metric.
INEFFECTIVE_DATA_QUERIES = ("smartcare_nodes_REG_Voice",)

# (column, query name, template) - checked in this order, as before.
FALLBACKS = (
    ("Voice_Data_Flag", "SCF_CCI_U_T_Data", data_queries.SCF_CCI_U_T_Data),
    ("DRA_NAME", "SCF_CCI_U_T_Data", data_queries.SCF_CCI_U_T_Data),
    ("VPCRF_NAME", "SCF_CCI_U_T_Data", data_queries.SCF_CCI_U_T_Data),
    ("OCS_NAME", "SCF_CCI_U_T_Data", data_queries.SCF_CCI_U_T_Data),
    ("TAC_DECIMAL", "smartcare_nodes_S1mme", data_queries.smartcare_nodes_S1mme),
    ("MME_Name", "smartcare_nodes_S1mme", data_queries.smartcare_nodes_S1mme),
)


def fallbacks_are_ineffective(complain_type):
    """
    Under the legacy Voice IMS filter (Voice_Data_Flag == 'Voice') no fallback
    can change the Voice response: SCF only ever yields 'Data', and TAC/MME are
    not Voice metrics. Used only when MI_SKIP_INEFFECTIVE_QUERIES is on.
    """
    return (complain_type != DATA and config.voice_ims_filter() == "legacy"
            and config.skip_ineffective_queries())


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


def voice_flag_column():
    return "Voice_Data_Flag" if config.voice_ims_filter() == "legacy" else "Service_flag"


def voice_metrics():
    flag = voice_flag_column()
    return OrderedDict([
        ("Suspected_Sites", VOICE_BASE),
        ("Suspected_Cells", VOICE_BASE),
        ("Cluster_Counts", VOICE_BASE + ("CLUSTER",)),
        ("Region_Counts", VOICE_BASE + ("REGION",)),
        ("Sub_Region_Counts", VOICE_BASE + ("SUBREGION",)),
        ("CSCF_Name", ("p_cscf_Name", "IMPU_TEL_URI1")),
        ("AS_Name", ("AS_NAME", "IMPU_TEL_URI1")),
        ("MSC_Name", ("MSC_NAME", "MSISDN")),
        ("DRA_NAME", ("MSISDN", flag, "DRA_NAME")),
        ("OCS_NAME", ("MSISDN", flag, "OCS_NAME")),
        ("PCRF", ("MSISDN", flag, "VPCRF_NAME")),
    ])
