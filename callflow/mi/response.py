"""
API response bodies. Keys, order and shapes are exactly what the Prototype
view returned; nothing is added to the body (diagnostics go to the job record
and response headers, so existing consumers are unaffected).
"""


def data_body(m, hub):
    return {
        'suspected_sites': m['suspected_sites'],
        'suspected_cells_raw': m['suspected_cells_raw'],
        'suspected_sgws': m['suspected_sgws'],
        'HUB_Counts': hub,
        'rat_counts': m['rat_counts'],
        'DRA_NAME': m['DRA_NAME'],
        'OCS_NAME': m['OCS_NAME'],
        'VPCRF_NAME': m['VPCRF_NAME'],
        'region_counts': m['region_counts'],
        'subregion_counts': m['subregion_counts'],
        'sgsn_counts': m['sgsn_counts'],
        'tac_counts': m['tac_counts'],
        'mme_counts': m['mme_counts'],
    }


def voice_body(m, hub):
    return {
        'Suspected_Sites': m['Suspected_Sites'],
        'Suspected_Cells': m['Suspected_Cells'],
        'Cluster_Counts': m['Cluster_Counts'],
        'Region_Counts': m['Region_Counts'],
        'Sub_Region_Counts': m['Sub_Region_Counts'],
        'HUB_Counts': hub,
        'CSCF_Name': m['CSCF_Name'],
        'AS_Name': m['AS_Name'],
        'MSC_Name': m['MSC_Name'],
        'DRA_NAME': m['DRA_NAME'],
        'OCS_NAME': m['OCS_NAME'],
        'PCRF_Name': m['PCRF_NAME'],
        'MME_Name': m['MME_Name'],
    }


def error_body(message):
    return {'error': message}
