# callflow/services/smartcare_service.py
import logging
from django.core.cache import cache
from django.conf import settings
import pandas as pd
import datetime
from datetime import timedelta

from callflow.core.utils.file_cache import make_smartcare_cache_key
from ..utils.connections import smartcareConnection
from ..constants.queries import smartcare_nodes_QUERIES_data, smartcare_nodes_Voice , smartcare_rate_QUERIES_data
from ..utils.data_utils import process_sitename ,convert_java_types, safe_unique
from .smartcare_query_runner import (
    execute_smartcare_queries,
    calculate_tablename,
    format_sql_in,
)
from callflow.core.utils.data_utils import process_sitename
from ..utils.java_sanitize import sanitize_for_cache
logger = logging.getLogger(__name__)
def smartcare_Validation_Inputs(msisdn_list,startdate,enddate):
    errors = []

    # ---------- Time validation ----------
    if enddate - startdate > timedelta(minutes=420):
        errors.append("Time_Stamp should not exceed 30 min")

    # ---------- MSISDN count validation ----------
    if len(msisdn_list) < 1:
        errors.append("MSISDN list should contain at least 1 MSISDNs")

    # ---------- MSISDN format validation ----------
    for msisdn in msisdn_list:
        msisdn_str = str(msisdn)
        if (
            not msisdn_str.isdigit()
            or len(msisdn_str) != 10
            or not msisdn_str.startswith(("10", "11", "12","15"))
        ):
            errors.append(
                f"Invalid MSISDN: {msisdn}. "
                "MSISDN must start with '10' and be exactly 10 digits"
            )

    # ---------- Final result ----------
    if errors:
        raise ValueError("; ".join(errors))
    
    return True

def smartcareNodesQueries(
    msisdn_list,
    startdate : datetime.datetime,
    enddate : datetime.datetime,
    logs_file : str,
    complain_type=None,
):
  
    if complain_type == 'Voice':
        queries_dict = smartcare_nodes_Voice
        query_category = "voice"
    else:  # Default to Data
        queries_dict = smartcare_nodes_QUERIES_data
        query_category = "data"
    
    tablename = calculate_tablename(startdate)   
    msisdn_str = format_sql_in(msisdn_list, quote=True)
    params = {
        'start_date': str(startdate),
        'end_date': str(enddate),
        'tablename': tablename,
        'msisdn_tuple': msisdn_str,
    }
    logger.info(f"Running {query_category.upper()} queries for {len(msisdn_list)} MSISDNs")
    # ==========================================
    # تقسيم النطاق الزمني لأجزاء أصغر (نفس أسلوب smartcareRateQueries)
    # ==========================================
    CHUNK_HOURS  = 2
    chunks = []
    current = startdate
    while current < enddate:
        chunk_end = min(current + datetime.timedelta(hours=CHUNK_HOURS), enddate)
        chunks.append((current, chunk_end))
        current = chunk_end

    logger.info(f"[SMARTCARE NODES] split into {len(chunks)} chunks of {CHUNK_HOURS}h each")

    all_chunks_data = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        chunk_params = {
            **params,
            "start_date": str(chunk_start),
            "end_date": str(chunk_end),
        }
        logger.info(f"[SMARTCARE NODES] chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")

        chunk_data = execute_smartcare_queries(
            queries_dict=queries_dict,
            params=chunk_params,
            log_path=logs_file,
        )
        if not chunk_data.empty:
            all_chunks_data.append(chunk_data)

    raw_df = pd.concat(all_chunks_data, ignore_index=True) if all_chunks_data else pd.DataFrame()
    logger.info(f"[SMARTCARE NODES] combined total -> {len(raw_df)} rows across {len(chunks)} chunks")

    return raw_df, query_category


def smartcareRateQueries(
    nodes_data: pd.DataFrame,
    startdate : datetime.datetime,
    enddate : datetime.datetime,
    logs_file : str,
    complain_type=None,
):
  
    if complain_type != "Data":
        return pd.DataFrame()

    if nodes_data is None or nodes_data.empty:
        logger.warning("[SMARTCARE RATE] No nodes data available")
        return pd.DataFrame()

    # ==========================================
    # 1. Select the rate queries dictionary
    # ==========================================

    queries_dict = smartcare_rate_QUERIES_data

    # ==========================================
    # 2. Calculate table name
    # ==========================================

    tablename = calculate_tablename(startdate)

    # ==========================================
    # 3. Get required values from nodes_data
    # ==========================================
    print('Nodes Data Here to test-----')
    print( nodes_data.columns)
    msisdn = (
        nodes_data["MSISDN"]
        .dropna()
        .unique()
        .tolist()
    )
    mme_ids = (
        nodes_data["MME_ID"]
        .dropna()
        .unique()
        .tolist()
    )

    hss_ids = (
        nodes_data["HSS_ID"]
        .dropna()
        .unique()
        .tolist()
    )

    ggsn_ids = (
        nodes_data["GGSN_ID"]
        .dropna()
        .unique()
        .tolist()
    )

    aaa_client_ips = (
        nodes_data["AAA_CLIENT_IP"]
        .dropna()
        .unique()
        .tolist()
    )

    aaa_server_ips = (
        nodes_data["AAA_SERVER_IP"]
        .dropna()
        .unique()
        .tolist()
    )

    # ==========================================
    # 4. Build params for ALL rate queries
    # ==========================================

    params = {
        "start_date": str(startdate),
        "end_date": str(enddate),
        "tablename": tablename,

        "MME_ID": format_sql_in(
            mme_ids,
            quote=False
        ),

        "HSS_ID": format_sql_in(
            hss_ids,
            quote=False
        ),

        "GGSN_ID": format_sql_in(
            ggsn_ids,
            quote=False
        ),

        "AAA_CLIENT_IP": format_sql_in(
            aaa_client_ips,
            quote=True
        ),

        "AAA_SERVER_IP": format_sql_in(
            aaa_server_ips,
            quote=True
        ),
    }

    logger.info(
        f"[SMARTCARE RATE] Running "
        f"{len(queries_dict)} rate queries"
    )

    # ==========================================
    # 5. Execute exactly like Nodes
    # ==========================================

    CHUNK_HOURS = 2
    chunks = []
    current = startdate
    while current < enddate:
        chunk_end = min(current + datetime.timedelta(hours=CHUNK_HOURS), enddate)
        chunks.append((current, chunk_end))
        current = chunk_end

    logger.info(f"[SMARTCARE RATE] split into {len(chunks)} chunks of {CHUNK_HOURS}h each")

    all_chunks_data = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        chunk_params = {
            **params,
            "start_date": str(chunk_start),
            "end_date": str(chunk_end),
        }
        logger.info(f"[SMARTCARE RATE] chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")

        chunk_data = execute_smartcare_queries(
            queries_dict=queries_dict,
            params=chunk_params,
            log_path=logs_file,
        )
        if not chunk_data.empty:
            all_chunks_data.append(chunk_data)

    rate_data = pd.concat(all_chunks_data, ignore_index=True) if all_chunks_data else pd.DataFrame()
    logger.info(f"[SMARTCARE RATE] combined total -> {len(rate_data)} rows across {len(chunks)} chunks")

    return rate_data

def process_smartcare_data(nodes_data: pd.DataFrame, complain_type: str, ts: str, log_path: str):
    """Process data differently based on complain_type"""
    
    query_category = "voice" if complain_type == "Voice" else "data"

    if nodes_data.empty:
        return {
            "query_category": query_category,
            "processed_data": pd.DataFrame(),
            "metrics": {},
            "packetNodes": {},
            "suspected_cells_raw": None,
        }

    nodes_data = convert_java_types(nodes_data)
    metrics = {}
    packetNodes = {}
    suspected_cells_raw = {}
    # Common column fix
    if 'SGW_SGSN_NAME' in nodes_data.columns and 'SGSN_SGW_NAME' in nodes_data.columns:
        nodes_data['SGW_SGSN_NAME'] = nodes_data['SGW_SGSN_NAME'].combine_first(
            nodes_data['SGSN_SGW_NAME']
        )
        nodes_data.drop(columns=['SGSN_SGW_NAME'], inplace=True)

    if complain_type == "Voice":
        # ==================== VOICE PROCESSING ====================
        # TODO: Implement your real voice aggregation here
        data = nodes_data.copy()  

    else:
        # ==================== DATA PROCESSING (Existing) ====================
        
        data, site_counts, cell_counts, suspected_sgw  = smartcareOutput_Aggregated(
            nodes_data, log_path, complain_type, ts
        )
        # logger.info(f"cellsName from process_smartcare_data are {data['cell_name']} ")
        
        packetNodes = packet_nodes(nodes_data, ts)
        metrics = calculate_subs_per_cell_site_handset(nodes_data, ts)

    return {
        "query_category": query_category,
        "processed_data": data,
        "metrics": metrics,
        "packetNodes": packetNodes,
        "suspected_cells_raw": suspected_cells_raw,
    }
def fetch_smartcare_nodes_data(
    msisdn_list,
    start_dt: datetime.datetime ,
    end_dt: datetime.datetime ,
    imsi: str = '',
    complain_type: str = 'Data',
    log_path: str = '',
    view: str = "default",
    ts: str = '',
    timeout_seconds: int = 36000
):
    if not all([start_dt, end_dt]):
        raise ValueError("start_dt and end_dt are required")

    cache_key = make_smartcare_cache_key(msisdn_list, start_dt, end_dt ,complain_type)

    cached_result = cache.get(cache_key)
    if cached_result is not None:
        logger.info(f"[CACHE HIT] Smartcare nodes - key: {cache_key}")
        return (cached_result)

    logger.info(f"[CACHE MISS] Fetching Smartcare nodes - key: {cache_key}")

    try:
        
        nodes_data, query_catagory = smartcareNodesQueries(
            msisdn_list,
            startdate=start_dt,
            enddate=end_dt,
            logs_file=log_path,
            complain_type=complain_type,
        )

        if nodes_data is None or nodes_data.empty:
            raise ValueError("No Smartcare data returned")
        
        # rate_data = smartcareRateQueries(
        #     nodes_data,
        #     startdate=start_dt,
        #     enddate=end_dt,
        #     logs_file=log_path,
        #     complain_type=complain_type,
        # )
        # print('Data Which Returned from Rate Data')
        # print(rate_data.columns.tolist())
        
        # logger.info(
        #     f"[SMARTCARE - RateData] "
        #     f"columns={rate_data.columns.tolist()}"
        # )
        logger.info(
            f"[SMARTCARE] nodes_data shape={nodes_data.shape}"
        )

        # logger.info(
        #     f"[SMARTCARE] rate_data shape={rate_data.shape}"
        #     if isinstance(rate_data, pd.DataFrame)
        #     else "[SMARTCARE] rate_data is not DataFrame"
        # )
        result = process_smartcare_data(nodes_data, complain_type, ts, log_path)
        # result["rate_data"] = rate_data

        if ts:
            nodes_data.to_excel(settings.OUTPUT_DIR / f"sc_nodes_{view}_{query_catagory}_filter_{ts}.xlsx", index=False)
            # rate_data.to_excel(settings.OUTPUT_DIR / f"sc_rate_{view}_{query_catagory}_filter_{ts}.xlsx", index=False)
        
        result = sanitize_for_cache(result)
        cache.set(cache_key, result, timeout=timeout_seconds)
        logger.info(f"Smartcare data cached for {timeout_seconds}s - key: {cache_key}")

        return result

    except Exception as e:
        logger.exception("Error in fetch_smartcare_nodes_data")
        raise
def calculate_top_remedy_with_subs(metrics: dict, remedy_output: dict):
    """This function is DATA-SPECIFIC only"""
    if not remedy_output:
        return {}

    subs_data = metrics.get('subs_per_site', [])
    
    if isinstance(subs_data, int):
        subs_map = {"subs_per_site": subs_data}
    elif isinstance(subs_data, dict):
        subs_map = {subs_data.get("label"): subs_data.get("value")}
    else:
        subs_map = {
            item.get("label"): item.get("value")
            for item in subs_data
            if isinstance(item, dict) and "label" in item
        }

    for remedy_key, remedy_val in remedy_output.items():
        subs_count = sum(
            float(value)
            for label, value in subs_map.items()
            if label
            and str(label) in str(remedy_key)
            and value is not None
        )
        remedy_val["subs_count"] = subs_count
    top_10_remedy_output = dict(
        sorted(
            remedy_output.items(),
            key=lambda item: item[1].get("subs_count", 0),
            reverse=True
        )[:10]
    )
    return top_10_remedy_output
def smartcareOutput_Aggregated(data, logs_file, complain_type, timestamp): 
    suspected_cells = [] 
    suspected_sites = [] 
    suspected_sgw = {} 
    suspected_cells_raw = [] 
    cell_counts = {} 
    site_counts = {} 
    try: 
        if data is None or data.empty: 
            return data, site_counts, cell_counts, suspected_sgw 
        # Only SGW is required now; cell_name is optional 
        if 'SGW_SGSN_NAME' not in data.columns: 
            raise KeyError("Missing required column: SGW_SGSN_NAME") 
        data = data.copy() 
        data['SGW_SGSN_NAME'] = data['SGW_SGSN_NAME'].astype(str).str.strip() 
        # ---------- Optional cell_name handling ---------- 
        has_cell_name = 'cell_name' in data.columns 
        if has_cell_name: 
            data = data.dropna(subset=['cell_name']) 
            data['cell_name'] = data['cell_name'].astype(str).str.strip() 
            # Drop rows where cell_name became empty string after strip 
            data = data[data['cell_name'] != ''] 
            if not data.empty: 
                cell_counts = data['cell_name'].value_counts().to_dict() 
                suspected_cells_raw = data['cell_name'].unique().tolist() 
                # suspected_cells = tuple(suspected_cells_raw) 
                # Optional site extraction (only if cell_name exists) 
                try: 
                    data = process_sitename(data, 'cell_name', logs_file) 
                    if 'modified_site_name' in data.columns: 
                        site_counts = data['modified_site_name'].value_counts().to_dict() 
                    else: 
                        with open(logs_file, 'a') as logs_f: 
                            logs_f.write('modified_site_name column missing after process_sitename\n') 
                except Exception as e: 
                    with open(logs_file, 'a') as logs_f: 
                        logs_f.write(f'site processing error: {str(e)}\n') 
        if data.empty: 
            return data, site_counts, cell_counts, suspected_sgw 
        # ---------- SGW / PGW processing (always) ---------- 
        try: 
            data.rename(columns={'SGW_SGSN_NAME': 'SGW_PGW_NAME'}, inplace=True) 
            data['SGW_SGSN_NAME'] = data['SGW_PGW_NAME']  # Keep original for reference
        except Exception as e: 
            with open(logs_file, 'a') as logs_f: 
                logs_f.write(f'SGW rename error: {str(e)}\n') 
        if 'SGW_PGW_NAME' in data.columns: 
            data['SGW_PGW_NAME'] = ( 
                data['SGW_PGW_NAME'] 
                .str.replace('_vSGW-vPGW_', '', regex=False) 
                .str.replace('_SGW-PGW_', '', regex=False) 
            ) 
            suspected_sgw = data['SGW_PGW_NAME'].value_counts().to_dict() 
    except Exception as e: 
        with open(logs_file, 'a') as logs_f: 
            logs_f.write('Smartcare Output error : ' + str(e) + '\n') 
    # return data, site_counts, cell_counts, suspected_sgw, suspected_cells_raw 
    return data, site_counts, cell_counts, suspected_sgw 

def packet_nodes( nodesdata, ts) -> dict:
    nodesdata = nodesdata.copy()
    nodesdata = nodesdata.replace(r'^\s*$', pd.NA, regex=True)
    result = {}
    # Define node type to possible column names
    mappings = {
        'SGSN': ['SGSN_MME_NAME'],
        'MME': ['MME_Name'],
        'GGSN': ['SGW_SGSN_NAME', 'SGSN_SGW_NAME', 'GGSN_PGW_NAME'],
        'HSS': ['HSS_Name'],
        'PCRF': ['VPCRF_NAME'],
        'OCC': ['OCS_NAME'],
        'DSC': ['DRA_NAME'],
        'DNS': ['DNS_Server_Name'],
    }
    for node_type, possible_cols in mappings.items():
        # Find the first available column for this node type
        col = next((c for c in possible_cols if c in nodesdata.columns), None)
        if not col:
            continue
        df = nodesdata[nodesdata[col].notna() & nodesdata['MSISDN'].notna()]
        if df.empty:
            continue
        counts = df.groupby(col)['MSISDN'].nunique().to_dict()
        result[node_type] = [{"label": k, "value": v} for k, v in counts.items()]

    return result
        

def calculate_subs_per_cell_site_handset(nodesdata, ts: str) -> dict: 
    """ 
    Calculate subscribers per site / cell / handset type. 
    cell_name is optional — not every nodesdata payload contains it. 
    """ 
    try: 
        logger.info('data Columns') 
        logger.info(nodesdata.columns) 
        if nodesdata is None or nodesdata.empty: 
            return { 
                'subs_per_cell': [], 
                'subs_per_site': [], 
                'subs_per_handset_type': [], 
            } 
        nodesdata = nodesdata.copy() 
        # ---------- Optional cell_name / site handling ---------- 
        subs_per_cell = [] 
        subs_per_site_list = [] 
        has_cell_name = 'cell_name' in nodesdata.columns 
        if has_cell_name: 
            # Only derive site name when we have cell_name 
            if 'modified_site_name' not in nodesdata.columns: 
                try: 
                    nodesdata = process_sitename( 
                        nodesdata, 
                        'cell_name', 
                        settings.OUTPUT_DIR / "logs" / "metrics.log" 
                    ) 
                except Exception as e: 
                    logger.warning(f"process_sitename failed (cell_name present but processing failed): {e}") 
            if 'modified_site_name' in nodesdata.columns: 
                nodesdata.loc[:, 'modified_site_name'] = nodesdata['modified_site_name'].replace('', None) 
                subs_per_site = ( 
                    nodesdata.groupby('modified_site_name')['MSISDN'] 
                    .nunique() 
                    .dropna() 
                    .to_dict() 
                ) 
                subs_per_site_list = [ 
                    {"label": site, "value": int(count)} 
                    for site, count in subs_per_site.items() 
                    if pd.notna(site) 
                ] 
            # Cell-level counts (only when column exists) 
            subs_per_cell_raw = ( 
                nodesdata.groupby('cell_name')['MSISDN'] 
                .nunique() 
                .dropna() 
                .to_dict() 
            ) 
            subs_per_cell = [ 
                {"label": cell, "value": int(count)} 
                for cell, count in subs_per_cell_raw.items() 
                if pd.notna(cell) and str(cell).strip() != '' 
            ] 
        else: 
            logger.info("cell_name column missing — skipping cell/site metrics") 
        # ---------- Handset type (always attempted) ---------- 
        subs_per_handset_type = [] 
        if 'IMEI_Brand' in nodesdata.columns and 'MSISDN' in nodesdata.columns: 
            handset_counts = ( 
                nodesdata.groupby('IMEI_Brand')['MSISDN'] 
                .nunique() 
                .dropna() 
                .to_dict() 
            ) 
            subs_per_handset_type = [ 
                {"label": brand, "value": int(count)} 
                for brand, count in handset_counts.items() 
                if pd.notna(brand) 
            ] 
        logger.info( 
            f"Basic Smartcare metrics calculated: " 
            f"{len(subs_per_site_list)} sites, " 
            f"{len(subs_per_cell)} cells, " 
            f"{len(subs_per_handset_type)} handset types" 
        ) 
        return { 
            'subs_per_cell': subs_per_cell, 
            'subs_per_site': subs_per_site_list, 
            'subs_per_handset_type': subs_per_handset_type, 
        } 
    except Exception as e: 
        logger.exception("Error calculating subs per site/cell/handsettype") 
        return { 
            'subs_per_cell': [], 
            'subs_per_site': [], 
            'subs_per_handset_type': [], 
        }  

def voice_specific_aggregation(nodesdata):
    pass