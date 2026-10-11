# callflow/core/services/scf_kpi_service.py
import datetime
import logging
import hashlib
from django.core.cache import cache
from django.conf import settings
import pandas as pd
from ..constants.smartcare_failure_queries import scf_kpi_queries
from .smartcare_query_runner import (
    execute_smartcare_queries,
    calculate_tablename,
    format_sql_in,
)
logger = logging.getLogger(__name__)

def extract_affected_nodes(nodes_data: pd.DataFrame) -> dict:
    print('nodes_data in extract_affected_nodes')
    print(nodes_data)
    if nodes_data is None:
        return {}
    cols = ['MME_ID', 'SGW_DATA_IP', 'GGSN_ID', 'HSS_ID', 'DRA_ID',
            'VPCRF_OCS_ID', 'AAA_CLIENT_IP', 'AAA_SERVER_IP']
    node_dict = {}
    for col in cols:
        if col in nodes_data.columns:
            vals = nodes_data[col].dropna().unique().tolist()
            if vals:
                node_dict[col] = vals
    return node_dict


def fetch_scf_kpi_data(
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    msisdn_list,  #Added
    nodes_data: pd.DataFrame ,
    log_path: str = '',
    timeout_seconds: int = 36000
) -> pd.DataFrame:

    msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"   #Added

    
    # Build cache key
    cache_key = f"scf_kpi_{hashlib.sha256(f'{msisdn_str}|{start_dt.date()}|{end_dt.date()}'.encode()).hexdigest()}"
    # Added msisdn_str in cache

    cached = cache.get(cache_key)
    if cached:
        logger.info(f"[CACHE HIT] SCF KPI - {cache_key}")
        return pd.read_json(cached)

    logger.info(f"[CACHE MISS] Running SCF KPI queries - {cache_key}")
    tablename = calculate_tablename(start_dt)
    if isinstance(nodes_data, str):
        try:
            nodes_data = pd.read_json(nodes_data)
        except Exception as e:
            logger.error(f"Failed to parse nodes_data JSON: {e}")
            return pd.DataFrame()
    affected = extract_affected_nodes(nodes_data)
    
    params = {
        'start_date': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'end_date': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'msisdn_tuple': msisdn_str,
        'tablename': tablename,
        'log_path': log_path,
    }
    # Added MSISDN in params and in expected_node_keys
    expected_node_keys = [
        'MME_ID', 'SGW_DATA_IP', 'GGSN_ID', 'HSS_ID', 'DRA_ID',
        'VPCRF_OCS_ID', 'AAA_CLIENT_IP', 'AAA_SERVER_IP'
    ]
    for key in expected_node_keys:
            if key in affected and affected[key]:
                params[key] = affected[key]
            else:
                params[key] = tuple()   # safe empty value
                logger.debug(f"[fetch_scf_kpi_data][{key}] not present in nodes_data → using empty tuple (no data for this node)")
    # Convert lists → SQL IN strings
    for key in list(params.keys()):
            val = params[key]
            if isinstance(val, (list, tuple)):
                quote = key in {'SGW_DATA_IP', 'AAA_CLIENT_IP', 'AAA_SERVER_IP', 'GGSN_ID'}
                params[key] = format_sql_in(val, quote=quote)

    result = execute_smartcare_queries(
        queries_dict=scf_kpi_queries,   
        params=params,
        log_path=log_path,
    )
    print(' smartcare failuer result')
    print(result)
    if not result.empty and log_path:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = settings.OUTPUT_DIR / f"scf_kpi_{ts}"

        # Excel limit = 1,048,576 rows per sheet
        EXCEL_MAX_ROWS = 1_048_576

        if len(result) > EXCEL_MAX_ROWS:
            # Use CSV (no row limit) + optional compression
            csv_path = base_path.with_suffix('.csv.gz')   # or .csv if you prefer uncompressed
            result.to_csv(csv_path, index=False, compression='gzip')
            logger.warning(
                f"SCF KPI data too large for Excel ({len(result):,} rows). "
                f"Saved as compressed CSV: {csv_path}"
            )
        else:
            # Safe to use Excel
            xlsx_path = base_path.with_suffix('.xlsx')
            result.to_excel(xlsx_path, index=False)
            logger.info(f"SCF KPI data saved to Excel: {xlsx_path}")

    if not result.empty:
        cache.set(cache_key, result.to_json(), timeout=timeout_seconds)

    return result  
        
    