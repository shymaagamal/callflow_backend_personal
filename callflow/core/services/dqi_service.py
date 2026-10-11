import datetime
import logging
import hashlib
from django.core.cache import cache
from django.conf import settings
import pandas as pd
from ..constants.DQI_queries import DQI_Queries
from ..utils.trino_client import run_trino_query
from .smartcare_query_runner import (
    execute_smartcare_queries,
    calculate_tablename,
    format_sql_in,
)
logger = logging.getLogger(__name__)

def fetch_dqi_data(
    cell_list,
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    log_path: str = '',
    timeout_seconds: int = 36000
) -> pd.DataFrame:
    
    cell_key = ",".join(sorted(str(m) for m in cell_list))

    cache_key_raw = (
        f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        f"|{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        f"|{cell_key}"
    )
    # Build cache key
    cache_key = (
        f"DQI_{hashlib.sha256(cache_key_raw.encode()).hexdigest()}"
    )
    
    cached = cache.get(cache_key)
    if cached:
        logger.info(f"[CACHE HIT] DQI - {cache_key}")
        return pd.read_json(cached)

    logger.info(f"[CACHE MISS] Running DQI queries - {cache_key}")
    tablename = calculate_tablename(start_dt)
    cell_str = "(" + ",".join(f"'{m}'" for m in cell_list) + ")"  
    
    logger.info(f"cell_ list : {cell_str}")
    logger.info(f"tablename : {tablename}")

    params = {
        'start_date': start_dt.strftime('%Y/%m/%d %H:%M'),
        'end_date': end_dt.strftime('%Y/%m/%d %H:%M'),
        'cell_list': cell_str,
        'tablename': tablename,
    }

    
    print('params --- DQI')
    print(params)
    # result = execute_smartcare_queries(
    #     queries_dict=DQI_Queries,   
    #     params=params,
    #     log_path=log_path,
    # )
    query = DQI_Queries["dqi_script"].format(**params)

    result = run_trino_query(query)

    print('result' , result)

    if not result.empty and log_path:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        result.to_excel(settings.OUTPUT_DIR / f"DQI_{ts}.xlsx", index=False)

    if not result.empty:
        cache.set(cache_key, result.to_json(), timeout=timeout_seconds)

    return result  
        
    