# callflow/core/services/splunk_service.py
"""
FIXED VERSION (DSC path only -- everything else unchanged).
 
The non-DSC path already correctly used ThreadPoolExecutor across KPIs.
The DSC path did too, but only WITHIN each region -- the outer loop over
the 4 regions (RMD, ZAH, TG, HQ) was sequential:
 
    for region in DSC_REGIONS:
        with ThreadPoolExecutor(...) as executor:
            ...
 
That's 4x sequential rounds of network calls for DSC specifically. This
version runs all 4 regions concurrently as well (each spawning its own
small KPI-level thread pool), so DSC analysis time drops from
"sum of 4 regions" to "the slowest region".
 
"""

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..constants.endpoints import SPLUNK_ENDPOINTS
from ..utils.file_cache import cache_get, cache_set, make_splunk_cache_key
import logging
from pandas.errors import EmptyDataError

# logger = logging.getLogger(__name__)
SPLUNK_CACHE_TTL = 36000
DSC_REGIONS = ['RMD', 'ZAH', 'TG', 'HQ']
proxies = {
    "http": None,
    "https": None,
}
def splunkIntegration(user, password, formatted_startDate_3, formatted_endDate_3, logs_file, node,request_logger):
    headers = {"Content-Type": "application/json"}
    failed_kpis = []
    request_logger.info(f"SPLUNK INTEGRATION CALLED - Node filter: {node}")
        # ── Cache check ───────────────────────────────────────────────────────────
    cache_key = make_splunk_cache_key(node, formatted_startDate_3, formatted_endDate_3)
    cached = cache_get(cache_key)
    if cached is not None:
        request_logger.info(f"[SPLUNK CACHE HIT] node={node}")
        return cached
    data = pd.DataFrame()
    def process_kpi(kpi, api_endpoint, region=None):
        """Handles one KPI API call and returns its dataframe"""
        if not kpi.startswith(node):
            return None

        try:
            if region and "Node_Name" in api_endpoint:
                api_endpoint = api_endpoint.replace("Node_Name", region)
                request_logger.debug(f"Replaced Node_Name → {region} for KPI: {kpi}")
            api_endpoint = api_endpoint.replace("start_time", formatted_startDate_3)
            api_endpoint = api_endpoint.replace("end_time", formatted_endDate_3)
            request_logger.debug(f"Fetching KPI: {kpi} from {api_endpoint}")
            response = requests.get(
                api_endpoint,
                headers=headers,
                auth=HTTPBasicAuth(user, password),
                verify=False,
                timeout=60,proxies=proxies
            )

            if response.status_code != 200:
                error_msg = f"Splunk API failed for {kpi} - Status: {response.status_code}"
                request_logger.warning(error_msg)
                with open(logs_file, "a") as logs_f:
                    logs_f.write(error_msg + "\n")
                failed_kpis.append(kpi)
                return None

            try:
                df = pd.read_csv(StringIO(response.text))
            except EmptyDataError:
                request_logger.warning(f"No data returned for KPI: {kpi}")
                return None
            # breakpoint()
            possible_time_cols = ['_time', 'DATETIME_ID', 'time', 'timestamp']
            time_col = next((col for col in possible_time_cols if col in df.columns), None)

            if time_col:
                df.rename(columns={time_col: 'timestamp'}, inplace=True)
            else:
                # Fallback: assume first column is time
                df.rename(columns={str(df.columns[0]): 'timestamp'}, inplace=True)
                request_logger.warning(f"No known time column in {kpi} - using first column as timestamp")
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])


            if df.empty:
                request_logger.warning(f"Empty data after parsing for {kpi}")
                return None
            

            if "_span" in df.columns:
                df.drop("_span", axis=1, inplace=True)
                
            prefix = f"{kpi}_{region}_" if region else f"{kpi}_"
            # Prefix KPI name
            df.columns = ['timestamp'] + [f"{prefix}_{col}" for col in df.columns[1:]]
            return df
        except requests.exceptions.RequestException as e:
            request_logger.error(f"Splunk connection failed for {kpi}: {str(e)}")
            if "407" in str(e):
                request_logger.error("Proxy Authentication Required - Check NO_PROXY or proxy settings")
            with open(logs_file, "a") as logs_f:
                logs_f.write(f"Splunk connection failed for {kpi}: {str(e)}\n")
            failed_kpis.append(kpi)
            return None
        except Exception as e:
            request_logger.exception(f"Unexpected error parsing Splunk KPI {kpi}: {str(e)}")
            with open(logs_file, "a") as logs_f:
                logs_f.write(f"Unexpected error parsing Splunk KPI {kpi}: {str(e)}\n")
            failed_kpis.append(kpi)
            return None   

    if node == "DSC":
        request_logger.info(f"DSC multi-region mode activated - regions: {DSC_REGIONS} (now parallel)")
 
        def process_region(region):
            request_logger.info(f"Fetching DSC KPIs for region: {region}")
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(process_kpi, kpi, api, region=region)
                    for kpi, api in SPLUNK_ENDPOINTS.items() if kpi.startswith("DSC_")
                ]
                region_data = pd.DataFrame()
                for future in as_completed(futures):
                    df = future.result()
                    if df is not None and not df.empty:
                        region_data = df if region_data.empty else pd.merge(region_data, df, on="timestamp", how="outer")
                return region_data
 
        region_dfs = []
        # regions  run concurrently instead of one-after-another
        with ThreadPoolExecutor(max_workers=len(DSC_REGIONS)) as region_executor:
            region_futures = {region_executor.submit(process_region, region): region for region in DSC_REGIONS}
            for future in as_completed(region_futures):
                region_data = future.result()
                if not region_data.empty:
                    region_dfs.append(region_data)
 
        if region_dfs:
            combined = pd.concat(region_dfs, ignore_index=True)
            numeric_cols = combined.select_dtypes(include=['number']).columns.tolist()
            if 'timestamp' in combined.columns:
                data = combined.groupby('timestamp')[numeric_cols].sum().reset_index()
    else:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_kpi, kpi, api) for kpi, api in SPLUNK_ENDPOINTS.items()]
            for future in as_completed(futures):
                df = future.result()
                if df is not None and not df.empty:
                    if data.empty:
                        data = df
                    else:
                        try:
                            data = pd.merge(data, df, on="timestamp", how="outer")
                        except Exception as e:
                            request_logger.error(f"Merge failed: {e}")
 
    if not data.empty:
        data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')
        data = data.dropna(subset=['timestamp']).sort_values('timestamp')
 
    total_kpis = len([k for k in SPLUNK_ENDPOINTS if k.startswith(node)])
    if len(failed_kpis) == total_kpis and total_kpis > 0:
        raise RuntimeError(f"Splunk integration failed for all {total_kpis} KPIs for node {node}. Check logs for details.")
 
    if not data.empty:
        cache_set(cache_key, data, ttl_seconds=SPLUNK_CACHE_TTL)
    request_logger.info(f"Splunk finished - {len(data)} rows, {len(failed_kpis)} failed KPIs")
    return data
