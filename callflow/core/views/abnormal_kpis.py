# callflow/core/views/abnormal_kpis.py
"""
What changed vs. the original:
 
1. THE FIX: _process_splunk looped over 5 nodes (SGSN, MME, GGSN, DSC, DNS)
   SEQUENTIALLY, each one a full multi-day Splunk query. That's the literal
   cause of "analyzing each system separately takes huge time" for the
   abnormal-KPIs flow. Now all 5 nodes run concurrently via ThreadPoolExecutor
   -- same fix shape as the Smartcare query runner, different file.
2. All three concrete views (Splunk/NPP/Viavi-abnormal) now go through the
   job pattern (JobBackedAPIView) instead of blocking the request, so the
   frontend gets the same {job_id} -> poll -> result flow as Smartcare.
3. _process_npp, _process_viavi, _build_time_series_data, _safe_process_system
   are UNCHANGED from your original file -- only _process_splunk and the
   request-handling shell around all three changed.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from rest_framework.response import Response
from rest_framework import status
import logging
import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from callflow.core.jobs import job_manager
from callflow.core.utils.logging_utils import log_step
from callflow.core.views.job_base import JobBackedAPIView
from .base import BaseCallFlowAPIView
from callflow.core.services.smartcare_service import fetch_smartcare_nodes_data
from callflow.core.services.splunk_service import splunkIntegration
from callflow.core.services.npp_service import nppIntegration
from callflow.core.constants.endpoints import VIAVIE_KPIS_MAPPING, NODE_COLUMNS_FROM_SMARTCARE, SPLUNK_SUFFIX_TO_SMARTCARE_NODE
from callflow.core.services.kpi_analyzer import KPIAnalyzer
from callflow.core.utils.data_utils import map_splunk_kpi_to_smartcare, process_sitename
from callflow.core.services.viavi_service import viaviQueries
from callflow.core.utils.connections import viaviConnection
from django.conf import settings
from datetime import timedelta

logger = logging.getLogger(__name__)

class BaseAbnormalKPIsAPIView(JobBackedAPIView):
    """
    Orchestrates abnormal KPI analysis for ONE system (splunk/npp/viavi),
    set via `system` on the concrete subclass below.
    """
    system = "splunk"  # overridden per subclass
    @property
    def job_type(self):
        return f"abnormal_{self.system}"

    def build_log_context(self, request):
        msisdn_list = request.data.get('msisdn_list')
        if not msisdn_list:
            raise ValueError("msisdn_list is required")

        window = self.parse_request_time_range(request.data)
        complain_type = request.data.get('Complain_Type', 'Data')
        imsi = request.data.get('imsi', '')
        log_path, ts = self.get_log_file(f"abnormal_{self.system}", window['start_dt'], window['end_dt'])

        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": msisdn_list, "complain_type": complain_type, "imsi": imsi,
        }
    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list, complain_type, imsi):
        with log_step(request_logger, "fetch_smartcare_for_abnormal", job_id, job_manager, progress_percent=15):
            smartcare_data = fetch_smartcare_nodes_data(
                msisdn_list, window['start_dt'], window['end_dt'], imsi, complain_type, log_path, "abnormal", ts
            )
 
        customers_per_cell = {
            str(k): v
            for k, v in smartcare_data['processed_data'].groupby('cell_name')['MSISDN'].nunique().to_dict().items()
        }
        analyzer = KPIAnalyzer()
        query_time_range = self.build_analysis_time_windows(window['start_dt'], window['end_dt'])
 
        if self.system == "splunk":
            with log_step(request_logger, "process_splunk_nodes_parallel", job_id, job_manager, progress_percent=70):
                result = self._safe_process_system(
                    "splunk", self._process_splunk,
                    (request_logger, analyzer, smartcare_data, query_time_range, log_path, ts),
                )
            return {"splunk": result["data"], "system_status": result["status"], "errors": result["error"]}
 
        elif self.system == "npp":
            with log_step(request_logger, "process_npp", job_id, job_manager, progress_percent=70):
                result = self._safe_process_system(
                    "npp", self._process_npp,
                    (analyzer, smartcare_data, query_time_range, log_path, ts, customers_per_cell),
                )
            return {"npp": result["data"], "system_status": result["status"], "errors": result["error"]}
 
        else:  # viavi
            with log_step(request_logger, "process_viavi", job_id, job_manager, progress_percent=70):
                result = self._safe_process_system(
                    "viavi", self._process_viavi,
                    (analyzer, smartcare_data, query_time_range, window, log_path, ts, customers_per_cell),
                )
            return {"viavi": result["data"], "system_status": result["status"], "errors": result["error"]}
    def _safe_process_system(self, system_name: str, process_func, args: tuple) -> Dict[str, Any]:
        """
        Wrapper that runs a system processor safely.
        Returns a dict with 'data', 'status', and optional 'error'.
        """
        try:
            data = process_func(*args)
            return {
                "data": data,
                "status": "success",
                "error": None
            }
        except Exception as e:
            logger.error(f"{system_name.capitalize()} processing failed: {str(e)}", exc_info=True)
            return {
                "data": [],
                "status": "failed",
                "error": f"{system_name} is currently unavailable: {str(e)}"
            }
    def _process_npp(
        self,
        analyzer: KPIAnalyzer,
        smartcare_data: pd.DataFrame,
        query_time_range: Dict[str, Any],
        log_path: str,
        ts: str,
        customers_per_cell: Dict[str, int]
    ) -> List[Dict[str, Any]]:
        """Process NPP KPIs and return analyzed results with time-series data."""
        cells_raw = smartcare_data['processed_data']['cell_name'].dropna().unique().tolist()

        npp_output = nppIntegration(
            cells_raw,
            start_date=query_time_range['query_start_3days'],
            end_date=query_time_range['query_end_3days'],
            logs_file=log_path,
            timestamp=ts,
            resolution='daily'
        )
        npp_output.to_csv(settings.OUTPUT_DIR / f"npp_15min_kpi_{ts}.csv")
        npp_analysis = analyzer.analyze_npp(
            npp_output,
            '15m',
            query_time_range['current_start'],
            query_time_range['current_end'],
            query_time_range['prev_start_1day'],
            query_time_range['prev_end_1day'],
            ts
        )

        results = []
        for cell, cell_details in npp_analysis.get("affected_cells_details", {}).items():
            degraded_kpis = []
            cell_data = npp_output[npp_output['Cell'] == cell].copy()

            for kpi_name, kpi_details in cell_details["kpis"].items():
                if kpi_name not in cell_data.columns:
                    continue

                kpi_data = self._build_time_series_data(
                    df=cell_data,
                    value_col=kpi_name,
                    time_col='DATETIME_ID',
                    start=query_time_range["query_start_3days"],
                    end=query_time_range['query_end_3days']
                )

                degraded_kpis.append({
                    "name": kpi_name,
                    "current_value": f"{kpi_details['current_value']:.2f}",
                    "previous_value": f"{kpi_details.get('previous_value', 0):.2f}",
                    "deviation_pct": kpi_details.get("deviation_pct"),
                    "severity": kpi_details.get("severity", "unknown"),
                    "message": kpi_details.get("insight_message", ""),
                    "kpi_data": kpi_data,
                    "cell_name": cell,
                })

            results.append({
                "cell_name": cell,
                "system": "NPP",
                "impacted_customers": customers_per_cell.get(cell, 0),
                "degraded_kpis": degraded_kpis
            })

        return results        

    def _process_viavi(
            self,
            analyzer: KPIAnalyzer,
            smartcare_data: pd.DataFrame,
            query_time_range: Dict[str, Any],
            window,
            log_path: str,
            ts: str,
            customers_per_cell: Dict[str, int]
        ) -> List[Dict[str, Any]]:
            """Process Viavi KPIs and return analyzed results with time-series data."""
            sites_df = process_sitename(smartcare_data['processed_data'],'cell_name',log_path)
            suspected_sites = sites_df['modified_site_name'].dropna().unique().tolist()
            
            viavi_conn = viaviConnection(log_path)
            #  add variable  , its value is window['formatted_startDate'] -2 days,
            #  add pass it to viave queries
            three_days_before = (pd.to_datetime(window['formatted_startDate']) - timedelta(days=3)).strftime('%Y-%m-%d %I:%M %p')
            viavi_output = viaviQueries(
                viavi_conn,
                three_days_before,
                window['formatted_endDate'],
                suspected_sites,
                log_path,
                ts
            )
            viavi_output.to_csv(settings.OUTPUT_DIR / f"viavi_op_kpi_{ts}.csv")

            viavi_analysis = analyzer.analyze_viavi(
                viavi_output,
                suspected_sites,
                query_time_range['current_start'],
                query_time_range['current_end'],
                query_time_range['prev_start_1day'],
                query_time_range['prev_end_1day'],
                ts
            )
            logger.info(f"viavi_analysis is {viavi_analysis}")
            results = []
            for site, site_details in viavi_analysis.get("affected_sites_details", {}).items():
                match = re.search(r'(ERD|HU|HUK|ERK)\d+', site)
                site_code = match.group(0) if match else None
                search_key = site_code[2:] if site_code else None

                matched_cell = next(
                    (c for c in customers_per_cell if search_key and search_key in c),
                    None
                )
                if matched_cell is None:
                    continue
                impacted_customers = customers_per_cell.get(matched_cell, 0)
                if impacted_customers == 0:
                    continue

                degraded_kpis = []
                site_data = viavi_output[viavi_output['FLWID'] == site].copy()
                site_data['PERIOD_TIMESTAMP'] = pd.to_datetime(site_data['PERIOD_TIMESTAMP'])

                for kpi_name, kpi_details in site_details["kpis"].items():
                    if kpi_name not in site_data.columns:
                        continue

                    kpi_data = self._build_time_series_data(
                        df=site_data,
                        value_col=kpi_name,
                        time_col='PERIOD_TIMESTAMP',
                        start=query_time_range['query_start_3days'],
                        end=query_time_range["query_end_3days"]
                    )

                    degraded_kpis.append({
                        "name": VIAVIE_KPIS_MAPPING.get(kpi_name, kpi_name),
                        "current_value": f"{kpi_details['current_value']:.2f}",
                        "previous_value": f"{kpi_details.get('previous_value', 0):.2f}",
                        "deviation_pct": kpi_details.get("deviation_pct"),
                        "severity": kpi_details.get("severity", "unknown"),
                        "message": kpi_details.get("insight_message", ""),
                        "kpi_data": kpi_data,
                        "site_name": search_key,
                    })

                results.append({
                    "site_name": search_key,
                    "system": "Viavi",
                    "impacted_customers": impacted_customers,
                    "degraded_kpis": degraded_kpis
                })

            return results
   
    def _process_splunk(self, request_logger, analyzer, smartcare_data, query_time_range, log_path, ts) -> List[Dict[str, Any]]:
        """
        Process Splunk KPIs across all 5 core nodes CONCURRENTLY.
 
        ORIGINAL BEHAVIOR (kept identical per-node, just parallelized):
        for each node, run splunkIntegration() over the 3-day window,
        analyze with KPIAnalyzer, map degraded KPIs back to Smartcare
        columns to compute impacted_subs (except DNS, which has no mapping).
        """
        nodes = ["SGSN", "MME", "GGSN", "DSC", "DNS"]
 
        def process_one_node(node) -> List[Dict[str, Any]]:
            node_results: List[Dict[str, Any]] = []
            try:
                request_logger.info(f"[ABNORMAL-SPLUNK] processing node: {node}")
                splunk_output = splunkIntegration(
                    settings.SPLUNK_USER, settings.SPLUNK_PASSWORD,
                    query_time_range['query_start_3days_str'], query_time_range['query_end_3days_str'],
                    log_path, node,request_logger
                )
                if splunk_output.empty:
                    return node_results
 
                splunk_output['timestamp'] = pd.to_datetime(splunk_output['timestamp'], errors='coerce')
                splunk_output = splunk_output.dropna(subset=['timestamp']).sort_values('timestamp').set_index('timestamp')
                splunk_output.to_csv(settings.OUTPUT_DIR / f"splunk_{node}_kpi_{ts}.csv")
 
                alarms = analyzer.analyze_core_node(
                    df=splunk_output, node=node,
                    current_start=query_time_range['current_start'], current_end=query_time_range['current_end'],
                    prev_start=query_time_range['prev_start_1day'], prev_end=query_time_range['prev_end_1day'],
                )
                splunk_output = splunk_output.reset_index()
                request_logger.info(f"[ABNORMAL-SPLUNK] {node} -> {len(alarms)} alarms")
 
                degraded_kpis = []
                for alarm in alarms:
                    kpi_col = alarm.get('kpi_column')
                    current_value = alarm.get('current_value')
                    kpi_data = self._build_time_series_data(
                        df=splunk_output, value_col=kpi_col, time_col='timestamp',
                        start=query_time_range['query_start_3days'], end=query_time_range['query_end_3days'],
                    )
 
                    if node == "DNS":
                        node_results.append({
                            "node": "DNS",
                            "system": "splunk",
                            "degraded_kpis": [{
                                "name": kpi_col,
                                "current_value": round(float(current_value), 2) if current_value not in (None, "") else None,
                                "previous_value": alarm.get('prev_value'),
                                "deviation_pct": alarm.get('deviation_percentage'),
                                "severity": alarm.get('severity'),
                                "message": alarm.get('message'),
                                "kpi_data": kpi_data,
                            }],
                        })
                        continue
 
                    mapped_value, column, suffix = map_splunk_kpi_to_smartcare(kpi_col, smartcare_data['processed_data'])
                    if mapped_value is None or column is None or suffix is None:
                        continue
 
                    node_type = NODE_COLUMNS_FROM_SMARTCARE.get(column)
                    if not node_type:
                        continue
 
                    impacted_subs = smartcare_data['processed_data'][
                        smartcare_data['processed_data'][column].astype(str).str.contains(str(mapped_value), case=False, na=False)
                    ]['MSISDN'].nunique()
 
                    degraded_kpis.append({
                        "name": mapped_value,
                        "current_value": round(float(current_value), 2) if current_value not in (None, "") else None,
                        "previous_value": alarm.get('prev_value'),
                        "deviation_pct": alarm.get('deviation_percentage'),
                        "severity": alarm.get('severity'),
                        "message": alarm.get('message'),
                        "kpi_data": kpi_data,
                    })
                    node_results.append({
                        "node": node_type,
                        "impacted_subs": impacted_subs,
                        "Kpi_Column": kpi_col,
                        "mapped_value": mapped_value,
                        "degraded_kpis": degraded_kpis,
                        "system": "splunk",
                    })
 
            except Exception as e:
                request_logger.error(f"Splunk node {node} failed (continuing with other nodes): {e}")
 
            return node_results   
        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            futures = {executor.submit(process_one_node, node): node for node in nodes}
            for future in as_completed(futures):
                node = futures[future]
                try:
                    results.extend(future.result())
                except Exception as e:
                    request_logger.error(f"Unexpected error collecting result for node {node}: {e}")
 
        return sorted(results, key=lambda x: x.get('impacted_subs', 0), reverse=True)
        
    
    def _build_time_series_data(
            self,
            df: pd.DataFrame,
            value_col: str,
            time_col: str,
            start: datetime,
            end: datetime
        ) -> List[Dict[str, Any]]:
            """
            Shared helper: filter time range, aggregate mean per timestamp, build chart-ready data points.
            """
            filtered = df[
                (df[time_col] >= start) & (df[time_col] <= end)
            ][[time_col, value_col]].dropna(subset=[value_col])

            if filtered.empty:
                return []

            aggregated = (
                filtered.groupby(time_col)[value_col]
                .mean()
                .reset_index()
                .sort_values(time_col)
            )

            return [
                {
                    "datetime": row[time_col].isoformat() + 'Z',
                    "value": round(float(row[value_col]), 3)
                }
                for _, row in aggregated.iterrows()
            ]          

class SplunkAbnormalAPIView(BaseAbnormalKPIsAPIView):
    system = "splunk"
 
 
class NppAbnormalAPIView(BaseAbnormalKPIsAPIView):
    system = "npp"
 
 
class ViaviAbnormalAPIView(BaseAbnormalKPIsAPIView):
    system = "viavi"
    