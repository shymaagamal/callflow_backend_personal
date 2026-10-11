from rest_framework.response import Response
from rest_framework import status
import logging

from callflow.core.jobs import job_manager
from callflow.core.utils.logging_utils import log_step
from callflow.core.views.job_base import JobBackedAPIView

from .base import BaseCallFlowAPIView
from callflow.core.services.splunk_service import splunkIntegration
from callflow.core.services.kpi_analyzer import KPIAnalyzer
from django.conf import settings
import pandas as pd
logger = logging.getLogger(__name__)


class SplunkIntegrationAPIView(JobBackedAPIView):
    job_type = "splunk"
    def build_log_context(self, request):
            window = self.parse_request_time_range(request.data)
            node = (request.data.get('node', '') or '').strip().upper()
            if not node:
                raise ValueError("Parameter 'node' is required (e.g., 'MME')")
    
            query_time_range = self.build_analysis_time_windows(window['start_dt'], window['end_dt'])
            log_path, ts = self.get_log_file("splunk", window['start_dt'], window['end_dt'])
    
            return {
                "log_path": log_path,
                "ts": ts,
                "node": node,
                "query_time_range": query_time_range,
            }
    
    def run_job(self, job_id, request_logger, log_path, ts, node, query_time_range):
        with log_step(request_logger, "fetch_splunk_data", job_id, job_manager, progress_percent=30):
            splunk_output = splunkIntegration(
                settings.SPLUNK_USER, settings.SPLUNK_PASSWORD,
                query_time_range['query_start_1day_str'], query_time_range['query_end_1day_str'],
                log_path, node,request_logger
            )
 
        if splunk_output.empty:
            raise ValueError("No data returned from Splunk")
 
        if 'timestamp' in splunk_output.columns:
            splunk_output['timestamp'] = pd.to_datetime(splunk_output['timestamp'], errors='coerce')
            splunk_output = splunk_output.dropna(subset=['timestamp']).set_index('timestamp').sort_index()
        splunk_output.to_csv(settings.OUTPUT_DIR / f"splunk_{node}_kpi_{ts}.csv")
 
        with log_step(request_logger, "analyze_core_node", job_id, job_manager, progress_percent=80):
            analyzer = KPIAnalyzer()
            alarms = analyzer.analyze_core_node(
                df=splunk_output, node=node,
                current_start=query_time_range['current_start'], current_end=query_time_range['current_end'],
                prev_start=query_time_range['prev_start_1day'], prev_end=query_time_range['prev_end_1day'],
            )
        raw_data_json = splunk_output.reset_index().to_json(orient='records', date_format='iso')
        return {
            "alarm_count": len(alarms),
            "alarms": alarms,
            "query_range": f"{query_time_range['query_start_1day_str']} -> {query_time_range['query_end_1day_str']}",
            "current_window": f"{query_time_range['current_start']} -> {query_time_range['current_end']}",
            "previous_day_window": f"{query_time_range['prev_start_1day']} -> {query_time_range['prev_end_1day']}",
             "raw_data": raw_data_json,
        }