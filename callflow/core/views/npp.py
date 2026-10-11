# callflow/core/views/npp.py
import pandas as pd
from django.conf import settings

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..utils.file_cache import cache_get, make_smartcare_cache_key
from ..services.npp_service import calculate_affected_hub_taclac_metrics, nppIntegration
from ..services.kpi_analyzer import KPIAnalyzer


class _BaseNppAPIView(JobBackedAPIView):
    """
    Shared logic for both NPP endpoints. NOTE: preserving original behavior
    exactly -- the "15min" view actually queries NPP with resolution='daily'
    in the original code (that naming mismatch existed before this refactor;
    not changing the behavior here, just flagging it in case it's actually
    a pre-existing bug worth a separate look).
    """
    resolution = "daily"
    analyzer_freq = "15m"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        suspected_cells_raw = request.data.get('suspected_cells_raw', [])
        complain_type = request.data.get('Complain_Type')
        msisdn_list = request.data.get('msisdn_list')

        log_path, ts = self.get_log_file(f"npp_{self.job_type}", window['start_dt'], window['end_dt'])
        query_time_range = self.build_analysis_time_windows(window['start_dt'], window['end_dt'])

        return {
            "log_path": log_path, "ts": ts, "window": window,
            "suspected_cells_raw": suspected_cells_raw, "complain_type": complain_type,
            "msisdn_list": msisdn_list, "query_time_range": query_time_range,
        }

    def run_job(self, job_id, request_logger, log_path, ts, window, suspected_cells_raw,
                complain_type, msisdn_list, query_time_range):
        print('suspected_cells_raw')
        print(suspected_cells_raw)
        
        if self.resolution == "daily":
            start_date, end_date = query_time_range['query_start_1day'], query_time_range['query_end_1day']
        else:
            start_date, end_date = window['start_dt'], window['end_dt']

        with log_step(request_logger, "fetch_npp_data", job_id, job_manager, progress_percent=30):
            npp_output = nppIntegration(
                suspected_cells_raw, start_date=start_date, end_date=end_date,
                logs_file=log_path, timestamp=ts, resolution=self.resolution,
            )
            
        if npp_output.empty:
            return {"cells_affected_count": 0, "cells_affected_list": []}

        npp_output.to_csv(settings.OUTPUT_DIR / f"npp_{self.job_type}_kpi_{ts}.csv")
        raw_data_json = npp_output.to_json(orient='records', date_format='iso')
        with log_step(request_logger, "analyze_npp", job_id, job_manager, progress_percent=70):
            analyzer = KPIAnalyzer()
            metrics = analyzer.analyze_npp(
                npp_output, self.analyzer_freq,
                query_time_range['current_start'], query_time_range['current_end'],
                query_time_range['prev_start_1day'], query_time_range['prev_end_1day'], ts,
            )

        response_data = {
            "cells_affected_count": metrics['affected_cells_count'],
            "cells_affected_list": metrics['affected_cells_list'],
            "affected_cells_details": metrics['affected_cells_details'],
            "raw_data": raw_data_json,
        }

        # Original 15min view additionally cross-references cached Smartcare
        # data for hub/TAC/LAC metrics. Preserved here, gated to the daily path
        # to match original behavior exactly.
        if self.resolution == "daily":
            with log_step(request_logger, "load_cached_smartcare_for_hub_metrics", job_id, job_manager, progress_percent=90):
                cache_key = make_smartcare_cache_key(msisdn_list, window['start_dt'], window['end_dt'], complain_type)
                cached_smartcare_data = cache_get(cache_key)
                request_logger.info(f"this is the step of load_cached_smartcare_for_hub_metrics {cached_smartcare_data}")

            affected_metrics = {}
            if (cached_smartcare_data is not None
                    and isinstance(cached_smartcare_data.get('processed_data'), pd.DataFrame)
                    and not cached_smartcare_data['processed_data'].empty):
                affected_metrics = calculate_affected_hub_taclac_metrics(
                    cached_smartcare_data['processed_data'], metrics.get('affected_cells_list', []), ts
                )
            response_data["affected_metrics"] = affected_metrics

        return response_data


class NppIntegration15minAPIView(_BaseNppAPIView):
    job_type = "npp_15min"
    resolution = "daily"   # matches original behavior exactly
    analyzer_freq = "15m"


class NppIntegrationHourlyAPIView(_BaseNppAPIView):
    job_type = "npp_hourly"
    resolution = "hourly"
    analyzer_freq = "1H"