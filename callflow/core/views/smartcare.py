# callflow/core/views/smartcare.py
"""
Rewritten again to sit on JobBackedAPIView (views/job_base.py), the same
base every other view in this project now uses. This file no longer needs
its own post()/get()/threading code -- that's all inherited. Only the two
required methods remain: build_log_context (fast, synchronous request
parsing) and run_job (the actual long-running work, runs in a background
thread, every step wrapped in log_step()).

Behavior is otherwise identical to the previous version: same fetch ->
cache -> remedy-lookup pipeline, same response shape (now also includes
`ts` alongside `cache_key`, since RootCauseAPIView needs both to look up
this run's generated files without touching the old singleton file).
"""

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..utils.file_cache import make_smartcare_cache_key, cache_set
from ..services.smartcare_service import (
    fetch_smartcare_nodes_data,
    calculate_top_remedy_with_subs,
)
from ..services.remedy_service import remedy_ticket


class SmartcareIntegrationAPIView(JobBackedAPIView):
    job_type = "smartcare"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        msisdn_list = request.data.get('msisdn_list')
        complain_type = request.data.get('Complain_Type')
        imsi = request.data.get('imsi')
        log_path, ts = self.get_log_file("smartcare", window['start_dt'], window['end_dt'])

        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": msisdn_list, "complain_type": complain_type, "imsi": imsi,
        }

    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list, complain_type, imsi):
        with log_step(request_logger, "fetch_smartcare_nodes_data", job_id, job_manager, progress_percent=15):
            smartcare_result = fetch_smartcare_nodes_data(
                msisdn_list=msisdn_list,
                imsi=imsi,
                complain_type=complain_type,
                start_dt=window['start_dt'],
                end_dt=window['end_dt'],
                log_path=log_path,
                view="smartcare",
                ts=ts,
            )

        with log_step(request_logger, "cache_smartcare_result", job_id, job_manager, progress_percent=55):
            cache_key = make_smartcare_cache_key(
                msisdn_list, window['start_dt'], window['end_dt'], complain_type
            )
            cache_set(cache_key, smartcare_result)

        query_category = smartcare_result.get('query_category')
        processed_data = smartcare_result.get("processed_data")
        metrics = smartcare_result.get("metrics", {})
        packet_nodes_data = smartcare_result.get("packetNodes", {})
        suspected_cells_raw = smartcare_result.get("suspected_cells_raw")

        # cache_key + ts are what the frontend now passes to root-cause
        response_data = {"query_category": query_category, "cache_key": cache_key, "ts": ts}

        if query_category == "data":
            with log_step(request_logger, "remedy_ticket_lookup", job_id, job_manager, progress_percent=80):
                remedy_count, remedy_output = remedy_ticket(window, processed_data, log_path, ts)
                top_10_remedy_output = calculate_top_remedy_with_subs(metrics, remedy_output)

            response_data.update({
                "suspected_cells_raw": suspected_cells_raw,
                "metrics": metrics,
                "packetNodes": packet_nodes_data,
                "remedy": {"remedy_count": remedy_count, "remedy_output": top_10_remedy_output},
            })
        else:
            response_data["data_columns"] = (
                list(processed_data.columns) if processed_data is not None else []
            )

        return response_data