# callflow/core/views/smartcare_failure.py
from django.core.cache import cache

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..utils.file_cache import make_smartcare_cache_key
from ..services.smartcare_failure_service import fetch_scf_kpi_data
from ..services.smartcare_service import fetch_smartcare_nodes_data


class SmartcareSCFAPIView(JobBackedAPIView):
    job_type = "smartcare_failure"

    def build_log_context(self, request):
        msisdn_list = request.data.get('msisdn_list')
        if not msisdn_list:
            raise ValueError("msisdn_list is required")

        window = self.parse_request_time_range(request.data)
        complain_type = request.data.get('Complain_Type', 'Data')
        log_path, ts = self.get_log_file("scf_kpi", window['start_dt'], window['end_dt'])

        return {
            "log_path": log_path, "ts": ts, "window": window,
            "msisdn_list": msisdn_list, "complain_type": complain_type,
        }

    def run_job(self, job_id, request_logger, log_path, ts, window, msisdn_list, complain_type):
        cache_key = make_smartcare_cache_key(msisdn_list, window['start_dt'], window['end_dt'], complain_type)

        with log_step(request_logger, "load_cached_smartcare_nodes", job_id, job_manager, progress_percent=20):
            nodes_data = cache.get(cache_key)
            request_logger.info(f"from smartcare failure {nodes_data}")
            if nodes_data is None:
                request_logger.warning("Smartcare cache MISS for SCF - re-fetching nodes (slower path)")
                nodes_data = fetch_smartcare_nodes_data(
                    msisdn_list=msisdn_list, start_dt=window['start_dt'], end_dt=window['end_dt'],
                    complain_type=complain_type, log_path=log_path,
                )

        with log_step(request_logger, "fetch_scf_kpi_data", job_id, job_manager, progress_percent=70):
            kpi_data = fetch_scf_kpi_data(
                start_dt=window['start_dt'], end_dt=window['end_dt'], msisdn_list=msisdn_list,
                nodes_data=nodes_data['processed_data'], log_path=log_path,
            )

        return {"scf_kpi_data": kpi_data.to_dict(orient='records') if not kpi_data.empty else []}