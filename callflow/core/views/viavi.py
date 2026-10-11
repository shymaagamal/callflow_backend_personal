# callflow/core/views/viavi.py
import numpy as np
from django.conf import settings

from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..services.viavi_service import viaviQueries
from ..utils.connections import viaviConnection


class ViaviIntegrationAPIView(JobBackedAPIView):
    job_type = "viavi"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        suspected_sites = request.data.get('suspected_sites', [])
        log_path, ts = self.get_log_file("viavi", window['start_dt'], window['end_dt'])

        return {
            "log_path": log_path, "ts": ts, "suspected_sites": suspected_sites,
            "formatted_startDate": window['formatted_startDate'],
            "formatted_endDate": window['formatted_endDate'],
        }

    def run_job(self, job_id, request_logger, log_path, ts, suspected_sites,
                formatted_startDate, formatted_endDate):

        with log_step(request_logger, "open_viavi_connection", job_id, job_manager, progress_percent=15):
            viavi_conn = viaviConnection(log_path)
        if not viavi_conn:
            raise RuntimeError("Viavi database connection failed")

        with log_step(request_logger, "run_viavi_query", job_id, job_manager, progress_percent=60):
            viavi_output = viaviQueries(
                viavi_conn, formatted_startDate, formatted_endDate, suspected_sites, log_path, ts
            )

        viavi_output.to_csv(settings.OUTPUT_DIR / f"viavi_op_kpi_{ts}.csv")
        viavi_json_output = viavi_output.replace({np.nan: None}).to_json(orient='records', date_format='iso')

        return {"viavi": viavi_json_output}