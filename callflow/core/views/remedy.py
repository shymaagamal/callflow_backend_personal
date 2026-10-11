# callflow/core/views/remedy.py
from .job_base import JobBackedAPIView
from ..jobs import job_manager
from ..utils.logging_utils import log_step
from ..services.remedy_service import remedyTicketsData
from ..utils.classes import Connection
from ..constants.general_constants import (
    SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
    DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD,
)


class RemedyIntegrationMain(JobBackedAPIView):
    job_type = "remedy"

    def build_log_context(self, request):
        window = self.parse_request_time_range(request.data)
        suspected_sites = request.data.get('suspected_sites', [])
        log_path, ts = self.get_log_file("remedy", window['start_dt'], window['end_dt'])
        return {"log_path": log_path, "ts": ts, "window": window, "suspected_sites": suspected_sites}

    def run_job(self, job_id, request_logger, log_path, ts, window, suspected_sites):
        with log_step(request_logger, "remedy_db_query", job_id, job_manager, progress_percent=60):
            conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
                               DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
            conn.connect()
            remedy_output, status_count = remedyTicketsData(
                conn, window['start_dt'], window['end_dt'], suspected_sites, log_path, ts
            )

        top_10_remedy_output = dict(
            sorted(remedy_output.items(), key=lambda x: x[1].get("total", 0), reverse=True)[:10]
        )

        return {
            "remedy_tickets": remedy_output,
            "status": status_count,
            "top_10": top_10_remedy_output,
        }