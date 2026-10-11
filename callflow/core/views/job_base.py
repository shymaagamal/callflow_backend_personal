# callflow/core/views/job_base.py
"""
Shared base for every "validate -> long analysis -> notify frontend" endpoint.

This is what makes converting Splunk/NPP/Viavi/Remedy/SCF/Abnormal-KPIs to
the job pattern cheap: each subclass only writes the two methods below,
everything else (job creation, background thread, who-tracking, polling
endpoint, consistent error shape) is inherited.

Subclasses implement:
  - job_type: short string, identifies this endpoint in logs/job records
  - build_log_context(self, request) -> dict
        Parse/validate the request. Runs synchronously (must be fast).
        Must include "log_path" and "ts" keys. Everything else in the
        dict is passed as **kwargs into run_job.
  - run_job(self, job_id, request_logger, **kwargs) -> dict
        The actual long-running work. Runs in a background thread.
        Raise on failure; return the dict that becomes `result` on DONE.
        
        
POST
  ↓
Create Job
  ↓
Start Thread
  ↓
Return 202
  ↓
Thread executes
  ↓
DONE / FAILED
  ↓
Frontend polls GET /job/{id}
"""

import threading
import logging
from rest_framework.response import Response
from rest_framework import status
import os
from .base import BaseCallFlowAPIView
from ..jobs import job_manager
from ..utils.logging_utils import get_request_identity, build_request_logger

logger = logging.getLogger(__name__)


class JobBackedAPIView(BaseCallFlowAPIView):
    job_type = "generic"

    def build_log_context(self, request):
        raise NotImplementedError

    def run_job(self, job_id, request_logger, **kwargs):
        raise NotImplementedError

    def post(self, request):
        try:
            logger.info(f"POST worker pid={os.getpid()}")
            who = get_request_identity(request)
            context = self.build_log_context(request)
            log_path, ts = context["log_path"], context["ts"]

            request_logger = build_request_logger(log_path, ts, who)
            self.log_request(log_path, request, f"{self.job_type} Request")
            request_logger.info(f"[REQUEST] {self.job_type} requested by {who}")

            job_id = job_manager.create_job(who=who, job_type=self.job_type)

            worker = threading.Thread(
                target=self._run_safely,
                args=(job_id, request_logger, context),
                daemon=True,
            )
            worker.start()

            return Response({"job_id": job_id, "status": "PENDING"}, status=status.HTTP_202_ACCEPTED)

        except ValueError as ve:
            logger.warning(f"[{self.job_type}] input error: {ve}")
            return Response({"error": str(ve)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception:
            logger.exception(f"[{self.job_type}] post() error")
            return Response({"error": "Internal server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get(self, request, job_id=None):
        logger.info(f"GET worker pid={os.getpid()}")
        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return Response(job)
# it's considerd a wrapper because when there an exeption happen in the run_jop , thread doesn't crash silently 
# but instead it marks the jop as faild 
                # try:
                    # run_job()
                    # mark DONE
                # except:
                    # mark FAILED
    def _run_safely(self, job_id, request_logger, context):
        try:
            run_kwargs = {k: v for k, v in context.items() if k not in ("log_path", "ts")}
            run_kwargs["log_path"] = context["log_path"]
            run_kwargs["ts"] = context["ts"]
            result = self.run_job(job_id, request_logger, **run_kwargs)
            job_manager.update_job(job_id, status="DONE", step="completed", progress_percent=100, result=result)
            request_logger.info(f"[JOB DONE] {job_id}")
        except Exception as e:
            request_logger.exception(f"[JOB FAILED] {job_id}: {e}")
            job_manager.update_job(job_id, status="FAILED", step="failed", error=str(e))