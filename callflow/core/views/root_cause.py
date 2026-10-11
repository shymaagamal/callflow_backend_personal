"""
What changed vs. the original:
 
The original RootCauseAPIView.post() read a SINGLE GLOBAL FILE
(last_smartcare_time_window.json) to figure out "which analysis" to serve
files for. Under concurrent users, this is wrong by construction: User B's
root-cause request can silently serve User A's time window/files, because
there's only one file, shared by everyone, holding "whatever ran most
recently."
 
Fix: the frontend now passes `cache_key` explicitly (returned in the
Smartcare job's `result.cache_key` -- see views/smartcare.py) in the
root-cause POST body. We use that to scope file lookups instead of a
process-wide singleton.
 
Backward compatible: if `cache_key` is NOT provided (old frontend not
updated yet), we fall back to the legacy file with a loud warning log so
you know which callers still need migrating. Remove the fallback once the
frontend always sends cache_key.
"""

from fileinput import filename
import os

from django.conf import settings
from rest_framework.response import Response
from .base import BaseCallFlowAPIView
from django.http import FileResponse
import logging
import callflow.core.constants.general_constants as constants
import json
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)



class RootCauseAPIView(BaseCallFlowAPIView):
    def get(self, request, filename=None):
        logger.info("RootCauseAPIView get called filename=%s", filename)
        if not filename:
            return Response({"error": "Filename is required"}, status=400)

        # thresholds live outside callflow_output
        if "threshold" in filename.lower():
            path = settings.THRESHOLDING_SHEET_PATH
            if not os.path.isfile(path):
                return Response({"error": f"Threshold file not found: {path}"}, status=404)
            return FileResponse(open(path, "rb"), as_attachment=True, filename="thresholds.xlsx")

        folder_path = os.path.join(settings.BASE_DIR, "callflow_output")
        file_path = os.path.join(folder_path, os.path.basename(filename))
        if not os.path.isfile(file_path):
            return Response({"error": f"File not found: {filename}"}, status=404)
        return FileResponse(open(file_path, "rb"), as_attachment=True, filename=os.path.basename(file_path))
        
    def post(self, request):
        logger.info("RootCauseAPIView post called")
 
        cache_key = request.data.get("cache_key")
        ts = request.data.get("ts")
        start_date = None
        end_date = None
 
        if not cache_key:
            # Legacy fallback -- migrate callers to send cache_key explicitly.
            logger.warning(
                "RootCauseAPIView called without cache_key -- falling back to the "
                "legacy singleton file. This is per-process/global state and can "
                "serve the wrong user's data under concurrent traffic. Update the "
                "frontend to pass cache_key from the Smartcare job result."
            )
            cache_file = Path(settings.BASE_DIR) / "last_smartcare_time_window.json"
            if not cache_file.exists():
                return Response({
                    "message": "No recent Smartcare analysis found. Please run Smartcare first.",
                    "start_date": None, "end_date": None, "ts": None,
                    "kpi": [], "filter": [], "threshold": [],
                })
            with open(cache_file, "r") as f:
                legacy_data = json.load(f)
            ts = legacy_data.get("ts")
            start_date = datetime.fromisoformat(legacy_data.get("start_dt"))
            end_date = datetime.fromisoformat(legacy_data.get("end_dt"))
 
        if not ts:
            return Response({"error": "ts is required (or pass cache_key from a completed Smartcare job)"}, status=400)
 
        folder_path = os.path.join(settings.BASE_DIR, "callflow_output")
        files = os.listdir(folder_path)
 
        kpi_files, filter_files = [], []
        threshold_files = [{
            "name": settings.THRESHOLDING_SHEET_PATH,
            "url": f"/callflow/api/root-cause/{settings.THRESHOLDING_SHEET_PATH}",
        }]
 
        for file in files:
            file_info = {"name": file, "url": f"/callflow/api/root-cause/{file}"}
            if f"kpi_{ts}" in file:
                kpi_files.append(file_info)
            elif f"filter_{ts}" in file.lower():
                filter_files.append(file_info)
 
        logger.info(f"[ROOT-CAUSE] ts={ts} kpi_files={len(kpi_files)} filter_files={len(filter_files)}")
 
        return Response({
            "start_date": start_date,
            "end_date": end_date,
            "kpi": kpi_files,
            "filter": filter_files,
            "threshold": threshold_files,
        })