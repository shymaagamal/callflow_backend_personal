# NCI/views.py
"""
NCI is a public-facing API with an unknown/uncontrolled frontend, so the
default endpoint is a PLAIN SYNCHRONOUS request/response -- no job_id,
no polling contract assumed. Speed comes from caching + the concurrent
fetch in analysis_runner.run_nci_analysis, not from making the caller poll.

An optional callback_url escape hatch is included for callers who CAN
accept a webhook and would rather not hold a connection open -- purely
opt-in, unused unless a caller actually sends it.
"""

import logging
import threading

import numpy as np
import pandas as pd
from rest_framework.response import Response
from rest_framework import status

from callflow.core.views.base import BaseCallFlowAPIView
from callflow.core.utils.logging_utils import get_request_identity, build_request_logger
from callflow.core.jobs import job_manager

from .service.analysis_runner import run_nci_analysis

from django.utils.decorators import method_decorator
from .utils.authentication import require_api_token

logger = logging.getLogger(__name__)


def _json_safe(obj):
    """
    Recursively converts pandas/numpy types that DRF's default JSON
    encoder can't handle (Timestamp, NaT, NaN, numpy scalars) into plain
    JSON-safe Python values. Applied to the whole result dict before it
    goes into the Response.
    """
    if isinstance(obj, pd.DataFrame):
        return _json_safe(obj.to_dict(orient="records"))
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp,)):
        return None if pd.isna(obj) else obj.isoformat()
    if obj is pd.NaT:
        return None
    if isinstance(obj, (np.floating, np.integer)):
        return None if pd.isna(obj) else obj.item()
    if isinstance(obj, float) and pd.isna(obj):
        return None
    return obj

# @method_decorator(require_api_token, name="dispatch")
class NCIAnalysisAPIView(BaseCallFlowAPIView):
    """
    POST callflow/nci/analysis/
    body: {"site_names": ["SITE_A", "SITE_B", ...], "callback_url": "..."}  # callback_url optional

    Default (no callback_url): blocks until the 3-period x 3-source
    analysis completes and returns the full result in one response.

    With callback_url: returns 202 immediately with a job_id, runs the
    same analysis in a background thread, and POSTs the result to
    callback_url when done. job_id can still be polled via GET as a
    fallback for callers that have that capability but didn't want a
    webhook, or just for your own debugging/log correlation.
    """

    def post(self, request):
        site_names = request.data.get("site_names")
        if not site_names or not isinstance(site_names, list):
            return Response(
                {"error": "site_names (non-empty list) is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        for item in site_names:
            if not isinstance(item, dict) or "SITE" not in item or "FLAG" not in item:
                return Response(
                    {"error": "Each site_names item must contain SITE and FLAG"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        sites = [item["SITE"] for item in site_names]
        site_flags = {item["SITE"]: item["FLAG"] for item in site_names}

        callback_url = request.data.get("callback_url")
        who = get_request_identity(request)
        log_path, ts = self.get_log_file("nci", "now", "now")
        
        request_logger = build_request_logger(log_path, ts, who)
        request_logger.info(f"[NCI] requested by {who}, sites={len(site_names)}, callback_url={bool(callback_url)}")

        if not callback_url:
            # ── Synchronous path (default) ──────────────────────────────
            try:
                result = run_nci_analysis(sites, request_logger, log_path, ts)
                # Add FLAG to each site in sites_summary
                if isinstance(result, dict) and isinstance(result.get("sites_summary"), dict):
                    for site, summary in result["sites_summary"].items():
                        if site in site_flags:
                            summary["FLAG"] = site_flags[site]

                return Response(
                    _json_safe(result),
                    status=status.HTTP_200_OK
                )
            except Exception:
                request_logger.exception("[NCI] analysis failed")
                return Response({"error": "Internal server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # ── Async/webhook path (opt-in) ──────────────────────────────────
        job_id = job_manager.create_job(who=who, job_type="nci")
        worker = threading.Thread(
            target=self._run_and_callback,
            args=(job_id, sites, site_flags, request_logger, log_path, ts, callback_url),
            daemon=True,
        )
        worker.start()
        return Response({"job_id": job_id, "status": "PENDING"}, status=status.HTTP_202_ACCEPTED)

    def get(self, request, job_id=None):
        """Optional fallback poll endpoint for the callback_url path."""
        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return Response(job)

    def _run_and_callback(self, job_id, sites, site_flags, request_logger, log_path, ts, callback_url):
        import requests  # local import: only needed on this opt-in path

        try:
            result = run_nci_analysis(sites, request_logger, log_path, ts, job_id=job_id)
            if isinstance(result, dict) and isinstance(result.get("sites_summary"), dict):
                for site, summary in result["sites_summary"].items():
                    if site in site_flags:
                        summary["FLAG"] = site_flags[site]
            
            safe_result = _json_safe(result)
            job_manager.update_job(job_id, status="DONE", step="completed", progress_percent=100, result=safe_result)
            request_logger.info(f"[NCI JOB DONE] {job_id}")
        except Exception as e:
            request_logger.exception(f"[NCI JOB FAILED] {job_id}: {e}")
            job_manager.update_job(job_id, status="FAILED", step="failed", error=str(e))
            safe_result = {"job_id": job_id, "status": "FAILED", "error": str(e)}

        try:
            requests.post(callback_url, json=safe_result, timeout=10)
            request_logger.info(f"[NCI] callback POSTed to {callback_url}")
        except Exception as e:
            request_logger.error(f"[NCI] callback POST to {callback_url} failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# RCA for a list of sites -- same analysis as the RCA tool in the frontend
# (rca_tool_V24.html), run server-side on live NPP / Viavi / Remedy data with
# the existing KPIAnalyzer + KPIThresholds. See service/rca_site_runner.py.
# ─────────────────────────────────────────────────────────────────────────────
import datetime as _dt

from .service.rca_site_runner import run_rca_site_analysis, VALID_BASELINE_METHODS, DEFAULT_WINDOW_HOURS


def _parse_dt(value):
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Cannot parse datetime '{value}'")
    return parsed.to_pydatetime().replace(tzinfo=None)


def _attach_flags(result, site_flags):
    for site, row in result.get("sites_summary", {}).items():
        if site in site_flags:
            row["FLAG"] = site_flags[site]


# @method_decorator(require_api_token, name="dispatch")
class NCIRcaSitesAPIView(BaseCallFlowAPIView):
    """
    POST callflow/NCI/rca/
    body: {
      "site_names": [{"SITE": "LJ1333", "FLAG": "..."}, ...]   # same format as /analysis/ (plain strings also accepted)
      "start_date": "2026-10-11 10:00",   # optional, default end_date - 3h
      "end_date":   "2026-10-11 13:00",   # optional, default now (rounded down to the hour)
      "baseline_method": "auto",          # auto | 2h_before | yesterday  (RCA tool default: auto)
      "callback_url": "..."               # optional, same async contract as /analysis/
    }
    Response: same sites_summary fields as /analysis/ (critical_cases, minor_cases,
    trouble_tickets, FLAG) plus status, is_degraded, rca_score, rank,
    degraded_cells, degraded_routes; and ranking / degraded_sites lists.
    """

    def post(self, request):
        raw = request.data.get("site_names")
        if not raw or not isinstance(raw, list):
            return Response({"error": "site_names (non-empty list) is required"}, status=status.HTTP_400_BAD_REQUEST)

        sites, site_flags = [], {}
        for item in raw:
            site = str(item.get("SITE", "") if isinstance(item, dict) else item).strip()
            if not site:
                return Response({"error": "Each site_names item must be a site name or contain SITE"},
                                status=status.HTTP_400_BAD_REQUEST)
            if isinstance(item, dict) and "FLAG" in item:
                site_flags[site] = item["FLAG"]
            if site not in sites:
                sites.append(site)

        baseline_method = request.data.get("baseline_method", "auto")
        if baseline_method not in VALID_BASELINE_METHODS:
            return Response({"error": f"baseline_method must be one of {VALID_BASELINE_METHODS}"},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            end = _parse_dt(request.data.get("end_date")) or _dt.datetime.now().replace(minute=0, second=0, microsecond=0)
            start = _parse_dt(request.data.get("start_date")) or end - _dt.timedelta(hours=DEFAULT_WINDOW_HOURS)
            if start >= end:
                raise ValueError("start_date must be before end_date")
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        callback_url = request.data.get("callback_url")
        who = get_request_identity(request)
        log_path, ts = self.get_log_file("nci_rca", "now", "now")
        request_logger = build_request_logger(log_path, ts, who)
        request_logger.info(f"[NCI-RCA] by {who}, sites={len(sites)}, window={start}..{end}, baseline={baseline_method}")

        if not callback_url:
            try:
                result = run_rca_site_analysis(sites, start, end, request_logger, log_path, ts, baseline_method)
                _attach_flags(result, site_flags)
                return Response(_json_safe(result), status=status.HTTP_200_OK)
            except Exception:
                request_logger.exception("[NCI-RCA] analysis failed")
                return Response({"error": "Internal server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        job_id = job_manager.create_job(who=who, job_type="nci_rca")
        threading.Thread(
            target=self._run_and_callback,
            args=(job_id, sites, site_flags, start, end, baseline_method, request_logger, log_path, ts, callback_url),
            daemon=True,
        ).start()
        return Response({"job_id": job_id, "status": "PENDING"}, status=status.HTTP_202_ACCEPTED)

    def get(self, request, job_id=None):
        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return Response(job)

    def _run_and_callback(self, job_id, sites, site_flags, start, end, baseline_method,
                          request_logger, log_path, ts, callback_url):
        import requests
        try:
            result = run_rca_site_analysis(sites, start, end, request_logger, log_path, ts, baseline_method, job_id=job_id)
            _attach_flags(result, site_flags)
            safe_result = _json_safe(result)
            job_manager.update_job(job_id, status="DONE", step="completed", progress_percent=100, result=safe_result)
        except Exception as e:
            request_logger.exception(f"[NCI-RCA JOB FAILED] {job_id}: {e}")
            job_manager.update_job(job_id, status="FAILED", step="failed", error=str(e))
            safe_result = {"job_id": job_id, "status": "FAILED", "error": str(e)}
        try:
            requests.post(callback_url, json=safe_result, timeout=10)
        except Exception as e:
            request_logger.error(f"[NCI-RCA] callback POST to {callback_url} failed: {e}")
