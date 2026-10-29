# NCI/views.py
"""
NCI is a public-facing API with an unknown/uncontrolled frontend, so the
default endpoint is a PLAIN SYNCHRONOUS request/response -- no job_id,
no polling contract assumed. Speed comes from caching + the concurrent
fetch in analysis_runner.run_nci_analysis, not from making the caller poll.

One opt-in way to avoid holding the connection open (useful when a reverse
proxy in front of Gunicorn cuts long requests with a 502):

  * async=1   (query string ?async=1, or "async": true in the body)
              -> 202 {job_id, poll_url}; the caller polls GET <poll_url>
                 until status is DONE or FAILED.

The webhook (callback_url: POST the result to a caller URL) has been removed.
A callback_url in the request body is ignored (and logged).
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


def _truthy(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _is_async(request):
    """True when the caller asked for job mode via ?async=1 or {"async": true}."""
    if _truthy(request.query_params.get("async", "")):
        return True
    data = request.data if isinstance(request.data, dict) else {}
    return _truthy(data.get("async", ""))


def _log_ignored_callback(request, request_logger, tag):
    """The webhook was removed: a callback_url sent by an old caller is ignored."""
    data = request.data if isinstance(request.data, dict) else {}
    if data.get("callback_url"):
        request_logger.warning(f"{tag} callback_url is no longer supported and was ignored; "
                               f"use ?async=1 and poll the job instead")


def _poll_url(request, job_id):
    """Poll path for this job, built from the path the caller actually used."""
    return f"{request.path.rstrip('/')}/{job_id}/"


# @method_decorator(require_api_token, name="dispatch")
class NCIAnalysisAPIView(BaseCallFlowAPIView):
    """
    POST callflow/NCI/analysis/            (add ?async=1 for job mode)
    body: {"site_names": [{"SITE": "...", "FLAG": "..."}, ...],
           "async": true}             # optional, same as ?async=1

    Default: blocks until the 3-period x 3-source analysis completes and
    returns the full result in one response.

    With async=1: returns 202 {job_id, status, poll_url} immediately and
    runs the same analysis in a background thread.
    GET callflow/NCI/analysis/<job_id>/ returns the job; when status is
    DONE, job["result"] holds exactly what the synchronous call returns.
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

        async_mode = _is_async(request)
        who = get_request_identity(request)
        log_path, ts = self.get_log_file("nci", "now", "now")

        request_logger = build_request_logger(log_path, ts, who)
        request_logger.info(
            f"[NCI] requested by {who}, sites={len(site_names)}, async={async_mode}"
        )
        _log_ignored_callback(request, request_logger, "[NCI]")

        if not async_mode:
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

        # ── Job path: async=1 ───────────────────────────────────────────
        job_id = job_manager.create_job(who=who, job_type="nci")
        worker = threading.Thread(
            target=self._run_job,
            args=(job_id, sites, site_flags, request_logger, log_path, ts),
            daemon=True,
        )
        worker.start()
        request_logger.info(f"[NCI JOB STARTED] {job_id}")
        return Response(
            {"job_id": job_id, "status": "PENDING", "poll_url": _poll_url(request, job_id)},
            status=status.HTTP_202_ACCEPTED,
        )

    def get(self, request, job_id=None):
        """Poll endpoint for the async=1 path."""
        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return Response(job)

    def _run_job(self, job_id, sites, site_flags, request_logger, log_path, ts):
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
    POST callflow/NCI/rca/                 (add ?async=1 for job mode)
    body: {
      "site_names": [{"SITE": "LJ1333", "FLAG": "..."}, ...]   # same format as /analysis/ (plain strings also accepted)
      "start_date": "2026-10-11 10:00",   # optional, default end_date - 3h
      "end_date":   "2026-10-11 13:00",   # optional, default now (rounded down to the hour)
      "baseline_method": "auto",          # auto | 2h_before | yesterday  (RCA tool default: auto)
      "async": true                       # optional, same as ?async=1
    }
    Response: same sites_summary fields as /analysis/ (critical_cases, minor_cases,
    trouble_tickets, FLAG) plus status, is_degraded, rca_score, rank,
    degraded_cells, degraded_routes; and ranking / degraded_sites lists.
    In job mode this is job["result"] from GET callflow/NCI/rca/<job_id>/.
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

        async_mode = _is_async(request)
        who = get_request_identity(request)
        log_path, ts = self.get_log_file("nci_rca", "now", "now")
        request_logger = build_request_logger(log_path, ts, who)
        request_logger.info(
            f"[NCI-RCA] by {who}, sites={len(sites)}, window={start}..{end}, "
            f"baseline={baseline_method}, async={async_mode}"
        )
        _log_ignored_callback(request, request_logger, "[NCI-RCA]")

        if not async_mode:
            try:
                result = run_rca_site_analysis(sites, start, end, request_logger, log_path, ts, baseline_method)
                _attach_flags(result, site_flags)
                return Response(_json_safe(result), status=status.HTTP_200_OK)
            except Exception:
                request_logger.exception("[NCI-RCA] analysis failed")
                return Response({"error": "Internal server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        job_id = job_manager.create_job(who=who, job_type="nci_rca")
        threading.Thread(
            target=self._run_job,
            args=(job_id, sites, site_flags, start, end, baseline_method, request_logger, log_path, ts),
            daemon=True,
        ).start()
        request_logger.info(f"[NCI-RCA JOB STARTED] {job_id}")
        return Response(
            {"job_id": job_id, "status": "PENDING", "poll_url": _poll_url(request, job_id)},
            status=status.HTTP_202_ACCEPTED,
        )

    def get(self, request, job_id=None):
        if not job_id:
            return Response({"error": "job_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        job = job_manager.get_job(job_id)
        if job is None:
            return Response({"error": "job not found or expired"}, status=status.HTTP_404_NOT_FOUND)
        return Response(job)

    def _run_job(self, job_id, sites, site_flags, start, end, baseline_method,
                 request_logger, log_path, ts):
        try:
            result = run_rca_site_analysis(sites, start, end, request_logger, log_path, ts, baseline_method, job_id=job_id)
            _attach_flags(result, site_flags)
            safe_result = _json_safe(result)
            job_manager.update_job(job_id, status="DONE", step="completed", progress_percent=100, result=safe_result)
            request_logger.info(f"[NCI-RCA JOB DONE] {job_id}")
        except Exception as e:
            request_logger.exception(f"[NCI-RCA JOB FAILED] {job_id}: {e}")
            job_manager.update_job(job_id, status="FAILED", step="failed", error=str(e))
