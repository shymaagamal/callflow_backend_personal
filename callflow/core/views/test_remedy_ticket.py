# callflow/api/remedy_kpi.py
import datetime

import pandas as pd
from rest_framework.response import Response
from rest_framework import status as http_status

from ..constants.general_constants import (
    SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
    DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD,
)

from .base import BaseCallFlowAPIView
from callflow.core.services.remedy_service import remedyTicketsData
from ..utils.classes import Connection


def _parse_dt(value):
    if isinstance(value, datetime.datetime):
        return value
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError("invalid datetime: %s" % value)


def _json_safe(obj):
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime.datetime)):
        return None if pd.isna(obj) else obj.isoformat(sep=" ")
    if isinstance(obj, float) and pd.isna(obj):
        return None
    return obj


class RemedyKpiAPIView(BaseCallFlowAPIView):
    def post(self, request):
        conn = None
        pool = None
        try:
            sites = request.data.get("sites") or request.data.get("entities") or request.data.get("site_names") or []
            if isinstance(sites, str):
                sites = [item.strip() for item in sites.split(",") if item.strip()]
            sites = [str(item).strip() for item in sites if str(item).strip()]
            if not sites:
                raise ValueError("sites is required")

            start_dt = _parse_dt(request.data.get("start_date") or request.data.get("start_dt"))
            end_dt = _parse_dt(request.data.get("end_date") or request.data.get("end_dt"))
            if end_dt <= start_dt:
                raise ValueError("end_date must be after start_date")

            log_path, ts = self.get_log_file("remedy", start_dt, end_dt)
            conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB,
                               DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
            conn.connect()
            remedy_output, total_tickets = remedyTicketsData(
                conn, start_dt, end_dt, sites, log_path, ts,
            )
            conn = None

            return Response(_json_safe({
                "sites": sites,
                "requested_start": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "requested_end": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "query_start": (start_dt - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
                "total_tickets": total_tickets,
                "sites_with_tickets": list((remedy_output or {}).keys()),
                "data": remedy_output or {},
            }))
        except ValueError as e:
            if pool is not None and conn is not None:
                pool.invalidate(conn)
            return Response({"error": str(e)}, status=http_status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            if pool is not None and conn is not None:
                pool.invalidate(conn)
            return Response({"error": str(e)}, status=http_status.HTTP_500_INTERNAL_SERVER_ERROR)