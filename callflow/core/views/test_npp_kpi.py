# callflow/api/npp_kpi.py
import datetime

import pandas as pd
from rest_framework.response import Response
from rest_framework import status as http_status

from .base import BaseCallFlowAPIView
from ..services.npp_service import nppIntegration


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


class NppKpiAPIView(BaseCallFlowAPIView):
    def post(self, request):
        try:
            entities = request.data.get("entities") or request.data.get("site_names") or request.data.get("cells") or []
            if isinstance(entities, str):
                entities = [item.strip() for item in entities.split(",") if item.strip()]
            entities = [str(item).strip() for item in entities if str(item).strip()]
            if not entities:
                raise ValueError("entities is required")

            entity_type = request.data.get("entity_type") or "site"
            resolution = request.data.get("resolution") or "hourly"
            start_dt = _parse_dt(request.data.get("start_date") or request.data.get("start_dt"))
            end_dt = _parse_dt(request.data.get("end_date") or request.data.get("end_dt"))
            if end_dt <= start_dt:
                raise ValueError("end_date must be after start_date")

            log_path, ts = self.get_log_file("npp", start_dt, end_dt)
            df = nppIntegration(
                entities,
                start_dt,
                end_dt,
                log_path,
                ts,
                resolution,
                entity_type=entity_type,
            )
            records = []
            if isinstance(df, pd.DataFrame) and not df.empty:
                records = df.where(df.notna(), None).to_dict(orient="records")

            return Response({
                "entity_type": entity_type,
                "resolution": resolution,
                "count": len(records),
                "data": records,
            })
        except ValueError as e:
            return Response({"error": str(e)}, status=http_status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"error": str(e)}, status=http_status.HTTP_500_INTERNAL_SERVER_ERROR)