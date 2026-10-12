# callflow/api/viavi_kpi.py
import datetime

import pandas as pd
from rest_framework.response import Response
from rest_framework import status as http_status

from .base import BaseCallFlowAPIView
from callflow.core.services.viavi_service import viaviQueries
from callflow.core.utils.connections import viaviConnection


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


class ViaviKpiAPIView(BaseCallFlowAPIView):
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

            window = self.parse_request_time_range(request.data)
            
            # viaviQueries expects the same formatted strings NCI passes as window["formatted_*"]
            formatted_start = window['formatted_startDate']
            formatted_end = window['formatted_endDate']

            log_path, ts = self.get_log_file("viavi", window['start_dt'], window['end_dt'])
            viavi_conn = viaviConnection(log_path)
            if not viavi_conn:
                raise RuntimeError("Viavi database connection failed")
            df = viaviQueries(viavi_conn, formatted_start, formatted_end, sites, log_path, ts)
            conn = None

            records = []
            columns = []
            if isinstance(df, pd.DataFrame):
                columns = [str(c) for c in df.columns]
                if not df.empty:
                    records = df.where(df.notna(), None).astype(object).where(df.notna(), None).to_dict(orient="records")

            return Response({
                "sites": sites,
                "start": formatted_start,
                "end": formatted_end,
                "columns": columns,
                "count": len(records),
                "data": records,
            })
        except ValueError as e:
            if pool is not None and conn is not None:
                pool.invalidate(conn)
            return Response({"error": str(e)}, status=http_status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            if pool is not None and conn is not None:
                pool.invalidate(conn)
            return Response({"error": str(e)}, status=http_status.HTTP_500_INTERNAL_SERVER_ERROR)