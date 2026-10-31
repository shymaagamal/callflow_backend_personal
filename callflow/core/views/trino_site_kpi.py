# callflow/api/smartcare_site_kpi.py
import logging

from rest_framework.response import Response
from rest_framework import status as http_status

from .base import BaseCallFlowAPIView
from ..services.trino_site_kpi_service import fetch_trino_site_kpis

logger = logging.getLogger(__name__)

class TrinoSiteKpiAPIView(BaseCallFlowAPIView):
    def post(self, request):
        try:
            print("Received request for Trino Site KPIs")
            sites = request.data.get("site_names") or request.data.get("sites") or []
            if isinstance(sites, str):
                sites = [s.strip() for s in sites.split(",") if s.strip()]
            print(f"Fetching KPIs for sites: {sites}")
            payload = fetch_trino_site_kpis(
                sites,
                log_path="",
            )
            return Response(payload)
        except ValueError as e:
            logger.error(f"Value error occurred: {e}")
            return Response({"error": str(e)}, status=http_status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"error": str(e)}, status=http_status.HTTP_500_INTERNAL_SERVER_ERROR)