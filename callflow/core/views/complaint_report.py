import logging
from django.http import JsonResponse
from rest_framework.response import Response
from .base import BaseCallFlowAPIView
from callflow.core.services.complain_reports import retrieve_complaint_by_incident_no
logger = logging.getLogger(__name__)

class BaseComplainReportAPIView(BaseCallFlowAPIView):
    """
    Orchestrates abnormal KPI analysis across Smartcare, Splunk, NPP, and Viavi.
    Returns analyzed top degraded KPIs + time-series data for charting.
    """
    def post(self, request):
        incident_No = request.data.get('incident_no')
        df = retrieve_complaint_by_incident_no(incident_No)
        
        if df is None:
            return JsonResponse({
                "status": "not_found",
                "message": f"No complaint found for Incident No: {incident_No}"
            }, status=404)
    
        return JsonResponse({
            "status": "success",
            "data": df.to_dict(orient="records")
        })
    