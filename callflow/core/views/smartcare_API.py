# callflow/api/smartcare_site_kpi.py
import datetime
import logging
import os
import pandas as pd

from rest_framework.response import Response
from rest_framework import status as http_status

from callflow_portal import settings

from .base import BaseCallFlowAPIView
from ..services.smartcare_service import fetch_smartcare_nodes_data

logger = logging.getLogger(__name__)

class SmartcareAPIView(BaseCallFlowAPIView):
    def post(self, request):
        try:
            print("Received request for Smartcare Site KPIs")
            msisdn_list = request.data.get("msisdn_list") 
            start_dt = request.data.get("start_dt")
            end_dt = request.data.get("end_dt")
            complain_type = request.data.get("Complain_Type")
            print(f"Fetching KPIs for MSISDNs: {msisdn_list}, start_dt: {start_dt}, end_dt: {end_dt}, complain_type: {complain_type}")
            if isinstance(msisdn_list, str):
                msisdn_list = [s.strip() for s in msisdn_list.split(",") if s.strip()]
            payload = fetch_smartcare_nodes_data(
                msisdn_list,
                start_dt,
                end_dt,
                complain_type,
                log_path="",
            )
            result=payload["processed_data"]

            result = result.drop(columns= ['NODE'], errors='ignore')  # Drop 'NODE' column if it exists
            result['dest_host'] = result['dest_host'].str.split('.').str[0]  # Keep only the first part of dest_host
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            csv_path = os.path.join(settings.OUTPUT_DIR,"VOICE", f"smartcare_VOICE_{timestamp}.csv")
            if not os.path.exists(os.path.dirname(csv_path)):
                os.makedirs(os.path.dirname(csv_path))
            print(f"Saving Smartcare data to {csv_path}")
            pd.DataFrame(result).to_csv(csv_path, index=False)
            logger.info(f"Smartcare data saved to {csv_path}")
            return Response(result)
        except ValueError as e:
            logger.error(f"Value error occurred: {e}")
            return Response({"error": str(e)}, status=http_status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"error": str(e)}, status=http_status.HTTP_500_INTERNAL_SERVER_ERROR)