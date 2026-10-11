from callflow.core.services.dqi_service import fetch_dqi_data
from callflow.core.services.smartcare_service import fetch_smartcare_nodes_data
from callflow.core.utils.file_cache import make_smartcare_cache_key
from .base import BaseCallFlowAPIView
from rest_framework.response import Response
from django.core.cache import cache
import logging
import pandas as pd
from django.conf import settings


logger = logging.getLogger(__name__)

class DQIAPIView(BaseCallFlowAPIView):   # or add to existing view
    def post(self, request):
        try:
            logger.info("DQI View is called")
            print('We are start in DQI File -----')
            cell_list = request.data.get('cell_list')
            if not cell_list:
                raise ValueError("cell_list is required")
            window = self.parse_request_time_range(request.data)
            complain_type = request.data.get('Complain_Type', 'Data')
            log_path, ts = self.get_log_file("scf_kpi", window['start_dt'], window['end_dt'])
            cache_key = make_smartcare_cache_key(cell_list, window['start_dt'], window['end_dt'] ,complain_type)



            kpi_data = fetch_dqi_data(
                cell_list=cell_list,
                start_dt=window['start_dt'],
                end_dt=window['end_dt'],
                log_path=log_path
            )
            print('kpi_data', kpi_data)
            kpi_data.to_excel(settings.OUTPUT_DIR / f"DQI_KPI_filter_{ window['start_dt']}.xlsx", index=False)
            if not kpi_data.empty: kpi_data = kpi_data.astype(object).where( pd.notnull(kpi_data), None )
            return Response({
                    'dqi_data': kpi_data.to_dict(orient='records') if not kpi_data.empty else [],
                })
            
        except Exception as e:
            logger.exception("DQIAPIView error")
            return Response({'error': str(e)}, status=500)