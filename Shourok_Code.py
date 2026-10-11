#_____________ Constant file _______________:
smartcare_nodes_query = """SELECT
FROM_UNIXTIME(A.PROC_STARTTIME) as PROC_STARTTIME,
A.IMSI,
A.MSISDN,
A.IMEI,
C.TER_BRAND_NAME as IMEI_Brand,
C.TER_MODEL_NAME as IMEI_Model,
A.AAA_CLIENT_IP,
A.AAA_SERVER_IP,
A.RAT,
A.TAC,
B.cell_name,
H.NE_NAME as VPCRF_NAME,
J.NE_NAME as OCS_NAME,
F.NE_NAME as DRA_NAME,
E.MME_Name,
COALESCE(K.SGW_NAME, L.NE_NAME) as SGW_SGSN_NAME,
N.NE_NAME as p_cscf_Name,
O.NE_NAME as S_cscf_Name
FROM
(
SELECT
IMSI,MSISDN,IMEI,
NULL AS AAA_CLIENT_IP,  
NULL AS AAA_SERVER_IP,
sai_cgi_ecgi AS CELL,
SGSN_ID,
NULL AS MME_ID,
NULL AS SGW_DATA_IP,
NULL AS DRA_ID,NULL AS VPCRF_OCS_ID,NULL AS p_cscf_id,NULL AS s_cscf_id,
PROC_STARTTIME,
'3G' AS RAT,
NULL AS TAC
FROM ps.detail_cdr_gbiups_{tablename}
WHERE msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR IMSI IS NOT NULL OR MSISDN IS NOT NULL)
AND PROC_STARTTIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')

UNION ALL
SELECT
IMSI,
MSISDN,
IMEI,
NULL AS AAA_CLIENT_IP,  
NULL AS AAA_SERVER_IP,
sai_cgi_ecgi AS CELL,
NULL AS SGSN_ID,
MME_ID,
SGW_DATA_IP,
NULL AS DRA_ID,
NULL AS VPCRF_OCS_ID,
NULL AS p_cscf_id,
NULL AS s_cscf_id,
PROC_STARTTIME,
'4G' AS RAT,
CAST(
  conv(
    substr(
      substr(cast(rai_tai AS STRING), 5),
      length(substr(cast(rai_tai AS STRING), 5)) - 3,
      4
    ),
    16,
    10
  ) AS BIGINT
) AS TAC
FROM ps.DETAIL_CDR_S1MME_{tablename}
WHERE msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR MME_ID IS NOT NULL OR SGW_DATA_IP IS NOT NULL)
AND PROC_STARTTIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')
UNION ALL
SELECT
IMSI,
MSISDN,
NULL AS IMEI,
AAA_CLIENT_IP,  
AAA_SERVER_IP,
NULL AS CELL,
NULL AS SGSN_ID,
NULL AS MME_ID,
NULL AS SGW_DATA_IP,
DRA_ID,
VPCRF_OCS_ID,
NULL AS p_cscf_id,
NULL AS s_cscf_id,
CALC_TIME AS PROC_STARTTIME,
CASE
WHEN RAT = 0 THEN 'Unknown'
WHEN RAT = 1 THEN 'UTRAN'
WHEN RAT = 2 THEN 'GERAN'
WHEN RAT = 3 THEN 'WLAN'
WHEN RAT = 4 THEN 'GAN'
WHEN RAT = 5 THEN 'HSPA Evolution'
WHEN RAT = 6 THEN 'EUTRAN'
WHEN RAT = 9 THEN 'NR'
ELSE 'Unknown'
END as RAT,
NULL AS TAC
FROM ps.detail_cdr_sgidiameter_{tablename}
WHERE msisdn IN {msisdn_tuple}
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR DRA_ID IS NOT NULL OR VPCRF_OCS_ID IS NOT NULL)
AND CALC_TIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')
UNION ALL
SELECT
IMSI,
MSISDN,
IMEI,
NULL AS AAA_CLIENT_IP,  
NULL AS AAA_SERVER_IP,
Cell_id AS CELL,
NULL AS SGSN_ID,
NULL AS MME_ID,
NULL AS SGW_DATA_IP,
NULL AS DRA_ID,
NULL AS VPCRF_OCS_ID,
p_cscf_id,
s_cscf_id,
STARTTIME AS PROC_STARTTIME,
RAT as Rat,
NULL AS TAC
FROM sdr_nethouse.dw_bds_srv_volte_mo_call_bkpi_15min_{tablename}
WHERE msisdn IN {msisdn_tuple}
AND (Cell_id IS NOT NULL OR p_cscf_id IS NOT NULL OR s_cscf_id IS NOT NULL)
  AND STARTTIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')
UNION ALL
SELECT
IMSI,
MSISDN,
IMEI,
NULL AS AAA_CLIENT_IP,  
NULL AS AAA_SERVER_IP,
Cell_id AS CELL,
NULL AS SGSN_ID,
NULL AS MME_ID,
NULL AS SGW_DATA_IP,
NULL AS DRA_ID,
NULL AS VPCRF_OCS_ID,
p_cscf_id,
s_cscf_id,
STARTTIME AS PROC_STARTTIME,
RAT as Rat,
NULL AS TAC
FROM sdr_nethouse.dw_bds_srv_volte_mt_call_bkpi_15min_{tablename}
WHERE msisdn IN {msisdn_tuple}
AND (Cell_id IS NOT NULL OR p_cscf_id IS NOT NULL OR s_cscf_id IS NOT NULL)
AND STARTTIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')
) A
LEFT JOIN sdr_nethouse.dim_loc_cgisai B ON A.CELL = B.CGISAI
LEFT JOIN nethouse.dim_terminal C ON SUBSTR(A.IMEI, 1, 8) = C.TAC AND A.IMEI IS NOT NULL
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.cfg_DRA_NE_IP F ON A.DRA_ID = F.Global_NE_ID
LEFT JOIN sdr_nethouse.cfg_pcrf H ON A.VPCRF_OCS_ID = H.Global_ID
LEFT JOIN sdr_nethouse.cfg_ocs J ON A.VPCRF_OCS_ID = J.Global_ID
LEFT JOIN sdr_nethouse.dim_loc_sgw K ON A.SGSN_ID = K.SGW_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
LEFT JOIN nethouse.dim_dyn_ims_ne N ON A.p_cscf_id = N.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne O ON A.s_cscf_id = O.GLOBAL_NE_ID
WHERE msisdn IN {msisdn_tuple}
  AND PROC_STARTTIME BETWEEN UNIX_TIMESTAMP('{start_date}') AND UNIX_TIMESTAMP('{end_date}')
ORDER BY A.msisdn, A.PROC_STARTTIME;"""

# _______________________________________________________________________________

# ________________ APIs File __________________________

def smartcareNodesQueries(cursor, msisdn_list, imsi, startdate, enddate,  logs_file, timestamp):
  
  # msisdn = msisdn[1:] if msisdn.startswith('0') else msisdn
  # msisdn_tuple = tuple(msisdn_list)
  msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"

  ref_date = datetime.datetime(1970, 1, 1) 
  current_date = pd.to_datetime(startdate)
  try:
    for query in [smartcare_nodes_query]: 
      data = pd.DataFrame()

      while current_date <= pd.to_datetime(enddate):
        difference = current_date - ref_date
        tablename = difference.days
        current_date = current_date + timedelta(days=1)
      
        try:
          query = query.format(
                tablename=tablename,
                msisdn_tuple=msisdn_str,
                start_date=str(startdate),
                end_date=str(enddate) )
          
          cursor.execute(query)
          results = cursor.fetchall()
          columns = [desc[0] for desc in cursor.description]
          df = pd.DataFrame(results, columns=columns)
          data = pd.concat([data, df], ignore_index=True)

        # except Exception as e:   
        #   with open(logs_file, 'a') as logs_f:
        #     if results:
        #       logs_f.write(results)
        #     else:
        #       logs_f.write("Query failed before fetching results\n")

        #     logs_f.write(f'smartcareNodesQuery : {e} \n')
        #     logs_f.write(f"smartcareNodesQuery Error: {repr(e)} | Type: {type(e).__name__}\n")
        
        except Exception as e:
          # RETURN ERROR WITHOUT BREAKING FUNCTION
          return {
              "error": str(e),
              "query": query,
              "msisdn_list": msisdn_str,
              "table_name": tablename,
              "note": "Query failed"
          }
  
  finally:
        cursor.close()
  
  return data


def parse_dates(startDate, endDate):
    
    formatted_startDate = datetime.datetime.strptime(startDate, '%Y-%m-%dT%H:%M')
    start_hour = str(pd.to_datetime(formatted_startDate).hour)


    formatted_startDate = formatted_startDate.strftime('%Y-%m-%d %I:%M %p')
    startDate = startDate.replace('T',' ')
    formatted_startDate_2 = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
    formatted_startDate_3 = datetime.datetime.fromisoformat(startDate)
    formatted_startDate_3 = formatted_startDate_3.strftime("%m/%d/%Y:%H:%M:%S")
    formatted_startDate_4 = datetime.datetime.fromisoformat(startDate)
    formatted_startDate_4 = formatted_startDate_4.strftime("%Y/%m/%d %H:%M:%S")
                                                           
    
    formatted_endDate = datetime.datetime.strptime(endDate, '%Y-%m-%dT%H:%M')
    end_hour = str(pd.to_datetime(formatted_endDate).hour)
    formatted_endDate = formatted_endDate.strftime('%Y-%m-%d %I:%M %p')
    endDate = endDate.replace('T',' ')
    formatted_endDate_2 = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
    formatted_endDate_3 = datetime.datetime.fromisoformat(endDate)
    formatted_endDate_3 = formatted_endDate_3.strftime('%m/%d/%Y:%H:%M:%S')
    formatted_endDate_4 = datetime.datetime.fromisoformat(endDate)
    formatted_endDate_4 = formatted_endDate_4.strftime("%Y/%m/%d %H:%M:%S")
        

    date_formats = {
        'start_hour': start_hour,
        'end_hour': end_hour,
        'formatted_startDate': formatted_startDate,
        'formatted_endDate': formatted_endDate,
        'formatted_startDate_2': formatted_startDate_2,
        'formatted_endDate_2': formatted_endDate_2,
        'formatted_startDate_3': formatted_startDate_3,
        'formatted_endDate_3': formatted_endDate_3,
        'formatted_startDate_4': formatted_startDate_4,
        'formatted_endDate_4': formatted_endDate_4,
    }

    return date_formats

# ----------------------------------------------------------------------------------
# old Views.py
# --------------------------------------------------------------------------------------





import datetime 
import json
import pandas as pd

from django.shortcuts import render
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.db import connection
from django.core.paginator import Paginator
from django.http import JsonResponse


from callflow.core.apis import *
from general_apis.classes import *
from general_apis.constants import *

from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.pagination import PageNumberPagination
from callflow.core.services.kpi_analyzer import KPIAnalyzer

import logging

logger = logging.getLogger("callflow.core.views")




def parse_dates(startDate, endDate):
    
    formatted_startDate = datetime.datetime.strptime(startDate, '%Y-%m-%dT%H:%M')
    start_hour = str(pd.to_datetime(formatted_startDate).hour)
    formatted_startDate = formatted_startDate.strftime('%Y-%m-%d %I:%M %p')
    startDate = startDate.replace('T',' ')
    formatted_startDate_2 = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
    formatted_startDate_3 = datetime.datetime.fromisoformat(startDate)
    formatted_startDate_3 = formatted_startDate_3.strftime("%m/%d/%Y:%H:%M:%S")
    formatted_startDate_4 = datetime.datetime.fromisoformat(startDate)
    formatted_startDate_4 = formatted_startDate_4.strftime("%Y/%m/%d %H:%M:%S")
                                                           
    
    formatted_endDate = datetime.datetime.strptime(endDate, '%Y-%m-%dT%H:%M')
    end_hour = str(pd.to_datetime(formatted_endDate).hour)
    formatted_endDate = formatted_endDate.strftime('%Y-%m-%d %I:%M %p')
    endDate = endDate.replace('T',' ')
    formatted_endDate_2 = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
    formatted_endDate_3 = datetime.datetime.fromisoformat(endDate)
    formatted_endDate_3 = formatted_endDate_3.strftime('%m/%d/%Y:%H:%M:%S')
    formatted_endDate_4 = datetime.datetime.fromisoformat(endDate)
    formatted_endDate_4 = formatted_endDate_4.strftime("%Y/%m/%d %H:%M:%S")
        

    date_formats = {
        'start_hour': start_hour,
        'end_hour': end_hour,
        'formatted_startDate': formatted_startDate,
        'formatted_endDate': formatted_endDate,
        'formatted_startDate_2': formatted_startDate_2,
        'formatted_endDate_2': formatted_endDate_2,
        'formatted_startDate_3': formatted_startDate_3,
        'formatted_endDate_3': formatted_endDate_3,
        'formatted_startDate_4': formatted_startDate_4,
        'formatted_endDate_4': formatted_endDate_4,
    }

    return date_formats


class SmartcareIntegrationAPIView(APIView):
    # http_method_names = ['get', 'post', 'options', 'head']
    
    # # Add this for a nice title in browsable API
    
    # def get(self, request):
    #     return Response({"detail": "Smartcare Integration API - Use POST to upload file and query data."})
    def post(self, request):
            
        try:
            print("SmartcareIntegrationAPIView is called")
            uploaded_file = request.FILES.get('file')  # Excel file
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            complain_type = request.data.get('Complain_Type')  # Data / Voice 
            # msisdn = request.data.get('msisdn') 
            imsi = request.data.get('imsi')

            #msisdn ='01020170742'
            #imsi = '' 
            #start= '2025-10-01T14:16'
            #end = '2025-10-01T18:16'
            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)
            
            # Access MSISDN File
            try:
                df = pd.read_excel(uploaded_file)   
                msisdn_list = df['MSISDN'].dropna().astype(str).tolist()

            except Exception as e:
                return Response({"error": str(e)}, status=400)
            
            dates = parse_dates(start, end)

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')
            with open(logs_file, 'a') as logs_f:
                logs_f.write(f"\n{timestamp} - Smartcare Request\n{request.user}\n")
            
            Check_SmartCare_Validaion_Inputs = smartcare_Validation_Inputs(msisdn_list,dates['formatted_startDate_2'], dates['formatted_endDate_2'])
            
            cursor = smartcareConnection(logs_file)
            if cursor is None:
                return Response({'error': 'Database connection failed'}, status=500)
            
            # nodes_data = smartcareNodesQueries(cursor, msisdn_list, imsi, dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp)
            nodes_data = smartcareNodesQueries(msisdn_list, imsi,complain_type,dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp)
            # Trying discover which wrong point 
            if isinstance(nodes_data, dict) and "error" in nodes_data:
                return Response(nodes_data, status=400)
            data, site_counts, cell_counts, suspected_sgw = smartcareOutput_Aggregated(nodes_data ,logs_file, complain_type ,timestamp)
            # nodes_data, suspected_sites, suspected_cells, suspected_cells_raw, suspected_sgw = smartcareOutput(nodes_data, logs_file, timestamp)
            nodes_data.to_csv(r'/home/Shorouk/CallFlow/sc_nodes.csv') 

            smartcare_json_output = {
                col: nodes_data[col].dropna().astype(str).unique().tolist()
                for col in nodes_data.columns if col in ['IMSI','MSISDN', 'IMEI','IMEI_Brand', 'IMEI_Model', 'TAC','cell_name','MME_Name', 'SGW_PGW_NAME' ,'site_name',
                                                        'AAA_CLIENT_IP', 'AAA_SERVER_IP', 'RAT','TAC', 'VPCRF_NAME', 'OCS_NAME', 'DRA_NAME','p_cscf_Name','S_cscf_Name']
            }
            smartcare_json_output = json.dumps(smartcare_json_output, indent=4)


            return Response({
                # 'msisdn_list' : tuple(msisdn_list),
                # 'smartcare': smartcare_json_output,
                # 'suspected_sites': suspected_sites,
                # 'suspected_cells_raw': suspected_cells_raw,
                # 'suspected_sgws': suspected_sgw,
                'suspected_sites': site_counts,
                'suspected_cells_raw': cell_counts,
                'suspected_sgws': suspected_sgw,
            })
    
        except Exception as e:
            return Response({'error': str(e)}, status=500)


class SmartcareKpisIntegrationAPIView(APIView):

    def post(self, request):
            
        try:
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            uploaded_file = request.FILES.get('file')  # Excel file
            # msisdn = request.data.get('msisdn') 
            imsi = request.data.get('imsi')
            complain_type = request.data.get('Complain_Type')  # Data / Voice 
            
            #msisdn ='01020170742'
            #imsi = '' 
            #start= '2025-10-01T14:16'
            #end = '2025-10-01T18:16'


            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)
            
            # Access File to select MSISDN
            try:
                df = pd.read_excel(uploaded_file) 
                msisdn_list = df['MSISDN'].dropna().astype(str).tolist()
            except Exception as e:
                return Response({"error": str(e)}, status=400)

            print(df.head())
            dates = parse_dates(start, end)

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')
            with open(logs_file, 'a') as logs_f:
                logs_f.write(f"\n{timestamp} - Smartcare Request\n{request.user}\n")

            cursor = smartcareConnection(logs_file)
            if cursor is None:
                return Response({'error': 'Database connection failed'}, status=500)
            enterprise_data, quality_data, locations_info = smartcareKpisQueries(cursor, msisdn_list, imsi, dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp)
            enterprise_data.to_csv(r'/home/Shorouk/CallFlow/sc_kpis_enterprise.csv')
            quality_data.to_csv(r'/home/Shorouk/CallFlow/sc_kpis_quality.csv')

            
            return Response({
                'msisdn_list' : tuple(msisdn_list),
                'enterprise_kpis': enterprise_data.to_json(orient='records', default_handler=str),
                'quality_kpis': quality_data.to_json(orient='records', default_handler=str),
                'location': locations_info
            })
    
        except Exception as e:
            return Response({'error': str(e)}, status=500)


class ViaviIntegrationAPIView(APIView):
    def post(self, request):
        try:
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            suspected_sites = request.data.get('suspected_sites', [])

            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)

            dates = parse_dates(start, end)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')

            viavi_conn = viaviConnection(logs_file)
            viavi_output = viaviQueries(viavi_conn, dates['formatted_startDate'], dates['formatted_endDate'], suspected_sites, logs_file, timestamp)

            viavi_output.to_csv(r'/home/Shorouk/CallFlow/viavi_op.csv')

            viavi_json_output = viavi_output.replace({np.nan: None}).to_json(orient='records', date_format='iso')

            return Response({
                'viavi': viavi_json_output,
            })
        
        except Exception as e:
            return Response({'error': str(e)}, status=500)

class SplunkIntegrationAPIView(APIView):
    def post(self, request):
        try:
            logger.info(" SplunkIntegrationAPIView is called")
            start_time = request.data.get('start_date')
            end_time = request.data.get('end_date')
            suspected_sgw = request.data.get('suspected_sgw', [])
            node = request.data.get('node', '')
            print(f"Inputs: start={start_time}, end={end_time}, sgw={suspected_sgw}, node={node}")
        

            if not start_time or not end_time:
                return Response({'error': 'Missing start_time_date or end_date'}, status=400)

            dates = parse_dates(start_time, end_time)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')
            
            kpis_data = []
            # getKPIS(dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file)
            
            splunk_output = splunkIntegration(splunk_user, splunk_password, dates['formatted_startDate_3'], dates['formatted_endDate_3'], logs_file, kpis_data, suspected_sgw, node)                         
            print(f"Splunk raw output shape: {splunk_output.shape if not splunk_output.empty else 'EMPTY'}")
            breakpoint()
            splunk_output = splunkSitesMapping(suspected_sgw, splunk_output) 
            kpi_analyzer = KPIAnalyzer()
            breakpoint()
            alarms_kpi_issues = kpi_analyzer.analyze(splunk_output, node,start_time,end_time)
            return Response({
                'splunk': formatSplunkAPI(splunk_output, logs_file)
            })
        except Exception as e:
            return Response({'error': str(e)}, status=500)

class NppIntegration15minAPIView(APIView):
    def post(self, request):
        try:
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            suspected_cells_raw = request.data.get('suspected_cells_raw', [])

            #start= '2025-09-06T14:16'
            #end = '2025-09-07T18:16'
            #suspected_cells_raw = ['L21_LJ2709_02','L21_LJ1335_02','L21_LJ1335_03','L09_LJ1335_02']


            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)

            dates = parse_dates(start, end)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')

            npp_output = nppIntegration(suspected_cells_raw, dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp, 'daily')
            
            npp_output.to_csv('/home/Shorouk/CallFlow/npp.csv')
            
            npp_json_output = npp_output.to_json(orient='records')

            return Response({
                'npp': npp_json_output
            })
        except Exception as e:
            return Response({'error': str(e)}, status=500)


class NppIntegrationHourlyAPIView(APIView):
    
    def post(self, request):
        try:
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            suspected_cells_raw = request.data.get('suspected_cells_raw', [])

            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)

            dates = parse_dates(start, end)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')

            npp_hourly_output = nppIntegration(suspected_cells_raw, dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp, 'hourly')
            
            npp_hourly_output.to_csv('/home/Shorouk/CallFlow/npp_hourly.csv')
            
            npp_hourly_json_output = npp_hourly_output.to_json(orient='records')

            return Response({
                'npp_hourly': npp_hourly_json_output
            })
        except Exception as e:
            return Response({'error': str(e)}, status=500)


class RemedyIntegrationMain(APIView):

    def post(self, request):
        try:
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            suspected_sites = request.data.get('suspected_sites', [])
            print(suspected_sites)
            
            #start= '2025-09-06T14:16'
            #end = '2025-09-08T18:16'
            
            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)

            dates = parse_dates(start, end)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/logs.txt')
            
            # Establish database connection
            conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB, DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
            conn.connect()

            remedy_output , status_count = remedyTicketsData(conn, dates['formatted_startDate_2'], dates['formatted_endDate_2'], suspected_sites, logs_file, timestamp)
            print(remedy_output)

            remedy_json_output = remedy_output.replace({np.nan: None}).to_json(orient='records', date_format='iso')

            return Response({
                'remedy_tickets': remedy_json_output,
                'status': status_count
            })

      
        except Exception as e:
            return Response({'error': str(e)}, status=500)
        
        
        
        
        
        
        
        
        
        
        # APIS.PY
        

from io import StringIO
import os
from unittest import expectedFailure
import paramiko
import subprocess
import fnmatch
import pandas as pd
from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.conf import settings
from datetime import datetime, timedelta
from io import StringIO
# import cx_Oracle
import  cx_Oracle

import datetime
import requests
import json
import time
from pathlib import Path
import jaydebeapi
import numpy as np
from decimal import Decimal



from django.contrib.auth.models import User
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

from general_apis.classes import *
from general_apis.constants import *
from ..models import *
from callflow.core.constants import *


logger = logging.getLogger(__name__)
#___________________ Helper Functions _______________________#

def convert_java_types(df):
    def convert_value(x):
        # Handle native Python Decimal
        if isinstance(x, Decimal):
            return round(float(x),2)
        
        # Handle Java BigDecimal
        if str(type(x)).endswith("java.math.BigDecimal'>"):
            try:
              return round(float(x),2)
            except:
                return None
        
        # Handle Java Timestamp / Date
        if str(type(x)).startswith("<class 'java.sql.") or "Timestamp" in str(type(x)):
            return str(x)
        
        return x

    return df.applymap(convert_value)

#____________________________________  Smartcare INTEGRATION ______________________________________#
def smartcareConnection(logs_file):


  connection_properties = {
      "user": db_user,
      "password": db_password,
      "socketTimeout": "1000000", 
      "loginTimeout": "900",     
      "queryTimeout": "300000"       
  }

  conn = None
  try:
    conn = jaydebeapi.connect(
      jdbc_driver_class,
      jdbc_url,
      [db_user, db_password],
      jdbc_driver_jar,
      connection_properties 
  )
  except Exception as e:    
    with open(logs_file, 'a') as logs_f:
      logs_f.write(f'smartcareConnection : {e} \n')
      logs_f.write(str(conn))

  if conn is None:
      return None

  cursor = conn.cursor()


  return cursor

def smartcare_Validation_Inputs(msisdn_list,startdate,enddate):
    errors = []

    # ---------- Time validation ----------
    if enddate - startdate > timedelta(minutes=30):
        errors.append("Time_Stamp should not exceed 30 min")

    # ---------- MSISDN count validation ----------
    if len(msisdn_list) < 1:
        errors.append("MSISDN list should contain at least 1 MSISDNs")

    # ---------- MSISDN format validation ----------
    for msisdn in msisdn_list:
        msisdn_str = str(msisdn)
        print("msisdn------>",msisdn)
        if (
            not msisdn_str.isdigit()
            or len(msisdn_str) != 10
            or not msisdn_str.startswith(("10", "11", "12","15"))
        ):
            errors.append(
                f"Invalid MSISDN: {msisdn}. "
                "MSISDN must start with '10' and be exactly 10 digits"
            )

    # ---------- Final result ----------
    if errors:
        raise ValueError("; ".join(errors))
    
    return True

def smartcareNodesQueries(msisdn_list,imsi,complain_type,startdate,enddate,logs_file,timestamp):
    msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"  
  
    smartcare_Requierd_QUERIES = []
    if complain_type == 'Voice':
      smartcare_Requierd_QUERIES = smartcare_nodes_Voice
    elif complain_type == 'Data':
      smartcare_Requierd_QUERIES = smartcare_nodes_QUERIES_data
    else:
      smartcare_Requierd_QUERIES = smartcare_nodes_QUERIES_data
    
    ref_date = datetime.datetime(1970, 1, 1)

    final_data = []
    
    def run_query(query_template):
        cursor = smartcareConnection(logs_file)
        if cursor is None:
            return pd.DataFrame()

        data = pd.DataFrame()
        current_date = pd.to_datetime(startdate)
        
        try:
            Count = 0
            while current_date <= pd.to_datetime(enddate):
                tablename = (current_date - ref_date).days
                current_date += timedelta(days=1)

                query = query_template.format(
                    tablename=tablename,
                    msisdn_tuple=msisdn_str,
                    start_date=str(startdate),
                    end_date=str(enddate)
                )
                cursor.execute(query)
                results = cursor.fetchall()
                print('result')
                print(results)
                if results:
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
                    data = pd.concat([data, df], ignore_index=True)

        except Exception as e:
            with open(logs_file, 'a') as logs_f:
                logs_f.write(f'Query failed: {e}\n')

        finally:
            try:
                cursor.close()
                # cursor._connection.close()  
            except Exception:
                pass
        return data
    
    # ---------------- PARALLEL EXECUTION ---------------- #
    
    max_workers = min(5, len(smartcare_Requierd_QUERIES))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for name, query in smartcare_Requierd_QUERIES.items():
            print(name)
            futures.append(executor.submit(run_query, query))

        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                final_data.append(df)
    
    if final_data:
        return pd.concat(final_data, ignore_index=True)

    return pd.DataFrame()

def smartcareKpisQueries(cursor, msisdn_list, imsi, startdate, enddate, logs_file, timestamp):
    # msisdn = msisdn[1:] if msisdn.startswith('0') else msisdn
    msisdn_tuple = tuple(msisdn_list)
    ref_date = datetime.datetime(1970, 1, 1)

    # Prepare empty DataFrames for each query
    enterprise_data = pd.DataFrame()
    quality_data = pd.DataFrame()

    queries = {
        "enterprise": smartcare_enterprisekpis_query,
        "quality": smartcare_qualitykpis_query
    }

    for key, query_template in queries.items():
        data = pd.DataFrame()
        current_date = pd.to_datetime(startdate)  # reset for each query

        while current_date <= pd.to_datetime(enddate):
            tablename = (current_date - ref_date).days
            current_date += timedelta(days=1)

            try:
                query = query_template.format(
                    tablename=tablename,
                    msisdn_tuple=msisdn_tuple,
                    start_date=startdate,
                    end_date=enddate
                )
                cursor.execute(query)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(results, columns=columns)
                data = pd.concat([data, df], ignore_index=True)

            except Exception as e:
                with open(logs_file, 'a') as logs_f:
                    logs_f.write(f'smartcareKpisQuery ({key}) : {e}\n')
                    logs_f.write(f"smartcareKpisQuery Error: {repr(e)} | Type: {type(e).__name__}\n")

        if key == "enterprise":
            enterprise_data = data
        else:
            quality_data = data


    enterprise_data = convert_java_types(enterprise_data)
    quality_data = convert_java_types(quality_data)

    enterprise_data = enterprise_data.replace([float('inf'), float('-inf')], pd.NA)
    quality_data = quality_data.replace([float('inf'), float('-inf')], pd.NA)

    try:
       enterprise_data['DATE'] = pd.to_datetime(enterprise_data['DATE'], format="%Y-%m-%d %H:%M")
    except:
       enterprise_data['DATE'] = enterprise_data['DATE']

    clusters = enterprise_data['CLUSTER'].dropna().unique().tolist()
    regions = enterprise_data['REGION'].dropna().unique().tolist()
    subregions = enterprise_data['SUBREGION'].dropna().unique().tolist()

    locations_info = {'clusters':clusters, 'regions':regions, 'subregion':subregions }

    cursor.close()

    return enterprise_data, quality_data, locations_info

def smartcareOutput_Aggregated(data, logs_file,complain_type,timestamp):
  suspected_cells = []
  suspected_sites = []
  suspected_sgw = []
  suspected_cells_raw = []
  cell_counts = 0
  site_counts = 0

  try:
    data = data.fillna('')

    if not data.empty:

      data = data.dropna(subset=['cell_name', 'SGW_SGSN_NAME'])
      data = data[(data['cell_name'].str.strip() != '') & (data['SGW_SGSN_NAME'].str.strip() != '')]

      # __________ CELL COUNTS ___________ 
      cell_counts = data['cell_name'].value_counts().to_dict()

      suspected_cells_raw = list(set(data['cell_name'].to_list()))
      
      # -------- SITE LOGIC --------
      data['site_name'] = data['cell_name'].str.split('_L').str[1]
      data['site_name'] = data['site_name'].str.split('_').str[0]

      data['site_name_number'] = data['site_name'].str.extract(r'(\d+)')
      data['site_name_number'] = data['site_name_number'].astype(str)


      def getSiteLetter(site_name, site_number):
        return str(site_name).replace(str(site_number), '')
      data['site_name_letter'] = data.apply(lambda row: getSiteLetter(row['site_name'], row['site_name_number']), axis=1)

      data['modified_site_name_number'] = data['site_name_number'].str.zfill(4)
      data['modified_site_name'] = data['site_name_letter'] + data['modified_site_name_number']
      
      # ________ Site COUNTS ______________
      site_counts = data['modified_site_name'].value_counts().to_dict()

      suspected_cells = tuple(set(data['cell_name'].to_list()))
      suspected_sites = list(set(data['modified_site_name'].to_list()))

      try:
         data.rename(columns={'SGW_SGSN_NAME':'SGW_PGW_NAME'}, inplace=True)
      except Exception as e:
        with open(logs_file, 'a') as logs_f:
          logs_f.write('Smartcare Nodes Data Prep Error : ' + str(e) + '\n')
        
      
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_vSGW-vPGW_','', regex=False)
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_SGW-PGW_','', regex=False)
     
      suspected_sgw = data['SGW_PGW_NAME'].value_counts().to_dict()

    else:
      suspected_cells_raw = []
      suspected_sgw = []
      suspected_sites = []
      suspected_cells = []

      
  except Exception as e:
    with open(logs_file, 'a') as logs_f:
      logs_f.write('Smartcare Output error : ' + str(e) + '\n')

  print(data.columns)
  return data, site_counts, cell_counts, suspected_sgw


def smartcareOutput(data, logs_file, timestamp):  

  suspected_cells = []
  suspected_sites = []
  suspected_sgw = []
  suspected_cells_raw = []

  try:
    data = data.fillna('')

    if not data.empty:

      data = data.dropna(subset=['cell_name', 'SGW_SGSN_NAME'])
      data = data[(data['cell_name'].str.strip() != '') & (data['SGW_SGSN_NAME'].str.strip() != '')]

      suspected_cells_raw = list(set(data['cell_name'].to_list()))
      print('Cell_nameeeeeeeeeeeeeeeee')
      print(data['cell_name'].to_list())
      data['site_name'] = data['cell_name'].str.split('_L').str[1]
      data['site_name'] = data['site_name'].str.split('_').str[0]

      data['site_name_number'] = data['site_name'].str.extract(r'(\d+)')
      data['site_name_number'] = data['site_name_number'].astype(str)


      def getSiteLetter(site_name, site_number):
        return str(site_name).replace(str(site_number), '')
      data['site_name_letter'] = data.apply(lambda row: getSiteLetter(row['site_name'], row['site_name_number']), axis=1)

      data['modified_site_name_number'] = data['site_name_number'].str.zfill(4)
      data['modified_site_name'] = data['site_name_letter'] + data['modified_site_name_number']


      suspected_cells = tuple(set(data['cell_name'].to_list()))
      suspected_sites = list(set(data['modified_site_name'].to_list()))

      try:
         data.rename(columns={'SGW_SGSN_NAME':'SGW_PGW_NAME'}, inplace=True)
      except Exception as e:
        with open(logs_file, 'a') as logs_f:
          logs_f.write('Smartcare Nodes Data Prep Error : ' + str(e) + '\n')
        
      
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_vSGW-vPGW_','', regex=False)
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_SGW-PGW_','', regex=False)
      suspected_sgw = list(set(data['SGW_PGW_NAME'].to_list()))

    else:
      suspected_cells_raw = []
      suspected_sgw = []
      suspected_sites = []
      suspected_cells = []

      
  except Exception as e:
    with open(logs_file, 'a') as logs_f:
      logs_f.write('Smartcare Output error : ' + str(e) + '\n')


  return data, suspected_sites, suspected_cells, suspected_cells_raw, suspected_sgw

#____________________________________  TWAMP INTEGRATION ______________________________________#
def viaviConnection(logs_file):
  
  viavi_conn = None
  try:
    dsn_tns = cx_Oracle.makedsn(viavi_ip, viavi_port, service_name=viavi_service_name)
    viavi_conn = cx_Oracle.connect(user=viavi_user, password=viavi_password, dsn=dsn_tns)
    cur = viavi_conn.cursor()
  except Exception as e:
    with open(logs_file, 'a') as logs_f:
      logs_f.write(f'viaviConnection : ' + str(e) +  '\n')


  return viavi_conn


def viaviQueries(conn, start_date, end_date, suspected_sites, logs_file, timestamp):
  
  startdate_period = 'PM'
  if 'AM' in str(start_date):
    startdate_period = 'AM'

  enddate_period = 'PM'
  if 'AM' in str(end_date):
    enddate_period = 'AM'

  query_1 = f'''SELECT PERIOD_TIMESTAMP, FLWID, AVG_FD_MS, AVG_FDV_MS, ONEW_FLR_AZ, ONEW_FLR_ZA  
               FROM  ONEADM."vw_VIAVI_Ethernet_Raw"
               WHERE  (PERIOD_TIMESTAMP >= to_timestamp('{start_date}','YYYY-MM-DD HH:MI {startdate_period}'))
               AND (PERIOD_TIMESTAMP <= to_timestamp('{end_date}','YYYY-MM-DD HH:MI {enddate_period}'))
               AND ( '''

  
  if len(suspected_sites) > 0:
    if len(suspected_sites) == 1:
      for i, site in enumerate(suspected_sites):
        query_1 += f" FLWID LIKE '%{site}%' )"
    else:
      for i, site in enumerate(suspected_sites):
        if i == 0:
          query_1 += f" FLWID LIKE '%{site}%' "
        elif i == len(suspected_sites) - 1:
          query_1 += f" OR FLWID LIKE '%{site}%' ) "
        else:
          query_1 += f" OR FLWID LIKE '%{site}%' "
    
    query_1 += "ORDER BY PERIOD_TIMESTAMP"

    with open(logs_file, 'a') as logs_f:
      logs_f.write(query_1)
    
    
    viavi_output = None
    try:
      viavi_output = pd.read_sql_query(query_1, conn)
    except Exception as e:
      with open(logs_file, 'a') as logs_f:
        logs_f.write(f'viaviQueries : {e} \n')
      
    if viavi_output is None:
      viavi_output = pd.DataFrame(columns=['PERIOD_TIMESTAMP', 'FLWID', 'AVG_FD_MS', 'AVG_FDV_MS', 'ONEW_FLR_AZ', 'ONEW_FLR_ZA'])

  else:
    viavi_output = pd.DataFrame(columns=['PERIOD_TIMESTAMP', 'FLWID', 'AVG_FD_MS', 'AVG_FDV_MS', 'ONEW_FLR_AZ', 'ONEW_FLR_ZA'])


  return viavi_output


def viaviOutput(viavi_output, timestamp):

  fileName = f'Viavi_output_{timestamp}.csv'
  output_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/{fileName}')

  viavi_output.to_csv(output_file)
  viavi_url = static(f'/CallFlow/output/{fileName}')

  return  viavi_url


#____________________________________  NPP INTEGRATION _______________________________________#



def nppIntegration(
    suspected_cells_raw,
    start_date,
    end_date,
    logs_file,
    timestamp,
    resolution
):
    """
    Integrates with NPP service to fetch KPI data per cell for a time range.
    Loops over hours from start_date to end_date.
    """
    logger.info("nppIntegration is called")
    # ---------------- Time handling ----------------
    reference_date = pd.Timestamp("2020-01-01")
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    # Calculate start and end hour differences
    start_hour_diff = int((start_date - reference_date) / pd.Timedelta(hours=1))
    # end_hour_diff = int((end_date - reference_date) / pd.Timedelta(hours=1))
    filename = ""
    print(f"start_hour_diff {start_hour_diff} and end_hour_diff ")
    # ---------------- URL selection ----------------
    if resolution == "daily":
        url = "http://10.241.161.227:3000/get-kpis"
        filename = "npp_15min"
    elif resolution == "hourly":
        url = "http://10.241.161.227:3000/get-kpis/hourly"
        filename = "npp_hourly"
        
    else:
        raise ValueError(f"Invalid resolution: {resolution}")


    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.0V2nR3wHXDQI9ZRtxXut8euJmxjCRPWLsVPL0DWDKl8"
    }

    oss_df = pd.DataFrame()
    try:
        suspected_cells = json.loads(suspected_cells_raw) if isinstance(suspected_cells_raw, str) else suspected_cells_raw
    except:
        suspected_cells = []

    if not suspected_cells:
        logger.warning("No cells provided to nppIntegration")
        return pd.DataFrame()
    
    # for hour_diff in range(start_hour_diff, end_hour_diff + 1):
    for cell in suspected_cells:
        data = {
            "cellName": cell,
            "hour": start_hour_diff
        }
        try:
            print(f"Request → hour={start_hour_diff}, cell={cell}")
            
            response = requests.post(url, json=data, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"NPP API error for hour {start_hour_diff}, cell {cell}: {e}")
            continue
        except Exception as e:
            logger.error(f"NPP connection error: {e}")
            continue

        if response.status_code == 200:
            try:

                df = pd.read_csv(StringIO(response.text))
                df['DATETIME_ID'] = pd.to_datetime(df['DATETIME_ID'])
                oss_df = pd.concat([oss_df, df], ignore_index=True)
            except Exception as e:
                logger.error(f"NPP parsing error for cell {cell}: {e}")


           
    # Sort and group
    if not oss_df.empty:
        oss_df = oss_df.sort_values(by='DATETIME_ID')
        oss_df = oss_df.groupby(['DATETIME_ID', 'Cell', 'Vendor']).sum().reset_index()
    
    oss_df.to_csv(settings.OUTPUT_DIR / f"{filename}.csv" )

    return oss_df
    

def nppHourlyIntegration(suspected_cells_raw, start_date, end_date, logs_file, timestamp):

  reference_date = pd.Timestamp("2020-01-01")
  start_date = pd.Timestamp(start_date)

  hours_difference = (start_date - reference_date) / pd.Timedelta(hours=1)


  url = "http://10.241.161.227:3000//get-kpis/hourly "
  #"https://egzhr-wirkp04:3000/get-kpis"


  headers = {
      "Content-Type": "application/json",
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.0V2nR3wHXDQI9ZRtxXut8euJmxjCRPWLsVPL0DWDKl8"
  }

  oss_hourly_df = pd.DataFrame()

  for cell in suspected_cells_raw:
      data = {
          "cellName": cell,
          "hour": hours_difference
      }

      response = requests.post(url, json=data, headers=headers)

      if response.status_code == 200:
          try:
              # Convert response JSON to DataFrame
              try:
                df = pd.read_csv(StringIO(response.text), parse_dates=['DATETIME_ID'])
              except:
                df = pd.read_csv(StringIO(response.text))

              try:
                df = df[df['DATETIME_ID'] <= end_date] 
              except:
                 df = df

              #df["cellName"] = cell  

              # Concatenate to final DataFrame
              oss_hourly_df = pd.concat([oss_hourly_df, df], ignore_index=True)

          except Exception as e:   
            with open(logs_file, 'a') as logs_f:
              logs_f.write(f'NPP Error : {e} \n')
              logs_f.write(f'Failed to retrieve reponse for cell : {cell} \n')

      else:
            with open(logs_file, 'a') as logs_f:
              logs_f.write(f'API Call Failed : {cell} {response.status_code} {response.text} \n')

      oss_hourly_df['DATETIME_ID'] = pd.to_datetime(oss_hourly_df['DATETIME_ID'])
      oss_hourly_df.groupby([ 'DATETIME_ID','Cell', 'Vendor']).sum().reset_index()
      oss_hourly_df = oss_hourly_df.sort_values(by='DATETIME_ID')



  return oss_hourly_df
  


#____________________________________  E2E Report KPIS _______________________________________#

def getKPIS(formatted_startDate_2, formatted_endDate_2, logs_file):
   
  conn = Connection(SQL_DRIVER, DJANGO_SERVER, CORE_NW_DATABASE, DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
  conn.connect()

  formatted_startDate_2 = formatted_startDate_2.date()
  formatted_endDate_2 = formatted_endDate_2.date()
  
  query = f''' select Date As timestamp, NodeName, Value from CoreNW_KPIS.dbo.Utility2
              where kpi = 'PoolUtil' and Date >= '{formatted_startDate_2}' and Date <= '{formatted_endDate_2}'  '''
  
  kpis_data = conn.query_select(query)
  if kpis_data.shape[0] == 0:
     print('Trueeee')
     kpis_data = pd.DataFrame(columns=['timestamp','NodeName','Value'])
  kpis_data['Value'] = kpis_data['Value'].astype(float)
  kpis_data = kpis_data.pivot_table(index='timestamp', columns='NodeName', values='Value', aggfunc='sum').reset_index()
  kpis_data.columns = ['timestamp'] + [f'GGSN_PoolUtil_{col}' for col in kpis_data.columns[1:]]
  print(kpis_data)

  return kpis_data

   

#____________________________________  Splunk INTEGRATION _______________________________________#



def splunkIntegration(user, password, formatted_startDate_3, formatted_endDate_3, logs_file, kpis_data, suspected_sgw, node):
    headers = {"Content-Type": "application/json"}
    data = pd.DataFrame()
    print(f"\n{'='*50}")
    print(f"SPLUNK INTEGRATION CALLED")
    print(f"Node filter: {node}")
    print(f"SGW list: {suspected_sgw}")
    print(f"{'='*50}\n")
    def process_kpi(kpi, api_endpoint):
        """Handles one KPI API call and returns its dataframe"""
        print(f"Processing KPI: {kpi}")
        if not kpi.startswith(node):
            return None
        # DNS filtering
        # if kpi.startswith("DNS_"):
        #     dns_name = kpi.rsplit("_", 1)[1]
        #     if all(
        #         not (mme in mapping and dns_name in mapping[mme].get("DNS", []))
        #         for mme in suspected_sgw
        #     ):
        #         return None


        try:
            api_endpoint = api_endpoint.replace("start_time", formatted_startDate_3)
            api_endpoint = api_endpoint.replace("end_time", formatted_endDate_3)

            response = requests.get(
                api_endpoint,
                headers=headers,
                auth=HTTPBasicAuth(user, password),
                verify=False,
                timeout=60
            )
            print(f"Fetched {kpi}: {len(data)} rows")

            if response.status_code != 200:
                with open(logs_file, "a") as logs_f:
                    logs_f.write(f"Failed to connect to API. Status code: {response.status_code}\n")
                return pd.DataFrame(columns=["timestamp"])

            try:
                df = pd.read_csv(StringIO(response.text))

                # Validate or normalize columns
                if ("Test API status" in df.columns or "Test API Source" in df.columns) and df.shape[0] < 4:
                    return pd.DataFrame(columns=["timestamp"])

                if "_span" in df.columns:
                    df.drop("_span", axis=1, inplace=True)
                    
                rename_map = { "_time": "timestamp",
                              "DATETIME_ID": "timestamp",
                             "time": "timestamp"}
                df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

                # Prefix KPI name
                df.columns = [df.columns[0]] + [kpi + "_" + col for col in df.columns[1:]]

                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                except:
                    with open(logs_file, "a") as logs_f:
                        logs_f.write("No Timestamp column in \n")
                        logs_f.write(api_endpoint + "\n")
                    df.rename(columns={df.columns[0]: "timestamp"}, inplace=True)

                return df

            except Exception as e:
                with open(logs_file, "a") as logs_f:
                    logs_f.write(f"Parsing error: {e}\n")
                    logs_f.write(api_endpoint + "\n")
                return pd.DataFrame(columns=["timestamp"])

        except requests.exceptions.RequestException as e:
            with open(logs_file, "a") as logs_f:
                logs_f.write(f"Error connecting to the API: {e}\n")
            return pd.DataFrame(columns=["timestamp"])

    # Thread pool
    with ThreadPoolExecutor(max_workers=5) as executor:  
        futures = [executor.submit(process_kpi, kpi, api) for kpi, api in splunk_endpoints.items()]
        for future in as_completed(futures):
            df = future.result()
            if df is not None and not df.empty:
                if data.empty:
                    data = df
                else:
                    try:
                        data = pd.merge(data, df, on="timestamp", how="outer")
                    except Exception as e:
                        print(repr(e))

    # Final processing
    try:
        kpis_data["timestamp"] = pd.to_datetime(kpis_data["timestamp"])
        data["timestamp"] = pd.to_datetime(data["timestamp"])
        data = data.sort_values(by="timestamp")
    except Exception as e:
        print(str(e))

    try:
        data = pd.merge(data, kpis_data, on="timestamp", how="outer")
    except:
        data = data 

    return data


def splunkSitesMapping(suspected_sgw, splunk_output):
    filtered_columns = set()

    for mme in suspected_sgw:
        if mme in mapping:
            node_roles = mapping[mme]

            for role, node_value in node_roles.items():
                if role == "DNS":
                    for dns_node in node_value:
                        if dns_node:
                            matching_cols = [
                                col for col in splunk_output.columns
                                if col.startswith(f"{role}_") and dns_node in col
                            ]
                            filtered_columns.update(matching_cols)
                else:
                    if node_value:
                        matching_cols = [

                            col for col in splunk_output.columns
                            if col.startswith(f"{role}_") and node_value in col
                        ]
                        
                        filtered_columns.update(matching_cols)
                        print("shaimaaaaaaaa-----------?", filtered_columns)

    # Always keep timestamp
    if "timestamp" in splunk_output.columns:
        filtered_columns.add("timestamp")
      

    # Handle unmapped KPIs
    non_mapped_nodeskpis = []
    for nodekpi, endpoint in splunk_endpoints.items():
        if not any(col.startswith(nodekpi) for col in filtered_columns):
            non_mapped_nodeskpis.append(nodekpi)

    for nodekpi in non_mapped_nodeskpis:
        matching_cols = [col for col in splunk_output.columns if col.startswith(f"{nodekpi}_")]
        filtered_columns.update(matching_cols)

    # Return only filtered columns
    filtered_output = splunk_output[list(filtered_columns)]
    print("shaimaaaaaaaa salem-----------?", filtered_columns)

    return filtered_output

                  

def splunkOutput(splunk_output, timestamp):

  fileName = f'Splunk_output_{timestamp}.csv'
  output_file = os.path.join(settings.STATIC_ROOT, f'CallFlow/output/{fileName}')

  splunk_output.to_csv(output_file)
  splunk_url = static(f'/CallFlow/output/{fileName}')

  return  splunk_url


def formatSplunkAPI(splunk_output, logs_file):

  splunk_output.to_csv(r'/home/Shorouk/splunk_op.csv')
   
  #Splunk
  prefixes = ['Total', 'GGSN', 'DNS', 'GiFW', 'IMS', 'MME', 'SBG', 'BGF', 'MSC']
  splunk_json_output = {}

  for prefix in prefixes:
      prefix_cols = [col for col in splunk_output.columns if col.startswith(prefix + '_')]

      try:
        sub_df = splunk_output[['timestamp'] + prefix_cols]
      except: 
        sub_df = splunk_output[ prefix_cols]

      sub_df = sub_df.dropna(subset=sub_df.columns[1:], how='all')

      splunk_json_output[prefix] = sub_df.replace({np.nan: None}).to_dict(orient='records')

  return splunk_json_output


def remedyTicketsData(conn, start_date, end_date, suspected_sites, logs_file, timestamp):
   
   
  query = f""" SELECT * FROM Mobile_Squad.dbo.Remedy_Tickets 
               WHERE actual_fault_time >= '{start_date}' and  actual_fault_time <= '{end_date}' """
  
  if suspected_sites: 
    site_filters = " OR ".join([f"site LIKE '%{s}%'" for s in suspected_sites])
    query += f" AND ({site_filters})"

  print(query)
   
  data = conn.query_select(query) 

  assigned_count = (data['status'] == 'Assigned').sum()
  pending_count = (data['status'] == 'Pending').sum()
  inprogress_count = (data['status'] == 'In Progress').sum()
  resolved_count = (data['status'] == 'Resolved').sum()
  closed_count = (data['status'] == 'Closed').sum()
  cancelled_count = (data['status'] == 'Cancelled').sum() 

  status_count = {'assigned':assigned_count, 'pending':pending_count, 'inprogress':inprogress_count, 'resolved':resolved_count,
                  'closed':closed_count, 'cancelled':cancelled_count} 
  print(data)

  return data, status_count
   



 
mapping = {
    'BNS8': {
        'SGW': 'BNS8',
        'GGSN': 'BS-GGSN8',
        'GiFW': 'BS1',
        'DNS': ['HQ2', 'RMD'],
        'MME': 'BS_vSGSN_2',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'BNSCUPS': {
        'SGW': 'BNSCUPS',
        'GGSN': 'BS3_CUPS',
        'GiFW': 'BS3',
        'DNS': ['HQ2', 'HQ'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'MNS11': {
        'SGW': 'MNS11',
        'GGSN': 'MNS-GGSN',
        'GiFW': 'MNS',
        'DNS': ['HQ2', 'C5'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'MNS21': {
        'SGW': 'MNS21',
        'GGSN': 'MNS-vGGSN21',
        'GiFW': 'MNS2',
        'DNS': ['ZH', 'HQ'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'MOK9': {
        'SGW': 'MOK9',
        'GGSN': 'MKT-GGSN_CI-09',
        'GiFW': 'MKT',
        'DNS': ['RMD3', 'RMD'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'RMD16': {
        'SGW': 'RMD16',  #SC
        'GGSN': 'RM-GGSN16', #Sp
        'GiFW': 'RMD3', 
        'DNS': ['RMD3', 'HQ'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'RMD17': { 
        'SGW': 'RMD17',
        'GGSN': 'RM-GGSN17',
        'GiFW': 'RMD3',
        'DNS': ['RMD3', 'HQ'],
        'MME': 'RMD_vSGSN_8',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'TGM12': {
        'SGW': 'TGM12',
        'GGSN': 'TG-GGSN12',
        'GiFW': 'TG1',
        'DNS': ['ZH', 'C5'],
        'MME': 'TAG_vSGSN_4',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'TGM18': {
        'SGW': 'TGM18',
        'GGSN': 'TG-vGGSN18',
        'GiFW': 'TG2',
        'DNS': ['HQ', 'ZH'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'TGM19': {
        'SGW': 'TGM19',
        'GGSN': 'TG-GGSN19',
        'GiFW': 'TG2',
        'DNS': ['HQ', 'ZH'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'TNT15': {
        'SGW': 'TNT15',
        'GGSN': 'TNT-GGSN15',
        'GiFW': 'TNT',
        'DNS': ['C5', 'RMD'],
        'MME': 'TNT_vSGSN_3',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'ZHR13': {
        'SGW': 'ZHR13',
        'GGSN': 'ZH-GGSN13',
        'GiFW': 'ZHR',
        'DNS': ['ZH', 'HQ2'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'ZHR23': {
        'SGW': 'ZHR23',
        'GGSN': 'ZHR-vGGSN23',
        'GiFW': 'ZAH2nd-Gi_FW-Z01',
        'DNS': ['HQ2', 'ZH'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'HQ25': {
        'SGW': 'HQ25',
        'GGSN': 'HQ-vGGSN25',
        'GiFW': 'Gi-HQ-PRI-Z02.vodafone.com.eg',
        'DNS': ['HQ2', 'C5'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'ZHR24':{
        'SGW': 'ZHR24',
        'GGSN': 'ZAHCUPS-vGGSN24',
        'GiFW': 'ZAH2nd-Gi_FW-Z01',
        'DNS': ['HQ2', 'ZH'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'MNS22': {
        'SGW': 'MNS22',
        'GGSN': 'MNS-vGGSN22',
        'GiFW': 'MNS2',
        'DNS': ['ZH', 'HQ'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'RMD20': {
        'SGW': 'RMD20',
        'GGSN': 'RM-GGSN20',
        'GiFW': 'RMD2',
        'DNS': ['RMD', 'ZH'],
        'MME': '',
        'SBG': '',
        'BGF': '',
        'Total': ''
    },
    'ALX_vSGW-vPGW-10': {
        'SGW': 'ALX_vSGW-vPGW-10',
        'GGSN': 'ALX-GGSN_CI-10',
        'DNS': ['HQ2', 'C5', 'HQ' , 'TG' , 'ZH', 'RMD'],
        'SBG': '',
        'BGF': '',
        },
    'HQ_vSGW-vPGW-26': {
        'SGW': 'HQ_vSGW-vPGW-26',
        'GGSN': 'HQ-vGGSN26',
        'DNS': ['HQ2', 'C5', 'HQ' , 'TG' , 'ZH', 'RMD'],
        'SBG': '',
        'BGF': '',
        },
    'HQ_vSGW-PGW_CUPS-27': {
        'SGW': 'HQ_vSGW-PGW_CUPS-27',
        'GGSN': 'HQCUPS-vGGSN27',
        'DNS': ['HQ2', 'C5', 'HQ' , 'TG' , 'ZH', 'RMD'],
        'SBG': '',
        'BGF': '',
        },
    'RMD_vSGW_vPGW_28': {
        'SGW': 'RMD_vSGW_vPGW_28',
        'GGSN': 'RMD-vGGSN28',
        'DNS': ['HQ2', 'C5', 'HQ' , 'TG' , 'ZH', 'RMD'],
        'SBG': '',
        'BGF': '',
        },
    'TGM_vSGW-vPGW_18': {
        'SGW': 'TGM_vSGW-vPGW_18',
        'GGSN': 'TG-vGGSN18',
        'DNS': ['HQ2', 'C5', 'HQ' , 'TG' , 'ZH', 'RMD'],
        'SBG': '',
        'BGF': '',
    },
    'occ41': {
        'OCS': 'OCC41',
        'DSC': 'DSCHQ01(VIP)_EPC_Ro(OCC)',
    },
    'HQSAPC04ESY': {
        'PCRF': 'SAPC-HQ',
    },
}



def smartcareNodesQueries(msisdn_list,imsi=None,complain_type=None,
                          startdate=None, enddate=None, logs_file=None, timestamp=None):
    msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"  
  
    smartcare_Requierd_QUERIES = []
    if complain_type == 'Voice':
      smartcare_Requierd_QUERIES = smartcare_nodes_Voice
    elif complain_type == 'Data':
      smartcare_Requierd_QUERIES = smartcare_nodes_QUERIES_data
    else:
      smartcare_Requierd_QUERIES = smartcare_nodes_QUERIES_data
    
    ref_date = datetime.datetime(1970, 1, 1)

    final_data = []
    
    def run_query(query_name, query_template):
        cursor = smartcareConnection(logs_file)
        if cursor is None:
            return pd.DataFrame()

        data = pd.DataFrame()
        current_date = pd.to_datetime(startdate)
        
        try:
            Count = 0
            while current_date <= pd.to_datetime(enddate):
                tablename = (current_date - ref_date).days
                current_date += timedelta(days=1)

                query = query_template.format(
                    tablename=tablename,
                    msisdn_tuple=msisdn_str,
                    start_date=str(startdate),
                    end_date=str(enddate)
                )
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    columns = [desc[0] for desc in cursor.description]
                    logger.info(f"Query {query_name} returned {len(results)} rows with columns: {', '.join(columns)}")
                    df = pd.DataFrame(results, columns=columns)
                    
                    df['__source'] = query_name
                    data = pd.concat([data, df], ignore_index=True)

        except Exception as e:
            with open(logs_file, 'a') as logs_f:
                logs_f.write(f'Query failed: {e}\n')

        finally:
            try:
                cursor.close()
                # cursor._connection.close()  
            except Exception:
                pass
        return data
    
    # ---------------- PARALLEL EXECUTION ---------------- #
    
    max_workers = min(5, len(smartcare_Requierd_QUERIES))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for name, query in smartcare_Requierd_QUERIES.items():
            futures.append(executor.submit(run_query, name, query))

        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                final_data.append(df)
    
    if final_data:
        return pd.concat(final_data, ignore_index=True)

    return pd.DataFrame()

         