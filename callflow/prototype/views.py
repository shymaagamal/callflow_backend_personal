import datetime 
import json
import pandas as pd

from django.shortcuts import render
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.db import connection
from django.core.paginator import Paginator
from django.http import JsonResponse


from callflow.prototype.apis import *
from general_apis.classes import *
from general_apis.constants import *

from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.pagination import PageNumberPagination
from callflow.prototype.services.kpi_analyzer import KPIAnalyzer
import logging

logger = logging.getLogger("callflow.prototype.views")



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


class PrototypeSmartcareIntegrationAPIView(APIView):
    # http_method_names = ['get', 'post', 'options', 'head']
    
    # # Add this for a nice title in browsable API
    
    # def get(self, request):
    #     return Response({"detail": "Smartcare Integration API - Use POST to upload file and query data."})
    def post(self, request):
            
        try:
            logger.info("SmartcareIntegrationAPIView is called")
            uploaded_file = request.FILES.get('file')  # Excel file
            msisdn_list = request.data.get('msisdn_list')
            start = request.data.get('start_date')
            end = request.data.get('end_date')
            complain_type = request.data.get('Complain_Type')  # Data / Voice 
            # msisdn = request.data.get('msisdn') 
            imsi = request.data.get('imsi')
            if not start or not end:
                return Response({'error': 'Missing start_date or end_date'}, status=400)
            
            if not msisdn_list:
                return Response({'error': 'msisdn_list is required'}, status=400)
            # Access MSISDN File
            # try:
                # df = pd.read_excel(uploaded_file)   
                # msisdn_list = msisdn_list.tolist()
                
            # except Exception as e:
            #     return Response({"error": str(e)}, status=400)
            
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
            # breakpoint()
            nodes_data = smartcareNodesQueries(msisdn_list, imsi,complain_type,dates['formatted_startDate_2'], dates['formatted_endDate_2'], logs_file, timestamp)
            print('nodes_data')
            print(nodes_data.columns)

            if isinstance(nodes_data, dict) and "error" in nodes_data:
                return Response(nodes_data, status=400)
            # breakpoint()
            # if(complain_type == 'Data'):

            #     data, site_counts, cell_counts, suspected_sgw,rat_counts,region_counts,subregion_counts,sgsn_counts,tac_counts,mme_counts,DRA_NAME,OCS_NAME,PCRF = smartcareOutput_Aggregated_Data(nodes_data, logs_file, timestamp)
            #     cell_list = list(cell_counts.keys())
            #     affected_hub = {}
            #     if (nodes_data is not None):
            #         affected_hub = calculate_affected_hub_metrics(
            #             nodes_data, cell_list, timestamp
            #         )
            if complain_type == 'Data':

                MAX_RETRIES = 6

                for attempt in range(MAX_RETRIES):

                    (
                        data,
                        site_counts,
                        cell_counts,
                        suspected_sgw,
                        rat_counts,
                        region_counts,
                        subregion_counts,
                        sgsn_counts,
                        tac_counts,
                        mme_counts,
                        DRA_NAME,
                        OCS_NAME,
                        PCRF,
                    ) = smartcareOutput_Aggregated_Data(nodes_data, logs_file, timestamp)

                    # Mandatory outputs
                    required_checks = [
                        not data.empty,
                        bool(cell_counts),
                        bool(site_counts),
                        bool(suspected_sgw),
                        bool(region_counts),
                        bool(subregion_counts),
                        bool(sgsn_counts),
                        bool(DRA_NAME),
                    ]

                    # Overall completeness
                    all_checks = [
                        not data.empty,
                        bool(site_counts),
                        bool(cell_counts),
                        bool(suspected_sgw),
                        bool(rat_counts),
                        bool(region_counts),
                        bool(subregion_counts),
                        bool(sgsn_counts),
                        bool(tac_counts),
                        bool(mme_counts),
                        bool(DRA_NAME),
                        bool(OCS_NAME),
                        bool(PCRF),
                    ]

                    completion = sum(all_checks) / len(all_checks)

                    print(
                        f"Attempt {attempt + 1}: "
                        f"{sum(required_checks)}/{len(required_checks)} required, "
                        f"{completion:.0%} overall"
                    )

                    # Stop only if:
                    # 1. All mandatory values exist
                    # 2. Overall completion is at least 60%
                    if all(required_checks) and completion >= 0.85:
                        print("Aggregation successful.")
                        break

                cell_list = list(cell_counts.keys())

                affected_hub = {}
                if nodes_data is not None:
                    affected_hub = calculate_affected_hub_metrics(
                        nodes_data,
                        cell_list,
                        timestamp
                    )
            else:
                nodes_data.to_csv(r'/home/Shorouk/CallFlow/sc_nodes_before_voice.csv')
                data, site_counts, cell_counts, cluster_counts , region_counts , subregion_counts , p_cscf_name, AS_Name,MSC_NAME,DRA_NAME,OCS_NAME,PCRF = smartcareOutput_Aggregated_Voice(nodes_data, logs_file, timestamp)
                cell_list = list(cell_counts.keys())
                affected_hub = {}
                if (nodes_data is not None):
                    affected_hub = calculate_affected_hub_metrics(
                        nodes_data, cell_list, timestamp
                    )
                print('site_counts_voice')
                print(site_counts)
            # nodes_data, suspected_sites, suspected_cells, suspected_cells_raw, suspected_sgw = smartcareOutput(nodes_data, logs_file, timestamp)
            nodes_data.to_csv(r'/home/Shorouk/CallFlow/sc_nodes.csv') 
            print('nodes_data22222222')
            print(nodes_data.columns)
            smartcare_json_output = {
                col: nodes_data[col].dropna().astype(str).unique().tolist()
                for col in nodes_data.columns if col in ['IMSI','MSISDN', 'IMEI','IMEI_Brand', 'IMEI_Model', 'TAC','cell_name','MME_Name', 'SGW_PGW_NAME' ,'site_name',
                                                        'AAA_CLIENT_IP', 'AAA_SERVER_IP', 'RAT','TAC', 'VPCRF_NAME', 'OCS_NAME', 'DRA_NAME','p_cscf_Name','S_cscf_Name']
            }
            smartcare_json_output = json.dumps(smartcare_json_output, indent=4)
            if (complain_type == 'Data'):
                return Response({
                    # 'msisdn_list' : tuple(msisdn_list),
                    # 'smartcare': smartcare_json_output,
                    # 'suspected_sites': suspected_sites,
                    # 'suspected_cells_raw': suspected_cells_raw,
                    'suspected_sites': site_counts,
                    'suspected_cells_raw': cell_counts,
                    'suspected_sgws': suspected_sgw,
                    'HUB_Counts':affected_hub,
                    'rat_counts':rat_counts,
                    'DRA_NAME':DRA_NAME,
                    'OCS_NAME':OCS_NAME,
                    'PCRF':PCRF,
                    'region_counts':region_counts,
                    'subregion_counts':subregion_counts,
                    'sgsn_counts':sgsn_counts,
                    'tac_counts':tac_counts,
                    'mme_counts':mme_counts,
                    # 'msisdn_analysis':msisdn_analysis,
                    
                })
            else:
                return Response({
                    # 'msisdn_list' : tuple(msisdn_list),
                    # 'smartcare': smartcare_json_output,
                    # 'suspected_sites': suspected_sites,
                    # 'suspected_cells_raw': suspected_cells_raw,
                    # 'suspected_sgws': suspected_sgw,
                    'Suspected_Sites': site_counts,
                    'Suspected_Cells': cell_counts,
                    'Cluster_Counts':cluster_counts,
                    'Region_Counts':region_counts,
                    'Sub_Region_Counts':subregion_counts,
                    'HUB_Counts':affected_hub,
                    'CSCF_Name':p_cscf_name,
                    'AS_Name':AS_Name,
                    'MSC_Name':MSC_NAME,
                    'DRA_NAME':DRA_NAME,
                    'OCS_NAME':OCS_NAME,
                    'PCRF':PCRF
                })

        
        except Exception as e:
            return Response({'error': str(e)}, status=500)


