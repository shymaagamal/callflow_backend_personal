

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
from typing import List
import datetime
import requests
import json
import time
from pathlib import Path
import jaydebeapi
import numpy as np
from decimal import Decimal
import traceback



from django.contrib.auth.models import User
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

from general_apis.classes import *
from general_apis.constants import *
from callflow.models import *
from callflow.prototype.constants import *
import logging
from ..core.utils.data_utils import process_sitename

logger = logging.getLogger("callflow.prototype.apis")


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
      "timeout": "1000000", 
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

def smartcare_Validation_Inputs(msisdn_list: List[str],startdate,enddate):
    errors = []

    # ---------- Time validation ----------
    if enddate - startdate > timedelta(minutes=420):
        errors.append("Time_Stamp should not exceed 30 min")

    # ---------- MSISDN count validation ----------
    if len(msisdn_list) < 1:
        errors.append("MSISDN list should contain at least 1 MSISDNs")

    # ---------- MSISDN format validation ----------
    for msisdn in msisdn_list:
        msisdn_str = str(msisdn).strip()
        # print('msisdn........')
        print(msisdn)

        if (
            not msisdn_str.isdigit()
            or len(msisdn_str) != 10
            or not msisdn_str.startswith(("10", "11", "12","15"))
        ):
            errors.append(
                f"Invalid MSISDN: {msisdn}. "
                 "MSISDN must start with 10, 11, 12 or 15 and be exactly 10 digits"
            )

    # ---------- Final result ----------
    if errors:
        raise ValueError("; ".join(errors))
    
    return True

def smartcareNodesQueries(msisdn_list,imsi,complain_type,startdate,enddate,logs_file,timestamp):
    logger.info("smartcareNodesQueries is called")
    msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"  
    
    smartcare_Requierd_QUERIES = []
    if complain_type == 'Voice':
      smartcare_Requierd_QUERIES = smartcare_Voice_QUERIES
    elif complain_type == 'Data':
      smartcare_Requierd_QUERIES = smartcare_Data_QUERIES
    else:
      smartcare_Requierd_QUERIES = smartcare_main_QUERIES
    print("smartcare_Requierd_QUERIES----------->  ",smartcare_Requierd_QUERIES)
    ref_date = datetime.datetime(1970, 1, 1)

    final_data = []
    
    # def run_query(query_template, max_retries=5):
    #     cursor = smartcareConnection(logs_file)
    #     if cursor is None:
    #         return pd.DataFrame()

    #     data = pd.DataFrame()
    #     current_date = pd.to_datetime(startdate)
        
    #     try:
    #         Count = 0
    #         while current_date <= pd.to_datetime(enddate):
    #             tablename = (current_date - ref_date).days
    #             current_date += timedelta(days=1)

    #             query = query_template.format(
    #                 tablename=tablename,
    #                 msisdn_tuple=msisdn_str,
    #                 start_date=str(startdate),
    #                 end_date=str(enddate)
    #             )
    #             cursor.execute(query)

    #             results = cursor.fetchall()
    #             print("results---->",results)
    #             if results and cursor.description :
    #                 columns = [desc[0] for desc in cursor.description]
    #                 df = pd.DataFrame(results, columns=columns)
    #                 data = pd.concat([data, df], ignore_index=True)
       
    #     except Exception as e:
    #         with open(logs_file, 'a') as logs_f:
    #             logs_f.write(f'Query failed: {e}\n')

    #     finally:
    #         try:
    #             cursor.close()
    #             cursor._connection.close()  
    #         except Exception:
    #             pass
    #     return data
    def run_query(query_template, max_retries=5):
        cursor = smartcareConnection(logs_file)
        if cursor is None:
            return pd.DataFrame()

        data = pd.DataFrame()
        current_date = pd.to_datetime(startdate)

        while current_date <= pd.to_datetime(enddate):
            tablename = (current_date - ref_date).days
            current_date += timedelta(days=1)

            query = query_template.format(
                tablename=tablename,
                msisdn_tuple=msisdn_str,
                start_date=str(startdate),
                end_date=str(enddate)
            )

            # ============================================================
            for attempt in range(1, max_retries + 1):
                try:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    print("results---->", results)

                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        if results:
                            df = pd.DataFrame(results, columns=columns)
                        else:
                            df = pd.DataFrame(columns=columns)   # DataFrame فاضي لكن بالأعمدة محفوظة
                        data = pd.concat([data, df], ignore_index=True)

                    break  # نجحت المحاولة، اخرجي من لوب الـ retry

                except Exception as e:
                    if attempt < max_retries:
                        print(f"[RETRY] table={tablename} attempt {attempt}/{max_retries} failed: {e}")
                        # نفتح اتصال جديد لأن الاتصال القديم ممكن يبقى معطوب بعد timeout
                        try:
                            cursor.close()
                            cursor._connection.close()
                        except Exception:
                            pass
                        cursor = smartcareConnection(logs_file)
                        if cursor is None:
                            break
                        continue
                    else:
                        with open(logs_file, 'a') as logs_f:
                            logs_f.write(f'Query failed (table={tablename}, all {max_retries} attempts): {e}\n')
                        print(f"[QUERY ERROR] table={tablename}: {e}")

        try:
            cursor.close()
            cursor._connection.close()
        except Exception:
            pass

        return data
    # ---------------- PARALLEL EXECUTION ---------------- #
    
    # max_workers = min(5, len(smartcare_Requierd_QUERIES))

    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     futures = []

    #     for name, query in smartcare_Requierd_QUERIES.items():
    #         print(name)
    #         futures.append(executor.submit(run_query, query))

    #     for future in as_completed(futures):
    #         df = future.result()
    #         if not df.empty:
    #             final_data.append(df)
    
    # ---------------- SEQUENTIAL EXECUTION ---------------- #
    final_data = []
    for name, query in smartcare_Requierd_QUERIES.items():
        print(name)
        df = run_query(query)
        final_data.append(df)

    if final_data:
        result = pd.concat(final_data, ignore_index=True)

        # if 'Service_flag' not in result.columns:
        #     logger.warning("Service_flag column is missing. Running fallback query...")
        #     fallback_df = run_query(smartcare_DRA_Voice)   # your specific query
        #     if not fallback_df.empty:
        #         result = result.merge(fallback_df, on='MSISDN', how='left')  # or concat, depending on your data
        fallback_queries = {
                "Voice_Data_Flag":SCF_CCI_U_T_Data,
                "DRA_NAME": SCF_CCI_U_T_Data,
                "VPCRF_NAME": SCF_CCI_U_T_Data,
                "OCS_NAME": SCF_CCI_U_T_Data,
                "TAC_DECIMAL":smartcare_nodes_S1mme,
                "MME_Name":smartcare_nodes_S1mme,
        }
        for column, query in fallback_queries.items():

            if (
                column not in result.columns
                or result[column].isna().all()
            ):
                logger.warning(f"{column} is missing. Running fallback query...")

                fallback_df = run_query(query)
                if fallback_df.empty or "MSISDN" not in fallback_df.columns or column not in fallback_df.columns:
                    logger.warning(
                        f"[{column}] fallback query returned no usable data "
                        f"(empty={fallback_df.empty}, columns={list(fallback_df.columns)}) -- skipping merge"
                    )
                    continue
                fallback_df = fallback_df[["MSISDN", column]]

                before = len(result)

                if not fallback_df.empty:
                    result = result.merge(fallback_df, on="MSISDN", how="left")
                    result = result.drop_duplicates()
                    after = len(result)
                print(f"Dropped {before - after} duplicate rows")
        return result

    return pd.DataFrame()

def smartcareOutput_Aggregated_Voice(data, logs_file,timestamp):
  cell_distinct_msisdn = {}
  site_distinct_msisdn = {}
  cluster_distinct_msisdn ={}
  p_cscf_name ={}
  AS_Name = {}
  MSC_NAME = {}
  DRA_NAME = {}
  OCS_NAME = {}
  PCRF = {}
  subregion_distinct_msisdn = {}
  region_distinct_msisdn={}
  hub_distinct_msisdn = {}
  
  IMPU_distinct_msisdn ={}
  nodes_distinct_msisdn = {}
  s_cscf_Name = {}

  try:
    data = data.fillna('')
    print('data')
    data.to_csv(r'/home/Shorouk/CallFlow/sc_data.csv') 
    print(data.columns)
    
    if not data.empty:
      
      IMS_data = data.copy()

      data = data.dropna(subset=['CELL_NAME'])
      data = data[(data['CELL_NAME'].str.strip() != '')]
      
      # __________ CELL COUNTS ___________ 
      cell_distinct_msisdn = data.groupby('CELL_NAME')['MSISDN'].nunique().to_dict()
      suspected_cells_raw = list(set(data['CELL_NAME'].to_list()))
   
    #____________ CLUSTER COUNTS ____________
      cluster_distinct_msisdn = data.groupby('CLUSTER')['MSISDN'].nunique().to_dict()
    
    # __________ REGION COUNTS ______________
      region_distinct_msisdn = data.groupby('REGION')['MSISDN'].nunique().to_dict()
      subregion_distinct_msisdn = data.groupby('SUBREGION')['MSISDN'].nunique().to_dict()
    # ___________ 
    # -------- SITE LOGIC --------
      data['site_name'] = data['CELL_NAME'].str.split('_L').str[1]
      data['site_name'] = data['site_name'].str.split('_').str[0]

      data['site_name_number'] = data['site_name'].str.extract(r'(\d+)')
      data['site_name_number'] = data['site_name_number'].astype(str)


      def getSiteLetter(site_name, site_number):
        return str(site_name).replace(str(site_number), '')
      data['site_name_letter'] = data.apply(lambda row: getSiteLetter(row['site_name'], row['site_name_number']), axis=1)

      data['modified_site_name_number'] = data['site_name_number'].str.zfill(4)
      data['modified_site_name'] = data['site_name_letter'] + data['modified_site_name_number']
      
      site_distinct_msisdn = data.groupby('modified_site_name')['MSISDN'].nunique().to_dict()

      
    # ________ P_CSCF_NAME ______________
      p_cscf_name = distinct_count(IMS_data, 'p_cscf_Name', 'IMPU_TEL_URI1')
    
    #   ________ AS_NAME ______________
      AS_Name = distinct_count(IMS_data, 'AS_NAME', 'IMPU_TEL_URI1')
    
    #   ________ MSC_NAME ______________
      MSC_NAME = distinct_count(IMS_data, 'MSC_NAME', 'MSISDN')
    
    # Filter only VoLTE records
      IMS_data = IMS_data[IMS_data['Voice_Data_Flag'] == 'Voice']
    #   IMS_data.to_csv(r'/home/Shorouk/CallFlow/IMS_data.csv')
    #   ________ DRA_NAME ______________
      DRA_NAME = distinct_count(IMS_data, 'DRA_NAME', 'MSISDN')
    #   ___________ OCS_NAME ___________
      OCS_NAME =distinct_count(IMS_data, 'OCS_NAME', 'MSISDN')
          #  ___________ PCRF ___________
      PCRF =distinct_count(IMS_data, 'VPCRF_NAME', 'MSISDN')
    # ________ Site COUNTS ______________
      site_counts = data['modified_site_name'].value_counts().to_dict()
      suspected_cells = tuple(set(data['CELL_NAME'].to_list()))
      suspected_sites = list(set(data['modified_site_name'].to_list()))
      
    else:
      suspected_cells_raw = []
      suspected_sites = []
      suspected_cells = []

      
#   except Exception as e:
#     with open(logs_file, 'a') as logs_f:
#       logs_f.write('Smartcare Output error : ' + str(e) + '\n')

  except Exception as e:
    print("ERROR:", e)
    traceback.print_exc()
    with open(logs_file, 'a') as logs_f:
        logs_f.write(traceback.format_exc())

  return data,site_distinct_msisdn, cell_distinct_msisdn, cluster_distinct_msisdn,region_distinct_msisdn,subregion_distinct_msisdn,p_cscf_name,AS_Name,MSC_NAME,DRA_NAME,OCS_NAME,PCRF


def smartcareOutput_Aggregated_Data(data, logs_file,timestamp):
  print(data.columns)
  suspected_cells = []
  suspected_sites = []
  suspected_cells_raw = []
  cell_counts = {}
  site_counts = {}
  suspected_sgw = []
  cell_distinct_msisdn = {}
  site_distinct_msisdn = {}
  OCS_NAME = {}
  PCRF = {}
  DRA_NAME = {}
  rat_distinct_msisdn = {}
  region_distinct_msisdn = {}
  tac_distinct_msisdn = {}
  sgsn_distinct_msisdn = {}
  subregion_distinct_msisdn = {}
  mme_distinct_msisdn = {}
  msisdn_analysis =[]

  try:
    data = data.fillna('')
    print('data------------------')
    print(data.columns)
    
    if not data.empty:
      print("here cellname and ") 
      IMS_data = data.copy()
      print('IMS_data')     
      print(IMS_data.columns)
      data = data.dropna(subset=['cell_name', 'SGW_SGSN_NAME'])
      data = data[(data['cell_name'].str.strip() != '') & (data['SGW_SGSN_NAME'].str.strip() != '')]
        
      # __________ CELL COUNTS ___________ 
    #   cell_counts = data[['MSISDN', 'cell_name']].to_dict(orient='records')
      cell_distinct_msisdn = data.groupby('cell_name')['MSISDN'].nunique().to_dict()
      print('cell_distinct_msisdn')
      print(cell_distinct_msisdn)
      suspected_cells_raw = list(set(data['cell_name'].to_list()))
      
    # Filter only VoLTE records
      IMS_data = IMS_data[IMS_data['Voice_Data_Flag'] == 'Data']
    
    #   ________ DRA_NAME ______________
      DRA_NAME = distinct_count(IMS_data, 'DRA_NAME', 'MSISDN')
    #   ___________ OCS_NAME ___________
      OCS_NAME =distinct_count(IMS_data, 'OCS_NAME', 'MSISDN')
          #  ___________ PCRF ___________
      PCRF =distinct_count(IMS_data, 'VPCRF_NAME', 'MSISDN')

      # _________RAT COUNTS __________
      rat_distinct_msisdn = data.groupby('RAT')['MSISDN'].nunique().to_dict()
      # _________RAT COUNTS __________
      region_distinct_msisdn = data.groupby('REGION')['MSISDN'].nunique().to_dict()
      # _________SUBREGION COUNTS __________
      subregion_distinct_msisdn = data.groupby('SUBREGION')['MSISDN'].nunique().to_dict()
      # _________TAC COUNTS __________
      tac_distinct_msisdn = data.groupby('TAC_DECIMAL')['MSISDN'].nunique().to_dict()
      # _________SGSN COUNTS __________
      sgsn_distinct_msisdn = data.groupby('SGW_SGSN_NAME')['MSISDN'].nunique().to_dict()
      # _________MME COUNTS __________
      mme_distinct_msisdn = data.groupby('MME_Name')['MSISDN'].nunique().to_dict()
      
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
      
      site_distinct_msisdn = data.groupby('modified_site_name')['MSISDN'].nunique().to_dict()
      print('site_distinct_msisdn')
      print(site_distinct_msisdn)
      # ________ Site COUNTS ______________
      site_counts = data['modified_site_name'].value_counts().to_dict()

      suspected_cells = tuple(set(data['cell_name'].to_list()))
      suspected_sites = list(set(data['modified_site_name'].to_list()))
      
      

      msisdn_analysis = _analyze_msisdn_subscriptions(data, logs_file)   
      try:
            data.rename(columns={'SGW_SGSN_NAME':'SGW_PGW_NAME'}, inplace=True)
      except Exception as e:
            with open(logs_file, 'a') as logs_f:
             logs_f.write('Smartcare Nodes Data Prep Error : ' + str(e) + '\n')
            
        
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_vSGW-vPGW_','', regex=False)
      data['SGW_PGW_NAME'] = data['SGW_PGW_NAME'].str.replace('_SGW-PGW_','', regex=False)
      suspected_sgw = data.groupby('SGW_PGW_NAME')['MSISDN'].nunique().to_dict()
    #   suspected_sgw = data['SGW_PGW_NAME'].value_counts().to_dict()
      

    else:
      suspected_cells_raw = []
      suspected_sgw = []
      suspected_sites = []
      suspected_cells = []
      msisdn_analysis = []

      
  except Exception as e:
    print("ERROR:", e)
    traceback.print_exc()
    with open(logs_file, 'a') as logs_f:
        logs_f.write(traceback.format_exc())


  return data, site_distinct_msisdn, cell_distinct_msisdn, suspected_sgw, rat_distinct_msisdn,region_distinct_msisdn,subregion_distinct_msisdn,sgsn_distinct_msisdn,tac_distinct_msisdn,mme_distinct_msisdn,DRA_NAME,OCS_NAME,PCRF

def is_valid_aggregated_result(
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
):
    checks = [
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

    # Percentage of successful outputs
    success_ratio = sum(checks) / len(checks)

    return success_ratio >= 0.8, success_ratio

def distinct_count(df, group_col, value_col):
    return (
        df[df[group_col].fillna('').str.strip() != '']
        .groupby(group_col)[value_col]
        .nunique()
        .to_dict()
    )

def calculate_affected_hub_metrics(nodesdata, affected_cells: list, ts: str) -> dict:
    """
     Calculates HUB metrics
    using ONLY the NPP-affected cells from cached Smartcare data.
    """
    
    try:
        if not affected_cells:
            print("No affected cells provided for hub metrics")
            return {
                'subs_per_hub': [],
            }

        nodesdata = nodesdata.copy()
        # Filter to ONLY affected cells (this is the key change)
        affected_cells = [str(c).strip() for c in affected_cells if c]
        if 'cell_name' in nodesdata.columns:
            nodesdata = nodesdata[nodesdata['cell_name'].isin(affected_cells)].copy()
        elif 'CELL_NAME' in nodesdata.columns:
            nodesdata = nodesdata[nodesdata['CELL_NAME'].isin(affected_cells)].copy()

        if nodesdata.empty:
            print("No overlap between Smartcare data and NPP affected cells")
            return {
                'subs_per_hub': [],
            }

        # ==================== ORIGINAL FULL LOGIC (unchanged) ====================
        if 'cell_name' in nodesdata.columns:
            nodesdata = process_sitename(
                nodesdata,
                'cell_name',
                settings.OUTPUT_DIR / "logs" / "metrics.log"
            )
        elif 'CELL_NAME' in nodesdata.columns:
            nodesdata = process_sitename(
                nodesdata,
                'CELL_NAME',
                settings.OUTPUT_DIR / "logs" / "metrics.log"
            )

        nodesdata.loc[:, 'modified_site_name'] = nodesdata['modified_site_name'].replace('', None)

        subs_per_site = nodesdata.groupby('modified_site_name')['MSISDN'].nunique().dropna().to_dict()
        
        master_df = pd.read_excel(settings.MASTER_SHEET_PATH)
        master_df['SITE_ID'] = master_df['SITE_ID'].astype(str).str.strip()

        site_list = list(subs_per_site.keys())
        matched_df = master_df[master_df['SITE_ID'].isin(site_list)]

        if matched_df.empty:
            print(f"No matching SITE_ID found in master sheet for {len(site_list)} sites")
            return {
                'subs_per_hub': []
            }

        # All the original calculations (hubs)
        unique_hubs = matched_df['Hub'].dropna().unique().tolist()
        hubs_count = len(unique_hubs)
      

        site_to_hub = matched_df.set_index('SITE_ID')['Hub'].to_dict()
        nodesdata['Hub'] = nodesdata['modified_site_name'].map(site_to_hub)
        subs_per_hub = (
            nodesdata.dropna(subset=['Hub', 'MSISDN'])
            .groupby('Hub')['MSISDN'].nunique()
            .to_dict()
        )

        # subs_per_hub = [{"label": row['Hub'], "value": int(row['value'])} for _, row in subs_per_hub.iterrows()]

        matched_df.to_csv(settings.OUTPUT_DIR / f"matched_master_Prototype_{ts}.csv", index=False)

        logger.info(
            f"[AFFECTED HUB METRICS] hubs={hubs_count}"
            f"matched={len(matched_df)} (filtered to {len(affected_cells)} NPP cells)"
        )
        return {
            'HUB_Counts': subs_per_hub,
        }
    except Exception as e:
        logger.exception("Error calculating affected HUB from prototype")
        return {'hubs_affected_count': 0}
    
def _analyze_msisdn_subscriptions(nodes_data, logs_file):
    try: 
        analysis_df = (
                nodes_data.groupby(['TAC_DECIMAL', 'SGW_SGSN_NAME', 'cell_name'])['MSISDN']
                .nunique()
                .reset_index(name='Total Subscribers')
            )
        return analysis_df.to_dict('records')

    except Exception as e:
        with open(logs_file, 'a') as logs_f:
            logs_f.write(f'MSISDN Analysis Error: {str(e)}\n')
            # Optional: log available columns for debugging
            logs_f.write(f'Available columns: {list(nodes_data.columns) if not nodes_data.empty else "DataFrame empty"}\n')
        
        # Return empty list on error — safe and expected
        return []

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

   




