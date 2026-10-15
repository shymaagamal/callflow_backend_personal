

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
import threading
from ..core.utils.data_utils import process_sitename
from . import conf as prototype_conf
from . import runtime as prototype_runtime

logger = logging.getLogger("callflow.prototype.apis")
_jvm_lock = threading.Lock()


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
  """
  Returns a Smartcare cursor, or None when the connection fails (unchanged
  contract). Changes vs. before:
    * user/password/timeouts are passed as JDBC *properties* (the old code put
      the properties dict in jaydebeapi's `libs` argument, so no timeout was
      ever applied). Same mechanism core.utils.connections already uses.
    * setNetworkTimeout bounds a hung socket read.
    * the JVM is started once, under a lock (two threads must not race
      jpype.startJVM).
  """
  connection_properties = {
      "user": db_user,
      "password": db_password,
      "socketTimeout": str(prototype_conf.smartcare_socket_timeout_ms()),
      "connectTimeout": str(prototype_conf.smartcare_connect_timeout_ms()),
  }

  conn = None
  try:
    import jpype
    with _jvm_lock:
      if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={jdbc_driver_jar}")
    conn = jaydebeapi.connect(
      jdbc_driver_class,
      jdbc_url,
      connection_properties,
      jdbc_driver_jar,
    )
    try:
      conn.jconn.setNetworkTimeout(None, prototype_conf.smartcare_socket_timeout_ms())
    except Exception as timeout_err:
      logger.debug("setNetworkTimeout not applied: %s", timeout_err)
  except Exception as e:
    with open(logs_file, 'a') as logs_f:
      logs_f.write(f'smartcareConnection : {e} \n')
      logs_f.write(str(conn))
    ctx = prototype_runtime.current()
    if ctx:
      ctx.warning("smartcare_connect_failed", error=str(e))

  if conn is None:
      return None

  cursor = conn.cursor()


  return cursor


def close_cursor(cursor):
  """Close a cursor returned by smartcareConnection and its connection."""
  if cursor is None:
    return
  try:
    cursor.close()
  except Exception:
    pass
  try:
    cursor._connection.close()
  except Exception:
    pass


class _SmartcareSession:
  """
  One Smartcare connection for all (sequential) queries of a request, instead
  of one new connection per query. Re-opened after a failure, closed by the
  owner. `owns` = False when the caller passed an already open cursor.
  """
  def __init__(self, logs_file, cursor=None):
    self.logs_file = logs_file
    self.cursor = cursor
    self.connects = 0

  def get(self):
    if self.cursor is None:
      ctx = prototype_runtime.current()
      if ctx:
        with ctx.stage("smartcare_connect"):
          self.cursor = smartcareConnection(self.logs_file)
      else:
        self.cursor = smartcareConnection(self.logs_file)
      self.connects += 1
    return self.cursor

  def reset(self):
    close_cursor(self.cursor)
    self.cursor = None

  def close(self):
    self.reset()


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

# Queries whose columns never reach the Data response (see conf.skip_unused_data_queries).
UNUSED_DATA_QUERIES = ('smartcare_nodes_REG_Voice',)


def _select_queries(complain_type):
    if complain_type == 'Voice':
      queries = smartcare_Voice_QUERIES
    elif complain_type == 'Data':
      queries = smartcare_Data_QUERIES
    else:
      queries = smartcare_main_QUERIES
    queries = dict(queries)
    if complain_type == 'Data' and prototype_conf.skip_unused_data_queries():
      for name in UNUSED_DATA_QUERIES:
        queries.pop(name, None)
    return queries


def smartcareNodesQueries(msisdn_list,imsi,complain_type,startdate,enddate,logs_file,timestamp,cursor=None):
    """
    Same result as before (same queries, same per-day tables, same concat and
    the same fallback/merge rules). What changed:
      * one Smartcare connection for the whole request (`cursor` may be passed
        in by the caller; it is then closed by the caller),
      * a successful query result is memoised for the rest of this request,
        so the fallback block no longer re-runs SCF_CCI_U_T_Data (up to 4x)
        and S1MME (up to 2x) with identical parameters. Failed or partial
        results are NOT memoised, so the fallback can still recover them,
      * retries back off and stop after a time budget,
      * every execute/fetch is timed and logged; result rows are no longer
        printed to stdout.
    """
    ctx = prototype_runtime.current()
    msisdn_str = "(" + ",".join(f"'{m}'" for m in msisdn_list) + ")"

    smartcare_Requierd_QUERIES = _select_queries(complain_type)
    if ctx:
      ctx.event("smartcare_plan", complain_type=complain_type,
                queries=",".join(smartcare_Requierd_QUERIES.keys()))
    ref_date = datetime.datetime(1970, 1, 1)

    session = _SmartcareSession(logs_file, cursor=cursor)
    owns_session = cursor is None
    memo = {}
    failed_before = set()   # templates that already exhausted their attempts in this request
    configured_attempts = prototype_conf.smartcare_max_attempts()

    def _elapsed_smartcare():
      return ctx.smartcare_seconds if ctx else 0.0

    def run_query(query_template, name):
        memo_key = id(query_template)
        if prototype_conf.memoize_queries() and memo_key in memo:
            if ctx:
              ctx.record_query(name=name, memo_hit=True, rows=len(memo[memo_key]))
            return memo[memo_key]

        # A query that already failed all its attempts in this request is
        # tried once more (not another full retry cycle) when the fallback
        # block asks for it again.
        max_retries = 1 if memo_key in failed_before else configured_attempts

        cursor = session.get()
        if cursor is None:
            return pd.DataFrame()

        data = pd.DataFrame()
        complete = True
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

            for attempt in range(1, max_retries + 1):
                t0 = time.perf_counter()
                exec_s = fetch_s = 0.0
                try:
                    cursor.execute(query)
                    exec_s = time.perf_counter() - t0
                    t1 = time.perf_counter()
                    results = cursor.fetchall()
                    fetch_s = time.perf_counter() - t1

                    rows = 0
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        if results:
                            df = pd.DataFrame(results, columns=columns)
                        else:
                            df = pd.DataFrame(columns=columns)   # empty frame, columns kept
                        rows = len(df)
                        data = pd.concat([data, df], ignore_index=True)
                    if ctx:
                        ctx.smartcare_seconds += exec_s + fetch_s
                        ctx.record_query(name=name, table=tablename, attempt=attempt, ok=True,
                                         rows=rows, execute_s=round(exec_s, 3),
                                         fetch_s=round(fetch_s, 3), memo_hit=False)
                    break

                except Exception as e:
                    failed_s = time.perf_counter() - t0
                    if ctx:
                        ctx.smartcare_seconds += failed_s
                        ctx.record_query(name=name, table=tablename, attempt=attempt, ok=False,
                                         seconds=round(failed_s, 3), error=str(e)[:500], memo_hit=False)
                    over_budget = _elapsed_smartcare() >= prototype_conf.smartcare_retry_budget_s()
                    if attempt < max_retries and not over_budget:
                        # reconnect: the old connection may be broken after a timeout
                        session.reset()
                        delay = min(prototype_conf.smartcare_retry_backoff_max_s(),
                                    prototype_conf.smartcare_retry_backoff_s() * (2 ** (attempt - 1)))
                        if delay > 0:
                            time.sleep(delay)
                        cursor = session.get()
                        if cursor is None:
                            complete = False
                            break
                        continue
                    else:
                        complete = False
                        reason = "retry budget exhausted" if over_budget and attempt < max_retries else f"all {attempt} attempts"
                        with open(logs_file, 'a') as logs_f:
                            logs_f.write(f'Query failed (table={tablename}, {reason}): {e}\n')
                        logger.warning("[QUERY ERROR] %s table=%s: %s", name, tablename, e)
                        break
            if cursor is None:
                break

        if complete and prototype_conf.memoize_queries():
            memo[memo_key] = data
        elif not complete:
            failed_before.add(memo_key)
        return data

    try:
        # ---------------- SEQUENTIAL EXECUTION (unchanged on purpose: one
        # Smartcare session per request) ---------------- #
        final_data = []
        for name, query in smartcare_Requierd_QUERIES.items():
            if ctx:
              with ctx.stage("smartcare_query", query=name):
                df = run_query(query, name)
            else:
              df = run_query(query, name)
            final_data.append(df)

        if final_data:
            result = pd.concat(final_data, ignore_index=True)

            fallback_queries = {
                    "Voice_Data_Flag":SCF_CCI_U_T_Data,
                    "DRA_NAME": SCF_CCI_U_T_Data,
                    "VPCRF_NAME": SCF_CCI_U_T_Data,
                    "OCS_NAME": SCF_CCI_U_T_Data,
                    "TAC_DECIMAL":smartcare_nodes_S1mme,
                    "MME_Name":smartcare_nodes_S1mme,
            }
            fallback_names = {id(SCF_CCI_U_T_Data): "SCF_CCI_U_T_Data",
                              id(smartcare_nodes_S1mme): "smartcare_nodes_S1mme"}
            for column, query in fallback_queries.items():

                if (
                    column not in result.columns
                    or result[column].isna().all()
                ):
                    qname = f"fallback:{column}:{fallback_names.get(id(query), '?')}"
                    logger.warning(f"{column} is missing. Running fallback query...")
                    if ctx:
                      ctx.event("fallback", column=column, query=qname)
                      with ctx.stage("smartcare_fallback", column=column):
                        fallback_df = run_query(query, qname)
                    else:
                      fallback_df = run_query(query, qname)
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
                    logger.debug(f"Dropped {before - after} duplicate rows")
                    if ctx:
                      ctx.event("fallback_merged", column=column, rows_before=before, rows_after=after)
            if ctx:
              ctx.event("smartcare_result", rows=len(result), columns=len(result.columns),
                        connections_opened=session.connects)
            return result

        return pd.DataFrame()
    finally:
        # close what we opened; a caller-supplied cursor is closed by the caller
        # unless we had to replace it after a failure.
        if owns_session or session.cursor is not cursor:
            session.close()

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
    _dump_csv(data, 'sc_data.csv')
    
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


      data['site_name_letter'] = _site_letters(data['site_name'], data['site_name_number'])

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
    logger.warning("aggregation error: %s", e)
    ctx = prototype_runtime.current()
    if ctx:
      ctx.warning("aggregation_error", error=str(e))
    with open(logs_file, 'a') as logs_f:
        logs_f.write(traceback.format_exc())

  return data,site_distinct_msisdn, cell_distinct_msisdn, cluster_distinct_msisdn,region_distinct_msisdn,subregion_distinct_msisdn,p_cscf_name,AS_Name,MSC_NAME,DRA_NAME,OCS_NAME,PCRF


def smartcareOutput_Aggregated_Data(data, logs_file,timestamp):
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

    if not data.empty:
      IMS_data = data.copy()
      data = data.dropna(subset=['cell_name', 'SGW_SGSN_NAME'])
      data = data[(data['cell_name'].str.strip() != '') & (data['SGW_SGSN_NAME'].str.strip() != '')]
        
      # __________ CELL COUNTS ___________ 
    #   cell_counts = data[['MSISDN', 'cell_name']].to_dict(orient='records')
      cell_distinct_msisdn = data.groupby('cell_name')['MSISDN'].nunique().to_dict()
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


      data['site_name_letter'] = _site_letters(data['site_name'], data['site_name_number'])

      data['modified_site_name_number'] = data['site_name_number'].str.zfill(4)
      data['modified_site_name'] = data['site_name_letter'] + data['modified_site_name_number']
      
      site_distinct_msisdn = data.groupby('modified_site_name')['MSISDN'].nunique().to_dict()
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
    logger.warning("aggregation error: %s", e)
    ctx = prototype_runtime.current()
    if ctx:
      ctx.warning("aggregation_error", error=str(e))
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

def _site_letters(site_names, site_numbers):
    """
    Exactly str(site_name).replace(str(site_number), '') per row - the old
    getSiteLetter() - without DataFrame.apply(axis=1), which builds a Series
    for every row and was the slowest line of the aggregation.
    """
    return pd.Series(
        [str(a).replace(str(b), '') for a, b in zip(site_names, site_numbers)],
        index=site_names.index,
        dtype=object,
    )


def _dump_csv(df, filename, **to_csv_kwargs):
    """Request-specific copy + atomic refresh of the legacy fixed file."""
    ctx = prototype_runtime.current()
    if ctx is not None:
        ctx.dump_csv(df, filename, **to_csv_kwargs)
        return
    legacy_dir = prototype_conf.legacy_output_dir()
    if legacy_dir:
        try:
            prototype_runtime.atomic_write_csv(df, Path(legacy_dir) / filename, **to_csv_kwargs)
        except Exception as e:
            logger.warning("could not write %s: %s", filename, e)


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
            logger.debug("No affected cells provided for hub metrics")
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
            logger.debug("No overlap between Smartcare data and NPP affected cells")
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
        
        # cached per process, re-read only when the file changes
        master_df = prototype_runtime.get_master_sheet(settings.MASTER_SHEET_PATH)

        site_list = list(subs_per_site.keys())
        matched_df = master_df[master_df['SITE_ID'].isin(site_list)]

        if matched_df.empty:
            logger.debug(f"No matching SITE_ID found in master sheet for {len(site_list)} sites")
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

        try:
            prototype_runtime.atomic_write_csv(
                matched_df, settings.OUTPUT_DIR / f"matched_master_Prototype_{ts}.csv", index=False)
        except Exception as e:
            logger.warning("could not write matched_master_Prototype_%s.csv: %s", ts, e)

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

   




