from ..utils.data_utils import process_sitename
from typing import Tuple
from django.conf import settings
from ..utils.classes import Connection
from ..constants.general_constants import SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB, DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD
import pandas as pd
from datetime import datetime, timedelta
import logging
logger = logging.getLogger(__name__)


def remedyTicketsData(conn, start_date, end_date, suspected_sites, logs_file, timestamp):
   
    # Date Start From 'Last Week' TO 'Current End Date'
    last_week_date = start_date - timedelta(days=7)    
    query = f""" SELECT * FROM Mobile_Squad.dbo.Remedy_Tickets 
                WHERE actual_fault_time >= '{last_week_date}' and  actual_fault_time <= '{end_date}' """
    
    if suspected_sites: 
        site_filters = " OR ".join([f"site LIKE '%{s}%'" for s in suspected_sites])
        query += f" AND ({site_filters})"

    

    data = conn.query_select(query) 
    print('data.dtypes::',data.dtypes)
    
    if data.empty:
        return {}, 0
    
    data.to_csv(settings.OUTPUT_DIR / f"remedy_kpi_{start_date}_{end_date}.csv")

    data['site'] = data['site'].astype(str).str.strip()
    grouped = data.groupby('site')
    print('grouped')
    print(grouped)
    remedy_output = {}
    total_tickets = 0

    for site, group in grouped:
        status_counts = group['status'].value_counts().to_dict()
        site_total = len(group)
        
        remedy_output[site] = {
            "total": site_total,
            "status": status_counts,
            "tickets": group.drop(columns=["actual_resolution_time"], errors="ignore")
                            .to_dict(orient="records")
        }
        
        total_tickets += site_total
    
    # tickets_details = {
    #     site: group.where(pd.notnull(group), None).to_dict(orient="records")
    #     for site, group in grouped
    # }
    # print('tickets_details::' ,tickets_details)
    return remedy_output, total_tickets
    


            
def remedy_ticket (window,nodesdata,log_path,ts:str) ->  Tuple[int, pd.DataFrame]:
    nodesdata = nodesdata.copy()
    if 'modified_site_name' not in nodesdata.columns:
        nodesdata = process_sitename(nodesdata,'cell_name',settings.OUTPUT_DIR / "logs" / "remedy_ticket.log")
    suspected_sites = (
        nodesdata['modified_site_name']
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
    

    if not suspected_sites:
        logger.warning("No suspected sites found")
        return 0, pd.DataFrame()
    conn = Connection(SQL_DRIVER, DJANGO_SERVER, Mobile_squad_DB, DJANGO_SERVER_USERNAME, DJANGO_SERVER_PASSWORD)
    conn.connect()

    remedy_output, remedy_count   = remedyTicketsData(conn, window['start_dt'], window['end_dt'], suspected_sites, log_path,ts)
    # print('remedy_count:', remedy_count)
    # print('remedy_output:', remedy_output)
    return remedy_count, remedy_output

