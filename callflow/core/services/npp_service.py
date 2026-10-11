import requests
import pandas as pd
import json
import logging
from io import StringIO
from pathlib import Path
from django.conf import settings
from typing import Dict, Optional

from ..utils.data_utils import process_sitename
from ..utils.file_cache import cache_get, cache_set, make_npp_cache_key

_SITE_MAPPING: Optional[Dict[str, str]] = None
logger = logging.getLogger(__name__)
NPP_CACHE_TTL = 36000
def nppIntegration(
    suspected_entities_raw,
    start_date,
    end_date,
    logs_file,
    timestamp,
    resolution,
    entity_type: str = "cell"
):
    """
    Integrates with NPP service to fetch KPI data per cell for a time range.
    Loops over hours from start_date to end_date.
    """
    print(f"nppIntegration called - resolution: {resolution}, entity_type: {entity_type}")
    if entity_type not in ("cell", "site"):
        raise ValueError(f"Invalid entity_type: {entity_type}")
    if not suspected_entities_raw:
        print("No entities provided to nppIntegration")
        return pd.DataFrame()
    
    try:
        suspected_entities = json.loads(suspected_entities_raw) if isinstance(suspected_entities_raw, str) else suspected_entities_raw
    except Exception as e:
        raise ValueError(f"Invalid suspected_entities format: {str(e)}")
    # ── Cache check ───────────────────────────────────────────────────────────
    cache_key = make_npp_cache_key(suspected_entities, start_date, end_date, resolution, entity_type)
    cached = cache_get(cache_key)
    if cached is not None:
        print(f"[NPP CACHE HIT] {len(suspected_entities)} {entity_type}s, resolution={resolution}")
        return cached
    # ---------------- Time & URL setup ----------------
    reference_date = pd.Timestamp("2020-01-01")
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)
    

    print(f"[NPP start date] {start_date} ")
    print(f"[NPP end date] {end_date} ")


    # Calculate start and end hour differences
    start_hour_diff = int((start_date - reference_date) / pd.Timedelta(hours=1))
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

    occ_df = pd.DataFrame()
    failed_entities = []
    error_msg = None
    for entity in suspected_entities:
        if entity_type == "site":
            data = {"cellName": "", "sitePhysical": entity, "hour": start_hour_diff}
        else:  # entity_type == "cell"
            data = {"cellName": entity, "sitePhysical": "", "hour": start_hour_diff}
        
    
        try:
            print(f"NPP request → {entity_type}={entity}, hour={start_hour_diff}")
            
            response = requests.post(url, json=data, headers=headers, timeout=10)
            print("=" * 60)
            print("Request URL:", url)
            print("Request Payload:", data)
            print("Status:", response.status_code)
            print("Response:", response.text)
            print("=" * 60)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            print('df NPP here ------')
            print(df)
            print(df.columns)
            print(f"data from NPP", df['DATETIME_ID'])
            df['DATETIME_ID'] = pd.to_datetime(df['DATETIME_ID'], errors='coerce')
            df = df.dropna(subset=['DATETIME_ID'])
            occ_df = pd.concat([occ_df, df], ignore_index=True)
            print('occ_df')
            print(occ_df)
            print(occ_df.columns)

        except requests.exceptions.RequestException as e:
            error_msg = f"NPP request failed for {entity_type} {entity}: {str(e)}"
            logger.error(error_msg)
            with open(logs_file, 'a') as f:
                f.write(error_msg + '\n')
            failed_entities.append(entity)

        except Exception as e:
            error_msg = f"Unexpected error in NPP processing for {entity_type} {entity}: {str(e)}"
            logger.exception(error_msg)
            with open(logs_file, 'a') as f:
                f.write(error_msg + '\n')
            failed_entities.append(entity)
           
    # ── Final aggregation & check ─────────────────────────────
    if not occ_df.empty:
        occ_df = occ_df.sort_values(by='DATETIME_ID')
        group_cols = ['DATETIME_ID', 'cell', 'Vendor']
        if 'sitePhysical' in occ_df.columns:
            group_cols.append('sitePhysical')
        occ_df = occ_df.groupby(group_cols).sum().reset_index()
        
    
    else:
            print(f"No NPP data returned for any {entity_type}. Failed {entity_type}s: {failed_entities}")
    # If ALL entities failed → raise so view reports failure
    if len(failed_entities) == len(suspected_entities) and suspected_entities:
        raise RuntimeError(f"NPP integration failed for all {len(suspected_entities)} {entity_type}s. Last error: {error_msg}")
    
    # ── Persist to cache ──────────────────────────────────────────────────────
    if not occ_df.empty:
        cache_set(cache_key, occ_df, ttl_seconds=NPP_CACHE_TTL)
    return occ_df
    
def calculate_affected_hub_taclac_metrics(nodesdata, affected_cells: list, ts: str) -> dict:
    """
     Calculates HUB / TAC / LAC / Region metrics
    using ONLY the NPP-affected cells from cached Smartcare data.
    """
    
    try:
        if not affected_cells:
            print("No affected cells provided for hub metrics")
            return {
                'hubs_affected_count': 0, 'tacs_affected_count': 0, 'lacs_affected_count': 0,
                'subs_per_cell': [], 'subs_per_site': [], 'subs_per_hub': [],
                'subs_per_tac': [], 'subs_per_lac': [], 'matched_sites_count': 0,
                'subs_per_handset_type': [], 'subs_per_region': [],
                'unique_region': [], 'region_count': 0
            }

        nodesdata = nodesdata.copy()
        # Filter to ONLY affected cells (this is the key change)
        affected_cells = [str(c).strip() for c in affected_cells if c]
        nodesdata = nodesdata[nodesdata['cell_name'].isin(affected_cells)].copy()

        if nodesdata.empty:
            print("No overlap between Smartcare data and NPP affected cells")
            return {
                'hubs_affected_count': 0, 'tacs_affected_count': 0, 'lacs_affected_count': 0,
                 'subs_per_hub': [],
                'subs_per_tac': [], 'subs_per_lac': [], 'matched_sites_count': 0,
                'subs_per_handset_type': [], 'subs_per_region': [],
                'unique_region': [], 'region_count': 0
            }

        # ==================== ORIGINAL FULL LOGIC (unchanged) ====================
        if 'modified_site_name' not in nodesdata.columns:
            nodesdata = process_sitename(nodesdata, 'cell_name',settings.OUTPUT_DIR / "logs" / "metrics.log")

        nodesdata.loc[:, 'modified_site_name'] = nodesdata['modified_site_name'].replace('', None)

        subs_per_site = nodesdata.groupby('modified_site_name')['MSISDN'].nunique().dropna().to_dict()
        
        master_df = pd.read_excel(settings.MASTER_SHEET_PATH)
        master_df['SITE_ID'] = master_df['SITE_ID'].astype(str).str.strip()

        site_list = list(subs_per_site.keys())
        matched_df = master_df[master_df['SITE_ID'].isin(site_list)]

        if matched_df.empty:
            print(f"No matching SITE_ID found in master sheet for {len(site_list)} sites")
            return {
                'hubs_affected_count': 0, 'tacs_affected_count': 0, 'lacs_affected_count': 0,
                'subs_per_hub': [],'subs_per_tac': [],
            'subs_per_lac': [], 'matched_sites_count': 0, 'subs_per_region': {}, 'unique_region':[],
            'region_count': 0
            }

        # All the original calculations (regions, hubs, tacs, lacs, mapping, etc.)
        unique_region = matched_df['TX-Region'].dropna().unique().tolist()
        region_count = len(unique_region)
        unique_hubs = matched_df['Hub'].dropna().unique().tolist()
        hubs_count = len(unique_hubs)
        unique_tacs = matched_df['TAC'].dropna().unique().tolist()
        tacs_count = len(unique_tacs)
        unique_lacs = matched_df['LAC'].dropna().unique().tolist()
        lacs_count = len(unique_lacs)

        site_to_hub = matched_df.set_index('SITE_ID')['Hub'].to_dict()
        nodesdata['Hub'] = nodesdata['modified_site_name'].map(site_to_hub)
        subs_per_hub = (
            nodesdata.dropna(subset=['Hub', 'MSISDN'])
            .groupby('Hub')['MSISDN'].nunique()
            .reset_index(name='value')
        )
        subs_per_hub = [{"label": row['Hub'], "value": int(row['value'])} for _, row in subs_per_hub.iterrows()]

        site_to_tac = matched_df.set_index('SITE_ID')['TAC'].to_dict()
        nodesdata['TAC'] = nodesdata['modified_site_name'].map(site_to_tac)
        subs_per_tac = (
            nodesdata.dropna(subset=['TAC', 'MSISDN'])
            .groupby('TAC')['MSISDN'].nunique()
            .reset_index(name='value')
        )
        subs_per_tac = [{"label": row['TAC'], "value": int(row['value'])} for _, row in subs_per_tac.iterrows()]

        site_to_lac = matched_df.set_index('SITE_ID')['LAC'].to_dict()
        nodesdata['LAC'] = nodesdata['modified_site_name'].map(site_to_lac)
        subs_per_lac = (
            nodesdata.dropna(subset=['LAC', 'MSISDN'])
            .groupby('LAC')['MSISDN'].nunique()
            .reset_index(name='value')
        )
        subs_per_lac = [{"label": row['LAC'], "value": int(row['value'])} for _, row in subs_per_lac.iterrows()]

        site_to_region = matched_df.set_index('SITE_ID')['TX-Region'].to_dict()
        nodesdata['Region'] = nodesdata['modified_site_name'].map(site_to_region)
        subs_per_region = (
            nodesdata.dropna(subset=['Region', 'MSISDN'])
            .groupby('Region')['MSISDN'].nunique()
            .reset_index(name='value')
        )
        subs_per_region = [{"label": row['Region'], "value": int(row['value'])} for _, row in subs_per_region.iterrows()]

        matched_df.to_csv(settings.OUTPUT_DIR / f"matched_master_{ts}.csv", index=False)

        logger.info(
            f"[AFFECTED HUB METRICS] hubs={hubs_count}, tacs={tacs_count}, lacs={lacs_count}, "
            f"matched={len(matched_df)} (filtered to {len(affected_cells)} NPP cells)"
        )

        return {
            'hubs_affected_count': hubs_count,
            'tacs_affected_count': tacs_count,
            'lacs_affected_count': lacs_count,
            'subs_per_hub': subs_per_hub,
            'subs_per_tac': subs_per_tac,
            'subs_per_lac': subs_per_lac,
            'matched_sites_count': len(matched_df),
            'subs_per_region': subs_per_region,
            'unique_region': unique_region,
            'region_count': region_count
        }

    except Exception as e:
        logger.exception("Error calculating affected hub/TAC/LAC metrics")
        return {'hubs_affected_count': 0, 'tacs_affected_count': 0, 'lacs_affected_count': 0,
                'subs_per_hub': [], 'matched_sites_count': 0}