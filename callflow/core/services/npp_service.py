import os

import requests
import pandas as pd
import json
import logging
from io import StringIO
from pathlib import Path
from django.conf import settings
from typing import Dict, Optional
import time
import threading
from ..utils.data_utils import process_sitename
from ..utils.file_cache import cache_get, cache_set, make_npp_cache_key
from ._parallel import run_in_order

_SITE_MAPPING: Optional[Dict[str, str]] = None
logger = logging.getLogger(__name__)
NPP_CACHE_TTL = 36000

# NPP requests (one per entity x hour) run in parallel inside a call.
# NPP_MAX_WORKERS: threads per nppIntegration call.
# NPP_MAX_CONCURRENT: cap on simultaneous NPP HTTP requests across ALL callers
# (e.g. NCI calls nppIntegration for several sites at once), to protect the NPP server.
NPP_MAX_WORKERS = getattr(settings, "NPP_MAX_WORKERS", 5)
NPP_MAX_CONCURRENT = getattr(settings, "NPP_MAX_CONCURRENT", 10)
_NPP_HTTP_SLOTS = threading.BoundedSemaphore(NPP_MAX_CONCURRENT)
def nppIntegration(
    suspected_entities_raw,
    start_date,
    end_date,
    logs_file,
    timestamp,
    resolution,
    entity_type: str = "cell",
    stats: Optional[dict] = None,
):
    print(f"nppIntegration called - resolution: {resolution}, entity_type: {entity_type}")
    print("=== PROXY ENV ===") 
    print("http_proxy:", os.environ.get("http_proxy") or os.environ.get("HTTP_PROXY"))
    print("https_proxy:", os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")) 
    print("no_proxy:", os.environ.get("no_proxy") or os.environ.get("NO_PROXY")) 

 
    if entity_type not in ("cell", "site"):
        raise ValueError(f"Invalid entity_type: {entity_type}")
    if not suspected_entities_raw:
        print("No entities provided to nppIntegration")
        return pd.DataFrame()

    try:
        suspected_entities = json.loads(suspected_entities_raw) if isinstance(suspected_entities_raw, str) else suspected_entities_raw
    except Exception as e:
        raise ValueError(f"Invalid suspected_entities format: {str(e)}")

    cache_key = make_npp_cache_key(suspected_entities, start_date, end_date, resolution, entity_type)
    cached = cache_get(cache_key)
    if cached is not None:
        print(f"[NPP CACHE HIT] {len(suspected_entities)} {entity_type}s, resolution={resolution}")
        if stats is not None:
            stats.update(cache_hit=True, requests=0, rows=len(cached))
        return cached

    reference_date = pd.Timestamp("2020-01-01")
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)
    if end_date <= start_date:
        end_date = start_date + pd.Timedelta(hours=1)

    hours = []
    cursor = start_date.floor("h")
    while cursor < end_date:
        hours.append(int((cursor - reference_date) / pd.Timedelta(hours=1)))
        cursor += pd.Timedelta(hours=1)
    if not hours:
        hours = [int((start_date - reference_date) / pd.Timedelta(hours=1))]

    routes = {
        ("cell", "15min"): "http://10.241.161.227:3000/get-kpis/",
        ("cell", "daily"): "http://10.241.161.227:3000/get-kpis/",
        ("cell", "hourly"): "http://10.241.161.227:3000/get-kpis/hourly",
        ("site", "15min"): "http://10.241.161.227:3000/get-kpis/site",
        ("site", "daily"): "http://10.241.161.227:3000/get-kpis/site",
        ("site", "hourly"): "http://10.241.161.227:3000/get-kpis/site-hourly",
    }
    url = routes.get((entity_type, resolution))
    if not url:
        raise ValueError(f"Invalid resolution: {resolution}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.0V2nR3wHXDQI9ZRtxXut8euJmxjCRPWLsVPL0DWDKl8",
    }

    occ_df = pd.DataFrame()
    failed = 0
    error_msg = None
    items = [(entity, hour) for entity in suspected_entities for hour in hours]
    attempts = len(items)
    err_lock = threading.Lock()
    latencies = []      # seconds per completed HTTP request (for the stats summary)
    empty = [0]
    wall_start = time.time()

    def fetch_one(item):
        # Same request/parsing as the old sequential loop; raises on failure.
        entity, hour = item
        if entity_type == "site":
            data = {"site": entity, "hour": hour}
        else:
            data = {"cellName": entity, "hour": hour}
        print(f"NPP request → {entity_type}={entity}, hour={hour}, url={url}")
        start = time.time()
        proxis = {
            "http": None,
            "https": None
        }
        with _NPP_HTTP_SLOTS:
            response = requests.post(url, json=data, headers=headers, timeout= (10,60), proxies=proxis)
        print(f"NPP request completed in {time.time() - start:.2f}s with status {response.status_code}")
        with err_lock:
            latencies.append(time.time() - start)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))

        logger.info(
            f"[NPP RESPONSE] entity_type={entity_type}, "
            f"entity={entity}, hour={hour}, "
            f"status={response.status_code}, "
            f"columns={df.columns.tolist()}, "
            f"rows={len(df)}"
        )

        print(
            f"[NPP RESPONSE] {entity_type}={entity}, hour={hour}, "
            f"columns={df.columns.tolist()}, rows={len(df)}"
        )
        if df.empty:
            with err_lock:
                empty[0] += 1
            return None
        df["DATETIME_ID"] = pd.to_datetime(df["DATETIME_ID"], errors="coerce")
        return df.dropna(subset=["DATETIME_ID"])

    def on_error(item, e):
        nonlocal failed, error_msg
        entity, hour = item
        msg = f"NPP request failed for {entity_type} {entity} hour {hour}: {e}"
        logger.error(msg)
        with err_lock:
            failed += 1
            error_msg = msg
            try:
                with open(logs_file, "a") as f:
                    f.write(msg + "\n")
            except Exception:  # a log-file problem must not abort the other requests
                logger.warning("Could not write NPP error to %s", logs_file)

    # Parallel, but results come back in the original (entity, hour) order, so
    # the concatenated frame is identical to the old sequential loop.
    results = run_in_order(items, fetch_one, max_workers=NPP_MAX_WORKERS,
                           first_sync=False, on_error=on_error)
    frames = [df for _item, df in results if df is not None]
    if frames:
        occ_df = pd.concat(frames, ignore_index=True)

    if stats is not None:
        lat = sorted(latencies)
        stats.update(
            cache_hit=False, entities=len(suspected_entities), hours=len(hours),
            requests=attempts, ok=attempts - failed - empty[0], empty=empty[0], failed=failed,
            workers=NPP_MAX_WORKERS, wall_s=round(time.time() - wall_start, 2),
            avg_s=round(sum(lat) / len(lat), 2) if lat else None,
            p90_s=round(lat[max(0, int(0.9 * len(lat)) - 1)], 2) if lat else None,
            max_s=round(lat[-1], 2) if lat else None,
        )

    if not occ_df.empty:
        occ_df = occ_df.sort_values(by="DATETIME_ID")
        if "SITE_PHYSICAL" in occ_df.columns and "sitePhysical" not in occ_df.columns:
            occ_df = occ_df.rename(columns={"SITE_PHYSICAL": "sitePhysical"})
        group_cols = ["DATETIME_ID", "cell", "Vendor"]
        if "site" in occ_df.columns:
            group_cols.append("site")
        elif "sitePhysical" in occ_df.columns:
            group_cols.append("sitePhysical")
        occ_df = occ_df.groupby(group_cols).sum(numeric_only=True).reset_index()
    else:
        logger.info(f"No NPP data returned. Failed requests: {failed}/{attempts}")

    if attempts and failed == attempts:
        raise RuntimeError(f"NPP integration failed for all {attempts} requests. Last error: {error_msg}")

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