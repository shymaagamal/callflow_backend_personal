"""
HUB metrics - calculate_affected_hub_metrics from the Prototype code, unchanged
in logic and in its three return shapes ({'HUB_Counts': {...}},
{'subs_per_hub': []}, {'hubs_affected_count': 0}). Changes: the master sheet
is cached per process (re-read when the file changes), matched_master.csv and
the site-name error log go to the job folder.
"""
import os
import threading

import pandas as pd
from django.conf import settings

from callflow.core.utils.data_utils import process_sitename

_lock = threading.Lock()
_cache = {}


def master_sheet(path=None):
    path = str(path or settings.MASTER_SHEET_PATH)
    st = os.stat(path)
    key = (st.st_mtime, st.st_size)
    with _lock:
        hit = _cache.get(path)
        if hit and hit[0] == key:
            return hit[1]
    df = pd.read_excel(path)
    df['SITE_ID'] = df['SITE_ID'].astype(str).str.strip()
    with _lock:
        _cache[path] = (key, df)
    return df


def clear_cache():
    with _lock:
        _cache.clear()


def affected_hub_metrics(nodesdata, affected_cells, ctx, job_dir):
    try:
        if not affected_cells:
            ctx.event("hub_metrics_skipped", reason="no affected cells")
            return {'subs_per_hub': []}

        nodesdata = nodesdata.copy()
        affected_cells = [str(c).strip() for c in affected_cells if c]
        if 'cell_name' in nodesdata.columns:
            nodesdata = nodesdata[nodesdata['cell_name'].isin(affected_cells)].copy()
        elif 'CELL_NAME' in nodesdata.columns:
            nodesdata = nodesdata[nodesdata['CELL_NAME'].isin(affected_cells)].copy()

        if nodesdata.empty:
            ctx.event("hub_metrics_skipped", reason="no overlap with affected cells")
            return {'subs_per_hub': []}

        sitename_log = str(job_dir / "sitename_errors.log")
        if 'cell_name' in nodesdata.columns:
            nodesdata = process_sitename(nodesdata, 'cell_name', sitename_log)
        elif 'CELL_NAME' in nodesdata.columns:
            nodesdata = process_sitename(nodesdata, 'CELL_NAME', sitename_log)

        nodesdata.loc[:, 'modified_site_name'] = nodesdata['modified_site_name'].replace('', None)
        subs_per_site = nodesdata.groupby('modified_site_name')['MSISDN'].nunique().dropna().to_dict()

        master_df = master_sheet()
        site_list = list(subs_per_site.keys())
        matched_df = master_df[master_df['SITE_ID'].isin(site_list)]
        if matched_df.empty:
            ctx.event("hub_metrics_skipped", reason="no SITE_ID match", sites=len(site_list))
            return {'subs_per_hub': []}

        hubs_count = len(matched_df['Hub'].dropna().unique().tolist())
        site_to_hub = matched_df.set_index('SITE_ID')['Hub'].to_dict()
        nodesdata['Hub'] = nodesdata['modified_site_name'].map(site_to_hub)
        subs_per_hub = (
            nodesdata.dropna(subset=['Hub', 'MSISDN'])
            .groupby('Hub')['MSISDN'].nunique()
            .to_dict()
        )
        try:
            matched_df.to_csv(job_dir / "matched_master.csv", index=False)
        except Exception as e:
            ctx.warning("export_failed", file="matched_master.csv", error=str(e))
        ctx.event("hub_metrics", hubs=hubs_count, matched_sites=len(matched_df), cells=len(affected_cells))
        return {'HUB_Counts': subs_per_hub}
    except Exception:
        ctx.exception("hub_metrics_failed")
        ctx.add_warning("metric_failed", metric="HUB_Counts")
        return {'hubs_affected_count': 0}
