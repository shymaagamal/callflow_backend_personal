from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from callflow_portal.NCI.utils.helpers import build_nci_time_windows, cells_to_sites
from callflow_portal.callflow.core.jobs import job_manager
from callflow_portal.callflow.core.utils.file_cache import cache_get, cache_set, make_npp_cache_key
from callflow_portal.callflow.core.services.npp_service import NPP_CACHE_TTL, nppIntegration


def fetch_npp_one_cell(cell, window, log_path, ts):
    cache_key = make_npp_cache_key([cell], window["start"], window["end"], "hourly")
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    df = nppIntegration([cell], window["start"], window["end"], log_path, ts, "hourly")
    if not df.empty:
        cache_set(cache_key, df, ttl_seconds=NPP_CACHE_TTL)
    return df


def run_nci_analysis(cell_names, request_logger, log_path, ts):
    windows = build_nci_time_windows()
    sites = cells_to_sites(cell_names, log_path)

    tasks = {}
    with ThreadPoolExecutor(max_workers=min(len(cell_names) * 3 + 6, 20)) as executor:
        for period_name, w in windows.items():
            for cell in cell_names:
                tasks[executor.submit(fetch_npp_one_cell, cell, w, log_path, ts)] = (period_name, "npp", cell)
            tasks[executor.submit(fetch_viavi, sites, w, log_path, ts)] = (period_name, "viavi", None)
            tasks[executor.submit(fetch_remedy, sites, w, log_path, ts)] = (period_name, "remedy", None)

        results = {p: {"npp": [], "viavi": None, "remedy": None} for p in windows}
        done = 0
        for future in as_completed(tasks):
            period_name, source, cell = tasks[future]
            try:
                data = future.result()
                if source == "npp":
                    results[period_name]["npp"].append(data)
                else:
                    results[period_name][source] = data
            except Exception as e:
                request_logger.error(f"[NCI] {source}/{period_name}/{cell} failed: {e}")
            done += 1
            job_manager.update_job(job_id, progress_percent=int(done/len(tasks)*100))  # if using the job path

    for period_name, r in results.items():
        r["npp"] = pd.concat(r["npp"], ignore_index=True) if r["npp"] else pd.DataFrame()
    return results