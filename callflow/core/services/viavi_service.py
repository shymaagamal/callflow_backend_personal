import pandas as pd
import logging
from ..utils.file_cache import cache_get, cache_set, make_viavi_cache_key
logger = logging.getLogger(__name__)
# Viavi data refreshes at 15-min periods; 30-min TTL is safe.
VIAVI_CACHE_TTL = 1800 
def viaviQueries(conn, start_date, end_date, suspected_sites, logs_file, timestamp):
  print(f"we are in viavi query nafshaaa")
  if not conn:
        error_msg = "No Viavi connection provided"
        logger.error(error_msg)
        with open(logs_file, 'a') as f:
            f.write(error_msg + '\n')
        raise ValueError(error_msg)
  
  if not suspected_sites:
        logger.info("No suspected sites provided → returning empty DataFrame")
        return pd.DataFrame(columns=['PERIOD_TIMESTAMP', 'FLWID', 'AVG_FD_MS', 'AVG_FDV_MS', 'ONEW_FLR_AZ', 'ONEW_FLR_ZA'])
  # ── Cache check ───────────────────────────────────────────────────────────
  cache_key = make_viavi_cache_key(suspected_sites, str(start_date), str(end_date))
  cached = cache_get(cache_key)
  if cached is not None:
      logger.info(f"[VIAVI CACHE HIT] {len(suspected_sites)} sites")
      return cached
  
  # ─────────────────────────────────────────────────────────────────────────
 
  # Build AM/PM periods
  start_period = 'PM' if 'PM' in str(start_date) else 'AM'
  end_period   = 'PM' if 'PM' in str(end_date) else 'AM'
  
  site_conditions = " OR ".join([f"FLWID LIKE '%{site}%'" for site in suspected_sites])
  query = f"""
        SELECT PERIOD_TIMESTAMP, FLWID, AVG_FD_MS, AVG_FDV_MS, ONEW_FLR_AZ, ONEW_FLR_ZA
        FROM ONEADM."vw_VIAVI_Ethernet_Raw"
        WHERE (PERIOD_TIMESTAMP >= to_timestamp('{start_date}', 'YYYY-MM-DD HH:MI {start_period}'))
          AND (PERIOD_TIMESTAMP <= to_timestamp('{end_date}', 'YYYY-MM-DD HH:MI {end_period}'))
          AND ({site_conditions})
        ORDER BY PERIOD_TIMESTAMP
    """

  
  

  with open(logs_file, 'a') as f:
    f.write(f"Viavi query executed:\n{query}\n\n")
  try:
        # Execute query
    viavi_output = pd.read_sql_query(query, conn)  
    logger.info(f"viavi_output")
    logger.info(viavi_output)
  # Ensure expected columns (safety)
    expected_cols = ['PERIOD_TIMESTAMP', 'FLWID', 'AVG_FD_MS', 'AVG_FDV_MS', 'ONEW_FLR_AZ', 'ONEW_FLR_ZA']
    missing = [c for c in expected_cols if c not in viavi_output.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Viavi result: {missing}")

    logger.info(f"Viavi query returned {len(viavi_output)} rows")
    # ── Persist to cache ──────────────────────────────────────────────────
    if not viavi_output.empty:
        cache_set(cache_key, viavi_output, ttl_seconds=VIAVI_CACHE_TTL)
    # ─────────────────────────────────────────────────────────────────────

    return viavi_output

  except Exception as e:
      error_msg = f"Viavi query failed: {str(e)}"
      logger.error(error_msg, exc_info=True)
      logger.info(f"error msg",error_msg)
      with open(logs_file, 'a') as f:
          f.write(error_msg + '\n')
          f.write(f"Query was:\n{query}\n")
      
      # Raise so upper layers can catch and report
      raise RuntimeError(error_msg) from e