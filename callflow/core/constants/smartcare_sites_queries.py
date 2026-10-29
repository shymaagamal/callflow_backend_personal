smartcare_site_cells = """SELECT CGISAI, LOGICAL_SITE_ID
FROM SDR_NETHOUSE.DIM_LOC_CGISAI
WHERE CGISAI IS NOT NULL
  AND LOGICAL_SITE_ID IN {site_tuple}
GROUP BY 1, 2
"""

smartcare_s1mme_by_site = """SELECT
    b.LOGICAL_SITE_ID,
    sum(CASE WHEN EMM_TRANS_TYPE = 48 AND EMM_REQ_SUCCED_FLAG IS NOT NULL THEN 1 ELSE 0 END) AS ATTACH_REQ_TIMES,
    sum(CASE WHEN EMM_TRANS_TYPE = 48 AND EMM_REQ_SUCCED_FLAG = 1 THEN 1 ELSE 0 END) AS ATTACH_Fail_TIMES,
    sum(CASE WHEN PROC_TYPE = 103 AND PROC_SUCCED_FLAG IS NOT NULL THEN 1 ELSE 0 END) AS TAU_REQ_TIMES,
    sum(CASE WHEN PROC_TYPE = 103 AND PROC_SUCCED_FLAG = 1 THEN 1 ELSE 0 END) AS TAU_FAIL_TIMES,
    sum(CASE WHEN AUTH_REQ_TIME_SEC IS NOT NULL AND AUTH_SUCCED_FLAG IS NOT NULL THEN 1 ELSE 0 END) AS S1MME_AUTHENTICATION_REQ_TIMES,
    sum(CASE WHEN AUTH_REQ_TIME_SEC IS NOT NULL AND AUTH_SUCCED_FLAG = 1 THEN 1 ELSE 0 END) AS S1MME_AUTHENTICATION_FAIL_TIMES
FROM ps.DETAIL_CDR_S1MME_{tablename} a
INNER JOIN (
    SELECT CGISAI, LOGICAL_SITE_ID
    FROM SDR_NETHOUSE.DIM_LOC_CGISAI
    WHERE CGISAI IS NOT NULL
      AND LOGICAL_SITE_ID IN {site_tuple}
    GROUP BY 1, 2
) b ON a.sai_cgi_ecgi = b.CGISAI
WHERE a.sai_cgi_ecgi IN {cgi_tuple}
  AND a.PROC_STARTTIME BETWEEN unix_timestamp('{start_date}')
                           AND unix_timestamp('{end_date}')
GROUP BY b.LOGICAL_SITE_ID
"""

smartcare_s6a_by_site = """SELECT
    b.LOGICAL_SITE_ID,
    sum(CASE WHEN TRANS_TYPE = 8 AND TRANS_SUCCED_FLAG IS NOT NULL THEN 1 ELSE 0 END) AS S6A_UPDATE_LOC_REQ_TIMES,
    sum(CASE WHEN TRANS_TYPE = 8 AND TRANS_SUCCED_FLAG = 0 THEN 1 ELSE 0 END) AS S6A_UPDATE_LOC_SUCC_TIMES,
    sum(CASE WHEN TRANS_TYPE = 8 AND TRANS_SUCCED_FLAG = 1 THEN 1 ELSE 0 END) AS S6A_UPDATE_LOC_FAIL_TIMES
FROM ps.detail_cdr_s6a_{tablename} a
INNER JOIN (
    SELECT CGISAI, LOGICAL_SITE_ID
    FROM SDR_NETHOUSE.DIM_LOC_CGISAI
    WHERE CGISAI IS NOT NULL
      AND LOGICAL_SITE_ID IN {site_tuple}
    GROUP BY 1, 2
) b ON a.sai_cgi_ecgi = b.CGISAI
WHERE a.sai_cgi_ecgi IN {cgi_tuple}
  AND a.TRANS_TYPE = 8
  AND a.CALC_TIME BETWEEN unix_timestamp('{start_date}')
                      AND unix_timestamp('{end_date}')
GROUP BY b.LOGICAL_SITE_ID
"""

smartcare_site_kpi_QUERIES = {
    "s1mme": smartcare_s1mme_by_site,
    "s6a": smartcare_s6a_by_site,
}

# ---------------------------------------------------------------------------
# Trino S1-MME site KPIs (replace the Smartcare JDBC queries above for the
# NCI site KPIs; see services/smartcare_site_kpi_service.py).
#
# Placeholders, filled by the service only:
#   {site_list}    ('SITE_A','SITE_B')   -- validated + quote-escaped site ids
#   {start_epoch}  {end_epoch}           -- integer epoch seconds, half-open
#                                           range [start, end) so the two
#                                           periods never overlap
#
# Both queries return the SAME columns (names and order):
#   Time, Site_Logical_Name, ATTACH_REQ_TIMES, ATTACH_FAIL_TIMES,
#   TAU_REQ_TIMES, TAU_FAIL_TIMES, S1MME_AUTHENTICATION_REQ_TIMES,
#   S1MME_AUTHENTICATION_FAIL_TIMES, FAIL_CAUSE_NAME
# ---------------------------------------------------------------------------

# Most recent 6 hours: ClickHouse 15-minute table.
trino_s1mme_recent_15min = """SELECT
    from_unixtime(STARTTIME) AS Time,
    a.Site_Logical_Name AS Site_Logical_Name,
    SUM(S1MME_ATTACH_REQ_TIMES) AS ATTACH_REQ_TIMES,
    SUM(S1MME_ATTACH_FAIL_TIMES) AS ATTACH_FAIL_TIMES,
    SUM(TAU_REQ_TIMES) AS TAU_REQ_TIMES,
    SUM(TAU_FAIL_TIMES) AS TAU_FAIL_TIMES,
    SUM(S1MME_AUTHENTICATION_REQ_TIMES) AS S1MME_AUTHENTICATION_REQ_TIMES,
    SUM(S1MME_AUTHENTICATION_FAIL_TIMES) AS S1MME_AUTHENTICATION_FAIL_TIMES,
    b.FAIL_CAUSE_NAME AS FAIL_CAUSE_NAME
FROM clickhouse.e2e.BKPI_SIGNAL_S1MME_CISAI_15MIN a
LEFT JOIN hive.smartcare.DIM_PSSAE_SIG_TRANS_CAUSE b
    ON a.PS_SIG_FAILCAUSE = CONCAT(
        CAST(b.PROBE_PROTOCOL_ID AS VARCHAR), '-',
        CAST(b.PROBE_TRANS_ID AS VARCHAR), '-',
        CAST(b.FAIL_TYPE_ID AS VARCHAR), '-',
        CAST(b.FAIL_CAUSE_ID AS VARCHAR)
    )
WHERE a.Site_Logical_Name IN {site_list}
  AND a.PROC_STARTTIME >= {start_epoch}
  AND a.PROC_STARTTIME < {end_epoch}
GROUP BY
    from_unixtime(STARTTIME),
    a.Site_Logical_Name,
    b.FAIL_CAUSE_NAME"""

# Older than 6 hours: Hive s1mme_kpi. This table has no authentication
# counters, so those two columns are NULL (not 0): "no data", not "no attempts".
# The cell -> site dimension is de-duplicated (as the old Smartcare query did
# with GROUP BY 1, 2) so a CGISAI listed twice cannot double the counts.
trino_s1mme_history_hive = """SELECT
    a.time_t AS Time,
    c.site_name AS Site_Logical_Name,
    SUM(CAST(a.s1mme_attach_req_times AS BIGINT)) AS ATTACH_REQ_TIMES,
    SUM(
        CAST(a.s1mme_attach_req_times AS BIGINT)
        - CAST(a.s1mme_attach_succ_times AS BIGINT)
    ) AS ATTACH_FAIL_TIMES,
    SUM(CAST(a.combined_tala_update_req_times AS BIGINT)) AS TAU_REQ_TIMES,
    SUM(
        CAST(a.combined_tala_update_req_times AS BIGINT)
        - CAST(a.combined_tala_update_succ_times AS BIGINT)
    ) AS TAU_FAIL_TIMES,
    CAST(NULL AS BIGINT) AS S1MME_AUTHENTICATION_REQ_TIMES,
    CAST(NULL AS BIGINT) AS S1MME_AUTHENTICATION_FAIL_TIMES,
    b.FAIL_CAUSE_NAME AS FAIL_CAUSE_NAME
FROM hive.smartcare.s1mme_kpi a
LEFT JOIN hive.smartcare.DIM_PSSAE_SIG_TRANS_CAUSE b
    ON a.PS_SIG_FAILCAUSE = CONCAT(
        CAST(b.PROBE_PROTOCOL_ID AS VARCHAR), '-',
        CAST(b.PROBE_TRANS_ID AS VARCHAR), '-',
        CAST(b.FAIL_TYPE_ID AS VARCHAR), '-',
        CAST(b.FAIL_CAUSE_ID AS VARCHAR)
    )
INNER JOIN (
    SELECT DISTINCT CGISAI, site_name
    FROM hive.smartcare.dim_loc_cgisai
    WHERE site_name IN {site_list}
) c ON a.sai_cgi_ecgi = c.CGISAI
WHERE a.PROC_STARTTIME >= {start_epoch}
  AND a.PROC_STARTTIME < {end_epoch}
GROUP BY
    a.time_t,
    c.site_name,
    b.FAIL_CAUSE_NAME"""

trino_s1mme_QUERIES = {
    "clickhouse_15min": trino_s1mme_recent_15min,   # most recent 6 hours
    "hive_s1mme_kpi": trino_s1mme_history_hive,     # everything older
}
