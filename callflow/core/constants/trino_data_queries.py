trino_s1mme_latest_6H = """select from_unixtime(STARTTIME) as mytime,msisdn,mme_name,'4G' as RAT,ter_brand_name,ter_model_name,b.FAIL_CAUSE_NAME,
SUM(cast(s1mme_attach_req_times as integer)) AS s1mme_attach_req_times,
SUM(cast(S1MME_ATTACH_FAIL_TIMES as integer)) AS S1MME_ATTACH_FAIL_TIMES,
SUM(cast(TAU_REQ_TIMES as integer)) AS TAU_REQ_TIMES,
SUM(cast(TAU_FAIL_TIMES as integer)) AS TAU_FAIL_TIMES,
SUM(cast(S1MME_AUTHENTICATION_REQ_TIMES as integer)) AS S1MME_AUTHENTICATION_REQ_TIMES,
SUM(cast(S1MME_AUTHENTICATION_FAIL_TIMES as integer)) AS S1MME_AUTHENTICATION_FAIL_TIMES
from clickhouse.e2e.BKPI_SIGNAL_S1MME_CISAI_15MIN a
left join HIVE.SMARTCARE.DIM_PSSAE_SIG_TRANS_CAUSE b
on a.PS_SIG_FAILCAUSE =
CONCAT(cast(b.PROBE_PROTOCOL_ID as VARCHAR),'-',cast(b.PROBE_TRANS_ID as VARCHAR),'-',cast(b.FAIL_TYPE_ID as VARCHAR),'-',cast(b.FAIL_CAUSE_ID as VARCHAR))
where msisdn IN {msisdn_tuple}
and STARTTIME BETWEEN to_unixtime(cast('{start_date}' as timestamp))
and to_unixtime(cast('{end_date}' as timestamp))
group by 1,2,3,4,5,6,7
order by 1"""

# S1MME after 6H: Hive Smartcare KPI table (via Trino), for windows older than the latest 6 hours.
trino_s1mme_after_6H = """select time_t as mytime,msisdn,mme_name,'4G' as RAT,ter_brand_name,ter_model_name,b.FAIL_CAUSE_NAME,
SUM(cast(s1mme_attach_req_times as integer)) AS s1mme_attach_req_times,
SUM(cast(s1mme_attach_succ_times as integer)) AS s1mme_attach_succ_times,
SUM(cast(periodic_tau_req_times as integer)) AS periodic_tau_req_times,
SUM(cast(periodic_tau_succ_times as integer)) AS periodic_tau_succ_times,
SUM(cast(combined_tala_update_req_times as integer)) AS combined_tala_update_req_times,
SUM(cast(combined_tala_update_succ_times as integer)) AS combined_tala_update_succ_times,
SUM(cast(0 as integer)) AS S1MME_AUTHENTICATION_REQ_TIMES,
SUM(cast(0 as integer)) AS S1MME_AUTHENTICATION_SUCC_TIMES
from HIVE.SMARTCARE.s1mme_kpi a
left join HIVE.SMARTCARE.DIM_PSSAE_SIG_TRANS_CAUSE b
on a.PS_SIG_FAILCAUSE =
CONCAT(cast(b.PROBE_PROTOCOL_ID as VARCHAR),'-',cast(b.PROBE_TRANS_ID as VARCHAR),'-',cast(b.FAIL_TYPE_ID as VARCHAR),'-',cast(b.FAIL_CAUSE_ID as VARCHAR))
where msisdn IN {msisdn_tuple}
and time_t between '{start_time_t}'
and '{end_time_t}'
group by 1,2,3,4,5,6,7
order by 1"""

# Picked by smartcare_service.smartcareNodesQueries (DATA) on the 6-hour rule.
trino_s1mme_QUERIES_latest_6H = {'trino_s1mme_latest_6H': trino_s1mme_latest_6H}
trino_s1mme_QUERIES_after_6H = {'trino_s1mme_after_6H': trino_s1mme_after_6H}

