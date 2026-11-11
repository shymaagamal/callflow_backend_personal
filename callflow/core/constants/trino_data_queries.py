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
and STARTTIME BETWEEN to_unixtime(cast({start_date} as timestamp))
and to_unixtime(cast({end_date} as timestamp))
group by 1,2,3,4,5,6,7
order by 1"""

