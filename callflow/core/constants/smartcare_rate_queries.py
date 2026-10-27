# ------ Rate KPIs Part ----------------------------
SCF_4G_attach_and_auth = """WITH cdr_data AS (
SELECT from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
a.MSISDN,
a.MME_ID,
a.SGW_DATA_IP,
a.EMM_CAUSE,
D.MME_NAME,
E.NE_NAME as SGW_NAME,
C.CAUSE,
sum(case when EMM_TRANS_TYPE = 48 and EMM_REQ_SUCCED_FLAG = 1 AND a.EMM_CAUSE NOT IN (1000, 997,998) then 1 else 0 end) as S1_MME_ATTACH_FAILURE_TIMES,
sum(case when AUTH_REQ_TIME_SEC is not null and AUTH_SUCCED_FLAG > 0 AND a.EMM_CAUSE NOT IN (1000, 997,998) then 1 else 0 end) as S1MME_AUTHENTICATION_Failure_TIMES,
sum(case when PROC_TYPE = 103 AND PROC_SUCCED_FLAG = 1 AND a.EMM_CAUSE NOT IN (1000, 997,998) THEN 1 ELSE 0 END) AS TAU_Failure_TIMES

FROM ps.detail_cdr_s1mme_{tablename} a
LEFT JOIN (
SELECT CAUSE_ID, MAX(CAUSE) AS CAUSE  
FROM SDR_NETHOUSE.DIM_FAILCAUSE_ALL  
WHERE PROTOCOL_ID = 232 
GROUP BY CAUSE_ID
) C ON A.EMM_CAUSE = C.CAUSE_ID
LEFT JOIN SDR_NETHOUSE.DIM_LOC_MME D ON A.MME_ID = D.MME_ID
LEFT JOIN SDR_NETHOUSE.EX_CFG_SGW_IP E ON A.SGW_DATA_IP = E.NE_IP
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND MSISDN IN {msisdn_tuple}
AND IMSI LIKE '60202%'
GROUP BY 
from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm'),
a.MSISDN,
a.MME_ID,
a.SGW_DATA_IP,
a.EMM_CAUSE,
D.MME_NAME,
E.NE_NAME,
C.CAUSE
)
SELECT 
myday,
MSISDN,
MME_ID,
SGW_DATA_IP,
EMM_CAUSE,
MME_NAME,
SGW_NAME,
CAUSE,
S1_MME_ATTACH_FAILURE_TIMES,
S1MME_AUTHENTICATION_Failure_TIMES,
TAU_Failure_TIMES
FROM cdr_data
ORDER BY myday, MME_ID;"""

SCF_S1mme_default_Bearer="""select FROM_UNIXTIME(CALC_TIME,'yyyy/MM/dd HH:mm') AS mytime,MME_NAME,PGW_NAME,UNIFIED_FAILCAUSE_NAME,
sum(case when INTERFACEID = 5 and TRANS_TYPE = 0 and TRANS_SUCCED_FLAG = 1 then 1 else 0 end) as DEF_BEARER_FAIL,
sum(case when INTERFACEID = 5 and TRANS_TYPE = 3 and TRANS_SUCCED_FLAG = 1 then 1 else 0 end) as DED_BEARER_FAIL

FROM PS.DETAIL_CDR_GTPV2_{tablename} a 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_MME b ON A.MME_ID = b.MME_ID
LEFT JOIN SDR_NETHOUSE.dim_loc_pgw c ON A.PGW_ID = c.PGW_ID
left join  nethouse.DIM_PSSAE_SIG_TRANS_CAUSE_NEW d on 
(CASE WHEN (PROTOCOL_ID) IS NOT NULL AND (TRANS_TYPE) IS NOT NULL AND (TRANS_CAUSE) IS NOT NULL 
THEN concat(PROTOCOL_ID,'-',TRANS_TYPE,'-255-',TRANS_CAUSE) END)
= concat(PROBE_PROTOCOL_ID,'-',PROBE_TRANS_ID,'-',FAIL_TYPE_ID,'-',FAIL_CAUSE_ID)
WHERE CALC_TIME BETWEEN unix_timestamp('{start_date}') AND unix_timestamp('{end_date}')
group by 1,2,3,4;"""

S11_default_Bearer="""select FROM_UNIXTIME(CALC_TIME,'yyyy/MM/dd HH:mm') AS mytime,MME_NAME,PGW_NAME,
sum(case when INTERFACEID = 5 and TRANS_TYPE = 0 and TRANS_SUCCED_FLAG is not null then 1 else 0 end) as DEF_BEARER_REQ,
sum(case when INTERFACEID = 5 and TRANS_TYPE = 0 and TRANS_SUCCED_FLAG = 0 then 1 else 0 end) as DEF_BEARER_SUCC,

sum(case when INTERFACEID = 5 and TRANS_TYPE = 3 and TRANS_SUCCED_FLAG is not null then 1 else 0 end) as DED_BEARER_REQ,
sum(case when INTERFACEID = 5 and TRANS_TYPE = 3 and TRANS_SUCCED_FLAG = 0 then 1 else 0 end) as DED_BEARER_SUCC

FROM PS.DETAIL_CDR_GTPV2_{tablename} a 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_MME b ON A.MME_ID = b.MME_ID
LEFT JOIN SDR_NETHOUSE.dim_loc_pgw c ON A.PGW_ID = c.PGW_ID
WHERE CALC_TIME BETWEEN unix_timestamp('{start_date}') AND unix_timestamp('{end_date}')
group by 1,2,3;"""


SCF_CCI_U_T ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
a.MSISDN,
a.charging_3gpp,
a.AAA_CLIENT_IP,  
a.AAA_SERVER_IP,
a.DRA_ID,
a.VPCRF_OCS_ID,
a.GGSN_PGW_ID,
H.NE_NAME as VPCRF_NAME,
J.NE_NAME as OCS_NAME,
F.NE_NAME as DRA_NAME,
K.GGSN_PGW_NAME,
b.UNIFIED_FAILCAUSE_NAME,
case when left(MS_IP,3) = '10.' then 'Data'
when  MS_IP != '' and MS_IP is not null and left(MS_IP,3) != '10.' then 'Voice' end as Voice_Data_Flag,

CASE WHEN ACCT_TERMINATE_CAUSE = 1 THEN 'DIAMETER_LOGOUT'
WHEN ACCT_TERMINATE_CAUSE = 2 THEN 'DIAMETER_SERVICE_NOT_PROVIDED'
WHEN ACCT_TERMINATE_CAUSE = 3 THEN 'DIAMETER_BAD_ANSWER'
WHEN ACCT_TERMINATE_CAUSE = 4 THEN 'DIAMETER_ADMINISTRATIVE'
WHEN ACCT_TERMINATE_CAUSE = 5 THEN 'DIAMETER_LINK_BROKEN'
WHEN ACCT_TERMINATE_CAUSE = 6 THEN 'DIAMETER_AUTH_EXPIRED'
WHEN ACCT_TERMINATE_CAUSE = 7 THEN 'DIAMETER_USER_MOVED'
WHEN ACCT_TERMINATE_CAUSE = 8 THEN 'DIAMETER_SESSION_TIMEOUT'
ELSE ACCT_TERMINATE_CAUSE
END AS ACCT_TERMINATE_CAUSE_DESC,

CASE WHEN ACCT_UPDATE_CAUSE = 0 THEN 'THRESHOLD'
WHEN ACCT_UPDATE_CAUSE = 1 THEN 'QHT'
WHEN ACCT_UPDATE_CAUSE = 2 THEN 'FINAL'
WHEN ACCT_UPDATE_CAUSE = 3 THEN 'QUOTA_EXHAUSTED'
WHEN ACCT_UPDATE_CAUSE = 4 THEN 'VALIDITY_TIME'
WHEN ACCT_UPDATE_CAUSE = 5 THEN 'OTHER_QUOTA_TYPE'
WHEN ACCT_UPDATE_CAUSE = 6 THEN 'RATING_CONDITION_CHANGE'
WHEN ACCT_UPDATE_CAUSE = 7 THEN 'FORCED_REAUTHORISATION'
WHEN ACCT_UPDATE_CAUSE = 8 THEN 'POOL_EXHAUSTED'
ELSE ACCT_UPDATE_CAUSE
END AS ACCT_UPDATE_CAUSE_DESC,

SUM(CASE when TRANS_TYPE=15 and TRANS_SUCCED_FLAG=1 and A.trans_cause <> 2001 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Initial_fail_times,
SUM(CASE when TRANS_TYPE=16 and TRANS_SUCCED_FLAG=1 and A.trans_cause <> 2001 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Update_fail_times,
SUM(CASE when TRANS_TYPE=17 and TRANS_SUCCED_FLAG=1 and A.trans_cause <> 2001 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_terminate_fail_times

FROM ps.DETAIL_CDR_SGiDiameter_{tablename} A


left join  nethouse.DIM_PSSAE_SIG_TRANS_CAUSE_NEW b on 
(CASE WHEN PROTOCOL_ID IS NOT NULL AND TRANS_TYPE  IS NOT NULL AND TRANS_CAUSE IS NOT NULL AND TRANS_CAUSE_TYPE IS NOT NULL THEN CONCAT(PROTOCOL_ID, '-', TRANS_TYPE, '-', TRANS_CAUSE_TYPE, '-', TRANS_CAUSE) END)
= CONCAT(PROBE_PROTOCOL_ID, '-', PROBE_TRANS_ID, '-', FAIL_TYPE_ID, '-', FAIL_CAUSE_ID)


LEFT JOIN sdr_nethouse.cfg_DRA_NE_IP F ON A.DRA_ID = F.Global_NE_ID
LEFT JOIN sdr_nethouse.cfg_pcrf H ON A.VPCRF_OCS_ID = H.Global_ID
LEFT JOIN sdr_nethouse.cfg_ocs J ON A.VPCRF_OCS_ID = J.Global_ID
left join nethouse.dim_loc_ggsn_pgw K ON a.GGSN_PGW_ID = K.GGSN_PGW_ID
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND 
(a.DRA_ID IN (6400001)
OR a.VPCRF_OCS_ID IN (400028,
'400027',
'400023',
'400021',
'500030',
'500020'))
AND msisdn IN {msisdn_tuple}
and imsi like '60202%'
GROUP BY
myday,
charging_3gpp,
a.MSISDN,
a.AAA_CLIENT_IP,  
a.AAA_SERVER_IP,
a.DRA_ID,
a.VPCRF_OCS_ID,
H.NE_NAME,
J.NE_NAME,
F.NE_NAME,
a.GGSN_PGW_ID,
K.GGSN_PGW_NAME,
b.UNIFIED_FAILCAUSE_NAME,
Voice_Data_Flag,
ACCT_TERMINATE_CAUSE_DESC,
ACCT_UPDATE_CAUSE_DESC;"""

SCF_DNS_Internet="""select 
from_unixtime(end_time,'yyyy-MM-dd HH:00') as mytime,
GGSN_ID,
MSISDN,
GGSN_PGW_NAME,
ERROR_CODE_FST,
FAILCAUSE_EN,
case When SERVER_IP in ('62.240.110.198','62.240.110.197','8.8.8.8','8.8.4.4') then SERVER_IP else 'Other' end as DNS_Server_IP,
case When SERVER_IP in ('62.240.110.197') then 'Vodafone_DNS.197'
When SERVER_IP in ('62.240.110.198') then 'Vodafone_DNS.198'
when SERVER_IP in ('8.8.8.8','8.8.4.4') then 'Google_DNS'
else 'Other' 
end as DNS_Server_Name,
sum(case when DNS_SOURCE_TYPE = 1 then (DNS_TRANS_NUM-DNS_SUCCEED_NUM) else 0 end) as MS_DNS_FAIL_TIMES
from ps.DETAIL_UFDR_DNS_{tablename} a 
left join nethouse.dim_loc_sgsn_sgw b ON a.SGSN_id = b.SGSN_SGW_ID 
left join nethouse.dim_loc_ggsn_pgw c ON a.GGSN_ID = c.GGSN_PGW_ID
left join nethouse.DIM_FAILCAUSE_INFO_DNS d on a.ERROR_CODE_FST = d.FAILCAUSE_ID
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND SERVER_IP IN ('62.240.110.197','62.240.110.198')
AND APN like "INTERNET.VODAFONE.NET"
and imsi like '60202%'
AND MSISDN IN {msisdn_tuple}
group by 
mytime,
MSISDN,
SERVER_IP,
GGSN_ID,
GGSN_PGW_NAME,
ERROR_CODE_FST,
FAILCAUSE_EN,
case 
When SERVER_IP in ('62.240.110.197') then 'Vodafone_DNS.197'
When SERVER_IP in ('62.240.110.198') then 'Vodafone_DNS.198'
when SERVER_IP in ('8.8.8.8','8.8.4.4') then 'Google_DNS'
else 'Other' 
end;"""

SCF_EPC_DNS="""select 
from_unixtime(calc_time,'yyyy-MM-dd HH:mm') as mytime,
cause,
d.FAILCAUSE_EN,
ne_id,
ne_ip,
dns_server_ip,
sum(case when trans_succed_flag = 1 then 1 else 0 end) as DNS_FAIL_TIMES
from ps.detail_cdr_epc_dns_{tablename} a
left join nethouse.DIM_FAILCAUSE_INFO_DNS d on a.cause = d.FAILCAUSE_ID
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND a.dns_server_ip IN ('10.5.106.132','10.5.106.133')
group by 
from_unixtime(calc_time,'yyyy-MM-dd HH:mm'),
cause,
d.FAILCAUSE_EN,
ne_id,
ne_ip,
dns_server_ip;"""

SCF_S6a_LU="""select 
from_unixtime(CALC_TIME,'yyyy-MM-dd HH:mm') as mytime,
a.MME_ID,
a.MSISDN,
MME_NAME,
HSS_ID,
NE_NAME as HSS_Name,

UNIFIED_FAILCAUSE_NAME,

sum(case when TRANS_TYPE = 8 and TRANS_SUCCED_FLAG = 1 then 1 else 0 end) as S6A_UPDATE_LOC_FAIL_TIMES,
sum(case when TRANS_TYPE = 35 and TRANS_SUCCED_FLAG = 1 then 1 else 0 end) as CHECK_IMEI_FAIL

from ps.detail_cdr_s6a_{tablename} a 

left join  nethouse.DIM_PSSAE_SIG_TRANS_CAUSE_NEW b on
(CASE WHEN ((PROTOCOL_ID) IS NOT NULL AND (TRANS_TYPE) IS NOT NULL AND (TRANS_CAUSE_TYPE) IS NOT NULL AND (TRANS_CAUSE) IS NOT NULL) THEN concat(PROTOCOL_ID,'-',TRANS_TYPE,'-',TRANS_CAUSE_TYPE,'-',TRANS_CAUSE) END)= concat(PROBE_PROTOCOL_ID,'-',PROBE_TRANS_ID,'-',FAIL_TYPE_ID,'-',FAIL_CAUSE_ID)


left join NETHOUSE.DIM_LOC_MME c on a.MME_ID = c.MME_ID
left join nethouse.dim_loc_hss d on a.HSS_ID = d.NE_ID
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND MSISDN IN {msisdn_tuple}
AND IMSI like '60202%'
AND TRANS_CAUSE NOT IN (5004)
group by 
from_unixtime(CALC_TIME,'yyyy-MM-dd HH:mm'),
a.MME_ID,
a.MSISDN,
MME_NAME,
HSS_ID,
NE_NAME,
UNIFIED_FAILCAUSE_NAME;"""



smartcare_rate_QUERIES_data ={
    'SCF_4G_attach_and_auth':SCF_4G_attach_and_auth,
    'SCF_S1mme_default_Bearer':SCF_S1mme_default_Bearer,
	'SCF_CCI_U_T': SCF_CCI_U_T,
    'SCF_DNS_Internet':SCF_DNS_Internet,
    'SCF_EPC_DNS':SCF_EPC_DNS,
    'SCF_S6a_LU':SCF_S6a_LU,
}
