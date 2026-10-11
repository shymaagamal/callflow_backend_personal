# All SQL queries (smartcare_nodes_*, etc.)# -----------------------------------
#       Portal
# ----------------------
smartcare_nodes_GBIUPS ="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
IMEI,
C.TER_BRAND_NAME as IMEI_Brand,
C.TER_MODEL_NAME as IMEI_Model,
sai_cgi_ecgi AS CELL,
B.cell_name,
a.SGSN_ID,
E.SGSN_MME_NAME,
'3G' AS RAT,
MS_IP
FROM ps.detail_cdr_gbiups_{tablename} A
LEFT JOIN sdr_nethouse.dim_loc_cgisai B ON A.sai_cgi_ecgi = B.CGISAI
LEFT JOIN sdr_nethouse.dim_loc_sgsn_mme E ON A.SGSN_ID = E.SGSN_MME_ID
LEFT JOIN nethouse.dim_terminal C ON SUBSTR(A.IMEI, 1, 8) = C.TAC AND A.IMEI IS NOT NULL
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR IMSI IS NOT NULL OR MSISDN IS NOT NULL)"""

smartcare_timeOut_Excep=""""SET spark.sql.broadcastTimeout=3600"""

smartcare_nodes_S1mme="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
IMEI,
C.TER_BRAND_NAME as IMEI_Brand,
C.TER_MODEL_NAME as IMEI_Model,
sai_cgi_ecgi AS CELL,
--B.cell_name,
a.MME_ID,
E.MME_Name,
SGW_DATA_IP,
L.NE_NAME as SGW_SGSN_NAME,
'4G' AS RAT,
MS_IP,
CAST(
conv(
substr(
substr(cast(rai_tai AS STRING), 5),
length(substr(cast(rai_tai AS STRING), 5)) - 3,
4
),
16,
10
) AS BIGINT
) AS TAC
FROM ps.DETAIL_CDR_S1MME_{tablename} A
LEFT JOIN(SELECT DISTINCT CGISAI, CELL_NAME, CONV(TAC, 16, 10) AS TAC_DECIMAL, SITE_NAME, LAYER4NAME, LAYER3NAME, LAYER2NAME FROM SDR_NETHOUSE.DIM_LOC_CGISAI) C ON A.sai_cgi_ecgi = C.CGISAI 
LEFT JOIN sdr_nethouse.dim_loc_cgisai B ON A.sai_cgi_ecgi = B.CGISAI
LEFT JOIN nethouse.dim_terminal C ON SUBSTR(A.IMEI, 1, 8) = C.TAC AND A.IMEI IS NOT NULL
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR a.MME_ID IS NOT NULL OR SGW_DATA_IP IS NOT NULL)"""


smartcare_nodes_S6a="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
IMEI,
C.TER_BRAND_NAME as IMEI_Brand,
C.TER_MODEL_NAME as IMEI_Model,
a.HSS_ID,
a.MME_ID,
K.NE_NAME as HSS_Name,
L.MME_NAME
FROM ps.DETAIL_CDR_S6a_{tablename} A
LEFT JOIN nethouse.dim_terminal C ON SUBSTR(A.IMEI, 1, 8) = C.TAC AND A.IMEI IS NOT NULL
left join NETHOUSE.DIM_LOC_MME L on a.MME_ID = L.MME_ID
left join nethouse.dim_loc_hss k on a.HSS_ID = k.NE_ID
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (IMSI IS NOT NULL OR MSISDN IS NOT NULL)"""

smartcare_nodes_SGIDiameter ="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
IMSI,
MSISDN,
a.AAA_CLIENT_IP,  
a.AAA_SERVER_IP,
a.DRA_ID,
a.VPCRF_OCS_ID,
COALESCE (H.NE_NAME,K1.NE_NAME, G1.NE_NAME) as VPCRF_NAME,
COALESCE (J.NE_NAME,K.NE_NAME, G.NE_NAME) as OCS_NAME,
F.NE_NAME as DRA_NAME,
CASE
WHEN RAT = 0 THEN 'Unknown'
WHEN RAT = 1 THEN 'UTRAN'
WHEN RAT = 2 THEN 'GERAN'
WHEN RAT = 3 THEN 'WLAN'
WHEN RAT = 4 THEN 'GAN'
WHEN RAT = 5 THEN 'HSPA Evolution'
WHEN RAT = 6 THEN 'EUTRAN'
WHEN RAT = 9 THEN 'NR'
ELSE 'Unknown'
END as RAT
FROM ps.detail_cdr_sgidiameter_{tablename} A

LEFT JOIN sdr_nethouse.cfg_DRA_NE_IP F ON A.DRA_ID = F.Global_NE_ID
LEFT JOIN sdr_nethouse.cfg_pcrf H ON A.VPCRF_OCS_ID = H.Global_ID
LEFT JOIN sdr_nethouse.cfg_ocs J ON A.VPCRF_OCS_ID = J.Global_ID

LEFT Join (
	select a.GLOBAL_ID, NE_NAME, NE_IP 
	from nethouse.cfg_ocs a
	left join nethouse.cfg_ocs_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) K
	on K.NE_IP = a.AAA_CLIENT_IP
	
LEFT Join (
	select a.GLOBAL_ID, NE_NAME, NE_IP 
	from nethouse.cfg_ocs a
	left join nethouse.cfg_ocs_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) G
	on G.NE_IP = a.AAA_SERVER_IP

LEFT Join (
	select a.GLOBAL_ID, NE_NAME, NE_IP 
	from nethouse.cfg_pcrf a
	left join nethouse.cfg_pcrf_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) K1
	on K1.NE_IP = a.AAA_CLIENT_IP
	
LEFT Join (
	select a.GLOBAL_ID, NE_NAME, NE_IP 
	from nethouse.cfg_pcrf a
	left join nethouse.cfg_pcrf_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) G1
	on G1.NE_IP = a.AAA_SERVER_IP

WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR a.DRA_ID IS NOT NULL OR a.VPCRF_OCS_ID IS NOT NULL)
AND ms_ip NOT LIKE '%fc%'"""

smartcare_nodes_DNS_Internet="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(end_time)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
a.SGSN_ID,
E.SGSN_SGW_NAME,
GGSN_ID,
GGSN_PGW_NAME,
SITE_NAME,
case When SERVER_IP in ('62.240.110.198','62.240.110.197','8.8.8.8','8.8.4.4') then SERVER_IP else 'Other' end as DNS_Server_IP,
case When SERVER_IP in ('62.240.110.197') then 'Vodafone_DNS.197'
When SERVER_IP in ('62.240.110.198') then 'Vodafone_DNS.198'
when SERVER_IP in ('8.8.8.8','8.8.4.4') then 'Google_DNS' else 'Other' end as DNS_Server_Name,
sum(case when DNS_SOURCE_TYPE = 1 then DNS_TRANS_NUM else 0 end) as MS_DNS_REQ_TIMES
from ps.DETAIL_UFDR_DNS_{tablename} a 
LEFT JOIN sdr_nethouse.dim_loc_sgsn_sgw E ON A.SGSN_ID = E.SGSN_SGW_ID
left join nethouse.dim_loc_ggsn_pgw c ON a.GGSN_ID = c.GGSN_PGW_ID
left join nethouse.DIM_LOC_CGISAI d ON a.LAST_SAI_CGI_ECGI = d.CGISAI
WHERE 
end_time BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
group by myday,a.SGSN_ID,SGSN_SGW_NAME,GGSN_ID,GGSN_PGW_NAME,LAST_SAI_CGI_ECGI,SITE_NAME,DNS_Server_IP,DNS_Server_Name;"""

smartcare_nodes_s1mme_4G_cell_only="""select
	from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
	MSISDN,
	'4G' AS RAT,
	c.cell_name, 
	c.site_name,
	c.layer2name as region,
	c.layer3name as subregion,
	c.layer4name as cluster,
	CAST(
		conv(
			substr(
				substr(cast(rai_tai AS STRING), 5),
				length(substr(cast(rai_tai AS STRING), 5)) - 3,
				4
			),
		16,
		10
		) AS BIGINT
		) AS TAC
FROM ps.DETAIL_CDR_S1MME_{tablename} A
LEFT JOIN(SELECT DISTINCT CGISAI, CELL_NAME, CONV(TAC, 16, 10) AS TAC_DECIMAL, SITE_NAME, LAYER4NAME, LAYER3NAME, LAYER2NAME FROM SDR_NETHOUSE.DIM_LOC_CGISAI) C ON A.sai_cgi_ecgi = C.CGISAI 
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}"""

smartcare_nodes_s1mme_4G_device_info="""SELECT 
	from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
	MSISDN,
	IMEI,
	C.TER_BRAND_NAME as IMEI_Brand,
	C.TER_MODEL_NAME as IMEI_Model
FROM ps.DETAIL_CDR_S1MME_{tablename} A
LEFT JOIN nethouse.dim_terminal C ON SUBSTR(A.IMEI, 1, 8) = C.TAC AND A.IMEI IS NOT NULL
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}"""

smartcare_nodes_s1mme_4G_core_nodes = """SELECT 
from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
a.MME_ID,
E.MME_Name,
SGW_DATA_IP,
L.NE_NAME as SGW_SGSN_NAME
FROM ps.DETAIL_CDR_S1MME_{tablename} A
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
where MSISDN in {msisdn_tuple}"""

smartcare_nodes_s1mme_4G_ms_ip = """SELECT 
from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
MS_IP
FROM ps.DETAIL_CDR_S1MME_{tablename} A
where MSISDN in {msisdn_tuple}"""

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


smartcare_nodes_dns ="""select FROM_UNIXTIME (begin_time, 'yyyy-MM-dd HH') as time,
  regexp_extract(DNS_DOMAIN, '([^.]+\.[^.]+)$', 1) as domain,
  COUNT (1) as nu_f_hits
from
  ps.DETAIL_UFDR_DNS_{tablename}
WHERE  begin_time BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}') AND msisdn IN {msisdn_tuple} 
GROUP by 1, 2"""
# ------------------------------------------------------
smartcare_nodes_QUERIES_data ={
    'smartcare_nodes_GBIUPS':smartcare_nodes_GBIUPS,
    'smartcare_nodes_S6a':smartcare_nodes_S6a,
    'smartcare_nodes_DNS_Internet':smartcare_nodes_DNS_Internet,

    'smartcare_nodes_S1mme':smartcare_nodes_S1mme,
    'smartcare_nodes_SGIDiameter':smartcare_nodes_SGIDiameter,

    # 'SCF_4G_attach_and_auth':SCF_4G_attach_and_auth,
    # 'SCF_S1mme_default_Bearer':SCF_S1mme_default_Bearer,
	# 'SCF_CCI_U_T': SCF_CCI_U_T,
    # 'SCF_DNS_Internet':SCF_DNS_Internet,
    # 'SCF_EPC_DNS':SCF_EPC_DNS,
    # 'SCF_S6a_LU':SCF_S6a_LU,

	'smartcare_nodes_dns':smartcare_nodes_dns,
    
	# "smartcare_nodes_s1mme_4G_cell_only":smartcare_nodes_s1mme_4G_cell_only,
    # "smartcare_nodes_s1mme_4G_device_info":smartcare_nodes_s1mme_4G_device_info,
    # "smartcare_nodes_s1mme_4G_core_nodes":smartcare_nodes_s1mme_4G_core_nodes,
    # "smartcare_nodes_s1mme_4G_ms_ip":smartcare_nodes_s1mme_4G_ms_ip
}
smartcare_rate_QUERIES_data ={
    'SCF_4G_attach_and_auth':SCF_4G_attach_and_auth,
    'SCF_S1mme_default_Bearer':SCF_S1mme_default_Bearer,
	'SCF_CCI_U_T': SCF_CCI_U_T,
    'SCF_DNS_Internet':SCF_DNS_Internet,
    'SCF_EPC_DNS':SCF_EPC_DNS,
    'SCF_S6a_LU':SCF_S6a_LU,
}



# ______________________________________________________________________ #

# __________________ Voice Queries _____________________________________
smartcare_Initial_REG_Voice ="""select
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
IMPU_TEL_URI1,
D.NE_NAME as p_cscf_Name,
E.NE_NAME as I_cscf_Name,
G.NE_NAME as S_cscf_Name,
I.NE_NAME as AS_NAME
from cs.tdr_ims_register_sip_comm_{tablename} a
LEFT JOIN 
nethouse.dim_dyn_ims_ne d 
ON a.p_cscf_id = d.GLOBAL_NE_ID
LEFT JOIN 
nethouse.dim_dyn_ims_ne E
ON a.i_cscf_id = E.GLOBAL_NE_ID
LEFT JOIN 
nethouse.dim_dyn_ims_ne G
ON a.s_cscf_id = G.GLOBAL_NE_ID
LEFT JOIN 
nethouse.dim_dyn_ims_ne H
ON a.hss_id = H.GLOBAL_NE_ID
LEFT JOIN 
nethouse.dim_dyn_ims_ne I
ON a.as_id = I.GLOBAL_NE_ID

Where IMPU_TEL_URI1 IN {msisdn_tuple}
AND STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
Group BY
myday,
IMPU_TEL_URI1,
p_cscf_Name,
I_cscf_Name,
S_cscf_Name,
AS_NAME"""

smartcare_VoLTE_CS_CDR_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.callerno,
D.MSC_NAME AS MSC_NODE
FROM CS.CDR_AIU_MOC_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID
Where callerno IN {msisdn_tuple}
AND STARTTIME BETWEEN unix_timestamp('{start_date}')
AND unix_timestamp('{end_date}')
GROUP BY
myday,
A.callerno,
D.MSC_NAME
ORDER BY A.callerno, myday;"""
 
smartcare_VoLTE_CS_CDR_2_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.calledno,
D.MSC_NAME AS MSC_NODE
FROM CS.CDR_AIU_MTC_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID
Where calledno IN {msisdn_tuple}
AND STARTTIME BETWEEN unix_timestamp('{start_date}')
AND unix_timestamp('{end_date}')
GROUP BY
myday,
A.calledno,
D.MSC_NAME
ORDER BY A.calledno, myday;"""

smartcare_SGS_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,MSISDN,
E.MME_Name,
M.MSC_NAME
FROM ps.DETAIL_CDR_SGS_{tablename} A 
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN nethouse.dim_loc_msc M on a.MSC_ID = M.MSC_ID
WHERE msisdn IN {msisdn_tuple}
 AND
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')"""

smartcare_HSS_Voice = """SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
IMPU_TEL_URI,
dest_host
from cs.TDR_IMS_HSS_TRANS_{tablename} a
Where IMPU_TEL_URI IN {msisdn_tuple} AND
STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND (INTERFACE=50 or INTERFACE=52)
GROUP BY
myday,
IMPU_TEL_URI,
dest_host"""

smartcare_cluster_Voice = """with all_data as(
SELECT
from_unixtime(starttime, 'yyyy/MM/dd HH:mm') AS myday,
IMSI,
msisdn,
case when ERAB_MODIFY_INDICATION_SUCC_TIMES > 0 then '5G' else '4G' end AS RAT,
sai_cgi_ecgi as Cell
FROM SDR_NETHOUSE.BKPI_SIGNAL_S1MME_CISAI_15MIN_{tablename}  --change this day
UNION ALL
SELECT
STARTTIME,
imsi,
msisdn,
CASE
WHEN rat = 1 THEN '2G'
WHEN rat = 2 THEN '3G'
ELSE 'Unknown'
END AS RAT,
sai_cgi_ecgi AS Cell
FROM SDR_NETHOUSE.BKPI_SIGNAL_GBIUPS_CISAI_15MIN_{tablename} 
UNION ALL  
SELECT
STARTTIME,
IMSI,
msisdn,
CASE
WHEN ACCESS_TYPE = 0 THEN '2G'
WHEN ACCESS_TYPE = 1 THEN '3G'
ELSE 'Unknown'
END AS RAT,
Cell_id AS Cell
FROM SDR_NETHOUSE.DW_BDS_NW_CS_AIUMM_BKPI_15MIN_{tablename} 
)
select 
from_unixtime(a.STARTTIME, 'yyyy/MM/dd HH:mm') AS myday,
a.IMSI,
a.msisdn,
a.RAT,
c.cell_name, 
c.site_name,
c.layer2name as region,
c.layer3name as subregion,
c.layer4name as cluster
from all_data A
LEFT JOIN(SELECT DISTINCT CGISAI, CELL_NAME, SITE_NAME, LAYER4NAME, LAYER3NAME, LAYER2NAME FROM SDR_NETHOUSE.DIM_LOC_CGISAI) C ON A.Cell = C.CGISAI 
WHERE 
starttime BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}"""
   
    
smartcare_nodes_Voice = {
    "smartcare_Initial_REG_Voice": smartcare_Initial_REG_Voice,
    "smartcare_VoLTE_CS_CDR_Voice": smartcare_VoLTE_CS_CDR_Voice,
    "smartcare_VoLTE_CS_CDR_2_Voice": smartcare_VoLTE_CS_CDR_2_Voice,
    "smartcare_SGS_Voice": smartcare_SGS_Voice,
    "smartcare_HSS_Voice": smartcare_HSS_Voice,
    "smartcare_cluster_Voice": smartcare_cluster_Voice
}
