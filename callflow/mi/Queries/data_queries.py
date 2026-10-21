smartcare_nodes_SGIDiameter="""SELECT 
MSISDN,
a.DRA_ID,
a.VPCRF_OCS_ID,
COALESCE (H.NE_NAME,K.NE_NAME, G.NE_NAME) as VPCRF_NAME,
COALESCE (J.NE_NAME,K.NE_NAME, G.NE_NAME) as OCS_NAME,
F.NE_NAME as DRA_NAME,
CALC_TIME AS PROC_STARTTIME,
case when apn like 'IMS' then 'VoLTE' else 'Data' end as Service_flag,
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
on K.NE_IP = a.AAA_SERVER_IP

LEFT Join (
select a.GLOBAL_ID, NE_NAME, NE_IP 
from nethouse.cfg_pcrf a
left join nethouse.cfg_pcrf_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) K1
on K.NE_IP = a.AAA_CLIENT_IP

LEFT Join (
select a.GLOBAL_ID, NE_NAME, NE_IP 
from nethouse.cfg_pcrf a
left join nethouse.cfg_pcrf_ip b on a.GLOBAL_ID = b.GLOBAL_ID ) G1
on K.NE_IP = a.AAA_SERVER_IP

WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple} AND (IMSI IS NOT NULL OR MSISDN IS NOT NULL)
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR a.DRA_ID IS NOT NULL OR a.VPCRF_OCS_ID IS NOT NULL)
AND TRIM(APN) NOT LIKE '%IMS%'"""

smartcare_nodes_S1mme="""SELECT
MSISDN,
c.TAC_DECIMAL,
C.cell_name,
C.LAYER4NAME AS CLUSTER,
C.LAYER3NAME AS SUBREGION,
C.LAYER2NAME AS REGION,
a.MME_ID,
E.MME_Name,
SGW_DATA_IP,
L.NE_NAME as SGW_SGSN_NAME,
PROC_STARTTIME,
'4G' AS RAT,
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
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR a.MME_ID IS NOT NULL OR SGW_DATA_IP IS NOT NULL)"""



smartcare_nodes_GBIUPS="""SELECT
MSISDN,
sai_cgi_ecgi AS CELL,
B.cell_name,
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION,
a.SGSN_ID,
K.SGW_NAME as SGW_SGSN_NAME,
PROC_STARTTIME,
'3G' AS RAT,
MS_IP
FROM ps.detail_cdr_gbiups_{tablename} A
LEFT JOIN sdr_nethouse.dim_loc_cgisai B ON A.sai_cgi_ecgi = B.CGISAI
LEFT JOIN sdr_nethouse.dim_loc_sgw K ON A.SGSN_ID = K.SGW_ID
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR IMSI IS NOT NULL OR MSISDN IS NOT NULL)"""


SCF_CCI_U_T_Data ="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
	MSISDN,
    a.AAA_CLIENT_IP,  
	a.AAA_SERVER_IP,
	a.DRA_ID,
	a.VPCRF_OCS_ID,
	a.GGSN_PGW_ID,
	H.NE_NAME as VPCRF_NAME,
	J.NE_NAME as OCS_NAME,
	F.NE_NAME as DRA_NAME,
	K.GGSN_PGW_NAME,

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

	SUM(CASE when TRANS_TYPE=15 and TRANS_SUCCED_FLAG is not null and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Initial_Req,
	SUM(CASE when TRANS_TYPE=16 and TRANS_SUCCED_FLAG is not null and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Update_Req,

	SUM(CASE when TRANS_TYPE=15 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Initial_Succ_times,
	SUM(CASE when TRANS_TYPE=16 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_Update_Succ_times,
	SUM(CASE when TRANS_TYPE=17 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end) as Credit_control_terminate_Succ_times,


	ROUND(100*((SUM(CASE when TRANS_TYPE=15 and PROTOCOL_ID = 8 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))/(SUM(CASE when TRANS_TYPE=15 and PROTOCOL_ID = 8 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))),2) as Credit_control_Initial_SR,

	ROUND(100*((SUM(CASE when TRANS_TYPE=16 and PROTOCOL_ID = 8 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))/(SUM(CASE when TRANS_TYPE=16 and PROTOCOL_ID = 8 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))),2) as Credit_control_update_SR,

	ROUND(100*((SUM(CASE when TRANS_TYPE=17 and PROTOCOL_ID = 8 and (TRANS_SUCCED_FLAG=0 or ( TRANS_SUCCED_FLAG=1 and trans_cause = 2001)) and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))/(SUM(CASE when TRANS_TYPE=17 and PROTOCOL_ID = 8 and (INTERFACEID=12 OR INTERFACEID=11) then 1 else 0 end))),2) as Credit_control_terminate_SR,

	Round (AVG (case when INTERFACEID = 11 and PROTOCOL_ID = 8 and TRANS_TYPE = 15 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as Gx_CCI_Delay,
	Round (AVG (case when INTERFACEID = 12 and PROTOCOL_ID = 8 and TRANS_TYPE = 15 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as GY_CCI_Delay,
	Round (AVG (case when INTERFACEID = 11 and PROTOCOL_ID = 8 and TRANS_TYPE = 16 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as Gx_CCU_Delay,
	Round (AVG (case when INTERFACEID = 12 and PROTOCOL_ID = 8 and TRANS_TYPE = 16 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as GY_CCU_Delay,
	Round (AVG (case when INTERFACEID = 11 and PROTOCOL_ID = 8 and TRANS_TYPE = 17 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as Gx_CCT_Delay,
	Round (AVG (case when INTERFACEID = 12 and PROTOCOL_ID = 8 and TRANS_TYPE = 17 and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as GY_CCT_Delay,
	Round (AVG (case when INTERFACEID = 11 and PROTOCOL_ID = 8  and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as Gx_Delay,
	Round (AVG (case when INTERFACEID = 12 and PROTOCOL_ID = 8  and TRANS_SUCCED_FLAG=0 then TRANS_RSP_TIME_SEC - TRANS_REQ_TIME_SEC else NULL END),2) as GY_Delay

	FROM ps.DETAIL_CDR_SGiDiameter_{tablename} A
	LEFT JOIN sdr_nethouse.cfg_DRA_NE_IP F ON A.DRA_ID = F.Global_NE_ID
	LEFT JOIN sdr_nethouse.cfg_pcrf H ON A.VPCRF_OCS_ID = H.Global_ID
	LEFT JOIN sdr_nethouse.cfg_ocs J ON A.VPCRF_OCS_ID = J.Global_ID
	left join nethouse.dim_loc_ggsn_pgw K ON a.GGSN_PGW_ID = K.GGSN_PGW_ID
	WHERE 
	CALC_TIME BETWEEN unix_timestamp('{start_date}') 
	AND unix_timestamp('{end_date}')
	AND msisdn IN {msisdn_tuple}
	AND 
	(a.DRA_ID IN (6400001)
	OR a.VPCRF_OCS_ID IN (400028,
	'400027',
	'400023',
	'400021',
	'500030',
	'500020'))
	and imsi like '60202%'
	and left(MS_IP,3) = '10.'
	GROUP BY
	myday,
    MSISDN,
	a.AAA_CLIENT_IP,  
	a.AAA_SERVER_IP,
	a.DRA_ID,
	a.VPCRF_OCS_ID,
	H.NE_NAME,
	J.NE_NAME,
	F.NE_NAME,
	a.GGSN_PGW_ID,
	K.GGSN_PGW_NAME,
	Voice_Data_Flag,
	ACCT_TERMINATE_CAUSE_DESC,
	ACCT_UPDATE_CAUSE_DESC;"""


smartcare_Data_QUERIES = {
    'smartcare_SGIDiameter_nodes':smartcare_nodes_SGIDiameter,
	'smartcare_S1mme_nodes':smartcare_nodes_S1mme,
    'smartcare_GBIUPS_nodes':smartcare_nodes_GBIUPS,
	'SCF_CCI_U_T_Data':SCF_CCI_U_T_Data,
}
