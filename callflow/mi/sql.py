"""
Smartcare (Spark SQL over JDBC) query text used by MI.

Moved VERBATIM from callflow/prototype/constants.py (lines 42-1023) so the SQL
sent to Smartcare is byte-for-byte what production ran before. Credentials,
mediation/Splunk/OSS constants from that file were NOT moved: MI does not use
them, and the Smartcare credentials now come from the environment (config.py).

Only the names referenced in catalog.py are executed. Note that
smartcare_nodes_REG_Voice is defined twice below (identical text); the query
dicts bind the first definition, exactly as before.
"""
# flake8: noqa

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

smartcare_nodes_Diameter_1="""select
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

smartcare_nodes_Diameter_2="""SELECT 
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

smartcare_nodes_Diameter_3="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
	MSISDN,
	a.MME_ID,
	E.MME_Name,
	SGW_DATA_IP,
	L.NE_NAME as SGW_SGSN_NAME
	FROM ps.DETAIL_CDR_S1MME_{tablename} A
	LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
	LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP"""

smartcare_nodes_Diameter_4="""
SELECT 
from_unixtime(floor(unix_timestamp(from_unixtime(PROC_STARTTIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
MSISDN,
a.MME_ID,
E.MME_Name,
SGW_DATA_IP,
L.NE_NAME as SGW_SGSN_NAME
FROM ps.DETAIL_CDR_S1MME_{tablename} A
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
"""

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


smartcare_network_voice_Initial_REG="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
a.IMPU_TEL_URI1,
B.cell_name,
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION,
D.NE_NAME as p_cscf_Name,
E.NE_NAME as I_cscf_Name,
G.NE_NAME as S_cscf_Name,
I.NE_NAME as AS_NAME
FROM cs.tdr_ims_register_sip_comm_{tablename} a
-- YOU NEED TO ADD THIS JOIN - adjust based on your actual schema
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI B ON A.ACCESS_INFO = B.CGISAI
LEFT JOIN nethouse.dim_dyn_ims_ne d 
ON a.p_cscf_id = d.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne E
ON a.i_cscf_id = E.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne G
ON a.s_cscf_id = G.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne H
ON a.hss_id = H.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne I
ON a.as_id = I.GLOBAL_NE_ID
LEFT JOIN nethouse.dim_dyn_ims_ne K
ON a.sgw_id = K.GLOBAL_NE_ID
-- Remove or fix this line - it has no ON clause
-- LEFT JOIN nethouse.cfg_ims_ne_ip F
WHERE IMPU_TEL_URI1 IN {msisdn_tuple}
AND STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
GROUP BY
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm'),
a.IMPU_TEL_URI1,
B.cell_name,
B.LAYER4NAME,
B.LAYER3NAME,
B.LAYER2NAME,
D.NE_NAME,
E.NE_NAME,
G.NE_NAME,
I.NE_NAME"""

smartcare_nodes_REG_Voice ="""SELECT
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
LEFT JOIN 
nethouse.dim_dyn_ims_ne K
ON a.sgw_id = K.GLOBAL_NE_ID
LEFT JOIN 
nethouse.cfg_ims_ne_ip F
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

smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.IMPU_TEL_URI AS MSISDN,
D.NE_TYPE_NAME AS NODE,
B.CELL_NAME,
B.SITE_NAME, 
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION
FROM CS.CDR_IMS_MO_CALL_LEG_SIP_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI B ON A.ACCESS_INFO = B.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_IMS_NE D ON A.S_CSCF_ID = D.GLOBAL_NE_ID OR A.P_CSCF_ID = D.GLOBAL_NE_ID
WHERE IMPU_TEL_URI IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}'))
GROUP BY
myday, 
A.IMPU_TEL_URI, 
D.NE_TYPE_NAME,
B.CELL_NAME, 
B.SITE_NAME, 
B.LAYER4NAME, 
B.LAYER3NAME, 
B.LAYER2NAME
ORDER BY A.IMPU_TEL_URI, myday;"""

smartcare_network_VoLTE_CFR_CSR_MT_CALLvoice="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.IMPU_TEL_URI AS MSISDN,
D.NE_TYPE_NAME AS NODE,
B.CELL_NAME,
B.SITE_NAME, 
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION
FROM CS.CDR_IMS_MT_CALL_LEG_SIP_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI B ON A.ACCESS_INFO = B.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_IMS_NE D ON A.S_CSCF_ID = D.GLOBAL_NE_ID OR A.P_CSCF_ID = D.GLOBAL_NE_ID
WHERE IMPU_TEL_URI IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}'))
GROUP BY
myday, 
A.IMPU_TEL_URI, 
D.NE_TYPE_NAME,
B.CELL_NAME, 
B.SITE_NAME, 
B.LAYER4NAME, 
B.LAYER3NAME, 
B.LAYER2NAME
ORDER BY A.IMPU_TEL_URI, myday;"""

smartcare_network_CS_MOC="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.callerno,
D.MSC_NAME AS MSC_NODE,
C.CELL_NAME,
C.SITE_NAME, 
C.LAYER4NAME AS CLUSTER,
C.LAYER3NAME AS SUBREGION,
C.LAYER2NAME AS REGION
FROM CS.CDR_AIU_MOC_{tablename} A 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI C ON CONCAT('60202',A.LASTLAC,A.LASTCI) = C.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID 
Where callerno IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}'))
GROUP BY
myday, 
A.callerno,
D.MSC_NAME,
C.CELL_NAME, 
C.SITE_NAME, 
C.LAYER4NAME, 
C.LAYER3NAME, 
C.LAYER2NAME
ORDER BY A.callerno, myday;"""

smartcare_network_CS_MTC="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.calledno,
D.MSC_NAME AS MSC_NODE,
C.CELL_NAME,
C.SITE_NAME, 
C.LAYER4NAME AS CLUSTER,
C.LAYER3NAME AS SUBREGION,
C.LAYER2NAME AS REGION
FROM CS.CDR_AIU_MTC_{tablename} A 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI C ON CONCAT('60202',A.LASTLAC,A.LASTCI) = C.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID
Where calledno IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}'))
GROUP BY
myday, 
A.calledno,
D.MSC_NAME,
C.CELL_NAME, 
C.SITE_NAME, 
C.LAYER4NAME, 
C.LAYER3NAME, 
C.LAYER2NAME
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

smartcare_PCRF_Voice = """SELECT
from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
IMSI,
MSISDN,
case when apn like 'IMS' then 'VOLTE' else 'Data' end as Service_flag,
COALESCE (H.NE_NAME,K1.NE_NAME, G1.NE_NAME) as VPCRF_NAME,
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
LEFT JOIN sdr_nethouse.cfg_pcrf H ON A.VPCRF_OCS_ID = H.Global_ID
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
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR a.VPCRF_OCS_ID IS NOT NULL)
AND ms_ip NOT LIKE '%fc%'"""

smartcare_OCS_Voice = """SELECT from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
IMSI,
MSISDN,
case when apn like 'IMS' then 'VOLTE' else 'Data' end as Service_flag,
COALESCE (J.NE_NAME,K.NE_NAME, G.NE_NAME) as OCS_NAME,
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
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR a.VPCRF_OCS_ID IS NOT NULL)
AND ms_ip NOT LIKE '%fc%'"""

smartcare_DRA_Voice="""SELECT from_unixtime(floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300, 'yyyy/MM/dd HH:mm') AS myday,
IMSI,
MSISDN,
case when apn like 'IMS' then 'VOLTE' else 'Data' end as Service_flag,
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
WHERE 
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (AAA_CLIENT_IP IS NOT NULL OR AAA_SERVER_IP IS NOT NULL OR a.DRA_ID IS NOT NULL OR a.VPCRF_OCS_ID IS NOT NULL)
AND ms_ip NOT LIKE '%fc%'"""

smartcare_cells_for_voice="""with all_data as(
    SELECT
				from_unixtime(starttime, 'yyyy/MM/dd HH:mm') AS myday,
				IMSI,
				msisdn,
				case when ERAB_MODIFY_INDICATION_SUCC_TIMES > 0 then '5G' else '4G' end AS RAT,
				sai_cgi_ecgi as Cell
			FROM SDR_NETHOUSE.BKPI_SIGNAL_S1MME_CISAI_15MIN_{tablename}
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
) select 
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
			AND msisdn IN {msisdn_tuple}
"""


smartcare_main_QUERIES ={
    'smartcare_nodes_GBIUPS':smartcare_nodes_GBIUPS,
    'smartcare_nodes_S1mme':smartcare_nodes_S1mme,
    'smartcare_nodes_SGIDiameter':smartcare_nodes_SGIDiameter,
    'smartcare_network_voice_Initial_REG':smartcare_network_voice_Initial_REG,
    'smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL':smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL,
    'smartcare_network_VoLTE_CFR_CSR_MT_CALLvoice':smartcare_network_VoLTE_CFR_CSR_MT_CALLvoice,
    'smartcare_network_CS_MOC':smartcare_network_CS_MOC,
    'smartcare_network_CS_MTC':smartcare_network_CS_MTC,
    'smartcare_cells_for_voice':smartcare_cells_for_voice
}

smartcare_Voice_QUERIES = {
    # 'smartcare_network_voice_Initial_REG':smartcare_network_voice_Initial_REG,
    

	'smartcare_DRA_Voice':smartcare_DRA_Voice,
    'smartcare_OCS_Voice':smartcare_OCS_Voice,
    'smartcare_PCRF_Voice':smartcare_PCRF_Voice,
    'smartcare_network_CS_MOC':smartcare_network_CS_MOC,
    'smartcare_network_CS_MTC':smartcare_network_CS_MTC,
	'smartcare_nodes_REG_Voice':smartcare_nodes_REG_Voice,
    'smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL':smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL,
    'smartcare_network_VoLTE_CFR_CSR_MT_CALLvoice':smartcare_network_VoLTE_CFR_CSR_MT_CALLvoice,
	'smartcare_SGS_Voice':smartcare_SGS_Voice,
    'smartcare_HSS_Voice':smartcare_HSS_Voice,
    
    # 'smartcare_PCRF_Voice':smartcare_nodes_SGIDiameter,
    # 'smartcare_cells_for_voice':smartcare_cells_for_voice
}
smartcare_Data_QUERIES = {
    # 'smartcare_SGIDiameter_nodes':smartcare_nodes_SGIDiameter,
   	
	'smartcare_S1mme_nodes':smartcare_nodes_S1mme,
    
	# 'smartcare_DRA_Voice':smartcare_DRA_Voice,
    # 'smartcare_OCS_Voice':smartcare_OCS_Voice,
    # 'smartcare_PCRF_Voice':smartcare_PCRF_Voice,
    
	'smartcare_nodes_REG_Voice':smartcare_nodes_REG_Voice,
    'smartcare_GBIUPS_nodes':smartcare_nodes_GBIUPS,
	'SCF_CCI_U_T_Data':SCF_CCI_U_T_Data,
}


# --------------------------------------------------
#               Queries of Voice and data seperated
# --------------------------------------------------



# __________________________ Data Queries ______________________ #
smartcare_nodes_GBIUPS_Data ="""SELECT
IMSI,
MSISDN,
sai_cgi_ecgi AS CELL,
B.cell_name,
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

smartcare_nodes_S1mme_Data = """SELECT
IMSI,
MSISDN,
sai_cgi_ecgi AS CELL,
B.cell_name,
a.MME_ID,
E.MME_Name,
SGW_DATA_IP,
L.NE_NAME as SGW_SGSN_NAME,
PROC_STARTTIME,
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
LEFT JOIN sdr_nethouse.dim_loc_cgisai B ON A.sai_cgi_ecgi = B.CGISAI
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN sdr_nethouse.ex_cfg_sgw_ip L ON A.SGW_DATA_IP = L.NE_IP
WHERE 
PROC_STARTTIME BETWEEN unix_timestamp('{start_date}')
AND unix_timestamp('{end_date}')
AND msisdn IN {msisdn_tuple}
AND (sai_cgi_ecgi IS NOT NULL OR a.MME_ID IS NOT NULL OR SGW_DATA_IP IS NOT NULL)"""

smartcare_nodes_SGIDiameter_Data = """SELECT
IMSI,
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

smartcare_nodes_Data = {
    "smartcare_nodes_GBIUPS_Data": smartcare_nodes_GBIUPS_Data,
    "smartcare_nodes_SGIDiameter_Data": smartcare_nodes_SGIDiameter_Data,
     "smartcare_nodes_S1mme_Data": smartcare_nodes_S1mme_Data,
}
# ______________________________________________________________________ #

# __________________ Voice Queries _____________________________________
smartcare_nodes_REG_Voice ="""SELECT
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
LEFT JOIN 
nethouse.dim_dyn_ims_ne K
ON a.sgw_id = K.GLOBAL_NE_ID
LEFT JOIN 
nethouse.cfg_ims_ne_ip F
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

smartcare_VoLTE_CFR_CSR_MSISDN_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.IMPU_TEL_URI AS MSISDN,
D.NE_TYPE_NAME AS NODE,
B.CELL_NAME,
B.SITE_NAME, 
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION
FROM CS.CDR_IMS_MO_CALL_LEG_SIP_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI B ON A.ACCESS_INFO = B.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_IMS_NE D ON A.S_CSCF_ID = D.GLOBAL_NE_ID OR A.P_CSCF_ID = D.GLOBAL_NE_ID
Where IMPU_TEL_URI IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
GROUP BY
myday, 
A.IMPU_TEL_URI, 
D.NE_TYPE_NAME,
B.CELL_NAME, 
B.SITE_NAME, 
B.LAYER4NAME, 
B.LAYER3NAME, 
B.LAYER2NAME
ORDER BY A.IMPU_TEL_URI, myday;"""

smartcare_VoLTE_CFR_CSR_IMSI_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.IMPI_TEL_URI AS IMSI,
D.NE_TYPE_NAME AS NODE,
B.CELL_NAME,
B.SITE_NAME, 
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION
FROM CS.CDR_IMS_MO_CALL_LEG_SIP_{tablename} A
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI B ON A.ACCESS_INFO = B.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_IMS_NE D ON A.S_CSCF_ID = D.GLOBAL_NE_ID OR A.P_CSCF_ID = D.GLOBAL_NE_ID
Where IMPU_TEL_URI IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
GROUP BY
myday, 
A.IMPI_TEL_URI, 
D.NE_TYPE_NAME,
B.CELL_NAME, 
B.SITE_NAME, 
B.LAYER4NAME, 
B.LAYER3NAME, 
B.LAYER2NAME
ORDER BY A.IMPI_TEL_URI, myday;"""

smartcare_CS_MOC_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.callerno,
D.MSC_NAME AS MSC_NODE,
C.CELL_NAME,
C.SITE_NAME, 
C.LAYER4NAME AS CLUSTER,
C.LAYER3NAME AS SUBREGION,
C.LAYER2NAME AS REGION
FROM CS.CDR_AIU_MOC_{tablename} A 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI C ON CONCAT('60202',A.LASTLAC,A.LASTCI) = C.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID 
Where callerno IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
GROUP BY
myday, 
A.callerno,
D.MSC_NAME,
C.CELL_NAME, 
C.SITE_NAME, 
C.LAYER4NAME, 
C.LAYER3NAME, 
C.LAYER2NAME
ORDER BY A.callerno, myday;"""

smartcare_CS_MTC_Voice = """SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.calledno,
D.MSC_NAME AS MSC_NODE,
C.CELL_NAME,
C.SITE_NAME, 
C.LAYER4NAME AS CLUSTER,
C.LAYER3NAME AS SUBREGION,
C.LAYER2NAME AS REGION
FROM CS.CDR_AIU_MTC_{tablename} A 
LEFT JOIN SDR_NETHOUSE.DIM_LOC_CGISAI C ON CONCAT('60202',A.LASTLAC,A.LASTCI) = C.CGISAI
LEFT JOIN SDR_NETHOUSE.DIM_DYN_MSC_IP D ON A.MSC_ID = D.MSC_ID
Where calledno IN {msisdn_tuple}
AND (STARTTIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
GROUP BY
myday, 
A.calledno,
D.MSC_NAME,
C.CELL_NAME, 
C.SITE_NAME, 
C.LAYER4NAME, 
C.LAYER3NAME, 
C.LAYER2NAME
ORDER BY A.calledno, myday;"""

smartcare_nodes_Voice = {
    "smartcare_nodes_REG_Voice": smartcare_nodes_REG_Voice,
    "smartcare_VoLTE_CFR_CSR_MSISDN_Voice": smartcare_VoLTE_CFR_CSR_MSISDN_Voice,
    "smartcare_VoLTE_CFR_CSR_IMSI_Voice": smartcare_VoLTE_CFR_CSR_IMSI_Voice,
    "smartcare_CS_MOC_Voice": smartcare_CS_MOC_Voice,
    "smartcare_CS_MTC_Voice": smartcare_CS_MTC_Voice,
}



