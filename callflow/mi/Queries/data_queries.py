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
when  MS_IP != '' and MS_IP is not null and left(MS_IP,3) != '10.' then 'Voice' end as Voice_Data_Flag
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
and left(MS_IP,3) = '10.' """



smartcare_Data_QUERIES = {
    'smartcare_SGIDiameter_nodes':smartcare_nodes_SGIDiameter,
	'smartcare_S1mme_nodes':smartcare_nodes_S1mme,
    'smartcare_GBIUPS_nodes':smartcare_nodes_GBIUPS,
	'SCF_CCI_U_T_Data':SCF_CCI_U_T_Data,
}
