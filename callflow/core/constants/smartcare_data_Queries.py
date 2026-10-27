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

smartcare_nodes_dns ="""select FROM_UNIXTIME (begin_time, 'yyyy-MM-dd HH') as time,
  regexp_extract(DNS_DOMAIN, '([^.]+\.[^.]+)$', 1) as domain,
  COUNT (1) as nu_f_hits
from
  ps.DETAIL_UFDR_DNS_{tablename}
WHERE  begin_time BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}') AND msisdn IN {msisdn_tuple} 
GROUP by 1, 2"""


smartcare_nodes_QUERIES_data ={
    'smartcare_nodes_GBIUPS':smartcare_nodes_GBIUPS,
    'smartcare_nodes_S1mme':smartcare_nodes_S1mme,
    'smartcare_nodes_S6a':smartcare_nodes_S6a,
    'smartcare_nodes_DNS_Internet':smartcare_nodes_DNS_Internet,
    'smartcare_nodes_SGIDiameter':smartcare_nodes_SGIDiameter,
	'smartcare_nodes_dns':smartcare_nodes_dns,
	# "smartcare_nodes_s1mme_4G_cell_only":smartcare_nodes_s1mme_4G_cell_only,
    # "smartcare_nodes_s1mme_4G_device_info":smartcare_nodes_s1mme_4G_device_info,
    # "smartcare_nodes_s1mme_4G_core_nodes":smartcare_nodes_s1mme_4G_core_nodes,
    # "smartcare_nodes_s1mme_4G_ms_ip":smartcare_nodes_s1mme_4G_ms_ip
}