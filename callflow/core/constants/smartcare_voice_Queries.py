
    
smartcare_DRA_Voice="""SELECT
    from_unixtime(
        floor(unix_timestamp(from_unixtime(A.CALC_TIME)) / 300) * 300,
        'yyyy/MM/dd HH:mm'
    ) AS myday,

    A.IMSI,
    A.MSISDN,

    CASE
        WHEN A.APN LIKE 'IMS' THEN 'VOLTE'
        ELSE 'Data'
    END AS Service_flag,

    F.NE_NAME AS DRA_NAME,

    COALESCE(J.NE_NAME, K.NE_NAME, G.NE_NAME) AS OCS_NAME,

    COALESCE(H.NE_NAME, K1.NE_NAME, G1.NE_NAME) AS PCRF_NAME,

    CASE
        WHEN A.RAT = 0 THEN 'Unknown'
        WHEN A.RAT = 1 THEN 'UTRAN'
        WHEN A.RAT = 2 THEN 'GERAN'
        WHEN A.RAT = 3 THEN 'WLAN'
        WHEN A.RAT = 4 THEN 'GAN'
        WHEN A.RAT = 5 THEN 'HSPA Evolution'
        WHEN A.RAT = 6 THEN 'EUTRAN'
        WHEN A.RAT = 9 THEN 'NR'
        ELSE 'Unknown'
    END AS RAT

FROM ps.detail_cdr_sgidiameter_{tablename} A

/* DRA */
LEFT JOIN sdr_nethouse.cfg_DRA_NE_IP F
    ON A.DRA_ID = F.Global_NE_ID

/* OCS */
LEFT JOIN sdr_nethouse.cfg_ocs J
    ON A.VPCRF_OCS_ID = J.Global_ID

LEFT JOIN (
    SELECT
        a.GLOBAL_ID,
        a.NE_NAME,
        b.NE_IP
    FROM nethouse.cfg_ocs a
    LEFT JOIN nethouse.cfg_ocs_ip b
        ON a.GLOBAL_ID = b.GLOBAL_ID
) K
    ON K.NE_IP = A.AAA_CLIENT_IP

LEFT JOIN (
    SELECT
        a.GLOBAL_ID,
        a.NE_NAME,
        b.NE_IP
    FROM nethouse.cfg_ocs a
    LEFT JOIN nethouse.cfg_ocs_ip b
        ON a.GLOBAL_ID = b.GLOBAL_ID
) G
    ON G.NE_IP = A.AAA_SERVER_IP

/* PCRF */
LEFT JOIN sdr_nethouse.cfg_pcrf H
    ON A.VPCRF_OCS_ID = H.Global_ID

LEFT JOIN (
    SELECT
        a.GLOBAL_ID,
        a.NE_NAME,
        b.NE_IP
    FROM nethouse.cfg_pcrf a
    LEFT JOIN nethouse.cfg_pcrf_ip b
        ON a.GLOBAL_ID = b.GLOBAL_ID
) K1
    ON K1.NE_IP = A.AAA_CLIENT_IP

LEFT JOIN (
    SELECT
        a.GLOBAL_ID,
        a.NE_NAME,
        b.NE_IP
    FROM nethouse.cfg_pcrf a
    LEFT JOIN nethouse.cfg_pcrf_ip b
        ON a.GLOBAL_ID = b.GLOBAL_ID
) G1
    ON G1.NE_IP = A.AAA_SERVER_IP

WHERE A.CALC_TIME BETWEEN unix_timestamp('{start_date}')
                      AND unix_timestamp('{end_date}')
  AND A.MSISDN IN {msisdn_tuple}
  AND (
        A.AAA_CLIENT_IP IS NOT NULL
        OR A.AAA_SERVER_IP IS NOT NULL
        OR A.DRA_ID IS NOT NULL
        OR A.VPCRF_OCS_ID IS NOT NULL
      )
  AND A.MS_IP NOT LIKE '%fc%'"""

smartcare_SGS_Voice ="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(CALC_TIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,MSISDN,
E.MME_Name,
M.MSC_NAME,
UNIFIED_CAUSE,
sum(case when TRANS_TYPE = 3 and TRANS_SUCCED_FLAG is not null then 1 else 0 end)as SGS_LU_REQ,
sum(case when TRANS_TYPE = 3 and TRANS_SUCCED_FLAG = 0 then 1 else 0 end) as SGS_LU_SUCC,
sum(case when TRANS_TYPE = 3 and TRANS_SUCCED_FLAG > 0 then 1 else 0 end) as SGS_LU_FAIL
FROM ps.DETAIL_CDR_SGS_{tablename} A 
LEFT JOIN sdr_nethouse.dim_loc_mme E ON A.MME_ID = E.MME_ID
LEFT JOIN nethouse.dim_loc_msc M on a.MSC_ID = M.MSC_ID
left join  nethouse.DIM_FAILCAUSE_ALL b on 
(CASE WHEN (FIRST_FAIL_PROTOCOL IS NOT NULL AND FIRST_FAIL_TRANS IS NOT NULL AND PD IS NOT NULL AND TRANS_CAUSE IS NOT NULL) THEN concat(FIRST_FAIL_PROTOCOL,'-',FIRST_FAIL_TRANS,'-',PD,'-',TRANS_CAUSE) END)
=concat(b.PROTOCOL_ID,'-',b.FIRFAILMSG_ID,'-',b.PD_ID,'-',b.CAUSE_ID)
WHERE msisdn IN {msisdn_tuple} AND
CALC_TIME BETWEEN unix_timestamp('{start_date}') 
AND unix_timestamp('{end_date}')
group by 1,2,3,4,5"""


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
C.LAYER2NAME AS REGION,
UNIFIED_CAUSE,
SUM(CASE WHEN SRVTYPE = 0 THEN 1 ELSE 0 END) AS CALL_ATTEMPTS,
SUM(CASE WHEN SRVTYPE = 0 AND (ALERT_TIME IS NOT NULL OR ANSWER_TIME IS NOT NULL OR SRVSTAT = 2) THEN 1 ELSE 0 END) AS SUCCESSFUL_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND ALERT_TIME IS NULL AND ANSWER_TIME IS NULL AND SRVSTAT <> 2 THEN 1 ELSE 0 END) AS FAILED_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND ANSWER_TIME IS NOT NULL THEN 1 ELSE 0 END) AS ANSWERED_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND CLEAR_REQ_TIME IS NOT NULL AND ANSWER_TIME IS NOT NULL AND ((REL_TIME IS NULL AND DISCONN_TIME IS NULL AND RELCMP_TIME IS NULL) OR (DISCONN_TIME IS NOT NULL AND REL_TIME IS NOT NULL AND A.PD = 3 AND CLEAR_REQ_TIME < REL_TIME) OR (DISCONN_TIME IS NOT NULL AND RELCMP_TIME IS NOT NULL AND A.PD = 10 AND CLEAR_REQ_TIME < RELCMP_TIME)) THEN 1 ELSE 0 END) AS DROPPED_CALLS_AFTER_ANS
FROM CS.CDR_AIU_MOC_{tablename} A 
LEFT JOIN NETHOUSE.DIM_FAILCAUSE_ALL E ON (CASE WHEN A.E2EFIRFAILPROT IS NOT NULL AND A.E2EFIRFAILPD IS NOT NULL AND A.E2EFIRFAILMSG IS NOT NULL AND A.E2EFIRFAILCAUSE IS NOT NULL 
THEN CONCAT_WS('-',A.E2EFIRFAILPROT,A.E2EFIRFAILPD,A.E2EFIRFAILMSG,A.E2EFIRFAILCAUSE) END) =
CONCAT_WS('-',PROTOCOL_ID,PD_ID,FIRFAILMSG_ID,CAUSE_ID)
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
C.LAYER2NAME,
UNIFIED_CAUSE
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
C.LAYER2NAME AS REGION,
UNIFIED_CAUSE,
SUM(CASE WHEN SRVTYPE = 0 THEN 1 ELSE 0 END) AS CALL_ATTEMPTS,
SUM(CASE WHEN SRVTYPE = 0 AND (ALERT_TIME IS NOT NULL OR ANSWER_TIME IS NOT NULL OR SRVSTAT = 2) THEN 1 ELSE 0 END) AS SUCCESSFUL_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND ALERT_TIME IS NULL AND ANSWER_TIME IS NULL AND SRVSTAT <> 2 THEN 1 ELSE 0 END) AS FAILED_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND ANSWER_TIME IS NOT NULL THEN 1 ELSE 0 END) AS ANSWERED_CALLS,
SUM(CASE WHEN SRVTYPE = 0 AND CLEAR_REQ_TIME IS NOT NULL AND ANSWER_TIME IS NOT NULL AND ((REL_TIME IS NULL AND DISCONN_TIME IS NULL AND RELCMP_TIME IS NULL) OR (DISCONN_TIME IS NOT NULL AND REL_TIME IS NOT NULL AND A.PD = 3 AND CLEAR_REQ_TIME < REL_TIME) OR (DISCONN_TIME IS NOT NULL AND RELCMP_TIME IS NOT NULL AND A.PD = 10 AND CLEAR_REQ_TIME < RELCMP_TIME)) THEN 1 ELSE 0 END) AS DROPPED_CALLS_AFTER_ANS
FROM CS.CDR_AIU_MTC_{tablename} A 
LEFT JOIN NETHOUSE.DIM_FAILCAUSE_ALL E ON (CASE WHEN A.E2EFIRFAILPROT IS NOT NULL AND A.E2EFIRFAILPD IS NOT NULL AND A.E2EFIRFAILMSG IS NOT NULL AND A.E2EFIRFAILCAUSE IS NOT NULL 
THEN CONCAT_WS('-',A.E2EFIRFAILPROT,A.E2EFIRFAILPD,A.E2EFIRFAILMSG,A.E2EFIRFAILCAUSE) END) =
CONCAT_WS('-',PROTOCOL_ID,PD_ID,FIRFAILMSG_ID,CAUSE_ID)
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
C.LAYER2NAME,
UNIFIED_CAUSE
ORDER BY A.calledno, myday;"""

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
B.LAYER2NAME AS REGION,
UNIFIED_CAUSE,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) then 1 else 0 end) as VoLTE_REQ_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43)
and ALERTING_TIME is null and ANSWER_TIME is null then 1 else 0 end) as VoLTE_FAIL_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) 
and ANSWER_TIME is not null then 1 else 0 end) as VoLTE_ANS_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) 
and ANSWER_TIME is not null and ABORT_FLAG = 0 then 1 else 0 end) as VoLTE_DROP_AFTER_ANS_TIMES
FROM CS.CDR_IMS_MO_CALL_LEG_SIP_{tablename} A
left join  nethouse.dim_failcause_all e on 
(CASE WHEN ( (FIRST_FAIL_PROT) IS NULL OR (FIRST_FAIL_MESSAGE) IS NULL OR (FIRST_FAIL_PD) IS NULL OR (FIRST_FAIL_CAUSE) IS NULL ) THEN '' ELSE concat(FIRST_FAIL_PROT,'-',FIRST_FAIL_MESSAGE,'-',FIRST_FAIL_PD,'-',FIRST_FAIL_CAUSE) END)
=concat(PROTOCOL_ID,'-',FIRFAILMSG_ID,'-',PD_ID,'-',CAUSE_ID)
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

smartcare_network_VoLTE_CFR_CSR_MT_CALL_voice="""SELECT
from_unixtime(
floor(unix_timestamp(from_unixtime(STARTTIME)) / 300) * 300
, 'yyyy/MM/dd HH:mm') AS myday,
A.IMPU_TEL_URI AS MSISDN,
D.NE_TYPE_NAME AS NODE,
B.CELL_NAME,
B.SITE_NAME, 
B.LAYER4NAME AS CLUSTER,
B.LAYER3NAME AS SUBREGION,
B.LAYER2NAME AS REGION,
UNIFIED_CAUSE,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) then 1 else 0 end) as VoLTE_REQ_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43)
and ALERTING_TIME is null and ANSWER_TIME is null then 1 else 0 end) as VoLTE_FAIL_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) 
and ANSWER_TIME is not null then 1 else 0 end) as VoLTE_ANS_TIMES,
sum(case when P_CSCF_ID is not null and SERVICE_TYPE in (0,1,51) and ACCESS_TYPE in (1,2,43) 
and ANSWER_TIME is not null and ABORT_FLAG = 0 then 1 else 0 end) as VoLTE_DROP_AFTER_ANS_TIMES
FROM CS.CDR_IMS_MT_CALL_LEG_SIP_{tablename} A
left join  nethouse.dim_failcause_all e on 
(CASE WHEN ( (FIRST_FAIL_PROT) IS NULL OR (FIRST_FAIL_MESSAGE) IS NULL OR (FIRST_FAIL_PD) IS NULL OR (FIRST_FAIL_CAUSE) IS NULL ) THEN '' ELSE concat(FIRST_FAIL_PROT,'-',FIRST_FAIL_MESSAGE,'-',FIRST_FAIL_PD,'-',FIRST_FAIL_CAUSE) END)
=concat(PROTOCOL_ID,'-',FIRFAILMSG_ID,'-',PD_ID,'-',CAUSE_ID)
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
B.LAYER2NAME,
UNIFIED_CAUSE
ORDER BY A.IMPU_TEL_URI, myday;"""

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
 
smartcare_nodes_Voice = {
    'smartcare_DRA_Voice':smartcare_DRA_Voice,
	'smartcare_SGS_Voice':smartcare_SGS_Voice,
    'smartcare_HSS_Voice':smartcare_HSS_Voice,
    'smartcare_network_CS_MOC':smartcare_network_CS_MOC,
    'smartcare_network_CS_MTC':smartcare_network_CS_MTC,
    'smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL':smartcare_network_voice_VoLTE_CFR_CSR_MO_CALL,
    'smartcare_network_VoLTE_CFR_CSR_MT_CALL_voice':smartcare_network_VoLTE_CFR_CSR_MT_CALL_voice,
    'smartcare_nodes_REG_Voice':smartcare_nodes_REG_Voice,
}
