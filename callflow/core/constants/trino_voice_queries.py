# Trino versions of the CS and VoLTE Smartcare queries (from "CS from Trino.sql"
# and "VoLTE From Trino.sql"). Used by smartcare_service.smartcareNodesQueries
# instead of smartcare_network_CS_MOC / _CS_MTC / _VoLTE_..._MO_CALL / _MT_CALL
# when the request window is older than the latest 6 hours.
#
# Placeholders (filled by smartcare_service only):
#   {msisdn_tuple}                 ('1018988812','1012345678')
#   {start_time_t} {end_time_t}    local 'YYYY/MM/DD HH:MM' (time_t format)

# CS MO + MT (replaces smartcare_network_CS_MOC and smartcare_network_CS_MTC)
trino_network_CS_MOC_MTC = """WITH DIM AS (
    SELECT FORMAT('%s-%s-%s-%s',PROTOCOL_ID,FIRFAILMSG_ID,PD_ID,CAUSE_ID) AS CAUSE_ID,CAUSE
    FROM HIVE.SMARTCARE.dim_cs_failcause_raw
),
BASE AS (
    SELECT
        A.TIME_T,
        A.msisdn,
        accesstype,
        cell_id,
        'MO' AS DIR,
        CAUSE,
        SUM(TRY_CAST(A.CALL_ATTEMPTS AS INT)) AS CALL_ATTEMPTS,
        SUM(CASE
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES = '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) = 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES = '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) > 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) = 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ANSWER_TIMES AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) > 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT)
        ELSE 0 END) AS ALERTING_FAILURES,
        SUM(TRY_CAST(A.ANSWER_TIMES AS INT)) AS ANSWER_TIMES,
        SUM(CASE WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' THEN TRY_CAST(A.ANSWER_TIMES AS INT) ELSE 0 END) AS CALLDROP
    FROM HIVE.SMARTCARE.CS_MO_CALL_KPI A
    LEFT JOIN DIM D ON FORMAT('%s-%s-%s-%s',A.FAILURE_PROTOCOL_ID,A.FAILURE_MSG_ID,A.FAILURE_SUB_PROTOCOL_ID,A.FAILURE_CAUSE_ID) = D.CAUSE_ID
    GROUP BY 1, 2, 3, 4, 5, 6

    UNION ALL

    SELECT
        A.TIME_T,
        A.msisdn,
        accesstype,
        cell_id,
        'MT' AS DIR,
        CAUSE,
        SUM(TRY_CAST(A.CALL_ATTEMPTS AS INT)) AS CALL_ATTEMPTS,
        SUM(CASE
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES = '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) = 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES = '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) > 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) = 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ANSWER_TIMES AS INT)
            WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' AND TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT) > 0 THEN TRY_CAST(A.CALL_ATTEMPTS AS INT) - TRY_CAST(A.ALERTING_SUCCESSES AS INT)
        ELSE 0 END) AS ALERTING_FAILURES,
        SUM(TRY_CAST(A.ANSWER_TIMES AS INT)) AS ANSWER_TIMES,
        SUM(CASE WHEN D.CAUSE IS NOT NULL AND A.ANSWER_TIMES <> '0' THEN TRY_CAST(A.ANSWER_TIMES AS INT) ELSE 0 END) AS CALLDROP
    FROM HIVE.SMARTCARE.CS_MT_CALL_KPI A
    LEFT JOIN DIM D ON FORMAT('%s-%s-%s-%s',A.FAILURE_PROTOCOL_ID,A.FAILURE_MSG_ID,A.FAILURE_SUB_PROTOCOL_ID,A.FAILURE_CAUSE_ID) = D.CAUSE_ID
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    TIME_T,
    msisdn,
    accesstype,
    cell_id,
    B.CELL_NAME,
    B.SITE_NAME,
    B.LAYER4NAME AS CLUSTER,
    B.LAYER3NAME AS SUBREGION,
    B.LAYER2NAME AS REGION,
    CAUSE,
    COALESCE(SUM(CASE WHEN DIR = 'MO' THEN CALL_ATTEMPTS END), 0) AS MO_CALL_ATTEMPTS,
    COALESCE(SUM(CASE WHEN DIR = 'MO' THEN ALERTING_FAILURES END), 0) AS MO_ALERTING_FAILURES,
    COALESCE(SUM(CASE WHEN DIR = 'MO' THEN ANSWER_TIMES END), 0) AS MO_ANSWER_TIMES,
    COALESCE(SUM(CASE WHEN DIR = 'MO' THEN CALLDROP END), 0) AS MO_CALLDROP,
    COALESCE(SUM(CASE WHEN DIR = 'MT' THEN CALL_ATTEMPTS END), 0) AS MT_CALL_ATTEMPTS,
    COALESCE(SUM(CASE WHEN DIR = 'MT' THEN ALERTING_FAILURES END), 0) AS MT_ALERTING_FAILURES,
    COALESCE(SUM(CASE WHEN DIR = 'MT' THEN ANSWER_TIMES END), 0) AS MT_ANSWER_TIMES,
    COALESCE(SUM(CASE WHEN DIR = 'MT' THEN CALLDROP END), 0) AS MT_CALLDROP
FROM BASE a
left join (select CGISAI,CELL_NAME,SITE_NAME,LAYER4NAME,LAYER3NAME,LAYER2NAME from HIVE.SMARTCARE.dim_loc_cgisai group by 1,2,3,4,5,6) b
on a.cell_id = b.CGISAI
where msisdn IN {msisdn_tuple}
and time_t between '{start_time_t}'
and '{end_time_t}'
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1"""

# VoLTE call drop MO + MT (replaces the drop part of the two VoLTE Smartcare queries)
trino_network_VoLTE_CDR_MO_MT = """WITH ALL_CDR AS (
    Select
        time_t,
        msisdn,
        cell_id,
        sbc_id,
        FIRST_FAIL_MESSAGE,
        FIRST_FAIL_PD,
        first_fail_cause,
        first_fail_prot,
        sum(cast(voice_mo_req_times as bigint)) as volte_req_times,
        sum(cast(voice_mo_drop_times_afterconn as bigint)) as voice_drop_times
    from Hive.smartcare.volte_mo_calldrop_kpi a
    Group By time_t, msisdn, cell_id, sbc_id, FIRST_FAIL_MESSAGE, FIRST_FAIL_PD, first_fail_cause, first_fail_prot

    union ALL

    Select
        time_t,
        msisdn,
        cell_id,
        sbc_id,
        FIRST_FAIL_MESSAGE,
        FIRST_FAIL_PD,
        first_fail_cause,
        first_fail_prot,
        sum(cast(voice_mT_req_times as bigint)) as volte_req_times,
        sum(cast(voice_mT_drop_times_afterconn as bigint)) as voice_drop_times
    from Hive.smartcare.volte_mt_calldrop_kpi a
    Group By time_t, msisdn, cell_id, sbc_id, FIRST_FAIL_MESSAGE, FIRST_FAIL_PD, first_fail_cause, first_fail_prot
)
Select
    time_t,
    msisdn,
    ne_name,
    cell_id,
    B.CELL_NAME,
    B.SITE_NAME,
    B.LAYER4NAME AS CLUSTER,
    B.LAYER3NAME AS SUBREGION,
    B.LAYER2NAME AS REGION,
    Cause,
    sum( volte_req_times) as total_volte,
    Sum (case
        when (volte_req_times) > 0 AND  c.cause  NOT IN ('2 received bye message', '16 Normal Call Clearing', '1000 Timeout', '16 User Inactivity', '16 Request accepted', '3 Release due to E-UTRAN Generated Reason', '487 Request Terminated', '1 received cancel message')
        then cast(voice_drop_times as bigint) else 0 end ) as voice_drop_times_without_failures
from ALL_CDR A
left join (select CGISAI,CELL_NAME,SITE_NAME,LAYER4NAME,LAYER3NAME,LAYER2NAME from HIVE.SMARTCARE.dim_loc_cgisai group by 1,2,3,4,5,6) b
on a.cell_id = b.CGISAI
LEFT JOIN hive.smartcare.DIM_FAILCAUSE_ALL C
    ON cast(A.FIRST_FAIL_MESSAGE as bigint) = C.FIRFAILMSG_ID
    AND cast( A.FIRST_FAIL_PD as bigint) = C.PD_ID
    AND cast(A.first_fail_cause as bigint) = C.CAUSE_ID
    and cast(a.first_fail_prot as bigint) = c.protocol_id
left join Hive.smartcare.dim_dyn_ims_ne d on cast(a.sbc_id as bigint) = d.global_ne_id
where msisdn IN {msisdn_tuple}
and time_t between '{start_time_t}'
and '{end_time_t}'
group by 1,2,3,4,5,6,7,8,9,10
order by 1"""

# VoLTE call failure (CFR) MO + MT
trino_network_VoLTE_CFR_MO_MT = """WITH ALL_CFR AS (
    SELECT
        time_t, cell_id, sbc_id, MSISDN, FIRST_FAIL_MESSAGE, FIRST_FAIL_PD, first_fail_prot,
        first_fail_cause, voice_mo_req_times as voice_req_times , voice_mo_conn_succ_times as voice_conn_succ_times
    FROM Hive.smartcare.volte_mo_call_kpi

    UNION ALL

    SELECT
        time_t, cell_id, sbc_id, MSISDN, FIRST_FAIL_MESSAGE, FIRST_FAIL_PD, first_fail_prot,
        first_fail_cause, voice_mt_req_times as voice_req_times, voice_mt_conn_succ_times as voice_conn_succ_times
    FROM Hive.smartcare.volte_mT_call_kpi
)
SELECT
    time_t,
    msisdn,
    d.ne_name,
    cell_id,
    B.CELL_NAME,
    B.SITE_NAME,
    B.LAYER4NAME AS CLUSTER,
    B.LAYER3NAME AS SUBREGION,
    B.LAYER2NAME AS REGION,
    c.cause,
    SUM(cast (voice_req_times as BIGINT)) as Requests,
    Sum (case
        when cast (voice_req_times as BIGINT ) > 0 AND first_fail_cause BETWEEN 400 AND 699 and first_fail_cause  not in (486, 487)
        then (cast(voice_req_times as BIGINT) - cast( voice_conn_succ_times as BIGINT))
        else 0 end) as Call_Failures_count
FROM ALL_CFR A
left join (select CGISAI,CELL_NAME,SITE_NAME,LAYER4NAME,LAYER3NAME,LAYER2NAME from HIVE.SMARTCARE.dim_loc_cgisai group by 1,2,3,4,5,6) b
on a.cell_id = b.CGISAI
LEFT JOIN (
    select FIRFAILMSG_ID,PD_ID,CAUSE_ID,cause,protocol_id
    from hive.smartcare.DIM_FAILCAUSE_ALL
    group by FIRFAILMSG_ID,PD_ID,CAUSE_ID,cause,protocol_id
) c
ON A.FIRST_FAIL_MESSAGE = C.FIRFAILMSG_ID AND A.FIRST_FAIL_PD = C.PD_ID AND A.first_fail_cause = C.CAUSE_ID and a.first_fail_prot = c.protocol_id
left join Hive.smartcare.dim_dyn_ims_ne d on cast(a.sbc_id as bigint) = d.global_ne_id
where msisdn IN {msisdn_tuple}
and time_t between '{start_time_t}'
and '{end_time_t}'
group by 1,2,3,4,5,6,7,8,9,10
order by 1"""

trino_voice_QUERIES = {
    'trino_network_CS_MOC_MTC': trino_network_CS_MOC_MTC,
    'trino_network_VoLTE_CDR_MO_MT': trino_network_VoLTE_CDR_MO_MT,
    'trino_network_VoLTE_CFR_MO_MT': trino_network_VoLTE_CFR_MO_MT,
}
