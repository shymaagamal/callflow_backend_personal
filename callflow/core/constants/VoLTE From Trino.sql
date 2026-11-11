


--VOLTE CDR
--============
WITH ALL_CDR AS (

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
	from
		Hive.smartcare.volte_mo_calldrop_kpi a
		
	Group By
		time_t,
		msisdn,
		cell_id,
		sbc_id,
		FIRST_FAIL_MESSAGE,
		FIRST_FAIL_PD,
		first_fail_cause,
		first_fail_prot

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
	from
		Hive.smartcare.volte_mt_calldrop_kpi a
	Group By
		time_t,
		msisdn,
		cell_id,
		sbc_id,
		FIRST_FAIL_MESSAGE,
		FIRST_FAIL_PD,
		first_fail_cause,
		first_fail_prot
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
			
	from 
		ALL_CDR A
		
left join (select CGISAI,CELL_NAME,SITE_NAME,LAYER4NAME,LAYER3NAME,LAYER2NAME from HIVE.SMARTCARE.dim_loc_cgisai group by 1,2,3,4,5,6) b
on a.cell_id = b.CGISAI

	LEFT JOIN 
		hive.smartcare.DIM_FAILCAUSE_ALL C 
			ON cast(A.FIRST_FAIL_MESSAGE as bigint) = C.FIRFAILMSG_ID 
			AND cast( A.FIRST_FAIL_PD as bigint) = C.PD_ID 
			AND cast(A.first_fail_cause as bigint) = C.CAUSE_ID 
			and cast(a.first_fail_prot as bigint) = c.protocol_id
			
			left join Hive.smartcare.dim_dyn_ims_ne d on cast(a.sbc_id as bigint) = d.global_ne_id
			
			
where msisdn = '1018988812' 
and time_t between '2026/10/27 09:20'
and '2026/10/30 13:45'

		
	group by 
		1,2,3,4,5,6,7,8,9,10
		
		order by 1
		
		
		
		
--==============================================================================

--CFR
--==========


WITH ALL_CFR AS (
    -- MO Source
    SELECT 
        time_t, cell_id, sbc_id, MSISDN, FIRST_FAIL_MESSAGE, FIRST_FAIL_PD, first_fail_prot,
        first_fail_cause, voice_mo_req_times as voice_req_times , voice_mo_conn_succ_times as voice_conn_succ_times
    FROM Hive.smartcare.volte_mo_call_kpi

    UNION ALL

    -- MT Source
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


LEFT JOIN 
	(
	select FIRFAILMSG_ID,PD_ID,CAUSE_ID,cause,protocol_id 
	from hive.smartcare.DIM_FAILCAUSE_ALL 
	group by FIRFAILMSG_ID,PD_ID,CAUSE_ID,cause,protocol_id  
	)c 
ON A.FIRST_FAIL_MESSAGE = C.FIRFAILMSG_ID AND A.FIRST_FAIL_PD = C.PD_ID AND A.first_fail_cause = C.CAUSE_ID and a.first_fail_prot = c.protocol_id

			left join Hive.smartcare.dim_dyn_ims_ne d on cast(a.sbc_id as bigint) = d.global_ne_id
	
	
			
where msisdn = '1018988812' 
and time_t between '2026/10/27 09:20'
and '2026/10/30 13:45'

	
	
	group by 
		1,2,3,4,5,6,7,8,9,10
		
		order by 1
		





