# Splunk endpoints, mappings (VIAVIE_KPIS_MAPPING, etc.)




SPLUNK_ENDPOINTS = {
  
  #Huawei GGSNS
  #!
  "GGSN_3GPDPSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20TEST_API%20earliest=start_time%20latest=end_time",

  "GGSN_4GSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%204G_SR_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_CPU": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MAX_CPU_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_Throughput": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Throughput_API%20earliest=start_time%20latest=end_time",
  
  #Huawei GGSNs
  "Total_SGWbearerSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Total_SGW_bearer_SR_API%20earliest=start_time%20latest=end_time",
  #!
  "Total_PGWbearerSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Total_PGW_bearer_SR_API%20earliest=start_time%20latest=end_time",
  
  "Total_GGSNPDPSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Total_GGSN_PDP_SR_API%20earliest=start_time%20latest=end_time",
  

  #GGSN
  "GGSN_Throughput": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Throughput_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_PDPSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_PDP_SR_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_CPU": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_CPU_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_NPU" : "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_NPU_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_PKTLoss" : "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_PKT_Loss_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_DedicatedBearerSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Dedicated_Bearer_SR_API%20earliest=start_time%20latest=end_time",
  #!
  "GGSN_DefaultBearerSR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Default_Bearer_SR_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_TotalDrops": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Total_Drop_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_ChargingAuthFail": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Charging_Auth_Fail_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_IMSAuthFail": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_IMS_Auth_Fail_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_AAAAuthExceed": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_AAA_Auth_Exceed_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_PDNRejection": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_PDN_Rejection_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_NoResources": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_No_Resources_API%20earliest=start_time%20latest=end_time",
  
  "GGSN_AddressOccupied": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20GGSN_Address_Occupied_API%20earliest=start_time%20latest=end_time",

  "GGSN_RLFDrop": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20RLF_Drop_GGSN_API%20earliest=start_time%20latest=end_time",
  
  #DNS
  "DNS_CPU_HQ": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ_CPU_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_HQ": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_HQ": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ_QPS_API%20earliest=start_time%20latest=end_time",
  
  "DNS_CPU_HQ2": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ2_CPU_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_HQ2": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ2_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_HQ2": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_HQ2_QPS_API%20earliest=start_time%20latest=end_time",
  
  "DNS_CPU_TG": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_TG_CPU_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_TG": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_TG_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_TG": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_TG_QPS_API%20earliest=start_time%20latest=end_time",
  
  "DNS_CPU_RMD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_RMD_CPU_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_RMD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_RMD_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_RMD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_RMD_QPS_API%20earliest=start_time%20latest=end_time",
  
  "DNS_CPU_ZH": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_ZH_CPU_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_ZH": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_ZH_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_ZH": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_ZH_QPS_API%20earliest=start_time%20latest=end_time",
  
  "DNS_HitRatio_RMD2": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_RMD2_Hit_Ratio_API%20earliest=start_time%20latest=end_time",
  
  "DNS_QPS_RMD2": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DNS_RMD2_QPS_API%20earliest=start_time%20latest=end_time",
  


  #GIFW
  "GiFW_Throughput": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Gi-FW_Throughput_API%20earliest=start_time%20latest=end_time",
  
  "GiFW_CPU": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Gi-FW_Throughput_API%20earliest=start_time%20latest=end_time",
  

  #IMS
   #!
  "IMS_CSCF-CPULOAD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20CSCF_CPULOAD_API%20earliest=start_time%20latest=end_time",
   #!
  "IMS_MTAS-CPULOAD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MTAS_CPULOAD_API%20earliest=start_time%20latest=end_time",
   
  #MSC
  "MSC_IPSTP-CPLoad": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MSC_IPSTP_CP_Load_API%20earliest=start_time%20latest=end_time",

  "MSC_IPSTP-SignallingLoad": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MSC_IPSTP_Signalling_Load_API%20earliest=start_time%20latest=end_time",

  #MME
  "MME_Overload-Protection-Rejection-Newconnection": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MME_Overload_Protection_Rejection_New_connection_API%20earliest=start_time%20latest=end_time",

  "MME_Overload-Protection-Rejection-Existingconnection": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20MME_Overload_Protection_Rejection_existing_connection_API%20earliest=start_time%20latest=end_time",

  "MME_CPULOAD": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_MME_CPULOAD_API%20earliest=start_time%20latest=end_time",

  #SBG  

  "SBG_CDR" : "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20CDR_API%20earliest=start_time%20latest=end_time",

  "SBG_Initial-Reg-SR" : "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20Initial_Reg_SR_API%20earliest=start_time%20latest=end_time" ,

  "SBG-CPULOAD-MAX": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SBG_CPULOAD_MAX_API%20earliest=start_time%20latest=end_time",

  "SBG-CPULOAD-AVG": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SBG_CPULOAD_AVG_API%20earliest=start_time%20latest=end_time",

  #BGF  

  "BGF_PKTLoss" : "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20PKT_Loss_API%20earliest=start_time%20latest=end_time",
  
  # === SGSN KPIs (from boss's new list) ===
    "SGSN_Intra_SR_2G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_1%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Active_PDPs_2G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_2%20earliest=start_time%20latest=end_time",
    "SGSN_Attach_SR_2G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_3%20earliest=start_time%20latest=end_time",
    "SGSN_PDP_SR_2G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_4%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Attached_Customers_2G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_5%20earliest=start_time%20latest=end_time",
    "SGSN_PDP_SR_2G_NW": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_6%20earliest=start_time%20latest=end_time",
    "SGSN_Attach_SR_3G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_7%20earliest=start_time%20latest=end_time",
    "SGSN_RAB_SR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_8%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Active_PDPs_3G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_9%20earliest=start_time%20latest=end_time",
    "SGSN_PDP_SR_3G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_10%20earliest=start_time%20latest=end_time",
    "SGSN_Attach_SR_3G_NW": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_11%20earliest=start_time%20latest=end_time",
    "SGSN_PDP_SR_NW_3G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_12%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Attempted_Customers_4G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_13%20earliest=start_time%20latest=end_time",
    "SGSN_CSFB_SR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_14%20earliest=start_time%20latest=end_time",
    "SGSN_Attach_Failure_4G_NW": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_15%20earliest=start_time%20latest=end_time",
    "SGSN_SMS_Paging_SR_4G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_16%20earliest=start_time%20latest=end_time",
    "SGSN_S1_HO_SR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_17%20earliest=start_time%20latest=end_time",
    "SGSN_SGS_LU_FR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_18%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Active_Bearers_4G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_19%20earliest=start_time%20latest=end_time",
    "SGSN_Number_of_Active_Attached_Customers_4G": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_20%20earliest=start_time%20latest=end_time",
    "SGSN_Load_Max": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_21%20earliest=start_time%20latest=end_time",
    # AP_DP Load Max per SGSN instance (grouped as SGSN_Load)
    "SGSN_AP_DP_Load_Max": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_22%20earliest=start_time%20latest=end_time",  # General one
    # You can add specific ones like SGSN9, SGSN8 if needed — but for now group as one
    "SGSN_Dedicated_Bearer_QCI1_FailureRate": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_37%20earliest=start_time%20latest=end_time",
    "SGSN_GBR_Bearer_Activation_Failure": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_38%20earliest=start_time%20latest=end_time",
    "SGSN_Default_Bearer_QCI5_FailureRate": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_39%20earliest=start_time%20latest=end_time",
    "SGSN_SRVCC_to_WCDMA_FailureRate": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_40%20earliest=start_time%20latest=end_time",
    "SGSN_SRVCC_to_GPRS_FailureRate": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20SGSN_API_41%20earliest=start_time%20latest=end_time",

    # === DSC KPIs  ===
    "DSC_GC_DSC_LOAD_CPU": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_1%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_GC_DSC_LOAD_DB": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_2%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_GC_BLADE_LOAD_CPU": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_3%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_Incoming_Transaction_SR_per_Peer": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_4%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_Outgoing_Transaction_SR_per_Peer": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_5%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_Traffic_Transaction_SR": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_6%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_Number_of_Traffic_Transactions_per_Second": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_7%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_Number_of_Transactions_per_Seconds_per_Peer": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_8%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_REQ_THROTTLING_REJECTED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_9%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_ROUTING_MSG_DROPPED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_10%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_ROUTING_MSG_REJECTED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_11%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_MSG_SIZE_EXCEEDED_NUM_OF_REQ_REJECTED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_12%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_ANS_TOO_BUSY": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_13%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_ANS_UNABLE_TO_DELIVER": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_14%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_REQ_NUM_OF_MSGS_RECEIVED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_15%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_REQ_NUM_OF_RETRANSMISSIONS": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_16%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_REQ_NUM_OF_MSGS_SENT": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_17%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_ANS_NUM_OF_MSGS_RECEIVED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_18%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_ANS_NUM_OF_MSGS_EBIT_SET": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_19%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_IN_ANS_NUM_OF_MSGS_REDIRECT_IND": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_20%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_ANS_NUM_OF_MSGS_SENT": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_21%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_ANS_NUM_OF_MSGS_EBIT_SET": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_22%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_GC_NUM_OF_UNANSWERED_TRANSACTIONS": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_23%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_REQ_THROTTLING_REJECTED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_24%20earliest=start_time%20latest=end_time%20Node=Node_Name",
    "DSC_CC_OUT_REQ_PEER_GROUP_THROTTLING_REJECTED": "https://172.21.57.24:8089/servicesNS/admin/search/search/jobs/export?output_mode=csv&search=savedsearch%20DSC_API_25%20earliest=start_time%20latest=end_time%20Node=Node_Name",
}

SPLUNK_SUFFIX_TO_SMARTCARE_NODE = {
    # MME/SGSN from mapping data
    'zah-vsgsn-05': 'ZHR_vMME_5',
    'vsgsn-mme': 'HQ_vMME_1',
    'tnt-vsgsn-03': 'TNT_vMME_3',
    'tag-vsgsn-04': 'TGM_vMME_4',
    'sgsn10': 'MNS_MME_10',
    'sgsn9': 'TGM_MME_9',
    'sgsn7': 'ZHR_MME_7',
    'sgsn8': 'RMD_MME_8',
    'sgsn06': 'HQ_MME_6',
    'rmd-vsgsn-08': 'RMD_vMME_8',
    'mok-vsgsn-06': 'MOK_vMME_6',
    'mns-vsgsn-07': 'MNS_vMME_7',
    'bs-vsgsn-02': 'BNS_vMME_2',

    
    # # DNS from mapping data
    'RMD': 'RMD_PRI_DNS',
    'HD': 'HQ_EDNS',
    # DSC from mapping data
    'rmd': 'RMD_DSmartcare',
    'zah': 'ZHR_DSmartcare02',
    'tag': 'TAG_DSmartcare',
    'hq': 'dSmartcarehq01.epc.mnc002.mcc602.3gppnetwork.org',

    # OCC from mapping data
    'occ6': 'OCC6',
    'occ3': 'OCC3',
    'occ38': 'OCC38',
    'occ35': 'OCC35',
    'occ36': 'OCC36',
    'occ34': 'OCC34',
    'occ40': 'OCC40',
    'occ19': 'OCC19',
    'occ53': 'OCC53',
    'occ41': 'OCC41',
    'occ32': 'OCC32',
    'occ31': 'OCC31',
    'occ30': 'OCC30',
    'occ11': 'OCC11',
    'occ1': 'OCC1',
    'occ45': 'OCC45',
    'occ44': 'OCC44',
    'occ14': 'OCC14',
    'occ8': 'OCC8',
    'occ58': 'OCC58',
    'occ57': 'OCC57',
    'occ56': 'OCC56',
    'occ55': 'OCC55',
    'occ16': 'OCC16',
    'occ13': 'OCC13',
    'occ17': 'OCC17',
    'occ23': 'OCC23',
    'occ22': 'OCC22',
    'occ21': 'OCC21',
    'occ20': 'OCC20',
    'occ51': 'OCC51',
    'occ7': 'OCC7',
    'occ18': 'OCC18',
    'occ50': 'OCC50',
    'occ49': 'OCC49',
    'occ48': 'OCC48',
    'occ29': 'OCC29',
    'occ28': 'OCC28',
    'occ27': 'OCC27',
    'occ26': 'OCC26',
    'occ25': 'OCC25',
    'occ42': 'OCC42',
    'occ24': 'OCC24',
    'occ9': 'OCC9',
    'occ10': 'OCC10',
    'occ2': 'OCC2',
    'dccn14': 'CCN14',
    'dccn19': 'DCCN19',
    'dccn16': 'DCCN16',
    'dccn18': 'DCCN18',

    # HSS from mapping data
    'DN1_HSSmartcareA4101': 'ca4hss101dl',
    'DN1_HSSmartcareA4102': 'ca4hss102dl',
    'DN1_hsshq201': 'hqhss201dl',
    'DN1_hsshq202': 'hqhss202dl',
    'DN1_hssrmd301': 'rmdhss301dl',
    'DN1_hssrmd302' : 'rmdhss302dl',
    
     # GGSN or SGSN from mapping data
    'ALX-GGSN_CI-10':'ALX_SGW-PGW_10',
    'BS-GGSN8': 'BNS_SGW-PGW_8',
    'HQCUPS-vGGSN27': 'HQ_vSGW-PGW_CUPS-27',
    'HQ-vGGSN25': 'HQ_vSGW-vPGW_25',
    'HQ-vGGSN26': 'HQ_vSGW-vPGW-26',
    'MKT-GGSN_CI-09': 'MOK_SGW-PGW_9',
    'MNS-GGSN': 'MNS_SGW-PGW_11',
    'MNS-vGGSN21': 'MNS_SGW-PGW_21',
    'MNS-vGGSN22': 'MNS_SGW-PGW_22',
    'old-ftp': 'BNS_SGW-PGW_CUPS',
    'RMD-vGGSN28': 'RMD_vSGW_vPGW_28',
    'RM-GGSN16': 'RMD_SGW-PGW_16',
    'RM-GGSN17': 'RMD_SGW-PGW_17',
    'RM-GGSN20': 'RMD_SGW-PGW_20',
    'TAGCUPS-vGGSN29': 'TGM_vSGW-PGW_CUPS-29',
    'TG-GGSN12': 'TGM_SGW-PGW_12',
    'TG-GGSN19': 'TGM_vSGW-vPGW_19',
    'TG-vGGSN18': 'TGM_vSGW-vPGW_19',
    'TNT-GGSN15': 'TNT_SGW-PGW_15',
    'ZAHCUPS-vGGSN24': 'ZHR_SGW-PGW_24',
    'ZH-GGSN13': 'ZHR_SGW-PGW_13',
    'ZHR-vGGSN23': 'ZHR_SGW-PGW_23' ,
}


NODE_COLUMNS_FROM_SMARTCARE = {
        'SGSN_MME_NAME': 'SGSN',      
        'MME_Name' : 'MME' ,           
        'SGSN_SGW_NAME':'GGSN',    
        'GGSN_PGW_NAME': 'GGSN',
        'HSS_Name': 'HSS',
        'VPCRF_NAME':'PCRF',
        'OCS_NAME':'OCC',
        'DRA_NAME':'DSC',
        'DNS_Server_Name': 'DNS'
    
}

VIAVIE_KPIS_MAPPING = {
  "AVG_FD_MS": "Latency",
  "AVG_FDV_MS" : "Jitter",
  "ONEW_FLR_AZ":"Pktloss DL",
  "ONEW_FLR_ZA": "Pktloss UL"
  
}