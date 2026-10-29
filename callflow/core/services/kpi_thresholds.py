
from typing import Optional, Dict , Any, List, Tuple

class KPIThresholds:
    """
     KPI thresholds and conditions for network nodes
    """
    
    MME_THRESHOLDS = {
        'MME_CPULOAD': [
            {'type': 'deviation', 'value': 80, 'operator': '>'},
        ],
        'MME_Overload-Protection-Rejection-Newconnection': [
            {'type': 'threshold', 'value': 1000, 'operator': '>'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'MME_Overload-Protection-Rejection-Existingconnection': [
            {'type': 'threshold', 'value': 1000, 'operator': '>'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
     
    }
    SGSN_THRESHOLD = {
        'SGSN_Attach_SR_2G':
            [
            {'type': 'threshold', 'value': 80, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
                
            ],
        'SGSN_PDP_SR_2G':
            [
            {'type': 'threshold', 'value': 80, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
                
            ],
        'SGSN_Attach_SR_3G':
            [
               {'type': 'threshold', 'value': 80, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
                
            ],
        'SGSN_PDP_SR_3G':
            [
              {'type': 'threshold', 'value': 80, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
            ],
        'SGSN_Attach_Failure_4G_NW':
            [
               {'type': 'threshold', 'value': 15, 'operator': '>'},
            {'type': 'deviation', 'value': 10, 'operator': '>'},
            ],
            
         'SGSN_GBR_Bearer_Activation_Failure':
            [
               {'type': 'threshold', 'value': 10, 'operator': '>'},
            {'type': 'deviation', 'value': 10, 'operator': '>'},
            ],
          'SGSN_Default_Bearer_QCI5_FailureRate':
            [
               {'type': 'threshold', 'value': 10, 'operator': '>'},
            {'type': 'deviation', 'value': 10, 'operator': '>'},
            ],            
                
    }
    DSC_THRESHOLD = {
        'DSC_GC_BLADE_LOAD_CPU': 
            [
               {'type': 'threshold', 'value': 70, 'operator': '>'},
            ],
            
    }
    GGSN_THRESHOLD = {
        'GGSN_PDPSR': 
            [
               {'type': 'threshold', 'value': 90, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
            ],
        'GGSN_CPU':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
            ],
        'GGSN_NPU':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
            ],
        'GGSN_PKTLoss':
            [
               {'type': 'deviation', 'value': 20, 'operator': '>'},
            ],
        'GGSN_RLFDrop':
            [
              {'type': 'deviation', 'value': 20, 'operator': '>'},
            ],
        'GGSN_TotalDrops':
            [
               {'type': 'deviation', 'value': 20, 'operator': '>'},
            ],
        'GGSN_PDNRejection':
            [
              {'type': 'deviation', 'value': 20, 'operator': '>'},
            ],
    }
    
    
    DNS_THRESHOLD = {
        'DNS_HitRatio_HQ':
            [
                {'type': 'threshold', 'value': 90, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
            ],
            'DNS_HitRatio_HQ2':
            [
                {'type': 'threshold', 'value': 90, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
            ],
            'DNS_CPU_HQ':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
               
            ],
            'DNS_CPU_HQ2':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
               
            ],
            'DNS_CPU_TG':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
               
            ],
            'DNS_CPU_RMD':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
               
            ],
            'DNS_CPU_ZH':
            [
                {'type': 'threshold', 'value': 80, 'operator': '>'},
               
            ],
            'DNS_QPS_HQ':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
            'DNS_QPS_HQ2':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
            'DNS_QPS_TG':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
            'DNS_QPS_RMD':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
            'DNS_QPS_ZH':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
            'DNS_QPS_RMD2':
            [
                {'type': 'deviation', 'value': -20, 'operator': '>'},
               
            ],
    }
    NPP_THRESHOLDS = {

        # ---------- Availability ----------
        'Unavail_Man': [
            {'type': 'threshold', 'value': 0, 'operator': '>', 'resolution': '15m'},
        ],

        'Unavailability': [
            {'type': 'threshold', 'value': 0, 'operator': '>', 'resolution': '15m'},
        ],

        # ---------- Call & Session KPIs ----------
        'CDR_CELL': [
            {'type': 'threshold', 'value': 0, 'operator': '>', 'resolution': '1H'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],

        'CFR': [
            {'type': 'threshold', 'value': 0.1, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        '4G_Service_Drop_Rate': [
            {'type': 'threshold', 'value': 5, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],

       '4G_Service_Setup_Fail_Rate': [
            {'type': 'threshold', 'value': 0.5, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'Drops_Over_Erlang': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],

        # ---------- Throughput ----------
        'AVG_DL_USER_THROUGHPUT': [
            {'type': 'threshold', 'value': 5, 'operator': '<', 'resolution': '15m'},
            {'type': 'deviation', 'value': -20, 'operator': '>'},
        ],
        'AVG_UL_USER_THROUGHPUT': [
            {'type': 'threshold', 'value': 0.1, 'operator': '<', 'resolution': '15m'},
            {'type': 'deviation', 'value': -20, 'operator': '>'},
        ],

        # ---------- ERAB ----------
        'ERAB_Retain': [
            {'type': 'deviation', 'value': -20, 'operator': '>', 'resolution': '15m'},
        ],
        'ERAB_Setup_SR': [
            {'type': 'threshold', 'value': 95, 'operator': '<', 'resolution': '15m'},
            {'type': 'deviation', 'value': -20, 'operator': '>'},
        ],

        # ---------- Radio Quality ----------
        'RSSI': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],
        'AVG_CQI': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '15m'},
        ],
        'CCE_Util': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '15m'},
        ],

        'DL_PRB_Util': [
            {'type': 'threshold', 'value': 50, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'UL_PRB_Util': [
            {'type': 'threshold', 'value': 50, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],

        # ---------- RACH ----------
        'RACH_SR': [
            {'type': 'deviation', 'value': -20, 'operator': '>', 'resolution': '1H'},
        ],

        # ---------- VoLTE ----------
        'Abnormal_Rel_VoLTE_Calls': [
            {'type': 'threshold', 'value': 1, 'operator': '>', 'resolution': '15m'},
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'Avg_Audio_Gap_DL': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '15m'},
        ],
        'Avg_Audio_Gap_UL': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '15m'},
        ],
        
        'DL_Disconns': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '15m'},
        ],
        'UL_Disconns': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],
        'VOIP_UL_IBLER': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],

        'VOIP_DL_IBLER': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],

        # ---------- Packet Loss ----------
        'UL_Pkt_Loss_FAR_UE_1': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],
        'UL_Pkt_Loss_FAR_UE_2': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],

        # ---------- Interference ----------
        'E/// Intererence': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],
        'Hu Interference': [
            {'type': 'deviation', 'value': 20, 'operator': '>', 'resolution': '1H'},
        ],
      
    }



    VIAVI_THRESHOLD = {
        'AVG_FD_MS': [
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'AVG_FDV_MS': [
            {'type': 'deviation', 'value': 20, 'operator': '>'},
        ],
        'ONEW_FLR_AZ': [
            {'type': 'threshold', 'value': 1, 'operator': '>'},
        ],
        'ONEW_FLR_ZA': [
            {'type': 'threshold', 'value': 1, 'operator': '>'},
        ],

        }
    

    

    
    # S1-MME per-site KPIs from Trino (NCI). Rules taken from the MME rows of
    # data/thresholds.xlsx: Attach_Failure_4G_NW (FR > 15% OR deviation > 10%),
    # TAU SR and Authentication SR (SR < 90% OR deviation > -10%).
    S1MME_SITE_THRESHOLDS = {
        'S1MME_ATTACH_FAILURE_RATE': [
            {'type': 'threshold', 'value': 15, 'operator': '>'},
            {'type': 'deviation', 'value': 10, 'operator': '>'},
        ],
        'S1MME_TAU_SR': [
            {'type': 'threshold', 'value': 90, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
        ],
        'S1MME_AUTH_SR': [
            {'type': 'threshold', 'value': 90, 'operator': '<'},
            {'type': 'deviation', 'value': -10, 'operator': '>'},
        ],
    }

    @classmethod
    def get_thresholds_for_system(cls, system_name: str) -> Dict[str, List[Dict]]:
        """Get all thresholds for a specific node"""
        name_upper = system_name.upper()
        threshold_map = {
            'MME': cls.MME_THRESHOLDS,
            'SGSN': cls.SGSN_THRESHOLD,
            'DSC':cls.DSC_THRESHOLD,
            'GGSN': cls.GGSN_THRESHOLD,
            'DNS':cls.DNS_THRESHOLD,
            'NPP': cls.NPP_THRESHOLDS,
            'VIAVI':cls.VIAVI_THRESHOLD,
            'S1MME_SITE': cls.S1MME_SITE_THRESHOLDS,
       
        }
        return threshold_map.get(name_upper, {})
    
    @classmethod
    def get_kpi_config(cls, system_name: str, kpi_name: str) -> Optional[List[Dict]]:
        """Get configuration for a specific KPI"""
        thresholds = cls.get_thresholds_for_system(system_name)
        if not thresholds:
            return None
        
        # Find the longest matching base key
        matched_key = None
        for key in thresholds:
            if key in kpi_name and (not matched_key or len(key) > len(matched_key)):
                matched_key = key

        return thresholds.get(matched_key) if matched_key else None
        