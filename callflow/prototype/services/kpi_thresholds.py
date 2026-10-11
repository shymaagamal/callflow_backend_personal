
class KPIThresholds:
    """
     KPI thresholds and conditions for network nodes
    """
    
    MME_THRESHOLDS = {
        'MME_CPULOAD': {
                 'rejections': 1000,
                 'deviation': 20
            },
        'MME_Overload-Protection-Rejection-Newconnection': {
            'rejections': 1000,
             'deviation': 20
        },
        'MME_Overload-Protection-Rejection-Existingconnection': {
            'rejections': 1000,
            'deviation': 20
        },
     
    }
    

    # GGSN_THRESHOLDS = {}
 
    

    

    
    @classmethod
    def get_thresholds_for_node(cls, node):
        """Get all thresholds for a specific node"""
        threshold_map = {
            'MME': cls.MME_THRESHOLDS,
            # 'GGSN': cls.GGSN_THRESHOLDS
       
        }
        return threshold_map.get(node, {})
    
    @classmethod
    def get_kpi_config(cls, node, kpi_name):
        """Get configuration for a specific KPI"""
        node_thresholds = cls.get_thresholds_for_node(node)
        
        
        for config_key in node_thresholds.keys():
            print("config_key", config_key )
            if config_key in kpi_name:
                return node_thresholds[config_key]
        
        return None