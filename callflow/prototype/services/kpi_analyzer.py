import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from .kpi_thresholds import KPIThresholds
from collections import defaultdict

class KPIAnalyzer:
    """
    Analyzes KPI data from Splunk and checks against thresholds
    """
    
    def __init__(self):
        self.thresholds = KPIThresholds()
    

        # ==================== STEP 1: Calculate Deviation Rate ====================
    def calculate_deviation_rate(self, data, threshold, window='1H'):
        print("calculate_deviation_rate---> ", data)



    # ==================== STEP 2: Calculate Rejections ====================
    def calculate_rejections(self, data , threshold):
        print("calculate_rejections---> ", data)

    # ==================== STEP 3: Group KPIs ====================

    def group_kpis_per_node(self, df) -> defaultdict:
        """
        Group columns by base KPI name 
        e.g.
         ('MME_CPULOAD', ['MME_CPULOAD_SGSNTNTVirtual', 'MME_CPULOAD_SGSN6', 'MME_CPULOAD_RMD-vSGSN-08'])
        Args:
            df: pd.DataFrame with 'timestamp' index
        
        Returns:
            dict: {base_kpi: list of columns}
        """
        kpi_groups = defaultdict(list)
        for col in df.columns:
            if col == 'timestamp':
                continue
            kpi_name = col.rsplit('_', 1) # [MME_CPULOAD,SGSNTNTVirtual]
            base_kpi_name = kpi_name[0]
            kpi_groups[base_kpi_name].append(col)
        return   kpi_groups  

            

    def analyze(self, df, node):
        """
        Main analysis function.
        
        Args:
            df: pd.DataFrame from Splunk, with timestamp index
            node:  e.g., 'MME'
        
        Returns:
            list: Alarms with details
        """
        kpis_group = self.group_kpis_per_node(df)
        node_thresholds = self.thresholds.get_thresholds_for_node(node)
        for base_kpi , cols in kpis_group.items():
            print("base_kpi", base_kpi)
            config = self.thresholds.get_kpi_config(node, base_kpi)
            if not config:
                continue
            for col in cols:
                if config["rejections"] : 
                    self.calculate_rejections(df[col].dropna(),  config["rejections"])
                if config["deviation"]:
                    self.calculate_deviation_rate(df[col].dropna(), config["deviation"])



           

            # if config["deviation"]    
            #  i have to check if condition type is deviation or FR or sr and based on this i will call related fun 
            # if dv rate or sr or fr exceeded threshold i have to do actions ?? like fill alarms = [] with each time any metric exceed the limit
        # breakpoint()
        
        return list 