import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from .kpi_thresholds import KPIThresholds
from collections import defaultdict
from typing import Optional, Dict , Any, List, Tuple
import logging
from typing import cast
from ..utils.site_mapping import load_site_mapping, get_site_name
logger = logging.getLogger(__name__)
class KPIAnalyzer:
    """
    Analyzes KPI data from Splunk and checks against thresholds
    """
    
    def __init__(self):
        self.thresholds = KPIThresholds()
    # ── Shared Utilities ────────────────────────────────────────────────────────
     
    @staticmethod
    def get_aggregation_method(base_kpi: str) -> str:
        """Determine how to aggregate KPI values (sum, mean, max)"""
        base_kpi_upper = base_kpi.upper()
        if any(word in base_kpi_upper for word in ['REJECTION', 'DROP', 'DISCONN', 'PKTLOSS', 'TOTALDROPS']):
            return 'sum'
        elif any(word in base_kpi_upper for word in ['SR', 'RATE', 'RATIO']):
            return 'mean'
        elif any(word in base_kpi_upper for word in ['LOAD', 'CPU', 'UTIL', 'INTERFERENCE', 'RSSI', 'CQI', 'IBLER']):
            return 'max'
        else:
            return 'mean'  # default
    
    @staticmethod
    def calculate_kpi_value( series: pd.Series, agg_method: str)-> float:
        """Aggregate a series safely — return 0.0 on empty/invalid data"""
        if series.empty or series.isna().all():
            return 0.0
 

        # 🔥 Force numeric conversion
        numeric_series = pd.to_numeric(series, errors="coerce")

        # Drop NaNs created by invalid strings
        numeric_series = numeric_series.dropna()

        # Optional: enforce non-negative KPIs
        numeric_series = numeric_series.clip(lower=0)

        if numeric_series.empty:
            return 0.0

        try:
            return float(numeric_series.agg(agg_method))
        except Exception:
            logger.exception("Aggregation failed, returning 0.0")
            return 0.0


    @staticmethod
    def _group_kpis( df) -> Dict[str, List[str]]:
        """
        Group columns by base KPI name (exclude timestamp) 
        e.g.
         ('MME_CPULOAD', ['MME_CPULOAD_SGSNTNTVirtual', 'MME_CPULOAD_SGSN6', 'MME_CPULOAD_RMD-vSGSN-08'])
        Args:
            df: pd.DataFrame with 'timestamp' index
        
        Returns:
            dict: {base_kpi: list of columns}
        """
        kpi_groups = defaultdict(list)
        for col in df.columns:
            if col == 'timestamp' :
                continue
            kpi_name = col.rsplit('_', 1) # [MME_CPULOAD,SGSNTNTVirtual]
            base_kpi_name = kpi_name[0]
            kpi_groups[base_kpi_name].append(col)
        return   kpi_groups  

    def get_degradation_insight(
        self,
        current_value: float,
        previous_value: float,
        kpi_name: str = "",
        ) -> dict:
            """Calculate deviation, direction, severity, and insight message"""
            if previous_value == 0:
                if current_value == 0:
                    return {
                        "deviation_pct": 0.0,
                        "direction": "stable",
                        "severity": "none",
                        "message": "No change (both periods zero)"
                    }
                pct = 100.0
                direction = "increase" if current_value > 0 else "decrease"
                return {
                    "deviation_pct": round(pct, 1),
                    "direction": direction,
                    "severity": "high" if direction == "decrease" else "increase",
                    "message": f"KPI went from 0 to {current_value:.2f} (new appearance)"
                }

            pct_change = round(100.0 * (current_value - previous_value) / previous_value, 1)
            direction = " "
            if pct_change > 0:
                direction = "increase"
            elif pct_change < 0:
                direction = "decrease"
            else:
                direction = "stable"

            # Determine severity (you can tune these thresholds)
            abs_pct = abs(pct_change)
            if abs_pct >= 30:
                severity = "high"
            elif abs_pct >= 15:
                severity = "medium"
            elif abs_pct >= 5:
                severity = "low"
            else:
                severity = "minor"

            # For KPIs where increase is bad (e.g. drop rate, latency), flip logic
            bad_direction = "increase" if any(word in kpi_name.upper() for word in ['DROP', 'REJECT', 'LOSS', 'LATENCY', 'DELAY']) else "decrease"
            if (bad_direction == direction) and abs_pct >= 15:
                severity = "high" if abs_pct >= 30 else "medium"
            message = (
                f"{kpi_name} {direction}d by {abs_pct:.1f}% "
                f"from {previous_value:.2f} to {current_value:.2f}"
            )

            return {
                "deviation_pct": pct_change,
                "direction": direction,
                "severity": severity,
                "message": message,
                "previous_value": round(previous_value, 2),
                "current_value": round(current_value, 2),
            }
  
    def _check_thresholds(
        self,
        current: float,
        previous: float,
        config_list: List[Dict[str, Any]],
        kpi_name: str = ""
    ) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Check absolute thresholds + deviation — return exceeded + details + insight"""
        exceeded = False
        details = []
        insight = self.get_degradation_insight(current, previous, kpi_name)
        bad_direction = "increase" if any(
                word in kpi_name.upper()
                for word in ['DROP', 'REJECT', 'LOSS', 'LATENCY', 'DELAY', 'CFR', 'CDR', 'FAIL', 'UNAVAIL']
            ) else "decrease"
        actual_direction = insight.get("direction", "stable")
        for cond in config_list:
            # Absolute threshold
            if cond['type'] == 'threshold' and cond.get('value') is not None:
                thres_value = cond['value']
                op = cond['operator']
                if (op == '>' and current > thres_value) or (op == '<' and current < thres_value):
                    if actual_direction != ("decrease" if bad_direction == "increase" else "increase"):
                        exceeded = True
                        details.append(f"Threshold {op} {thres_value} exceeded: {current:.2f}")

            # Deviation
            elif cond['type'] == 'deviation' and cond.get('value') is not None:
                dev_thresh_value = cond['value']
                op = cond['operator']
                dev_info = self.calculate_deviation_rate(current, previous, dev_thresh_value)
                if dev_info['exceeds']:
                    exceeded = True
                    details.append(f"Deviation exceeded: {dev_info['percentage']}% {dev_info['direction']}")

        return exceeded, details, insight
    # ==================== STEP 2:Calculate Deviation Rate ====================
    def calculate_deviation_rate(self, curr: float, prev: float, dev_thresh: Optional[float]
 = None ):
        if prev == 0:
            pct = 100.0 if curr > 0 else 0.0
            direction = "increase" if pct > 0 else "decrease"
        else:
            pct = round(100.0 * (curr - prev) / prev, 2)
            direction = "increase" if pct > 0 else "decrease"

        exceeds = False
        if dev_thresh is not None:
            if dev_thresh < 0:  # Decrease is bad (e.g., throughput -20)
                exceeds = pct < dev_thresh  # pct < -20 (big decrease)
            else:  # Increase is bad (e.g., CDR 20)
                exceeds = pct > dev_thresh  # pct > 20 (big increase)

        return {'percentage': pct, 'direction': direction, 'exceeds': exceeds}

        

            
    # ── Core Node Analysis (Splunk/MME/SGSN/GGSN/DSC/DNS) ───────────────────────
    def analyze_core_node(
        self,
        df: pd.DataFrame,
        node: str,
        current_start: pd.Timestamp,
        current_end: pd.Timestamp,
        prev_start: pd.Timestamp,
        prev_end: pd.Timestamp
    ) -> List[Dict[str, Any]]:
        """Analyze core network KPIs (Splunk style)"""
      
        # Ensure timestamp index
        if not df.index.name == 'timestamp':
            df = df.set_index('timestamp').sort_index()
        kpis_group = self._group_kpis(df)

        alarms = []
        for base_kpi , columns in kpis_group.items():
            config = self.thresholds.get_kpi_config(node, base_kpi)
            if not config:
                continue
            agg_method = self.get_aggregation_method(base_kpi)
            for col in columns:
                series = df[col].dropna()
                current_data = series.loc[current_start:current_end]
                if current_data.empty:
                    continue

                current_value = self.calculate_kpi_value(current_data, agg_method)
                prev_data = series.loc[prev_start:prev_end]
                prev_value = self.calculate_kpi_value(prev_data, agg_method) if not prev_data.empty else 0

                
                
                exceeded, details, insight = self._check_thresholds(
                    current_value, prev_value, config, base_kpi
                )
                if exceeded:
                        alarms.append({
                            'node': node,
                            'kpi_column': col,
                            'base_kpi': base_kpi,
                            'current_value': float(current_value),
                            'prev_value': float(prev_value),
                            'deviation_percentage': insight.get('deviation_pct'),
                            'deviation_direction': insight.get('direction'),
                            'severity': insight.get('severity'),
                            'details': details,
                            'message': insight.get('message', f"{base_kpi} alarm on {col}")
                        })
        return alarms
    
    
    # ── NPP Analysis ────────────────────────────────────────────────────────────
    def analyze_npp(
        self,
        npp_df: pd.DataFrame,
        resolution: str, # must be '15m' or '1H'
        current_start: pd.Timestamp,
        current_end: pd.Timestamp,
        prev_start: pd.Timestamp,
        prev_end: pd.Timestamp,
        ts: str,
        entity_type: str = "cell",
    ) -> Dict[str, Any]:
        """Analyze NPP cell-level KPIs"""
        entity_col = "cell" if entity_type == "cell" else "sitePhysical"
        if npp_df.empty:
            return {
                "affected_cells_count": 0,
                "affected_cells_list": [],
                "affected_cells_details": {}
            }
        thresholds = self.thresholds.get_thresholds_for_system('NPP')
        thresh_by_res = {
            k: v for k, v in thresholds.items()
                if next((c for c in v if c.get('resolution') == resolution), None)
            }
        if not thresh_by_res:
            logger.warning(f"No NPP thresholds for resolution {resolution}")
            return {"affected_cells_count": 0, "affected_cells_list": [], "affected_cells_details": {}}

        site_mapping = load_site_mapping() if entity_type == "cell" else {}
        affected_cells_details: Dict[str, Any] = {}
        for entity in npp_df[entity_col].unique():
            site_name = get_site_name(entity, site_mapping) if entity_type == "cell" else entity
            cell_data = npp_df[npp_df[entity_col] == entity]
            curr_data = cell_data[cell_data['DATETIME_ID'].between(current_start, current_end)]
            prev_data = cell_data[cell_data['DATETIME_ID'].between(prev_start, prev_end)]

            if curr_data.empty:
                continue

            cell_kpis = {}

            for kpi, config in thresh_by_res.items():
                if kpi not in curr_data.columns:
                    continue

                curr_value = self.calculate_kpi_value(curr_data[kpi], 'mean')
                prev_value = self.calculate_kpi_value(prev_data[kpi],'mean')

                exceeded, _, insight = self._check_thresholds(curr_value, prev_value, config, kpi)

                if exceeded:
                    cell_kpis[kpi] = {
                        "current_value": curr_value,
                        "previous_value": prev_value,
                        "deviation_pct": insight.get("deviation_pct"),
                        'deviation_direction': insight.get('direction'),
                        'severity': insight.get('severity'),
                        "insight_message": insight["message"],
                        'message': insight.get('message', f"{entity} alarm on {kpi}")
                    }    
                if cell_kpis:
                    affected_cells_details[entity] = {
                        "kpis": cell_kpis,
                        "site_name": site_name,
                        }
                    
        affected_cells_list = list(affected_cells_details.keys())

        logger.info(
            f"NPP {resolution} affected cells: "
            f"{len(affected_cells_list)} → {affected_cells_list}"
        )

        return {
            "affected_cells_count": len(affected_cells_list),
            "affected_cells_list": affected_cells_list,
            "affected_cells_details": affected_cells_details
        }

        
            
            
    def analyze_viavi( self,
        viavi_df: pd.DataFrame,
        susbected_sites,
        current_start: pd.Timestamp,
        current_end: pd.Timestamp,
        prev_start: pd.Timestamp,
        prev_end: pd.Timestamp,
        ts: str)-> Dict[str, Any]:   
             
        """Analyze Viavi site-level KPIs"""
        if viavi_df.empty:
            return {"affected_sites_count": 0, "affected_sites_list": [], "affected_sites_details": {}}
        thresholds = self.thresholds.get_thresholds_for_system('VIAVI') 
        if not thresholds:
            logger.warning("No Viavi thresholds available")
            return {"affected_sites_count": 0, "affected_sites_list": [], "affected_sites_details": {}}
        affected_sites_details: Dict[str, Any] = {}
        for flwid in viavi_df['FLWID'].unique():
            site_data = viavi_df[viavi_df['FLWID'] == flwid]
            curr_data = site_data[site_data['PERIOD_TIMESTAMP'].between(current_start, current_end)]
            prev_data = site_data[site_data['PERIOD_TIMESTAMP'].between(prev_start, prev_end)]

            if curr_data.empty:
                    continue

            site_kpis = {}

            for kpi, config in thresholds.items():
                if kpi not in curr_data.columns:
                    continue

                curr_value = self.calculate_kpi_value(curr_data[kpi], 'mean')
                
                prev_value = self.calculate_kpi_value(prev_data[kpi],'mean')
                exceeded, _, insight = self._check_thresholds(curr_value, prev_value, config, kpi)
     
                if exceeded:
                        site_kpis[kpi] = {
                            "current_value": curr_value,
                            "previous_value": prev_value,
                            "deviation_pct": insight.get("deviation_pct"),
                            'deviation_direction': insight.get('direction'),
                            'severity': insight.get('severity'),
                            "insight_message": insight["message"],
                            'message': insight.get('message', f"{flwid} alarm on {kpi}")

                        }

            if site_kpis:
                affected_sites_details[flwid] = {"kpis": site_kpis}
            
            affected_list = list(affected_sites_details.keys())
            logger.info(f"Viavi affected sites: {len(affected_list)} → {affected_list}")

        return {
            "affected_sites_count": len(affected_list),
            "affected_sites_list": affected_list,
            "affected_sites_details": affected_sites_details
        }