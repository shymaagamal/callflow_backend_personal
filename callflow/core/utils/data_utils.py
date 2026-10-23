# Pandas helpers (e.g., convert_java_types, process_sitename)
import pandas as pd
import numpy as np
from decimal import Decimal
import logging
from callflow.core.constants.endpoints import (
    SPLUNK_ENDPOINTS,
    SPLUNK_SUFFIX_TO_SMARTCARE_NODE,
    NODE_COLUMNS_FROM_SMARTCARE
)

logger = logging.getLogger(__name__)

def convert_java_types(df: pd.DataFrame) -> pd.DataFrame:
    """FINAL FIXED VERSION - handles BigDecimal correctly + better logging"""
    
    def convert_value(x):
        if pd.isna(x) or x is None:
            return None

        t = str(type(x)).lower()

     

        # === 1. Try integer conversion first (TAC and MME_ID are integers) ===
        if any(k in t for k in ["java.lang.long", "java.lang.integer", "biginteger"]):
            try:
                return int(x)
            except:
                pass

        # === 2. BigDecimal / HiveDecimal handling (FIXED) ===
        if isinstance(x, Decimal) or any(k in t for k in ["bigdecimal", "hivedecimal", "decimal"]):
            try:
                # More robust way - convert via string or doubleValue()
                if hasattr(x, 'doubleValue'):          # Java method
                    return float(x.doubleValue())
                else:
                    return float(str(x))               # fallback
            except Exception as e:
                logger.debug(f"BigDecimal conversion failed for {x}: {e}")
                return None

        # === 3. Timestamp / Date ===
        if any(k in t for k in ["timestamp", "java.sql.date", "datetime"]):
            try:
                return str(x)
            except:
                return str(x)

        # === 4. Generic fallback for ANY remaining Java object ===
        if any(k in t for k in ["java.", "hive", "hadoop", "org.apache"]):
            try:
                if hasattr(x, 'toString'):
                    return x.toString()
                return str(x)
            except:
                return str(x)

        return x

    # Apply to all object columns
    object_cols = df.select_dtypes(include=['object']).columns.tolist()
    for col in object_cols:
        # logger.info(f"[CONVERT] Processing column: {col}")
        df[col] = df[col].apply(convert_value)

    # Final verification

    return df
def process_sitename(data,cell_col_name ,logs_file):
    try:
        col = data[cell_col_name].fillna('').astype(str)
        data['site_name'] = (
            data[cell_col_name]
            .str.split('_L')
            .str[-1]
            .str.split('_')
            .str[0]
        )

        data['site_name_number'] = (
            data['site_name']
            .str.extract(r'(\d+)')[0]
            .fillna('')
        )

        data['site_name_letter'] = (
            data['site_name']
            .str.replace(r'\d+', '', regex=True)
        )

        data['modified_site_name_number'] = (
            data['site_name_number']
            .str.zfill(4)
        )
    
        data['modified_site_name'] = (
            data['site_name_letter'] + data['modified_site_name_number']
        )

    except Exception as e:
        with open(logs_file, 'a') as logs_f:
            logs_f.write('Process site name error : ' + str(e) + '\n')

        data['modified_site_name'] = ''

    return data      
            



def extract_suffix( kpi_column: str) -> str:
        """
        Extracts the real suffix by matching against known base KPI names from splunk_endpoints.
        Example: 
            "DSC_Incoming_Transaction_SR_per_Peer_RMD3_ggsn16_11" → "RMD3_ggsn16_11"
        """
        if not kpi_column:
            return ""

        # Find the longest matching base KPI name
        best_base = ""
        for base in SPLUNK_ENDPOINTS.keys():
            if kpi_column.startswith(base) and len(base) > len(best_base):
                best_base = base

        if best_base:
            # Remove base + any leading/trailing underscore
            suffix = kpi_column[len(best_base):].strip('_')
            return suffix
        else:
            # Fallback: last part after last _
            return kpi_column.split('_')[-1]

def map_splunk_kpi_to_smartcare(kpi_column: str, nodes_data):
    # Step 1: Extract suffix using base from splunk_endpoints
    suffix = extract_suffix(kpi_column).lower()
    logger.info(f"map_splunk_kpi_to_smartcare---> suffix :{suffix} kpi_column: {kpi_column}")
    if not suffix:
        logger.warning(f"No valid suffix from KPI: {kpi_column}")
    # Step 2: Lookup in flat mapping
    mapped_value = SPLUNK_SUFFIX_TO_SMARTCARE_NODE.get(suffix)   
    if not mapped_value:
            logger.warning(f"No mapping for suffix '{suffix}' from KPI '{kpi_column}'")
            return None, None, suffix 
        
    for column in NODE_COLUMNS_FROM_SMARTCARE :
        if column not in nodes_data.columns:
            continue
        try:
            col_series = nodes_data[column]
            col_series = col_series.fillna('').apply(lambda x: str(x))
            mask = col_series.str.contains(str(mapped_value), case=False, na=False)
            if mask.any():
                return mapped_value, column, suffix
        except Exception as e:
            logger.error(f"Error in column {column}: {e}")
            continue
    logger.warning(f"No column match for '{mapped_value}'")
    return None, None, suffix

def safe_unique(df: pd.DataFrame, col: str):
            if col in df.columns:
                return tuple(df[col].dropna().unique())
            return tuple()