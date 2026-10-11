import pandas as pd
import logging
from django.conf import settings
from typing import Dict, Optional


_SITE_MAPPING: Optional[Dict[str, str]] = None
logger = logging.getLogger(__name__)


def load_site_mapping() -> Dict[str, str]:
    global _SITE_MAPPING
    if _SITE_MAPPING is not None:
        return _SITE_MAPPING


    try:
        df = pd.read_excel(
            settings.MAPPING_SITENAMES_SHEET_PATH,
            usecols=["SiteID", "Site Name"],   # adjust column names if different
            dtype=str
        )
        df = df.dropna(subset=["SiteID"])
        df["SiteID"] = df["SiteID"].str.strip().str.upper()
        df["Site Name"] = df["Site Name"].str.strip()

        _SITE_MAPPING = dict(zip(df["SiteID"], df["Site Name"]))
        logger.info(f"Loaded {len(_SITE_MAPPING)} site mappings")   

    except Exception as e:
        print(f"Failed to load site mapping: {e}")
        _SITE_MAPPING = {}

    return _SITE_MAPPING

def get_site_name(cell_name: str, mapping: Dict[str, str]) -> str:
    if not isinstance(cell_name, str):
        return cell_name

    site_code = extract_site_code(cell_name)  
    if not site_code:
        return cell_name

    return mapping.get(site_code.upper(), cell_name)


import re

def extract_site_code(cell_name: str) -> Optional[str]:

    if not isinstance(cell_name, str) or '_' not in cell_name:
        return None

    parts = cell_name.upper().split('_')
    
    if len(parts) < 3:
        return None
    
    site_part = parts[1]  
    
   
    if not re.match(r'^L([DU])?\d{4,5}$', site_part):
        return None
    
    # If it doesn't match, fallback (rare)
    return  site_part[1:]