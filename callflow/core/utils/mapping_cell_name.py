from pathlib import Path
import sys
import pandas as pd
from django.conf import settings
import logging

logger = logging.getLogger(__name__)
_SITE_PHY_TO_CIDS = None

_CID4G_TO_SITE = None
_CIDLAC_TO_SITE = None

def load_site_phy_to_cid_lac(filename: str = "SITE_NAME_Mapping.csv") -> dict:
    """SITE_PHY (str) → list of CID (int). Cached globally."""
    global _SITE_PHY_TO_CIDS
    if _SITE_PHY_TO_CIDS is not None:
        return _SITE_PHY_TO_CIDS

    path = Path(settings.BASE_DIR) / "data" / filename
    if not path.is_file():
        logger.error("Mapping file missing: %s", path)
        _SITE_PHY_TO_CIDS = {}
        return {}

    df = pd.read_csv(path, dtype=str)
    df['CID'] = pd.to_numeric(df['CID'], errors='coerce')
    df = df.dropna(subset=['CID'])
    df['CID'] = df['CID'].astype(int)
    df['LAC'] = df['LAC'].astype(int)
    if 'Technology' in df.columns:
            df['Technology'] = df['Technology'].str.strip().str.upper()
    else:
        df['Technology'] = 'UNKNOWN'
        if 'CELL' not in df.columns:
            logger.error("Expected 'CELL' column not found in %s", filename)
            df['CELL'] = None
    mapping = {}
    for site_phy, group in df.groupby('SITE_PHY'):
        key = str(site_phy).strip()
        if key:
            mapping[key] = list(zip(group['CID'], group['LAC'], group['Technology'], group['CELL']))

    _SITE_PHY_TO_CIDS = mapping
    logger.info("Loaded %d SITE_PHY → (CID, LAC) mappings", len(mapping))
    return mapping

# ────────────────────────────────────────────────
# Transformation function (SITE_PHY → CGISAI)
# ────────────────────────────────────────────────
def cid_to_4g_cgisai(cid: int) -> str:
    """CID decimal → '60202' + 7-digit uppercase hex (padded with zeros)"""
    hex_str = f"{cid:07X}"          # 07X = exactly 7 chars, uppercase, leading zeros
    return f"60202{hex_str}"

def cid_lac_to_2g3g_cgisai(cid: int, lac: int) -> str:
    """CID + LAC → 60202 + 4-digit HEX CID + 4-digit HEX LAC (for 2G/3G)"""
    cid_hex = f"{cid & 0xFFFF:04X}"   # force exactly 4 hex digits
    lac_hex = f"{lac & 0xFFFF:04X}"   # force exactly 4 hex digits
    result = f"60202{lac_hex}{cid_hex}"
    logger.debug("cid_lac_to_2g3g_cgisai → %s (CID=%s, LAC=%s)", result, cid, lac)
    return result



def get_site_names(site_phy_list: list, tech_filter: str = None) -> list:
    """
    Returns CGISAI list based on Technology.
    - tech_filter='4G'     → only 4G
    - tech_filter=None     → only 2G and 3G (this is used by get_2g3g_site_names)
    """
    site_phy_to_pairs = load_site_phy_to_cid_lac()
    result = []
    
    for site_phy in site_phy_list:
        pairs = site_phy_to_pairs.get(str(site_phy).strip(), [])
        for cid, lac, tech in pairs:
            tech = str(tech).strip().upper()

            # ==================== FILTERING LOGIC ====================
            if tech_filter is None:
                # Special case: get_2g3g_site_names wants ONLY 2G + 3G
                if tech not in ('2G', '3G'):
                    logger.debug(f"Skipped (not 2G/3G) SITE_PHY={site_phy} CID={cid} Tech={tech}")
                    continue
            else:
                # Normal case: specific technology (4G, or future 5G etc.)
                if tech != tech_filter:
                    continue
            # =========================================================

            # Now map according to technology
            if tech == '4G':
                result.append(cid_to_4g_cgisai(cid))
            elif tech in ('2G', '3G'):
                result.append(cid_lac_to_2g3g_cgisai(cid, lac))
           

    logger.info(f"get_site_names({site_phy_list}, filter={tech_filter}) → {len(result)} CGISAI")
    return result

def get_4g_site_names(site_phy_list: list) -> list:
    return get_site_names(site_phy_list, tech_filter='4G')


def get_2g3g_site_names(site_phy_list: list) -> list:
    return get_site_names(site_phy_list, tech_filter=None)  # all non-4G

def _build_reverse_indexes():
    """Build both reverse lookup dicts from the forward mapping, once.
    Keyed purely by (CID) or (CID, LAC) - not by the Technology column,
    so it's robust even if Technology is missing/unreliable in the CSV.
    """
    global _CID4G_TO_SITE, _CIDLAC_TO_SITE
    if _CID4G_TO_SITE is not None and _CIDLAC_TO_SITE is not None:
        return

    site_phy_to_pairs = load_site_phy_to_cid_lac()
    cid4g_to_site = {}
    cidlac_to_site = {}

    for site_phy, pairs in site_phy_to_pairs.items():
        for cid, lac, tech, cell in pairs:
            cid4g_to_site.setdefault(cid, cell)
            cidlac_to_site.setdefault((cid, lac), cell)

    _CID4G_TO_SITE = cid4g_to_site
    _CIDLAC_TO_SITE = cidlac_to_site
    logger.info(
        "Built reverse indexes: %d CID(4G) entries, %d (CID,LAC)(2G/3G) entries",
        len(cid4g_to_site), len(cidlac_to_site)
    )


def cell_id_to_site_name(cell_id: str) -> str:
    """Decode a CGISAI string (as returned by Trino) back to SITE_PHY,
    branching on string length to determine 4G vs 2G/3G format."""
    if not cell_id:
        return None

    cell_id = str(cell_id).strip().upper()

    if not cell_id.startswith("60202"):
        logger.debug("Unrecognized cell_ID prefix: %s", cell_id)
        return None

    suffix = cell_id[5:]  # strip the '60202' MCC+MNC prefix
    _build_reverse_indexes()

    if len(suffix) == 7:
        # 4G format: 7 hex digits = CID
        try:
            cid = int(suffix, 16)
        except ValueError:
            logger.warning("Bad 4G CID hex in cell_ID: %s", cell_id)
            return None
        return _CID4G_TO_SITE.get(cid)

    elif len(suffix) == 8:
        # 2G/3G format: first 4 hex = LAC, last 4 hex = CID
        try:
            lac = int(suffix[:4], 16)
            cid = int(suffix[4:], 16)
        except ValueError:
            logger.warning("Bad 2G/3G LAC/CID hex in cell_ID: %s", cell_id)
            return None
        return _CIDLAC_TO_SITE.get((cid, lac))

    else:
        logger.warning("Unexpected cell_ID length (%d): %s", len(suffix), cell_id)
        return None


def map_cell_ids(cell_id_list: list) -> list:
    """Batch version for a pandas column."""
    return [cell_id_to_site_name(c) for c in cell_id_list]