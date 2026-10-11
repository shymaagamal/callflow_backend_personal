import pandas as pd
from pathlib import Path
from typing import Optional, Union, cast
import logging

logger = logging.getLogger(__name__)
def retrieve_complaint_by_incident_no(
    incident_no: str,
    directory: str = "/home/CallFlow/BO/complaint_ticket_location",
    sheet_name: Union[str, int] = "Tab 1",
    **kwargs
) -> Optional[pd.DataFrame]:
    """
    Django service function:
    - Loads the newest Excel report from the folder
    - Filters by Incident No (partial match, case insensitive)
    - Returns the matching rows as DataFrame
    - Returns None if Incident No does NOT exist (exactly as you requested)
    """
    dir_path = Path(directory)
    
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    # Find latest Excel file
    patterns = ["*.xlsx", "*.xls", "*.xlsm"]
    excel_files = []
    for pattern in patterns:
        excel_files.extend(dir_path.glob(pattern))
    
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    
    # Get the newest file
    latest_file = max(excel_files, key=lambda f: f.stat().st_mtime)
    
    print(f"✅ Loading latest complaint report: {latest_file.name}")  # remove in production if you want
    
    # Read Excel
    df = pd.read_excel(latest_file, sheet_name=sheet_name, header=3, **kwargs)
    # Clean column names
    df.columns = [str(col).strip() for col in df.columns]
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    
    # === Filter by Incident No ===
    incident_col = "Incident No"
    print(f"df.columns : {df.columns}")
    if incident_col not in df.columns:
        print(f"⚠️ Column '{incident_col}' not found in the report")
        return None
    
    # Flexible search (contains the number)
    mask = df[incident_col].astype(str).str.contains(
        str(incident_no).strip(), case=False, na=False
    )
    

    df_filtered = cast(pd.DataFrame, df.loc[mask])
    
    if df_filtered.empty:
        print(f"⚠️ No records found for Incident No: {incident_no}")
        return None
    
    print(f"✅ Found {len(df_filtered):,} matching row(s) for Incident No: {incident_no}")
    return df_filtered