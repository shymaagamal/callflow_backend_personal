import datetime
import pandas as pd
from callflow.core.utils.data_utils import process_sitename

# i set this  to 15 min because viave data refresh every 15 min and
# eventhough npp data refresh hourly 
# 15 minutes is the finest refresh rate among the sources.
# Using a bigger bucket (e.g. 1 hour) 
# would make Viavi data look more stale than it actually is.
NCI_CACHE_GRANULARITY_MINUTES = 15


def _round_down(dt: datetime.datetime, minutes: int) -> datetime.datetime:
    discard = datetime.timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    return dt - discard


def _add_formatted(window: dict) -> dict:
    """Viavi's Oracle query uses a 12-hour AM/PM mask, so it needs
    strftime('%Y-%m-%d %I:%M %p') rather than the raw datetime -- see
    base.py's parse_request_time_range, which does the same for the
    existing (non-NCI) Viavi view. NPP and Remedy are fine with the raw
    datetime objects, so those are left untouched."""
    window["formatted_start"] = window["start"].strftime("%Y-%m-%d %I:%M %p")
    window["formatted_end"] = window["end"].strftime("%Y-%m-%d %I:%M %p")
    return window
 

def build_nci_time_windows(now: datetime.datetime = None) -> dict:
    now = _round_down(now or datetime.datetime.now(), NCI_CACHE_GRANULARITY_MINUTES)
    current_start = now - datetime.timedelta(hours=12)
    current_end = now

    windows = {
        "current": {"start": current_start, "end": current_end},
        "prev_1day": {
            "start": current_start - datetime.timedelta(days=1),
            "end": current_end - datetime.timedelta(days=1),
        },
        "prev_1week": {
            "start": current_start - datetime.timedelta(days=7),
            "end": current_end - datetime.timedelta(days=7),
        },
    }
    for w in windows.values():
        _add_formatted(w)
    return windows



def cells_to_sites(cell_names: list, log_path) -> list:
    df = pd.DataFrame({"cell_name": cell_names})
    df = process_sitename(df, "cell_name", log_path)
    return df["modified_site_name"].dropna().unique().tolist()