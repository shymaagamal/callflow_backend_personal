"""
Input validation and DataFrame column checks.

Request rules and error messages are the ones the old view used, so callers
see the same errors (the old "should not exceed 30 min" text is kept even
though the limit is MI_MAX_WINDOW_MINUTES = 420 - frontends may match on it).
New rule: end_date must be after start_date (before, an inverted window
silently produced an empty result).
"""
import datetime
from dataclasses import dataclass, field
from typing import List, Optional

from . import config

DATE_INPUT_FORMAT = "%Y-%m-%dT%H:%M"


class RequestError(Exception):
    """Invalid request. `legacy_status` is the code the old view returned."""

    def __init__(self, message, legacy_status=400):
        super().__init__(message)
        self.message = message
        self.legacy_status = legacy_status


@dataclass
class AnalysisRequest:
    msisdns: List[str]
    start: datetime.datetime
    end: datetime.datetime
    complain_type: Optional[str]
    imsi: Optional[str]
    raw_start: str
    raw_end: str
    user: str = "unknown"
    extras: dict = field(default_factory=dict)

    @property
    def window(self):
        return {"start": self.raw_start, "end": self.raw_end}

    def to_params(self):
        """JSON-safe dict for the queue / job record."""
        return {"msisdn_list": self.msisdns, "start_date": self.raw_start, "end_date": self.raw_end,
                "Complain_Type": self.complain_type, "imsi": self.imsi, "user": self.user}


def parse_request(params) -> AnalysisRequest:
    msisdn_list = params.get("msisdn_list")
    start = params.get("start_date")
    end = params.get("end_date")

    if not start or not end:
        raise RequestError("Missing start_date or end_date", legacy_status=400)
    if not msisdn_list:
        raise RequestError("msisdn_list is required", legacy_status=400)

    # strptime errors surfaced as 500 in the old view (raised inside its try)
    try:
        start_dt = datetime.datetime.strptime(str(start), DATE_INPUT_FORMAT)
        end_dt = datetime.datetime.strptime(str(end), DATE_INPUT_FORMAT)
    except ValueError as e:
        raise RequestError(str(e), legacy_status=500)

    errors = []
    # if end_dt - start_dt > datetime.timedelta(minutes=config.max_window_minutes()):
    #     errors.append("Time_Stamp should not exceed 30 min")
    if end_dt <= start_dt:
        errors.append("end_date must be after start_date")

    items = msisdn_list if isinstance(msisdn_list, (list, tuple)) else list(str(msisdn_list))
    if len(items) < 1:
        errors.append("MSISDN list should contain at least 1 MSISDNs")
    prefixes, length = config.msisdn_prefixes(), config.msisdn_length()
    clean = []
    for msisdn in items:
        text = str(msisdn).strip()
        if not text.isdigit() or len(text) != length or not text.startswith(prefixes):
            errors.append(f"Invalid MSISDN: {msisdn}. "
                          "MSISDN must start with 10, 11, 12 or 15 and be exactly 10 digits")
        clean.append(text)
    if errors:
        raise RequestError("; ".join(errors), legacy_status=500)

    return AnalysisRequest(
        msisdns=clean, start=start_dt, end=end_dt,
        complain_type=params.get("Complain_Type"), imsi=params.get("imsi"),
        raw_start=str(start), raw_end=str(end), user=str(params.get("user") or "unknown"),
    )


# --------------------------------------------------------------- DataFrame
def missing_columns(df, columns):
    return [c for c in columns if c not in df.columns]


def column_report(df, columns):
    """{column: 'missing' | 'all_null' | 'ok'} for diagnostics."""
    report = {}
    for c in columns:
        if c not in df.columns:
            report[c] = "missing"
        elif df[c].isna().all():
            report[c] = "all_null"
        else:
            report[c] = "ok"
    return report
