"""
Input validation and DataFrame column checks.

Request rules and error messages are the ones the old view used, so callers
see the same errors (the old "should not exceed 30 min" text is kept even
though the limit is MI_MAX_WINDOW_MINUTES = 420 - frontends may match on it).
New rule: end_date must be after start_date (before, an inverted window
silently produced an empty result).
"""
import datetime
from collections import OrderedDict
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


def _parse_window(start, end):
    """(start_dt, end_dt, errors). Legacy messages; strptime errors -> RequestError (legacy 500)."""
    if not start or not end:
        raise RequestError("Missing start_date or end_date", legacy_status=400)
    # strptime errors surfaced as 500 in the old view (raised inside its try)
    try:
        start_dt = datetime.datetime.strptime(str(start), DATE_INPUT_FORMAT)
        end_dt = datetime.datetime.strptime(str(end), DATE_INPUT_FORMAT)
    except ValueError as e:
        raise RequestError(str(e), legacy_status=500)
    errors = []
    if end_dt - start_dt > datetime.timedelta(minutes=config.max_window_minutes()):
        errors.append("Time_Stamp should not exceed 30 min")
    if end_dt <= start_dt:
        errors.append("end_date must be after start_date")
    return start_dt, end_dt, errors


def _clean_msisdns(items):
    """(cleaned list in request order, errors) with the legacy MSISDN rule and message."""
    prefixes, length = config.msisdn_prefixes(), config.msisdn_length()
    clean, errors = [], []
    for msisdn in items:
        text = str(msisdn).strip()
        if not text.isdigit() or len(text) != length or not text.startswith(prefixes):
            errors.append(f"Invalid MSISDN: {msisdn}. "
                          "MSISDN must start with 10, 11, 12 or 15 and be exactly 10 digits")
        clean.append(text)
    return clean, errors


def parse_request(params) -> AnalysisRequest:
    msisdn_list = params.get("msisdn_list")
    start = params.get("start_date")
    end = params.get("end_date")

    if not start or not end:
        raise RequestError("Missing start_date or end_date", legacy_status=400)
    if not msisdn_list:
        raise RequestError("msisdn_list is required", legacy_status=400)
    if params.get("Complain_Type") not in ("Data", "Voice"):
        raise RequestError("Complain_Type must be Data or Voice", legacy_status=400)

    start_dt, end_dt, errors = _parse_window(start, end)

    items = msisdn_list if isinstance(msisdn_list, (list, tuple)) else list(str(msisdn_list))
    if len(items) < 1:
        errors.append("MSISDN list should contain at least 1 MSISDNs")
    clean, msisdn_errors = _clean_msisdns(items)
    errors.extend(msisdn_errors)
    if errors:
        raise RequestError("; ".join(errors), legacy_status=500)

    return AnalysisRequest(
        msisdns=clean, start=start_dt, end=end_dt,
        complain_type=params.get("Complain_Type"), imsi=params.get("imsi"),
        raw_start=str(start), raw_end=str(end), user=str(params.get("user") or "unknown"),
    )


# ------------------------------------------------- multi-complaint requests
@dataclass
class ComplaintGroup:
    index: int                       # 1-based position in the request
    category: str                    # canonical name (catalog.CATEGORY_COMPLAIN_TYPE)
    subcategory: str
    complain_type: str               # catalog.DATA / catalog.VOICE
    msisdns: List[str]               # unique, request order

    @property
    def key(self):
        return f"g{self.index:02d}"

    def to_params(self):
        return {"index": self.index, "category": self.category, "subcategory": self.subcategory,
                "complain_type": self.complain_type, "msisdn_list": self.msisdns}


@dataclass
class BatchRequest:
    groups: List[ComplaintGroup]
    start: datetime.datetime
    end: datetime.datetime
    raw_start: str
    raw_end: str
    user: str = "unknown"

    @property
    def msisdn_count(self):
        return sum(len(g.msisdns) for g in self.groups)

    def to_params(self):
        """JSON-safe dict for the queue / job record (`complaints` marks a multi-complaint job)."""
        return {"start_date": self.raw_start, "end_date": self.raw_end, "user": self.user,
                "complaints": [g.to_params() for g in self.groups]}

    def group_params(self, group):
        """The single-complaint params the existing pipeline.run() takes, for one group."""
        return {"msisdn_list": group.msisdns, "start_date": self.raw_start, "end_date": self.raw_end,
                "Complain_Type": group.complain_type, "imsi": None, "user": self.user}


def is_batch(params):
    return hasattr(params, "get") and params.get("complaints") is not None


def parse_batch(params) -> BatchRequest:
    """
    Validate a {"start_date", "end_date", "complaints": [...]} request. All
    problems are reported together; group errors are prefixed complaints[i]
    (0-based, like the JSON array). Always HTTP 400 (new contract).
    """
    from . import catalog

    if params.get("msisdn_list") or params.get("Complain_Type"):
        raise RequestError("Send either complaints or Complain_Type + msisdn_list, not both")
    try:
        start_dt, end_dt, errors = _parse_window(params.get("start_date"), params.get("end_date"))
    except RequestError as e:
        raise RequestError(e.message)

    complaints = params.get("complaints")
    if not isinstance(complaints, (list, tuple)) or not complaints:
        raise RequestError("; ".join(errors + ["complaints must be a non-empty list"]))
    if len(complaints) > config.max_complaint_groups():
        raise RequestError(f"complaints has {len(complaints)} groups; at most "
                           f"{config.max_complaint_groups()} are allowed per request")

    groups, seen = [], {}
    for i, item in enumerate(complaints):
        where = f"complaints[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{where} must be an object with category, subcategory and msisdn_list")
            continue
        category = catalog.resolve_category(item.get("category") or "")
        if category is None:
            errors.append(f"{where}.category {item.get('category')!r} is not supported; use one of: "
                          + ", ".join(catalog.CATEGORY_COMPLAIN_TYPE))
        subcategory = None
        if not str(item.get("subcategory") or "").strip():
            errors.append(f"{where}.subcategory is required")
        elif category is not None:
            subcategory, sub_error = catalog.resolve_subcategory(category, item["subcategory"])
            if sub_error:
                errors.append(f"{where}.subcategory: {sub_error}")
        msisdn_list = item.get("msisdn_list")
        if not isinstance(msisdn_list, (list, tuple)) or not msisdn_list:
            errors.append(f"{where}.msisdn_list must be a non-empty list")
            clean = []
        else:
            clean, msisdn_errors = _clean_msisdns(msisdn_list)
            errors.extend(f"{where}.msisdn_list: {e}" for e in msisdn_errors)
        if category is None or subcategory is None or not clean:
            continue
        pair = (category, subcategory.casefold())
        if pair in seen:
            errors.append(f"{where} repeats {category} / {subcategory} (already complaints[{seen[pair]}]); "
                          "put all its MSISDNs in one group")
            continue
        seen[pair] = i
        unique = list(OrderedDict.fromkeys(clean))
        groups.append(ComplaintGroup(index=len(groups) + 1, category=category, subcategory=subcategory,
                                     complain_type=catalog.complain_type_for(category), msisdns=unique))
    if errors:
        raise RequestError("; ".join(errors))
    return BatchRequest(groups=groups, start=start_dt, end=end_dt, raw_start=str(params.get("start_date")),
                        raw_end=str(params.get("end_date")), user=str(params.get("user") or "unknown"))


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
