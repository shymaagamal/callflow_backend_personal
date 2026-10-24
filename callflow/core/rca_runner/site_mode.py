# callflow/core/rca_runner/site_mode.py
"""
Request bodies of the two RCA run endpoints, and the inputs of the site run.

POST /callflow/api/rca/run         (MSISDN run, existing flow)
    {"msisdn": "01012345678,01098765432", "complain_type": "data",
     "start": "2026-10-21 10:00:00", "end": "2026-10-21 12:00:00"}
    The earlier keys still work: msisdn_list / Complain_Type / imsi / start_date / end_date.

POST /callflow/api/rca/sites/run   (site run)
    {"sitelist": ["SITE_A", "SITE_B"], "start": "...", "end": "..."}

The site run does not query Trino. The requested sites go straight to the
systems' existing retrieval functions (rca_runner/collect.py):
    NPP        nppIntegration(entity_type="site")  (NPP site endpoint)
    Viavi      viaviQueries(sites)
    Remedy     remedyTicketsData(sites)
    Splunk     unchanged (not site based)
    Smartcare  fetch_smartcare_site_kpis (the SmartcareSiteKpiAPIView implementation)
               for the sites and the request's time range
"""

import re

from ..services.smartcare_site_kpi_service import fetch_smartcare_site_kpis

_MSISDN_KEYS = ("msisdn", "msisdn_list")
_MSISDN_ONLY_KEYS = _MSISDN_KEYS + ("complain_type", "Complain_Type", "imsi")
_COMPLAIN_TYPES = {"data": "Data", "voice": "Voice"}
_MSISDN_RE = re.compile(r"^\d{6,15}$")
# Site ids go into the Smartcare SQL IN (...) list and the NPP/Viavi/Remedy
# queries, so only plain id characters are accepted.
_SITE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")

MSISDN_FORMAT = '{"msisdn", "complain_type", "start", "end"}'
SITE_FORMAT = '{"sitelist", "start", "end"}'


def _present(v):
    return v is not None and not (isinstance(v, str) and not v.strip()) \
        and not (isinstance(v, (list, tuple)) and not v)


def _as_list(value, name):
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple)):
        items = value
    else:
        raise ValueError(f"'{name}' must be a list or a comma-separated string")
    out = []
    for item in items:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    if not out:
        raise ValueError(f"'{name}' is empty")
    return out


def _one(data, *keys):
    vals = [data.get(k) for k in keys if _present(data.get(k))]
    if len({str(v) for v in vals}) > 1:
        raise ValueError(f"'{keys[0]}' and '{keys[1]}' were both given with different values")
    return vals[0] if vals else None


def _time_range(data):
    if not isinstance(data, dict):
        raise ValueError("the request body must be a JSON object")
    start = _one(data, "start", "start_date")
    end = _one(data, "end", "end_date")
    if not start or not end:
        raise ValueError("'start' and 'end' are required")
    return str(start), str(end)


def parse_msisdn_request(data):
    """Body of POST /rca/run -> {start, end, msisdn_list, complain_type, imsi}."""
    start, end = _time_range(data)
    if _present(data.get("sitelist")):
        raise ValueError("'sitelist' is not accepted here; send site requests "
                         f"{SITE_FORMAT} to /callflow/api/rca/sites/run")
    if not any(_present(data.get(k)) for k in _MSISDN_KEYS):
        raise ValueError(f"'msisdn' is required; expected {MSISDN_FORMAT}")
    msisdns = _as_list(_one(data, "msisdn", "msisdn_list"), "msisdn")
    bad = [m for m in msisdns if not _MSISDN_RE.match(m)]
    if bad:
        raise ValueError(f"invalid MSISDN value(s): {bad}")
    ctype = _one(data, "complain_type", "Complain_Type")
    if ctype is None:
        complain_type = "Data"                      # previous default
    else:
        # The Smartcare service compares against "Data" / "Voice" exactly.
        complain_type = _COMPLAIN_TYPES.get(str(ctype).strip().lower())
        if complain_type is None:
            raise ValueError(f"'complain_type' must be one of: data, voice (got {ctype!r})")
    return {"start": start, "end": end, "msisdn_list": msisdns,
            "complain_type": complain_type, "imsi": str(data.get("imsi") or "")}


def parse_site_request(data):
    """Body of POST /rca/sites/run -> {start, end, sitelist}."""
    start, end = _time_range(data)
    mixed = [k for k in _MSISDN_ONLY_KEYS if _present(data.get(k))]
    if mixed:
        raise ValueError(", ".join(f"'{k}'" for k in mixed) + " not accepted here; send MSISDN "
                         f"requests {MSISDN_FORMAT} to /callflow/api/rca/run")
    if not _present(data.get("sitelist")):
        raise ValueError(f"'sitelist' is required; expected {SITE_FORMAT}")
    sites = _as_list(data["sitelist"], "sitelist")
    bad = [s for s in sites if not _SITE_RE.match(s)]
    if bad:
        raise ValueError(f"invalid site id(s) in 'sitelist': {bad}")
    return {"start": start, "end": end, "sitelist": sites}


# ------------------------------------------------------------ site sources --
def fetch_smartcare(ctx):
    """The SmartcareSiteKpiAPIView implementation for the sites and this request's range.

    fetch_smartcare_site_kpis reports "no cells for these sites" and per-query
    failures inside its result instead of raising; a result with no rows and only
    query errors is a failure, not an empty success."""
    kpis = fetch_smartcare_site_kpis(ctx["sitelist"], ctx.get("log_path", ""),
                                     start=ctx["start_dt"], end=ctx["end_dt"])
    rows = sum(len(v) for w in (kpis.get("windows") or {}).values()
               for v in w.values() if isinstance(v, list))
    if not rows and kpis.get("errors"):
        raise RuntimeError(f"Smartcare site KPI queries failed: {kpis['errors']}")
    return {"site_run": True, "site_kpis": kpis}


def filter_csv(sites):
    """RCA filter file for the site run: the requested sites (SITE_NAME), so the
    tool scopes its analysis to them."""
    return "SITE_NAME\n" + "".join(f"{s}\n" for s in sites)
