"""
Data / Voice aggregation for MI.

The pandas expressions are the ones smartcareOutput_Aggregated_Data /
smartcareOutput_Aggregated_Voice used, so values are unchanged. What changed:

* Each metric is computed independently and guarded by its required columns
  (catalog.DATA_METRICS / voice_metrics()). Before, the whole function ran in
  one try/except, so the first missing column (production: 'VPCRF_NAME',
  'Voice_Data_Flag') silently emptied every metric computed after it.
* Computed once. The old view called the Data aggregation up to 6 times on
  the same frame; it never re-queried, so attempts 2..6 were identical.
* A metric that cannot be computed keeps its old empty value ({} - or [] for
  suspected_sgws, as before) and is reported via ctx.add_warning.
"""
import pandas as pd

from . import catalog


def distinct_count(df, group_col, value_col):
    """Unchanged from the Prototype code."""
    return (
        df[df[group_col].fillna('').str.strip() != '']
        .groupby(group_col)[value_col]
        .nunique()
        .to_dict()
    )


def site_letters(site_names, site_numbers):
    """Exactly str(site_name).replace(str(site_number), '') per row, without apply(axis=1)."""
    return pd.Series([str(a).replace(str(b), '') for a, b in zip(site_names, site_numbers)],
                     index=site_names.index, dtype=object)


def add_site_columns(frame, cell_col):
    """The Prototype 'SITE LOGIC' block (note: split('_L').str[1], not [-1])."""
    frame['site_name'] = frame[cell_col].str.split('_L').str[1]
    frame['site_name'] = frame['site_name'].str.split('_').str[0]
    frame['site_name_number'] = frame['site_name'].str.extract(r'(\d+)')
    frame['site_name_number'] = frame['site_name_number'].astype(str)
    frame['site_name_letter'] = site_letters(frame['site_name'], frame['site_name_number'])
    frame['modified_site_name_number'] = frame['site_name_number'].str.zfill(4)
    frame['modified_site_name'] = frame['site_name_letter'] + frame['modified_site_name_number']
    return frame


def _base_unavailable():
    raise KeyError("base row filter unavailable")


class _Metrics:
    def __init__(self, ctx, frame, requirements, result):
        self.ctx = ctx
        self.frame = frame
        self.requirements = requirements
        self.result = result
        self.unavailable = []

    def compute(self, key, fn, frame=None):
        frame = self.frame if frame is None else frame
        needed = self.requirements[key]
        missing = [c for c in needed if c not in frame.columns]
        if missing:
            self.unavailable.append(key)
            self.ctx.add_warning("metric_unavailable", metric=key, missing_columns=",".join(missing))
            return
        try:
            self.result[key] = fn()
        except Exception as e:
            self.unavailable.append(key)
            self.ctx.add_warning("metric_failed", metric=key, error=f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------- Data
DATA_EMPTY = (("suspected_sites", dict), ("suspected_cells_raw", dict), ("suspected_sgws", list),
              ("rat_counts", dict), ("DRA_NAME", dict), ("OCS_NAME", dict), ("PCRF_NAME", dict),
              ("region_counts", dict), ("subregion_counts", dict), ("sgsn_counts", dict),
              ("tac_counts", dict), ("mme_counts", dict))


def aggregate_data(nodes, ctx):
    """Returns (metrics dict keyed like the API response minus HUB_Counts, cell_list)."""
    out = {k: factory() for k, factory in DATA_EMPTY}
    data = nodes.fillna('')
    if data.empty:
        ctx.add_warning("no_smartcare_rows", complain_type="Data")
        return out, []
    for col in data.select_dtypes(include=["object"]).columns:
        data[col] = data[col].astype(str)
    req = catalog.DATA_METRICS
    m = _Metrics(ctx, data, req, out)
    ims = data.copy()

    base = None
    if all(c in data.columns for c in catalog.DATA_BASE):
        try:
            base = data.dropna(subset=['cell_name', 'SGW_SGSN_NAME'])
            base = base[(base['cell_name'].str.strip() != '') & (base['SGW_SGSN_NAME'].str.strip() != '')]
        except Exception as e:
            ctx.add_warning("base_filter_failed", error=f"{type(e).__name__}: {e}")
            base = None

    if base is not None:
        b = base
        m.compute("suspected_cells_raw", lambda: b.groupby('cell_name')['MSISDN'].nunique().to_dict(), b)
        m.compute("rat_counts", lambda: b.groupby('RAT')['MSISDN'].nunique().to_dict(), b)
        m.compute("region_counts", lambda: b.groupby('REGION')['MSISDN'].nunique().to_dict(), b)
        m.compute("subregion_counts", lambda: b.groupby('SUBREGION')['MSISDN'].nunique().to_dict(), b)
        m.compute("tac_counts", lambda: b.groupby('TAC_DECIMAL')['MSISDN'].nunique().to_dict(), b)
        m.compute("sgsn_counts", lambda: b.groupby('SGW_SGSN_NAME')['MSISDN'].nunique().to_dict(), b)
        m.compute("mme_counts", lambda: b.groupby('MME_Name')['MSISDN'].nunique().to_dict(), b)

        def sites():
            s = add_site_columns(b.copy(), 'cell_name')
            return s.groupby('modified_site_name')['MSISDN'].nunique().to_dict()
        m.compute("suspected_sites", sites, b)

        def sgws():
            s = b.rename(columns={'SGW_SGSN_NAME': 'SGW_PGW_NAME'})
            s['SGW_PGW_NAME'] = s['SGW_PGW_NAME'].str.replace('_vSGW-vPGW_', '', regex=False)
            s['SGW_PGW_NAME'] = s['SGW_PGW_NAME'].str.replace('_SGW-PGW_', '', regex=False)
            return s.groupby('SGW_PGW_NAME')['MSISDN'].nunique().to_dict()
        m.compute("suspected_sgws", sgws, b)
    else:
        for key in ("suspected_sites", "suspected_cells_raw", "suspected_sgws", "rat_counts", "region_counts",
                    "subregion_counts", "sgsn_counts", "tac_counts", "mme_counts"):
            m.compute(key, _base_unavailable)

    # if 'Voice_Data_Flag' in ims.columns:
    #     flagged = ims[ims['Voice_Data_Flag'] == 'Data']
    # else:
    #     flagged = ims
    m.compute("DRA_NAME", lambda: distinct_count(ims, 'DRA_NAME', 'MSISDN'), ims)
    m.compute("OCS_NAME", lambda: distinct_count(ims, 'OCS_NAME', 'MSISDN'), ims)
    m.compute("VPCRF_NAME", lambda: distinct_count(ims, 'VPCRF_NAME', 'MSISDN'), ims)

    ctx.event("aggregation_result", complain_type="Data", rows=len(data),
              base_rows=0 if base is None else len(base),
              unavailable=",".join(m.unavailable) or "none",
              **{f"n_{k}": len(v) for k, v in out.items()})
    return out, list(out["suspected_cells_raw"].keys())


# ---------------------------------------------------------------- Voice
VOICE_KEYS = ("Suspected_Sites", "Suspected_Cells", "Cluster_Counts", "Region_Counts", "Sub_Region_Counts",
              "CSCF_Name", "AS_Name", "MSC_Name", "DRA_NAME", "OCS_NAME", "PCRF_NAME" , "MME_Name")


def aggregate_voice(nodes, ctx, export=None):
    """Returns (metrics dict keyed like the Voice response minus HUB_Counts, cell_list)."""
    out = {k: {} for k in VOICE_KEYS}
    data = nodes.fillna('')
    if export is not None:
        export(data, 'sc_data.csv')          # the old code dumped this frame here
    if data.empty:
        ctx.add_warning("no_smartcare_rows", complain_type=ctx.complain_type)
        return out, []
    for col in data.select_dtypes(include=["object"]).columns:
        data[col] = data[col].astype(str)
    req = catalog.voice_metrics()
    m = _Metrics(ctx, data, req, out)
    ims = data.copy()

    base = None
    if all(c in data.columns for c in catalog.VOICE_BASE):
        try:
            base = data.dropna(subset=['CELL_NAME'])
            base = base[(base['CELL_NAME'].str.strip() != '')]
        except Exception as e:
            ctx.add_warning("base_filter_failed", error=f"{type(e).__name__}: {e}")
            base = None
    if base is not None:
        b = base
        m.compute("Suspected_Cells", lambda: b.groupby('CELL_NAME')['MSISDN'].nunique().to_dict(), b)
        m.compute("Cluster_Counts", lambda: b.groupby('CLUSTER')['MSISDN'].nunique().to_dict(), b)
        m.compute("Region_Counts", lambda: b.groupby('REGION')['MSISDN'].nunique().to_dict(), b)
        m.compute("Sub_Region_Counts", lambda: b.groupby('SUBREGION')['MSISDN'].nunique().to_dict(), b)

        def sites():
            s = add_site_columns(b.copy(), 'CELL_NAME')
            return s.groupby('modified_site_name')['MSISDN'].nunique().to_dict()
        m.compute("Suspected_Sites", sites, b)
    else:
        for key in ("Suspected_Sites", "Suspected_Cells", "Cluster_Counts", "Region_Counts", "Sub_Region_Counts"):
            m.compute(key, _base_unavailable)

    m.compute("CSCF_Name", lambda: distinct_count(ims, 'p_cscf_Name', 'IMPU_TEL_URI1'), ims)
    m.compute("AS_Name", lambda: distinct_count(ims, 'AS_NAME', 'IMPU_TEL_URI1'), ims)
    m.compute("MSC_Name", lambda: distinct_count(ims, 'MSC_NAME', 'MSISDN'), ims)

    # DRA / OCS / PCRF: taken as is from what smartcare_DRA_Voice / _OCS_Voice /
    # _PCRF_Voice return (only those rows carry DRA_NAME / OCS_NAME / VPCRF_NAME;
    # distinct_count ignores rows where the name is empty). No row filter - the
    # old Voice_Data_Flag == 'Voice' filter matched no Voice row, so these three
    # metrics were always empty.
    ctx.debug("ims columns", columns=",".join(ims.columns), rows=len(ims))
    m.compute("DRA_NAME", lambda: distinct_count(ims, 'DRA_NAME', 'MSISDN'), ims)
    m.compute("OCS_NAME", lambda: distinct_count(ims, 'OCS_NAME', 'MSISDN'), ims)
    m.compute("PCRF_NAME", lambda: distinct_count(ims, 'PCRF_NAME', 'MSISDN'), ims)
    m.compute("MME_Name", lambda: distinct_count(ims, 'MME_Name', 'MSISDN'), ims)

    ctx.event("aggregation_result", complain_type=ctx.complain_type, rows=len(data),
              base_rows=0 if base is None else len(base),
              unavailable=",".join(m.unavailable) or "none",
              **{f"n_{k}": len(v) for k, v in out.items()})
    return out, list(out["Suspected_Cells"].keys())