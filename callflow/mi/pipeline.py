"""
The MI Smartcare analysis, in two separate layers:

  FETCH   (the only part that talks to Smartcare/DataCube)
          query plan (<= MI_MAX_QUERIES_PER_REQUEST statements, checked first)
          -> DataCube session reservation (jobs.DataCubeReservation)
          -> database_connection -> query_execution
          fetch_plan() runs each query of each Complain_Type ONCE for the
          MSISDNs of every complaint group of that type -> RawData.

  ANALYSE (pandas only, no database)
          data_validation -> aggregation -> hub_metrics -> result_generation
          analyse() takes the rows of the MSISDNs it is given
          (RawData.result_for) and runs the unchanged Prototype calculations.

run() = fetch + analyse for one Complain_Type and one MSISDN list (the
single-complaint request); batch.py fetches once and analyses every
complaint group from the shared rows. run() returns (http_status, body);
bodies and error shapes match the old Prototype view.
"""
import time
from collections import OrderedDict

import pandas as pd

from types import SimpleNamespace

from . import aggregation, catalog, config, hub, jobs, outputs, query_plan, response, validation
from .smartcare import Executor, Session, SessionPool, classify_error, to_python


def _connect_with_retries(session, ctx):
    attempts = config.max_attempts()
    for attempt in range(1, attempts + 1):
        try:
            session.cursor()
            return True
        except Exception as e:
            kind = classify_error(e)
            if kind == "config":
                ctx.add_warning("smartcare_not_configured", error=str(e))
                return False
            if attempt < attempts:
                time.sleep(min(config.retry_backoff_max_s(), config.retry_backoff_s() * (2 ** (attempt - 1))))
    ctx.add_warning("smartcare_unreachable", attempts=attempts)
    return False


def _report_outcome(ctx, outcome, role="query"):
    if outcome.status == "ok":
        return
    failed_tables = [t["table"] for t in outcome.tables if t["status"] != "ok"]
    ctx.add_warning(f"{role}_{outcome.status}", query=outcome.name, kind=outcome.error_kind,
                    tables=",".join(str(t) for t in failed_tables), error=outcome.error)


# Columns that carry the subscriber number in the Smartcare rows (VoLTE /
# REG queries name it IMPU_TEL_URI / IMPU_TEL_URI1).
SUBSCRIBER_FIELDS = ("MSISDN", "IMPU_TEL_URI1", "IMPU_TEL_URI")


def analyzed_msisdns(result, requested):
    """How many of the requested MSISDNs appear in at least one Smartcare row."""
    wanted = {str(m).strip() for m in requested}
    found = set()
    for col in SUBSCRIBER_FIELDS:
        if col in result.columns:
            found.update(str(v).strip() for v in result[col].dropna().unique())
    return len(wanted & found)


# ----------------------------------------------------------------- FETCH
def subscriber_key(value):
    """The requested-MSISDN form of a value read from a subscriber column."""
    text = str(to_python(value)).strip()
    if text.endswith(".0") and text[:-2].isdigit():      # numeric column read as float
        text = text[:-2]
    return text


class RawData:
    """
    What the queries of ONE Complain_Type returned for the MSISDNs of all
    complaint groups of that type (each query executed once).

    result_for(msisdns) rebuilds, for any subset of those MSISDNs, exactly the
    frame the old per-group run produced: per query, the rows whose subscriber
    column holds one of the MSISDNs, turned into a DataFrame the same way the
    Executor builds it, then the same subscriber-column mapping and concat.
    """

    def __init__(self, complain_type, msisdns, queries, outcomes):
        self.complain_type = complain_type
        self.msisdns = list(msisdns)
        self.outcomes = OrderedDict((name, outcome) for (name, _t), outcome in zip(queries, outcomes))
        self.query_statuses = OrderedDict((name, o.status) for name, o in self.outcomes.items())

    @property
    def rows(self):
        return sum(len(o.frame) for o in self.outcomes.values())

    def full_result(self):
        """All rows, as one frame (shared export / diagnostics)."""
        frames = [catalog.normalise_subscriber_column(name, o.frame)[0] for name, o in self.outcomes.items()]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def frame_for(self, name, wanted):
        """Query `name`'s rows for the MSISDNs in `wanted` (a set of strings)."""
        outcome = self.outcomes[name]
        column = catalog.QUERY_SUBSCRIBER_COLUMN[name]
        data = pd.DataFrame()                       # Executor.run starts from the same empty frame
        for cols, rows in outcome.parts:
            lookup = {str(c).casefold(): i for i, c in enumerate(cols)}
            if column.casefold() not in lookup:
                raise KeyError(f"query {name} returned no {column} column, so its rows cannot be split "
                               f"by complaint group (columns: {', '.join(map(str, cols))})")
            i = lookup[column.casefold()]
            subset = [row for row in rows if subscriber_key(row[i]) in wanted]
            df = pd.DataFrame(subset, columns=cols) if subset else pd.DataFrame(columns=cols)
            data = pd.concat([data, df], ignore_index=True)
        return data

    def result_for(self, msisdns, ctx=None):
        wanted = {str(m).strip() for m in msisdns}
        frames = []
        for name in self.outcomes:
            frame, renamed = catalog.normalise_subscriber_column(name, self.frame_for(name, wanted))
            if renamed and ctx is not None:
                ctx.event("subscriber_column_mapped", query=name, column=renamed, to="MSISDN", rows=len(frame))
            frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def unmatched_rows(self):
        """Rows whose subscriber value is none of the requested MSISDNs (expected 0)."""
        wanted = {str(m).strip() for m in self.msisdns}
        total = 0
        for name, outcome in self.outcomes.items():
            column = catalog.QUERY_SUBSCRIBER_COLUMN[name].casefold()
            for cols, rows in outcome.parts:
                idx = [i for i, c in enumerate(cols) if str(c).casefold() == column]
                if idx:
                    total += sum(1 for r in rows if subscriber_key(r[idx[0]]) not in wanted)
        return total


class FetchResult:
    def __init__(self, plan):
        self.plan = plan
        self.raw = OrderedDict()            # complain_type -> RawData
        self.error = None                   # e.g. 'Database connection failed'
        self.http_status = None
        self.budget = None

    def summary(self):
        out = OrderedDict(self.plan.summary())
        b = self.budget
        out.update(executed=b.executed if b else 0, retries=b.retries if b else 0,
                   retries_denied=len(b.denied) if b else 0)
        return out


def fetch_plan(plan, ctx, connect_fn=None, on_wait=None):
    """
    Execute a query plan: reserve its DataCube sessions, connect, run every
    planned query once (in parallel, MI_QUERY_PARALLELISM), Data first then
    Voice, all under one RequestBudget. Returns FetchResult; raises
    jobs.DataCubeBusy when the global budget stays full for MI_DATACUBE_WAIT_S.
    """
    fetched = FetchResult(plan)
    ctx.event("query_plan", planned=plan.total, limit=plan.limit, retry_allowance=plan.retry_allowance,
              tables=",".join(map(str, plan.tables)), union_day_tables=plan.union_day_tables,
              msisdns=";".join(f"{catalog.COMPLAIN_TYPE_LABEL.get(ct, ct)}={len(m)}"
                               for ct, m in plan.msisdns_by_type.items()),
              queries=plan.describe(), parallelism=config.query_parallelism())
    reservation = jobs.DataCubeReservation(ctx.job_id)
    session = pool = None
    try:
        with ctx.stage("datacube_reservation", sessions=plan.total) as rec:
            reservation.reserve(plan.total, on_wait=on_wait)
            rec["in_use_after"] = jobs.datacube_status().get("in_use")

        session = Session(ctx, connect_fn)
        with ctx.stage("database_connection") as rec:
            connected = _connect_with_retries(session, ctx)
            rec["status"] = "ok" if connected else "failed"
        if not connected:
            fetched.error, fetched.http_status = 'Database connection failed', 500
            return fetched

        pool = SessionPool(ctx, connect_fn, first=session)
        fetched.budget = query_plan.RequestBudget(plan.limit, plan.total, reservation)
        for complain_type, msisdns in plan.msisdns_by_type.items():
            request = SimpleNamespace(msisdns=msisdns, start=plan.window[0], end=plan.window[1])
            executor = Executor(ctx, request, pool, budget=fetched.budget)
            queries = [(q.name, q.template) for q in plan.queries_for(complain_type)]
            with ctx.stage("query_execution", complain_type=complain_type, msisdns=len(msisdns),
                           parallelism=config.query_parallelism()):
                outcomes = executor.run_many(queries)
            raw = RawData(complain_type, msisdns, queries, outcomes)
            fetched.raw[complain_type] = raw
            for name, outcome in raw.outcomes.items():
                ctx.event("query_result", complain_type=complain_type, query=name, status=outcome.status,
                          rows=len(outcome.frame), kind=outcome.error_kind or "none")
            unmatched = raw.unmatched_rows()
            if unmatched:
                ctx.warning("rows_without_requested_msisdn", complain_type=complain_type, rows=unmatched)
        ctx.event("datacube_queries_done", **fetched.summary())
        return fetched
    finally:
        if pool is not None:
            pool.close()                       # closes every session, including the first
        elif session is not None:
            session.close()
        reservation.finish(fetched.budget.executed if fetched.budget else 0)


def plan_for(msisdns_by_type, start, end):
    return query_plan.build_plan(msisdns_by_type, start, end)


# --------------------------------------------------------------- ANALYSE
def analyse(raw, msisdns, ctx, req_params=None):
    """
    The unchanged Prototype calculations for `msisdns` (all requested in one
    Complain_Type), on the rows already fetched in `raw`. No database access.
    Leaves on ctx: query_statuses, result_rows, analyzed_msisdns.
    """
    complain_type = raw.complain_type
    ctx.query_statuses = OrderedDict(raw.query_statuses)
    ctx.result_rows, ctx.analyzed_msisdns = None, None
    try:
        if req_params is not None:
            outputs.atomic_write_json(req_params, ctx.dir / "request.json")
        for outcome in raw.outcomes.values():
            _report_outcome(ctx, outcome)
        result = raw.result_for(msisdns, ctx)
        ctx.result_rows = len(result)
        ctx.analyzed_msisdns = analyzed_msisdns(result, msisdns)
        csv_path = outputs.export_csv(result, 'sc_nodes_raw.csv', ctx.dir, job_log=ctx.log)
        ctx.event("raw_result_exported", complain_type=complain_type, rows=len(result),
                  columns=len(result.columns), csv=csv_path)

        with ctx.stage("data_validation") as rec:
            requirements = (catalog.DATA_METRICS if complain_type == catalog.DATA
                            else catalog.voice_metrics())
            needed = sorted({c for cols in requirements.values() for c in cols})
            report = validation.column_report(result, needed)
            rec["rows"], rec["columns"] = len(result), len(result.columns)
            ctx.event("column_report", **{k: v for k, v in report.items()})

        export = lambda df, name: outputs.export_csv(df, name, ctx.dir, job_log=ctx.log)  # noqa: E731
        with ctx.stage("aggregation", complain_type=complain_type, rows=len(result)):
            if complain_type == catalog.DATA:
                metrics, cells = aggregation.aggregate_data(result, ctx)
            else:
                export(result, 'sc_nodes_before_voice.csv')
                metrics, cells = aggregation.aggregate_voice(result, ctx, export=export)

        with ctx.stage("hub_metrics", cells=len(cells)):
            ctx.event("hub_metrics_start", complain_type=complain_type, cells=len(cells),
                      sample_cells=",".join(map(str, cells[:50])))
            hub_counts = hub.affected_hub_metrics(result, cells, ctx, ctx.dir)

        with ctx.stage("result_generation"):
            export(result, 'sc_nodes.csv')
            body = (response.data_body(metrics, hub_counts) if complain_type == catalog.DATA
                    else response.voice_body(metrics, hub_counts))
        return 200, body
    except Exception as e:
        ctx.exception("analysis_error")
        return 500, response.error_body(str(e))


# ------------------------------------------------------------------- RUN
def run(params, ctx, connect_fn=None, on_wait=None):
    """
    Single-complaint request (one Complain_Type, one MSISDN list): plan ->
    fetch -> analyse. Besides (http_status, body) it leaves on ctx:
    query_statuses, result_rows, analyzed_msisdns, datacube (query counts).
    """
    ctx.query_statuses, ctx.result_rows, ctx.analyzed_msisdns = OrderedDict(), None, None
    ctx.datacube = None
    try:
        with ctx.stage("input_validation"):
            try:
                req = validation.parse_request(params)
            except validation.RequestError as e:
                ctx.event("input_invalid", error=e.message)
                return e.legacy_status, response.error_body(e.message)
            try:
                plan = plan_for({req.complain_type: req.msisdns}, req.start, req.end)
            except query_plan.QueryPlanError as e:
                ctx.event("query_plan_rejected", error=str(e))
                return 400, response.error_body(str(e))
        outputs.atomic_write_json(req.to_params(), ctx.dir / "request.json")
        outputs.legacy_static_log(f"\n{ctx.started_at} - MI Smartcare Request\n{req.user}\njob_id={ctx.job_id}\n")

        try:
            fetched = fetch_plan(plan, ctx, connect_fn, on_wait=on_wait)
        except jobs.DataCubeBusy as e:
            ctx.add_warning("datacube_busy", error=str(e))
            return 503, response.error_body(str(e))
        ctx.datacube = fetched.summary()
        if fetched.error:
            return fetched.http_status, response.error_body(fetched.error)
        return analyse(fetched.raw[req.complain_type], req.msisdns, ctx)
    except Exception as e:
        ctx.exception("analysis_error")
        return 500, response.error_body(str(e))
