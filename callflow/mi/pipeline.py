"""
The MI Smartcare analysis, stage by stage:

  input_validation -> database_connection -> query_execution -> fallback_queries
  -> data_validation -> aggregation -> hub_metrics -> result_generation

run() returns (http_status, body). Bodies and error shapes match the old
Prototype view. Each stage is timed and logged by JobContext.stage().
"""
import time

import pandas as pd

from . import aggregation, catalog, config, hub, outputs, response, validation
from .smartcare import Executor, Session, classify_error


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


def _apply_fallbacks(result, executor, main_ok, ctx, complain_type):
    """
    The Prototype fallback rules, with two fixes:
      * skip when the fallback query is a main query that already succeeded:
        its rows are already in `result`, so a re-run adds nothing. (Before,
        the re-run was merged in and an existing all-null column became
        VPCRF_NAME_x / VPCRF_NAME_y -> KeyError 'VPCRF_NAME' downstream.)
      * an all-null column is dropped before merging the fallback values in,
        so the merge cannot create _x/_y columns either.
    """
    for column, qname, template in catalog.FALLBACKS:
        if column in result.columns and not result[column].isna().all():
            continue
        state = "missing" if column not in result.columns else "all_null"
        if catalog.fallbacks_are_ineffective(complain_type):
            ctx.event("fallback_skipped", column=column, reason="cannot affect response",
                      setting="MI_SKIP_INEFFECTIVE_QUERIES")
            continue
        if id(template) in main_ok:
            ctx.event("fallback_skipped", column=column, column_state=state, query=qname,
                      reason="query already succeeded in this job; its rows are already present")
            continue
        ctx.event("fallback_start", column=column, column_state=state, query=qname)
        with ctx.stage("fallback_query", column=column, query=qname) as rec:
            outcome = executor.run(f"fallback:{column}:{qname}", template)
            rec["status"] = outcome.status
        _report_outcome(ctx, outcome, role="fallback")
        fb = outcome.frame
        if fb.empty or "MSISDN" not in fb.columns or column not in fb.columns:
            ctx.event("fallback_no_data", column=column, rows=len(fb))
            continue
        fb = fb[["MSISDN", column]]
        if column in result.columns:
            result = result.drop(columns=[column])
        before = len(result)
        result = result.merge(fb, on="MSISDN", how="left").drop_duplicates()
        ctx.event("fallback_merged", column=column, rows_before=before, rows_after=len(result))
    return result


def run(params, ctx, connect_fn=None):
    session = None
    try:
        with ctx.stage("input_validation"):
            try:
                req = validation.parse_request(params)
            except validation.RequestError as e:
                ctx.event("input_invalid", error=e.message)
                return e.legacy_status, response.error_body(e.message)
        outputs.atomic_write_json(req.to_params(), ctx.dir / "request.json")
        outputs.legacy_static_log(f"\n{ctx.started_at} - MI Smartcare Request\n{req.user}\njob_id={ctx.job_id}\n")

        session = Session(ctx, connect_fn)
        with ctx.stage("database_connection") as rec:
            connected = _connect_with_retries(session, ctx)
            rec["status"] = "ok" if connected else "failed"
        if not connected:
            return 500, response.error_body('Database connection failed')

        executor = Executor(ctx, req, session)
        ctx.event("query_plan", complain_type=req.complain_type, tables=",".join(map(str, executor.tables)),
                  queries=",".join(catalog.query_set(req.complain_type).keys()))
        frames, main_ok = [], set()
        with ctx.stage("query_execution"):
            for name, template in catalog.query_set(req.complain_type).items():
                with ctx.stage("query", query=name) as rec:
                    outcome = executor.run(name, template)
                    rec["status"], rec["rows"] = outcome.status, len(outcome.frame)
                _report_outcome(ctx, outcome)
                if outcome.complete:
                    main_ok.add(id(template))
                frames.append(outcome.frame)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        with ctx.stage("fallback_queries"):
            result = _apply_fallbacks(result, executor, main_ok, ctx, req.complain_type)
        session.close()                        # no more Smartcare work in this job

        with ctx.stage("data_validation") as rec:
            requirements = (catalog.DATA_METRICS if req.complain_type == catalog.DATA
                            else catalog.voice_metrics())
            needed = sorted({c for cols in requirements.values() for c in cols})
            report = validation.column_report(result, needed)
            rec["rows"], rec["columns"] = len(result), len(result.columns)
            ctx.event("column_report", **{k: v for k, v in report.items()})

        export = lambda df, name: outputs.export_csv(df, name, ctx.dir, job_log=ctx.log)  # noqa: E731
        with ctx.stage("aggregation", complain_type=req.complain_type, rows=len(result)):
            if req.complain_type == catalog.DATA:
                metrics, cells = aggregation.aggregate_data(result, ctx)
            else:
                export(result, 'sc_nodes_before_voice.csv')
                metrics, cells = aggregation.aggregate_voice(result, ctx, export=export)

        with ctx.stage("hub_metrics", cells=len(cells)):
            hub_counts = hub.affected_hub_metrics(result, cells, ctx, ctx.dir)

        with ctx.stage("result_generation"):
            export(result, 'sc_nodes.csv')
            body = (response.data_body(metrics, hub_counts) if req.complain_type == catalog.DATA
                    else response.voice_body(metrics, hub_counts))
        return 200, body

    except Exception as e:
        ctx.exception("analysis_error")
        return 500, response.error_body(str(e))
    finally:
        if session is not None:
            session.close()
