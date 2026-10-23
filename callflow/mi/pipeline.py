"""
The MI Smartcare analysis, stage by stage:

  input_validation -> database_connection -> query_execution
  -> data_validation -> aggregation -> hub_metrics -> result_generation

Each Complain_Type runs only its own query set (catalog.query_set); there are
no fallback queries.

run() returns (http_status, body). Bodies and error shapes match the old
Prototype view. Each stage is timed and logged by JobContext.stage().
"""
import time

import pandas as pd

from . import aggregation, catalog, config, hub, outputs, response, validation
from .smartcare import Executor, Session, SessionPool, classify_error


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


def run(params, ctx, connect_fn=None):
    session = pool = None
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

        pool = SessionPool(ctx, connect_fn, first=session)
        executor = Executor(ctx, req, pool)
        queries = list(catalog.query_set(req.complain_type).items())
        ctx.event("query_plan", complain_type=req.complain_type, tables=",".join(map(str, executor.tables)),
                  queries=",".join(name for name, _ in queries), parallelism=config.query_parallelism())
        frames = []
        with ctx.stage("query_execution", parallelism=config.query_parallelism()):
            outcomes = executor.run_many(queries)
            for (name, template), outcome in zip(queries, outcomes):    # original order
                _report_outcome(ctx, outcome)
                frame, renamed = catalog.normalise_subscriber_column(name, outcome.frame)
                if renamed:
                    ctx.event("subscriber_column_mapped", query=name, column=renamed, to="MSISDN",
                              rows=len(frame))
                frames.append(frame)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        csv_path = outputs.export_csv(result, 'sc_nodes_raw.csv', ctx.dir, job_log=ctx.log)
        ctx.event("raw_result_exported", complain_type=req.complain_type, rows=len(result),
                  columns=len(result.columns), csv=csv_path)
        ctx.event("smartcare_sessions", opened=pool.connects)
        pool.close()                           # no more Smartcare work in this job

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
            ctx.event("hub_metrics_start", complain_type=req.complain_type, cells=len(cells),
                      sample_cells=",".join(map(str, cells[:50])))
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
        if pool is not None:
            pool.close()                       # closes every session, including the first
        elif session is not None:
            session.close()
