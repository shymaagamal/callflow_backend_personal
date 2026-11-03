"""
Multi-complaint MI requests: one parent job, one existing pipeline.run() per
complaint group.

    POST {"start_date", "end_date", "complaints": [{category, subcategory, msisdn_list}, ...]}

* Category -> Complain_Type comes from catalog.CATEGORY_COMPLAIN_TYPE.
* The parent job holds ONE slot (runner.execute_job) and runs the groups one
  after another, so Smartcare load stays MI_MAX_CONCURRENT x
  MI_QUERY_PARALLELISM sessions and no extra threads are started. Inside a
  group the queries still run in parallel (Executor.run_many), with the usual
  bounded retries and retry budget.
* Each group gets its own JobContext: folder MI/jobs/<day>/<job_id>/groups/gNN/
  (job.log, request.json, summary.json, CSVs), job_id "<job_id>-gNN", and tags
  parent_job_id / group / category / subcategory / complain_type on every line.
  Groups never share files, and a failing group does not stop the others.
* After every group the categorised body is written to the job record, so the
  status endpoint shows finished groups while the rest are still running.

Group status
    COMPLETED            Smartcare answered; analysis_result is the pipeline body
    PARTIALLY_COMPLETED  some queries failed after retries; analysis_result is
                         what the rest returned (see failed_queries / warnings)
    NO_DATA              every query succeeded but returned no rows
    FAILED               the analysis failed (connection, crash, or every
                         query that could return rows failed); see error
    PENDING / RUNNING    not finished yet
Parent status
    COMPLETED            every group COMPLETED or NO_DATA
    FAILED               every group FAILED
    PARTIALLY_COMPLETED  anything in between
"""
import time
from collections import OrderedDict

from . import catalog, outputs, pipeline, validation
from .mi_logging import JobContext
from .smartcare import to_python_deep

COMPLETED = "COMPLETED"
PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
NO_DATA = "NO_DATA"
FAILED = "FAILED"
PENDING = "PENDING"
RUNNING = "RUNNING"

# Executor outcome statuses that mean Smartcare did not answer (after retries).
# "unavailable" (day table not created) is not a failure: there is no data.
QUERY_FAILURES = ("failed", "partial", "gave_up")


# ------------------------------------------------------------- statuses
def group_status(http_status, body, gctx):
    """(status, error, failed_queries) of one finished group."""
    statuses = getattr(gctx, "query_statuses", None) or {}
    failed = [name for name, status in statuses.items() if status in QUERY_FAILURES]
    if http_status >= 400:
        error = body.get("error") if isinstance(body, dict) else str(body)
        return FAILED, error or f"analysis failed (HTTP {http_status})", failed
    if not getattr(gctx, "result_rows", None):
        if failed:
            return FAILED, "Smartcare queries failed: " + ", ".join(failed), failed
        return NO_DATA, None, failed
    if failed:
        return PARTIALLY_COMPLETED, None, failed
    return COMPLETED, None, failed


def overall_status(statuses):
    statuses = list(statuses)
    if statuses and all(s == FAILED for s in statuses):
        return FAILED
    if all(s in (COMPLETED, NO_DATA) for s in statuses):
        return COMPLETED
    return PARTIALLY_COMPLETED


def http_status_for(status):
    return 500 if status == FAILED else 200


# ------------------------------------------------------------------ body
def _entry(group, status=PENDING):
    return OrderedDict([
        ("group", group.key),
        ("category", group.category),
        ("complain_type", group.complain_type),
        ("subcategory", group.subcategory),
        ("requested_msisdns", len(group.msisdns)),
        ("analyzed_msisdns", None),
        ("status", status),
        ("analysis_result", None),
        ("error", None),
        ("failed_queries", []),
        ("warnings", []),
        ("duration_s", None),
    ])


def build_body(job_id, status, raw_start, raw_end, entries):
    """
    {"job_id", "status", "start_date", "end_date", "summary", "results": [
       {"category", "complain_type": "DATA"|"VOICE",
        "subcategories": [{"subcategory", "requested_msisdns", "analyzed_msisdns",
                           "status", "analysis_result", "error", ...}]}]}
    Categories and subcategories keep the request order.
    """
    categories = OrderedDict()
    for e in entries:
        cat = categories.setdefault((e["category"], e["complain_type"]), OrderedDict([
            ("category", e["category"]),
            ("complain_type", catalog.COMPLAIN_TYPE_LABEL.get(e["complain_type"], e["complain_type"])),
            ("subcategories", []),
        ]))
        cat["subcategories"].append(OrderedDict((k, v) for k, v in e.items()
                                                if k not in ("category", "complain_type")))
    counts = OrderedDict((s, 0) for s in (COMPLETED, PARTIALLY_COMPLETED, NO_DATA, FAILED, RUNNING, PENDING))
    for e in entries:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    summary = OrderedDict([("groups", len(entries))] + [(k.lower(), v) for k, v in counts.items()])
    return OrderedDict([("job_id", job_id), ("status", status), ("start_date", raw_start),
                        ("end_date", raw_end), ("summary", summary), ("results", list(categories.values()))])


def _entries_from_body(body):
    for cat in (body or {}).get("results") or []:
        for sub in cat.get("subcategories") or []:
            yield cat, sub


def mark_lost(job, message):
    """
    The worker died mid-batch (no heartbeat): keep the groups that finished,
    fail the rest. Returns (status, http_status, body) for the job record.
    """
    body = job.get("result")
    if not isinstance(body, dict) or "results" not in body:
        return FAILED, 500, {"job_id": job.get("job_id"), "status": FAILED, "error": message, "results": []}
    statuses = []
    for _cat, sub in _entries_from_body(body):
        if sub.get("status") in (PENDING, RUNNING):
            sub["status"], sub["error"] = FAILED, message
        statuses.append(sub["status"])
    status = overall_status(statuses)
    body["status"] = status
    summary = OrderedDict([("groups", len(statuses))])
    for s in (COMPLETED, PARTIALLY_COMPLETED, NO_DATA, FAILED, RUNNING, PENDING):
        summary[s.lower()] = statuses.count(s)
    body["summary"] = summary
    return status, http_status_for(status), body


def _compact_warnings(items, limit=20):
    out = []
    for w in items[:limit]:
        item = {k: v for k, v in w.items() if v not in (None, "")}
        if isinstance(item.get("error"), str) and len(item["error"]) > 300:
            item["error"] = item["error"][:300] + "..."
        out.append(item)
    return out


# ------------------------------------------------------------- execution
def _run_group(job_id, req, group, ctx, connect_fn):
    """Run the existing pipeline for one group in its own JobContext; never raises."""
    entry = _entry(group, RUNNING)
    tags = OrderedDict([("parent_job_id", job_id), ("group", group.key), ("category", group.category),
                        ("subcategory", group.subcategory), ("complain_type", group.complain_type)])
    started = time.perf_counter()
    http_status, body = 500, {"error": "group did not run"}
    gctx = None
    try:
        gctx = JobContext(f"{job_id}-{group.key}", who=req.user, complain_type=group.complain_type,
                          msisdn_count=len(group.msisdns), window={"start": req.raw_start, "end": req.raw_end},
                          mode=ctx.mode, day=ctx.day, tags=tags, directory=ctx.dir / "groups" / group.key)
        with gctx:
            http_status, body = pipeline.run(req.group_params(group), gctx, connect_fn=connect_fn)
            body = to_python_deep(body)
            status, error, failed = group_status(http_status, body, gctx)
            gctx.status, gctx.http_status, gctx.error = status, http_status, error
            try:
                outputs.atomic_write_json({"http_status": http_status, "status": status, "body": body},
                                          gctx.dir / "result.json")
            except Exception:
                gctx.exception("result_write_failed")
    except Exception as e:                      # JobContext already logged the traceback
        status, error, failed = FAILED, f"{type(e).__name__}: {e}", []
        ctx.exception("group_crashed", **tags)
    if gctx is not None:
        ctx.smartcare_seconds += gctx.smartcare_seconds
    entry.update(
        analyzed_msisdns=getattr(gctx, "analyzed_msisdns", None),
        status=status,
        analysis_result=body if status in (COMPLETED, PARTIALLY_COMPLETED) else None,
        error=error,
        failed_queries=failed,
        warnings=_compact_warnings(gctx.warnings if gctx else []),
        duration_s=round(time.perf_counter() - started, 3),
    )
    ctx.event("group_done", group=group.key, category=group.category, subcategory=group.subcategory,
              complain_type=group.complain_type, requested_msisdns=len(group.msisdns),
              analyzed_msisdns=entry["analyzed_msisdns"], status=status, http_status=http_status,
              duration_s=entry["duration_s"], failed_queries=",".join(failed) or "none",
              warnings=len(entry["warnings"]), error=error or "none")
    return entry


def run_groups(job_id, params, ctx, connect_fn=None, publish=None):
    """
    Run every complaint group of a multi-complaint job.
    Returns (http_status, body, status, error). `publish(**fields)` stores
    progress on the job record (jobs.update_job bound to job_id).
    """
    publish = publish or (lambda **_: None)
    try:
        req = validation.parse_batch(params)
    except validation.RequestError as e:
        ctx.event("input_invalid", error=e.message)
        return 400, {"error": e.message}, FAILED, e.message
    outputs.atomic_write_json(req.to_params(), ctx.dir / "request.json")
    groups = req.groups
    ctx.event("batch_plan", groups=len(groups), msisdns=req.msisdn_count,
              plan=";".join(f"{g.key}={g.category}/{g.subcategory}/{g.complain_type}/{len(g.msisdns)}"
                            for g in groups))

    entries = [_entry(g) for g in groups]
    for i, group in enumerate(groups):
        entries[i]["status"] = RUNNING
        publish(step=f"group {i + 1}/{len(groups)}: {group.category} / {group.subcategory}",
                progress_percent=10 + int(85 * i / len(groups)), groups_done=i,
                result=build_body(job_id, RUNNING, req.raw_start, req.raw_end, entries))
        with ctx.stage("group", group=group.key, category=group.category, subcategory=group.subcategory,
                       complain_type=group.complain_type, msisdns=len(group.msisdns)) as rec:
            entries[i] = _run_group(job_id, req, group, ctx, connect_fn)
            rec["status"] = entries[i]["status"].lower()
        if entries[i]["status"] != COMPLETED:
            ctx.add_warning(f"group_{entries[i]['status'].lower()}", group=group.key, category=group.category,
                            subcategory=group.subcategory, error=entries[i]["error"])
    publish(groups_done=len(groups), progress_percent=95)

    status = overall_status(e["status"] for e in entries)
    body = build_body(job_id, status, req.raw_start, req.raw_end, entries)
    failed = [f"{e['category']} / {e['subcategory']}: {e['error']}" for e in entries if e["status"] == FAILED]
    error = "; ".join(failed)[:2000] if failed else None
    ctx.event("batch_done", status=status, groups=len(groups),
              **{k: v for k, v in body["summary"].items() if k != "groups"})
    return http_status_for(status), body, status, error
