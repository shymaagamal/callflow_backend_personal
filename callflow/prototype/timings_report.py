"""
Summarise callflow_output/logs/prototype_timings.jsonl.

    ../portalenv/bin/python -m callflow.prototype.timings_report            # last 50 requests
    ../portalenv/bin/python -m callflow.prototype.timings_report --last 200
    ../portalenv/bin/python -m callflow.prototype.timings_report --file /path/prototype_timings.jsonl

Prints, per stage and per Smartcare query: count, p50, p95, max seconds and the
share of total request time, so the dominant operation is obvious.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


def _pct(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    k = min(len(values) - 1, max(0, int(round(p / 100.0 * (len(values) - 1)))))
    return values[k]


def _default_file():
    try:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "callflow_portal.settings")
        import django
        django.setup()
        from callflow.prototype import conf
        return str(conf.output_dir() / "logs" / "prototype_timings.jsonl")
    except Exception:
        return "callflow_output/logs/prototype_timings.jsonl"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=None)
    ap.add_argument("--last", type=int, default=50)
    args = ap.parse_args(argv)
    path = args.file or _default_file()
    with open(path, encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()][-args.last:]
    if not rows:
        print("no requests recorded")
        return 0

    totals = [r.get("total_s", 0) for r in rows]
    total_time = sum(totals) or 1.0
    status = defaultdict(int)
    for r in rows:
        status[f"{r.get('status')}/{r.get('http_status')}"] += 1
    print(f"{len(rows)} requests  total p50={_pct(totals, 50):.1f}s p95={_pct(totals, 95):.1f}s "
          f"max={max(totals):.1f}s  statuses={dict(status)}")

    stages = defaultdict(list)
    for r in rows:
        for name, sec in (r.get("stages") or {}).items():
            stages[name].append(sec)
    print("\nSTAGE                       n     p50      p95      max   share")
    for name, vals in sorted(stages.items(), key=lambda kv: -sum(kv[1])):
        print(f"{name:24s} {len(vals):4d} {_pct(vals, 50):8.2f} {_pct(vals, 95):8.2f} "
              f"{max(vals):8.2f}  {100 * sum(vals) / total_time:5.1f}%")

    queries = defaultdict(lambda: {"exec": [], "fetch": [], "rows": [], "fail": 0, "memo": 0})
    for r in rows:
        for q in r.get("queries") or []:
            d = queries[q.get("name", "?")]
            if q.get("memo_hit"):
                d["memo"] += 1
            elif q.get("ok"):
                d["exec"].append(q.get("execute_s", 0))
                d["fetch"].append(q.get("fetch_s", 0))
                d["rows"].append(q.get("rows", 0))
            else:
                d["fail"] += 1
    print("\nSMARTCARE QUERY                          ok  fail memo  exec_p95 fetch_p95  rows_p95")
    for name, d in sorted(queries.items(), key=lambda kv: -(sum(kv[1]["exec"]) + sum(kv[1]["fetch"]))):
        print(f"{name[:38]:38s} {len(d['exec']):4d} {d['fail']:5d} {d['memo']:4d} "
              f"{_pct(d['exec'], 95):9.2f} {_pct(d['fetch'], 95):9.2f} {_pct(d['rows'], 95):9.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
