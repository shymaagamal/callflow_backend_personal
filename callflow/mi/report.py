"""
Where does MI time go? Summarises MI/logs/<date>/timings.jsonl.

    ../portalenv/bin/python -m callflow.mi.report               # last 50 jobs, last 7 days
    ../portalenv/bin/python -m callflow.mi.report --last 200 --days 14
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
    return values[min(len(values) - 1, max(0, int(round(p / 100.0 * (len(values) - 1)))))]


def load(days):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "callflow_portal.settings")
    import django
    django.setup()
    from callflow.mi import config
    logs = config.output_dir() / "logs"
    rows = []
    for day_dir in sorted(p for p in logs.iterdir() if p.is_dir())[-days:] if logs.is_dir() else []:
        f = day_dir / "timings.jsonl"
        if f.exists():
            rows += [json.loads(line) for line in f.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--last", type=int, default=50)
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args(argv)
    rows = load(args.days)[-args.last:]
    if not rows:
        print("no MI jobs recorded")
        return 0
    totals = [r.get("total_s", 0) for r in rows]
    grand = sum(totals) or 1.0
    statuses, warnings = defaultdict(int), defaultdict(int)
    for r in rows:
        statuses[f"{r.get('status')}/{r.get('http_status')}"] += 1
        for w in r.get("warnings") or []:
            warnings[w.get("code")] += 1
    print(f"{len(rows)} jobs  total p50={_pct(totals, 50):.1f}s p95={_pct(totals, 95):.1f}s max={max(totals):.1f}s")
    print(f"statuses: {dict(statuses)}   partial: {sum(1 for r in rows if r.get('partial'))}")
    if warnings:
        print("warnings: " + ", ".join(f"{k}={v}" for k, v in sorted(warnings.items(), key=lambda kv: -kv[1])))

    stages = defaultdict(list)
    for r in rows:
        for name, sec in (r.get("stages") or {}).items():
            stages[name].append(sec)
    print("\nSTAGE                     n     p50      p95      max   share")
    for name, vals in sorted(stages.items(), key=lambda kv: -sum(kv[1])):
        print(f"{name:22s} {len(vals):4d} {_pct(vals, 50):8.2f} {_pct(vals, 95):8.2f} {max(vals):8.2f}  "
              f"{100 * sum(vals) / grand:5.1f}%")

    q = defaultdict(lambda: {"exec": [], "fetch": [], "rows": [], "fail": defaultdict(int), "memo": 0})
    for r in rows:
        for e in r.get("queries") or []:
            d = q[e.get("name", "?")]
            if e.get("memo_hit"):
                d["memo"] += 1
            elif e.get("ok"):
                d["exec"].append(e.get("execute_s", 0))
                d["fetch"].append(e.get("fetch_s", 0))
                d["rows"].append(e.get("rows", 0))
            else:
                d["fail"][e.get("kind", "error")] += 1
    print("\nSMARTCARE QUERY                          ok memo  exec_p95 fetch_p95 rows_p95  failures")
    for name, d in sorted(q.items(), key=lambda kv: -(sum(kv[1]["exec"]) + sum(kv[1]["fetch"]))):
        fails = ",".join(f"{k}={v}" for k, v in d["fail"].items()) or "-"
        print(f"{name[:38]:38s} {len(d['exec']):3d} {d['memo']:4d} {_pct(d['exec'], 95):9.2f} "
              f"{_pct(d['fetch'], 95):9.2f} {_pct(d['rows'], 95):8.0f}  {fails}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
