"""
DataCube query plan and per-request query budget.

The number of SQL queries a request sends is capped and known before it
starts (separately from DataCube sessions, which are 4 per request):

* build_plan() lists every statement a request will send BEFORE anything runs:
  one per query of each Complain_Type present (Data = smartcare_Data_QUERIES,
  Voice = smartcare_Voice_QUERIES), whatever the number of complaint groups or
  MSISDNs - the MSISDNs of all groups of one type go into ONE IN (...) list.
  A window that crosses midnight stays one statement per query
  (MI_UNION_DAY_TABLES) or is counted per day table. If the plan exceeds
  MI_MAX_QUERIES_PER_REQUEST (12) the request is refused before any query.

* RequestBudget is checked right before every cursor.execute(): each planned
  statement may run once; a retry runs only if the plan left room under the
  cap (limit - planned). It can never let a request execute more than the cap.
  It counts queries, not DataCube sessions (4 per request, see pipeline.fetch_plan).
"""
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List

from . import catalog, config


class QueryPlanError(ValueError):
    """The request cannot run within the DataCube query limits (reported as HTTP 400)."""


@dataclass
class PlannedQuery:
    complain_type: str
    name: str
    template: str
    subscriber_column: str
    statements: int                    # DataCube executions on the first attempt


@dataclass
class QueryPlan:
    msisdns_by_type: "OrderedDict[str, List[str]]"
    tables: List[int]
    union_day_tables: bool
    limit: int
    window: tuple = None               # (start, end) datetimes
    queries: List[PlannedQuery] = field(default_factory=list)

    @property
    def total(self):
        return sum(q.statements for q in self.queries)

    @property
    def retry_allowance(self):
        return max(0, self.limit - self.total)

    def queries_for(self, complain_type):
        return [q for q in self.queries if q.complain_type == complain_type]

    def summary(self):
        by_type = OrderedDict()
        for ct, msisdns in self.msisdns_by_type.items():
            qs = self.queries_for(ct)
            by_type[catalog.COMPLAIN_TYPE_LABEL.get(ct, ct)] = OrderedDict([
                ("msisdns", len(msisdns)), ("queries", len(qs)), ("statements", sum(q.statements for q in qs))])
        return OrderedDict([("limit", self.limit), ("planned", self.total),
                            ("retry_allowance", self.retry_allowance),
                            ("day_tables", list(self.tables)), ("union_day_tables", self.union_day_tables),
                            ("by_complain_type", by_type)])

    def describe(self):
        """One line for the logs: Data:SGIDiameter,S1mme,...|Voice:..."""
        return "|".join(f"{catalog.COMPLAIN_TYPE_LABEL.get(ct, ct)}:"
                        + ",".join(q.name for q in self.queries_for(ct)) for ct in self.msisdns_by_type)


def build_plan(msisdns_by_type, start, end):
    """
    msisdns_by_type: {catalog.DATA|catalog.VOICE: [unique msisdns]} (types with
    no MSISDNs are skipped). Raises QueryPlanError when the request would need
    more than MI_MAX_QUERIES_PER_REQUEST statements, an IN list would be longer
    than MI_MAX_MSISDNS_PER_QUERY, or a query has no known subscriber column.
    """
    from .smartcare import day_tables
    tables = day_tables(start, end)
    union = config.union_day_tables()
    per_query = 1 if (union or len(tables) <= 1) else len(tables)
    ordered = OrderedDict((ct, list(msisdns_by_type[ct])) for ct in catalog.complain_types_in_order()
                          if msisdns_by_type.get(ct))
    plan = QueryPlan(msisdns_by_type=ordered, tables=tables, union_day_tables=union,
                     limit=config.max_queries_per_request(), window=(start, end))
    problems = []
    for ct, msisdns in ordered.items():
        label = catalog.COMPLAIN_TYPE_LABEL.get(ct, ct)
        if len(msisdns) > config.max_msisdns_per_query():
            problems.append(f"{label} complaints have {len(msisdns)} unique MSISDNs; at most "
                            f"{config.max_msisdns_per_query()} fit in one query")
        for name, template in catalog.query_set(ct).items():
            column = catalog.QUERY_SUBSCRIBER_COLUMN.get(name)
            if not column:
                problems.append(f"query {name} has no subscriber column in catalog.QUERY_SUBSCRIBER_COLUMN, "
                                "so its rows cannot be split by complaint group")
            plan.queries.append(PlannedQuery(ct, name, template, column, per_query))
    if plan.total > plan.limit:
        parts = ", ".join(f"{k} {v['statements']}" for k, v in plan.summary()["by_complain_type"].items())
        hint = (f" (the window touches {len(tables)} day tables and MI_UNION_DAY_TABLES is off)"
                if per_query > 1 else "")
        problems.append(f"this request needs {plan.total} DataCube queries ({parts}){hint}; "
                        f"the limit is {plan.limit} per request")
    if problems:
        raise QueryPlanError("; ".join(problems))
    return plan


def statements(template, tables, msisdn_sql, start, end, union=None):
    """
    [(label, sql)] to execute for one query: one statement per day table, or,
    with union on and several tables, ONE statement joining the per-day
    statements with UNION ALL (same rows as running them one by one and
    concatenating, which is what MI did before).
    """
    union = config.union_day_tables() if union is None else union
    parts = [(str(t), template.format(tablename=t, msisdn_tuple=msisdn_sql, start_date=str(start),
                                      end_date=str(end))) for t in tables]
    if not union or len(parts) <= 1:
        return parts
    body = "\nUNION ALL\n".join(f"SELECT * FROM (\n{sql.strip().rstrip(';').rstrip()}\n) mi_day_{i}"
                                for i, (_t, sql) in enumerate(parts, 1))
    return [("+".join(t for t, _ in parts), body)]


class BudgetDenied(Exception):
    def __init__(self, reason, detail):
        super().__init__(detail)
        self.reason, self.detail = reason, detail


class RequestBudget:
    """
    Counts SQL query executions of ONE MI submission (shared by its Data and
    Voice fetches and all their threads). These are NOT DataCube sessions:
    a request opens 4 sessions (its connections) however many queries run on
    them. acquire() is called immediately before each cursor.execute():

    * the first execution of a planned statement uses its planned slot;
    * any other execution is a retry: it needs room under the cap
      (limit - planned - retries used).

    Raises BudgetDenied otherwise; `executed` can never exceed `limit`.
    """

    def __init__(self, limit, planned, on_execute=None):
        if planned > limit:
            raise QueryPlanError(f"planned {planned} queries exceed the limit {limit}")
        self.limit, self.planned = limit, planned
        self.on_execute = on_execute
        self.executed = 0
        self.retries = 0
        self.retry_succeeded = 0
        self.retry_failed = 0
        self.denied = []
        self._first = set()
        self._lock = threading.Lock()

    @property
    def retry_allowance(self):
        return self.limit - self.planned

    def acquire(self, statement_key):
        """Returns the execution's sequence number (1..limit)."""
        with self._lock:
            if self.executed >= self.limit:
                self._deny("request_limit", statement_key,
                           f"query limit reached: {self.executed} of {self.limit} DataCube queries used")
            if statement_key not in self._first and len(self._first) < self.planned:
                self._first.add(statement_key)
                kind = "planned"
            else:
                if self.retries >= self.retry_allowance:
                    self._deny("request_limit", statement_key,
                               f"no retry left: the plan uses {self.planned} of {self.limit} DataCube queries")
                self.retries += 1
                kind = "retry"
            self.executed += 1
            seq = self.executed
        if self.on_execute is not None:
            self.on_execute(seq=seq, kind=kind, statement=statement_key)
        return seq

    def retry_result(self, ok):
        with self._lock:
            if ok:
                self.retry_succeeded += 1
            else:
                self.retry_failed += 1

    def _deny(self, reason, key, detail):
        self.denied.append({"statement": key, "reason": reason})
        raise BudgetDenied(reason, detail)

    def summary(self):
        return OrderedDict([("limit", self.limit), ("planned", self.planned), ("executed", self.executed),
                            ("retries", self.retries), ("retry_succeeded", self.retry_succeeded),
                            ("retry_failed", self.retry_failed), ("retries_denied", len(self.denied))])
