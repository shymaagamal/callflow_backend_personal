"""
Smartcare access for MI: one JDBC session per job, per-day table execution,
error classification and bounded retries.

Error classes (from the messages Smartcare actually returns):
  unavailable  "Table or view not found: ps.DETAIL_CDR_SGS_20463"   -> never retried
  query_error  other AnalysisException / ParseException / permission -> never retried
  timeout      java.net.SocketTimeoutException, "timed out"          -> retried, reconnect
  connection   reset / refused / broken pipe / closed / transport     -> retried, reconnect
  error        anything else                                          -> retried
"""
import datetime
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional
import jaydebeapi
import jpype
import pandas as pd

from . import config
logger = logging.getLogger("callflow.mi.smartcare")

_jvm_lock = threading.Lock()
EPOCH = datetime.datetime(1970, 1, 1)

_PATTERNS = (
    ("unavailable", re.compile(r"Table or view not found|TABLE_OR_VIEW_NOT_FOUND|NoSuchTableException|"
                               r"Path does not exist", re.I)),
    ("query_error", re.compile(r"AnalysisException|ParseException|SemanticException|cannot resolve|"
                               r"AccessControlException|Permission denied", re.I)),
    ("timeout", re.compile(r"SocketTimeoutException|timed out|\btimeout\b", re.I)),
    ("connection", re.compile(r"Connection reset|Connection refused|Broken pipe|connection (is )?closed|"
                              r"No route to host|TTransportException|EOFException|Communication link", re.I)),
)
NON_RETRYABLE = {"unavailable", "query_error", "config"}


class SmartcareConfigError(RuntimeError):
    pass


def classify_error(exc) -> str:
    if isinstance(exc, SmartcareConfigError):
        return "config"
    text = f"{type(exc).__name__}: {exc}"
    for kind, pattern in _PATTERNS:
        if pattern.search(text):
            return kind
    return "error"


def short_error(exc, limit=600):
    text = " ".join(str(exc).split())
    m = re.search(r"(Table or view not found: [\w.]+|SocketTimeoutException[^;\[]*|AnalysisException: [^;\[]{0,200})", text)
    head = m.group(1) if m else text
    return head[:limit]


# ------------------------------------------------------------- connection
def connect():
    """Open a Smartcare cursor (cursor._connection is the JDBC connection)."""
    user, password = config.smartcare_user(), config.smartcare_password()
    
    if not user or not password:
        raise SmartcareConfigError("MI_SMARTCARE_USER / MI_SMARTCARE_PASSWORD are not configured")


    jar = config.smartcare_jdbc_jar()
    with _jvm_lock:
        if not jpype.isJVMStarted():
            args = [f"-Djava.class.path={jar}"]
            if config.jvm_max_heap():
                args.append(f"-Xmx{config.jvm_max_heap()}")
            jpype.startJVM(jpype.getDefaultJVMPath(), *args)
    props = {
        "user": str(user),
        "password": str(password),
        "socketTimeout": str(config.socket_timeout_ms()),
        "connectTimeout": str(config.connect_timeout_ms()),
    }
    conn = jaydebeapi.connect(config.smartcare_jdbc_driver(), config.smartcare_jdbc_url(), props, jar)
    logger.debug("Smartcare JDBC connected to %s as %s", config.smartcare_jdbc_url(), user)
    try:
        conn.jconn.setNetworkTimeout(None, config.socket_timeout_ms())
    except Exception:
        pass
    return conn.cursor()


def close_cursor(cursor):
    if cursor is None:
        return
    for closer in (lambda: cursor.close(), lambda: cursor._connection.close()):
        try:
            closer()
        except Exception:
            pass


class Session:
    """One Smartcare connection reused by every (sequential) query of a job."""

    def __init__(self, ctx, connect_fn=None):
        self.ctx = ctx
        self.connect_fn = connect_fn or connect
        self.cursor_obj = None
        self.connects = 0

    def cursor(self):
        if self.cursor_obj is None:
            t0 = time.perf_counter()
            try:
                self.cursor_obj = self.connect_fn()
            except Exception as e:
                self.ctx.warning("smartcare_connect_failed", seconds=round(time.perf_counter() - t0, 3),
                                 kind=classify_error(e), error=short_error(e))
                raise
            if self.cursor_obj is None:          # legacy connect returned None on failure
                raise ConnectionError("Smartcare connection returned None")
            self.connects += 1
            self.ctx.event("smartcare_connected", seconds=round(time.perf_counter() - t0, 3),
                           connection_no=self.connects)
        return self.cursor_obj

    def reset(self):
        close_cursor(self.cursor_obj)
        self.cursor_obj = None

    close = reset


# ------------------------------------------------------------- execution
@dataclass
class QueryOutcome:
    name: str
    frame: pd.DataFrame
    status: str = "ok"                 # ok | partial | failed | unavailable | gave_up
    tables: List[dict] = field(default_factory=list)
    error_kind: Optional[str] = None
    error: Optional[str] = None
    memo_hit: bool = False

    @property
    def complete(self):
        return self.status == "ok"


def day_tables(start, end):
    """Smartcare day-table suffixes (days since 1970-01-01) touched by the window."""
    if config.fix_midnight_tables():
        tables, day = [], start.date()
        while day <= end.date():
            tables.append((datetime.datetime.combine(day, datetime.time()) - EPOCH).days)
            day += datetime.timedelta(days=1)
        return tables
    tables, current = [], pd.to_datetime(start)          # previous behaviour
    while current <= pd.to_datetime(end):
        tables.append((current - EPOCH).days)
        current += datetime.timedelta(days=1)
    return tables


class Executor:
    def __init__(self, ctx, request, session):
        self.ctx = ctx
        self.request = request
        self.session = session
        self.msisdn_sql = "(" + ",".join(f"'{m}'" for m in request.msisdns) + ")"
        self.tables = day_tables(request.start, request.end)
        self._memo = {}
        self._failed = {}

    def succeeded(self, template):
        return id(template) in self._memo

    def run(self, name, template) -> QueryOutcome:
        key = id(template)
        if key in self._memo:
            cached = self._memo[key]
            self.ctx.record_query(name=name, memo_hit=True, rows=len(cached.frame), ok=True)
            return QueryOutcome(name=name, frame=cached.frame, status="ok", tables=cached.tables, memo_hit=True)

        failures = self._failed.get(key, 0)
        if failures >= 2:
            # failed its full retry cycle AND its single fallback retry in this job
            self.ctx.record_query(name=name, ok=False, skipped=True, kind="gave_up",
                                  error="query already failed twice in this job; not executed again")
            return QueryOutcome(name=name, frame=pd.DataFrame(), status="gave_up",
                                error_kind="gave_up", error="already failed twice in this job")
        attempts_allowed = 1 if failures else config.max_attempts()
        data = pd.DataFrame()
        outcomes = []
        for table in self.tables:
            sql_text = template.format(tablename=table, msisdn_tuple=self.msisdn_sql,
                                       start_date=str(self.request.start), end_date=str(self.request.end))
            result = self._run_table(name, table, sql_text, attempts_allowed)
            has_frame = result.pop("_frame", False)
            df = result.pop("_df", None)
            if result["status"] == "ok" and has_frame:
                data = pd.concat([data, df], ignore_index=True)      # same concat as before
            outcomes.append(result)

        statuses = {o["status"] for o in outcomes}
        if statuses == {"ok"}:
            status = "ok"
        elif "ok" in statuses:
            status = "partial"
        elif statuses == {"unavailable"}:
            status = "unavailable"
        else:
            status = "failed"
        failed = [o for o in outcomes if o["status"] != "ok"]
        outcome = QueryOutcome(name=name, frame=data, status=status, tables=outcomes,
                               error_kind=failed[-1]["kind"] if failed else None,
                               error=failed[-1]["error"] if failed else None)
        if outcome.complete:
            self._memo[key] = outcome
        else:
            self._failed[key] = failures + 1
        return outcome

    def _run_table(self, name, table, sql_text, attempts_allowed):
        last_kind = last_error = None
        for attempt in range(1, attempts_allowed + 1):
            t0 = time.perf_counter()
            exec_s = fetch_s = 0.0
            try:
                cursor = self.session.cursor()
                cursor.execute(sql_text)
                exec_s = time.perf_counter() - t0
                t1 = time.perf_counter()
                rows = cursor.fetchall()
                fetch_s = time.perf_counter() - t1
                df, columns = None, 0
                if cursor.description:
                    cols = [d[0] for d in cursor.description]
                    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
                    columns = len(cols)
                self.ctx.smartcare_seconds += exec_s + fetch_s
                self.ctx.record_query(name=name, table=table, attempt=attempt, ok=True,
                                      rows=0 if df is None else len(df), columns=columns,
                                      execute_s=round(exec_s, 3), fetch_s=round(fetch_s, 3))
                return {"table": table, "status": "ok", "attempts": attempt,
                        "rows": 0 if df is None else len(df), "_frame": df is not None, "_df": df}
            except Exception as e:
                spent = time.perf_counter() - t0
                self.ctx.smartcare_seconds += spent
                last_kind, last_error = classify_error(e), short_error(e)
                over_budget = self.ctx.smartcare_seconds >= config.retry_budget_s()
                will_retry = (attempt < attempts_allowed and last_kind not in NON_RETRYABLE
                              and not over_budget)
                self.ctx.record_query(name=name, table=table, attempt=attempt, ok=False,
                                      kind=last_kind, seconds=round(spent, 3), error=last_error,
                                      will_retry=will_retry)
                self.ctx.debug("smartcare_query_error_detail", name=name, table=table,
                               attempt=attempt, error=str(e)[:4000])
                if not will_retry:
                    if over_budget and attempt < attempts_allowed and last_kind not in NON_RETRYABLE:
                        self.ctx.warning("retry_budget_exhausted", name=name, table=table,
                                         smartcare_s=round(self.ctx.smartcare_seconds, 1))
                    break
                self.session.reset()             # connection may be broken after a timeout
                delay = min(config.retry_backoff_max_s(), config.retry_backoff_s() * (2 ** (attempt - 1)))
                if delay > 0:
                    time.sleep(delay)
        status = "unavailable" if last_kind == "unavailable" else "failed"
        return {"table": table, "status": status, "attempts": attempt, "kind": last_kind, "error": last_error}
