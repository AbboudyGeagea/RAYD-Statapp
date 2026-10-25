"""
utils/worklist_engine.py
────────────────────────────────────────────────────────────────
Dynamic Worklists: definition → SQL → rows.

    definition (JSON) ──compile()──▶ SQL + bound parameters ──run()──▶ rows
                                                    │
                       hand-written SQL (IT users) ─┘   on the rpt_<db> role only

THE COMPILER TRUSTS NOTHING IN THE DEFINITION BUT ITS SHAPE. Every identifier —
dataset, column, grouping, measure — is looked up in utils/worklist_catalog and
refused if absent; every value is a bound parameter. The SQL shown to the user is
the SQL that runs, parameters listed beside it.

THE EXECUTOR IS THE SANDBOX, not the compiler. Generated and hand-written SQL both
run on a connection that LOGS IN as the per-database rpt_ role of migration 0136
(see its header for why a role switch on the app's connection would not hold):
SELECT on rpt.* only, read-only transactions, a statement timeout, and the result
wrapped in an outer LIMIT so a runaway query cannot fill the app's memory either.

Quick dates are resolved here, at run time, on the hospital clock — "last month" in
a saved worklist means last month on the day it runs.
"""
import calendar
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine, text
from db import db

from utils import worklist_catalog as cat

logger = logging.getLogger("WORKLISTS")

ROW_CAP = 2000            # rows a Run shows
CSV_CAP = 100_000         # rows an export may hold
STATEMENT_TIMEOUT_MS = 20_000
OPTIONS_CAP = 300


class WorklistError(ValueError):
    """A definition the catalog does not allow, or SQL the sandbox refused."""


# ── Dates ─────────────────────────────────────────────────────────────────────

def _month_start(d, back=0):
    y, m = d.year, d.month - back
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


def _next_month(d):
    return _month_start(date(d.year + (d.month == 12), d.month % 12 + 1, 1))


def date_bounds(c, now=None):
    """[start, end) as naive local datetimes for one date criterion, or None."""
    now = now or datetime.now()
    today = now.date()
    mode = (c or {}).get('mode') or 'none'
    if mode == 'none':
        return None
    if mode == 'quick':
        q = c.get('quick')
        monday = today - timedelta(days=today.weekday())
        spans = {
            'today':         (today, today + timedelta(1)),
            'yesterday':     (today - timedelta(1), today),
            'this_week':     (monday, monday + timedelta(7)),
            'last_week':     (monday - timedelta(7), monday),
            'last_7_days':   (today - timedelta(6), today + timedelta(1)),
            'last_30_days':  (today - timedelta(29), today + timedelta(1)),
            'this_month':    (_month_start(today), _next_month(today)),
            'last_month':    (_month_start(today, 1), _month_start(today)),
            'last_3_months': (_month_start(today, 3), _month_start(today)),
            'this_year':     (date(today.year, 1, 1), date(today.year + 1, 1, 1)),
            'last_year':     (date(today.year - 1, 1, 1), date(today.year, 1, 1)),
        }
        if q not in spans:
            raise WorklistError(f'unknown quick date {q!r}')
        a, b = spans[q]
        return datetime.combine(a, datetime.min.time()), datetime.combine(b, datetime.min.time())
    if mode == 'relative':
        try:
            n = int(c.get('n'))
        except (TypeError, ValueError):
            raise WorklistError('the number of hours/days must be a whole number')
        if not 1 <= n <= 3660:
            raise WorklistError('the number of hours/days must be between 1 and 3660')
        unit = c.get('unit') or 'days'
        if unit not in ('hours', 'days'):
            raise WorklistError(f'unknown unit {unit!r}')
        span = timedelta(hours=n) if unit == 'hours' else timedelta(days=n)
        now = now.replace(microsecond=0)
        return (now, now + span) if c.get('dir') == 'next' else (now - span, now)
    if mode == 'fixed':
        try:
            a = date.fromisoformat(str(c.get('from')))
            b = date.fromisoformat(str(c.get('to')))
        except (TypeError, ValueError):
            raise WorklistError('fixed dates must be YYYY-MM-DD')
        if b < a:
            a, b = b, a
        return (datetime.combine(a, datetime.min.time()),
                datetime.combine(b + timedelta(1), datetime.min.time()))
    raise WorklistError(f'unknown date mode {mode!r}')


# ── Compiler ──────────────────────────────────────────────────────────────────

_BUCKET_SQL = {
    'day':     "to_char({c}, 'YYYY-MM-DD')",
    'week':    "to_char(date_trunc('week', {c}), 'IYYY-\"W\"IW')",
    'month':   "to_char({c}, 'YYYY-MM')",
    'quarter': "to_char({c}, 'YYYY-\"Q\"Q')",
    'year':    "to_char({c}, 'YYYY')",
}
_BLANK = '(blank)'


def _col(name):
    # Only ever called with catalog column names (lower-case identifiers); quoting
    # is belt and braces.
    if not re.fullmatch(r'[a-z_][a-z0-9_]*', name):
        raise WorklistError(f'bad column {name!r}')
    return f'd."{name}"'


def _like_escape(v):
    return v.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


def _dimension(ds, dim_id):
    """(sql expression, label) for a grouping the catalog offers this dataset."""
    offered = dict(cat.dimensions_for(ds))
    if dim_id not in offered:
        raise WorklistError(f'cannot group {cat.DATASETS[ds]["label"]} by {dim_id!r}')
    if ':' in dim_id:
        fid, bucket = dim_id.split(':', 1)
        expr = _BUCKET_SQL[bucket].format(c=_col(cat.FIELD[fid]['col']))
        return f"COALESCE({expr}, '{_BLANK}')", offered[dim_id]
    f = cat.FIELD[dim_id]
    if f['id'] == 'hour':
        return f"COALESCE(lpad(d.\"hour\"::text, 2, '0'), '{_BLANK}')", offered[dim_id]
    if f['id'] == 'weekday':
        return (f"COALESCE(d.\"weekday\"::text || ' ' || "
                f"(ARRAY['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])[d.\"weekday\"], '{_BLANK}')",
                offered[dim_id])
    return f"COALESCE({_col(f['col'])}::text, '{_BLANK}')", offered[dim_id]


def _criterion(f, c, params, now):
    """SQL for one criterion (or None when it filters nothing), filling params."""
    col, pid = _col(f['col']), f['id']
    t = f['type']
    if t == 'date':
        bounds = date_bounds(c, now)
        if not bounds:
            return None
        params[f'{pid}_from'], params[f'{pid}_to'] = bounds
        return f'{col} >= :{pid}_from AND {col} < :{pid}_to'
    if t == 'multi':
        values = [v for v in (c.get('values') or []) if v is not None]
        if not values:
            return None
        if not isinstance(c.get('values'), list):
            raise WorklistError(f'{f["label"]}: values must be a list')
        want_blank = _BLANK in values
        values = [v for v in values if v != _BLANK]
        opts = f.get('options')
        if isinstance(opts, list):           # fixed list: values must be among them
            allowed = {v for v, _ in opts}
            bad = [v for v in values if v not in allowed]
            if bad:
                raise WorklistError(f'{f["label"]}: {bad[0]!r} is not one of its values')
        else:
            values = [str(v) for v in values]
        parts = []
        if values:
            params[pid] = values
            parts.append(f'{col} = ANY(:{pid})')
        if want_blank:
            parts.append(f'{col} IS NULL')
        return parts[0] if len(parts) == 1 else '(' + ' OR '.join(parts) + ')'
    if t == 'text':
        value = str(c.get('value') or '').strip()
        if not value:
            return None
        op = c.get('op') or 'contains'
        if op == 'equals':
            params[pid] = value.lower()
            return f'lower({col}) = :{pid}'
        if op == 'starts with':
            params[pid] = _like_escape(value) + '%'
            return f"{col} ILIKE :{pid} ESCAPE '\\'"
        if op in ('contains', 'does not contain'):
            params[pid] = '%' + _like_escape(value) + '%'
            neg = 'NOT ' if op == 'does not contain' else ''
            return f"{neg}COALESCE({col}, '') ILIKE :{pid} ESCAPE '\\'"
        raise WorklistError(f'{f["label"]}: unknown operator {op!r}')
    if t == 'yesno':
        v = c.get('value') or 'any'
        if v == 'any':
            return None
        if v not in ('yes', 'no'):
            raise WorklistError(f'{f["label"]}: expected yes or no')
        return f'{col} IS TRUE' if v == 'yes' else f'{col} IS NOT TRUE'
    if t == 'range':
        parts = []
        for k, op in (('min', '>='), ('max', '<=')):
            raw = c.get(k)
            if raw in (None, ''):
                continue
            try:
                params[f'{pid}_{k}'] = float(raw)
            except (TypeError, ValueError):
                raise WorklistError(f'{f["label"]}: {raw!r} is not a number')
            parts.append(f'{col} {op} :{pid}_{k}')
        return ' AND '.join(parts) or None
    raise WorklistError(f'unknown field type {t!r}')


def compile_definition(defn, now=None):
    """
    Returns {'sql', 'params', 'kind', 'dims', 'measures', 'columns'}.
    Raises WorklistError for anything the catalog does not offer.
    """
    if not isinstance(defn, dict):
        raise WorklistError('a worklist definition is an object')
    ds = defn.get('dataset')
    if ds not in cat.DATASETS:
        raise WorklistError(f'unknown dataset {ds!r}')
    criteria = defn.get('criteria') or {}
    if not isinstance(criteria, dict):
        raise WorklistError('criteria must be an object')

    params, where = {}, []
    for fid, c in criteria.items():
        f = cat.FIELD.get(fid)
        if not f or ds not in f['datasets']:
            raise WorklistError(f'{fid!r} is not a field of {cat.DATASETS[ds]["label"]}')
        sql = _criterion(f, c or {}, params, now)
        if sql:
            where.append(sql)

    out = defn.get('output') or {}
    kind = out.get('kind') or 'list'
    view = cat.DATASETS[ds]['view']
    where_sql = ('\nWHERE ' + '\n  AND '.join(where)) if where else ''
    columns_offered = dict(cat.COLUMNS[ds])

    if kind == 'list':
        cols = out.get('columns') or cat.DATASETS[ds]['list_default']
        if not isinstance(cols, list):
            raise WorklistError('columns must be a list')
        for c in cols:
            if c not in columns_offered:
                raise WorklistError(f'{c!r} is not a column of {cat.DATASETS[ds]["label"]}')
        sort = out.get('sort') or cat.DATASETS[ds]['default_date']
        if sort not in columns_offered:
            raise WorklistError(f'cannot sort by {sort!r}')
        direction = 'DESC' if out.get('desc', True) else 'ASC'
        sql = (f'SELECT {", ".join(_col(c) + " AS " + chr(34) + c + chr(34) for c in cols)}'
               f'\nFROM {view} d{where_sql}'
               f'\nORDER BY {_col(sort)} {direction} NULLS LAST')
        return {'sql': sql, 'params': params, 'kind': kind, 'dims': [], 'measures': [],
                'columns': [{'id': c, 'label': columns_offered[c]} for c in cols]}

    if kind not in ('table', 'chart'):
        raise WorklistError(f'unknown output {kind!r}')
    dims = [d for d in (out.get('rows'), out.get('cols')) if d]
    measures = out.get('measures') or ['count']
    if not isinstance(measures, list):
        raise WorklistError('measures must be a list')
    offered_m = dict(cat.measures_for(ds))
    for m in measures:
        if m not in offered_m:
            raise WorklistError(f'{m!r} is not a measure of {cat.DATASETS[ds]["label"]}')
    if len(dims) == 2:
        measures = measures[:1]     # a split by columns shows one measure across them
    sel, dim_meta = [], []
    for i, dim_id in enumerate(dims):
        expr, label = _dimension(ds, dim_id)
        sel.append(f'{expr} AS "{("row", "col")[i]}"')
        dim_meta.append({'id': dim_id, 'label': label})
    sel += [f'{cat.MEASURES[m]["sql"]} AS "{m}"' for m in measures]
    sql = f'SELECT {", ".join(sel)}\nFROM {view} d{where_sql}'
    if dims:
        idx = ', '.join(str(i + 1) for i in range(len(dims)))
        sql += f'\nGROUP BY {idx}\nORDER BY {idx}'
    return {'sql': sql, 'params': params, 'kind': kind, 'dims': dim_meta,
            'measures': [{'id': m, 'label': offered_m[m],
                          'additive': cat.MEASURES[m]['additive']} for m in measures],
            'columns': None}


def literal(v):
    """A parameter as an SQL literal, for the "Edit SQL" starting point only."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, (int, float, Decimal)):
        return repr(float(v)) if isinstance(v, float) else str(v)
    if isinstance(v, datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, (list, tuple)):
        return 'ARRAY[' + ', '.join(literal(x) for x in v) + ']'
    return "'" + str(v).replace("'", "''") + "'"


def inline_sql(compiled):
    """The compiled SQL with its parameters written in, longest names first."""
    sql = compiled['sql']
    for name in sorted(compiled['params'], key=len, reverse=True):
        sql = re.sub(r'(?<![:\w]):' + re.escape(name) + r'\b',
                     lambda _m, v=compiled['params'][name]: literal(v), sql)
    return sql.replace('FROM rpt.', 'FROM ')


def display_params(params):
    return {k: (v.isoformat(sep=' ') if isinstance(v, datetime) else v)
            for k, v in params.items()}


# ── Executor ──────────────────────────────────────────────────────────────────

_engine = {'key': None, 'engine': None}


def _credentials():
    rows = db.session.execute(text(
        "SELECT key, value FROM settings WHERE key IN ('worklist_db_role', 'worklist_db_password')"
    )).fetchall()
    found = {k: v for k, v in rows}
    if not found.get('worklist_db_role') or not found.get('worklist_db_password'):
        raise WorklistError('the worklist database role is not set up (migration 0136)')
    return found['worklist_db_role'], found['worklist_db_password']


def _report_engine():
    role, password = _credentials()
    key = (str(db.engine.url), role, password)
    if _engine['key'] != key:
        if _engine['engine'] is not None:
            _engine['engine'].dispose()
        from db import pg_connect_options
        options = ' '.join(filter(None, [
            pg_connect_options(),
            '-c default_transaction_read_only=on',
            f'-c statement_timeout={STATEMENT_TIMEOUT_MS}',
        ]))
        url = db.engine.url.set(username=role, password=password)
        _engine['engine'] = create_engine(url, pool_size=3, max_overflow=3, pool_pre_ping=True,
                                          connect_args={'options': options})
        _engine['key'] = key
    return _engine['engine']


def _jsonable(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=' ', timespec='minutes')
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, memoryview)):
        return None
    return v


_SELECT_START = re.compile(r'^\s*(?:(?:--[^\n]*\n)|(?:/\*.*?\*/)|\s)*(select|with)\b',
                           re.IGNORECASE | re.DOTALL)


def check_hand_sql(sql):
    """Refuse the obvious non-queries early, with a readable message. The real
    boundary is the role; this only makes the common mistake a clear error."""
    sql = (sql or '').strip().rstrip(';').strip()
    if not sql:
        raise WorklistError('the SQL is empty')
    if not _SELECT_START.match(sql):
        raise WorklistError('worklist SQL must be a single SELECT (or WITH ... SELECT)')
    return sql


def run(compiled=None, hand_sql=None, cap=ROW_CAP):
    """
    Execute generated (compiled) or hand-written SQL in the sandbox.
    Returns {'columns', 'rows', 'truncated', 'elapsed_ms'}; raises WorklistError
    with the database's own message when the query fails.
    """
    started = time.monotonic()
    engine = _report_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text(f'SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)}'))
            if hand_sql is not None:
                sql = check_hand_sql(hand_sql)
                # No parameter binding at all for hand SQL: the user writes literals,
                # and a '%' or ':10' in them must reach Postgres untouched.
                cur = conn.connection.cursor()
                cur.execute(f'SELECT * FROM (\n{sql}\n) AS worklist_q LIMIT {int(cap) + 1}')
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
            else:
                result = conn.execute(
                    text(f'SELECT * FROM (\n{compiled["sql"]}\n) AS worklist_q '
                         f'LIMIT {int(cap) + 1}'), compiled['params'])
                columns = list(result.keys())
                rows = result.fetchall()
            conn.rollback()
    except WorklistError:
        raise
    except Exception as exc:
        msg = str(getattr(exc, 'orig', exc)).strip().split('\n')[0]
        if 'statement timeout' in msg:
            msg = f'the query ran longer than {STATEMENT_TIMEOUT_MS // 1000} s and was stopped'
        elif 'permission denied' in msg:
            msg += ' — worklists can read the rpt views only (rpt.exams, rpt.orders, rpt.reports)'
        logger.info("worklist query refused or failed: %s", msg)
        raise WorklistError(msg)
    truncated = len(rows) > cap
    rows = rows[:cap]
    return {'columns': columns, 'rows': [[_jsonable(v) for v in r] for r in rows],
            'truncated': truncated,
            'elapsed_ms': round((time.monotonic() - started) * 1000)}


def options(dataset, field_id, limit=OPTIONS_CAP):
    """The values a 'distinct' picker offers, from the data itself."""
    f = cat.FIELD.get(field_id)
    if dataset not in cat.DATASETS or not f or dataset not in f['datasets']:
        raise WorklistError('unknown field')
    if isinstance(f.get('options'), list):
        return [{'value': v, 'label': l} for v, l in f['options']]
    compiled = {'sql': f'SELECT DISTINCT {_col(f["col"])} AS v FROM {cat.DATASETS[dataset]["view"]} d '
                       f'WHERE {_col(f["col"])} IS NOT NULL ORDER BY 1', 'params': {}}
    res = run(compiled, cap=limit)
    return [{'value': r[0], 'label': str(r[0])} for r in res['rows']]


def validate_for_save(defn):
    """Compile once so a saved worklist is always runnable; hand SQL is checked too."""
    compile_definition(defn)
    if defn.get('sql'):
        check_hand_sql(defn['sql'])
    json.dumps(defn)
    return defn
