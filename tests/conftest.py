"""
tests/conftest.py
────────────────────────────────────────────────────────────────
Shared fixtures for the HL7 pipeline suite.

    python -m pytest tests            (from the repo root, stack running)

EVERYTHING HERE RUNS AGAINST A REAL POSTGRES, in a throwaway database.
The pipeline's behaviour lives in SQL as much as in Python — ON CONFLICT
inference, partial indexes, savepoints, statement timeouts, the surrogate-key
function, NOW() versus naive HL7 timestamps — and a stub cannot be wrong in the
ways Postgres can. So the session builds `rayd_pytest` from init-db/schema.sql
plus every migration, exactly as a fresh install does, and every test drives the
real listener code against it.

The database is never etl_db. The session refuses to start if RAYD_TEST_DB names
the configured POSTGRES_DB, so a mistyped variable cannot wipe live data.

Connection, in order of preference:
    RAYD_TEST_ADMIN_URL   postgresql://user:pass@host:port/postgres
    POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_HOST / POSTGRES_PORT
                          (defaults: etl_user / etl_pass / localhost / 5432 —
                           the dev stack's published port)

THE TWO ORIGINAL SCRIPTS (test_hl7_parse.py, test_hl7_fieldmap.py) are excluded
from collection. They replace sys.modules['db'] with a stub at import time and call
sys.exit, so importing them here would poison the real db module for every test
after them. They stay runnable as scripts. (The third, test_ray7_rules.py, tested
rule functions the 0135 rebuild retired; test_ray7_units.py replaces it.)
"""
import os
import re
import socket
import sys
import time
from contextlib import contextmanager
from urllib.parse import urlparse, urlunparse

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT, TESTS):
    if p not in sys.path:
        sys.path.insert(0, p)

collect_ignore = ['test_hl7_parse.py', 'test_hl7_fieldmap.py']

TEST_DB = os.environ.get('RAYD_TEST_DB', 'rayd_pytest')

# Everything HL7 traffic produces. Cleared before every test, so no test can pass
# or fail on what an earlier one left behind.
#
# DELETE in foreign-key order (children first), not TRUNCATE: on Docker Desktop's
# volume a TRUNCATE of this set costs ~450 ms and the DELETEs ~3 ms, which is the
# difference between a five-minute suite and a thirty-second one.
DATA_TABLES = [
    'ray7_findings', 'hl7_study_events', 'hl7_patients', 'hl7_message_archive',
    'ray7_study_state', 'hl7_surrogate_keys',
    'order_status_log', 'hl7_orders',
    'hl7_oru_analysis', 'ai_nlp_cache', 'crn_notifications', 'hl7_oru_rule_cache',
    'oru_classification_reviews', 'hl7_oru_reports', 'hl7_scn_studies',
    'std_pps_person_reference', 'std_pps', 'etl_didb_raw_images', 'etl_didb_serieses',
    'etl_didb_studies', 'etl_patient_view', 'etl_orders',
    'std_worklist_arrivals', 'std_worklist_exam_done', 'std_worklist_scheduled',
    'std_resources_ris', 'ray7_interface_stats', 'ray7_unknown_values',
]

# Seeded configuration. Snapshotted once after migrations and restored before
# every test, so a test that edits a rule or the ladder cannot leak that edit.
CONFIG_TABLES = [
    'ray7_rules', 'hl7_status_map', 'hl7_result_status_map', 'ray7_ladder_profile',
    'hl7_field_mappings', 'hl7_field_targets', 'settings', 'aetitle_modality_map',
]

# The bootstrap app.py runs before migrations, reproduced here because it is inline
# in create_app() rather than importable.
_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS hl7_oru_reports (
    id               SERIAL PRIMARY KEY,
    procedure_code   VARCHAR(100),
    procedure_name   TEXT,
    modality         VARCHAR(20),
    physician_id     VARCHAR(100),
    patient_id       VARCHAR(100),
    accession_number VARCHAR(100),
    report_text      TEXT,
    impression_text  TEXT,
    result_datetime  TIMESTAMP,
    received_at      TIMESTAMP DEFAULT NOW()
)
"""


def _admin_url():
    url = os.environ.get('RAYD_TEST_ADMIN_URL')
    if url:
        return url
    return 'postgresql://{u}:{p}@{h}:{port}/postgres'.format(
        u=os.environ.get('POSTGRES_USER', 'etl_user'),
        p=os.environ.get('POSTGRES_PASSWORD', 'etl_pass'),
        h=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=os.environ.get('POSTGRES_PORT', '5432'))


def _with_db(url, dbname):
    parts = urlparse(url)
    return urlunparse(parts._replace(path='/' + dbname))


def _load_schema(dsn):
    """
    init-db/schema.sql through psycopg2, doing the three things psql would:
    skip meta-commands (\\restrict, pg_dump 14.19+), and feed each
    `COPY ... FROM stdin` block its data through copy_expert.
    """
    import io
    import psycopg2
    with open(os.path.join(ROOT, 'init-db', 'schema.sql'), encoding='utf-8') as f:
        lines = f.read().splitlines()

    def clean(sql):
        # pg_dump empties search_path, which breaks the unqualified statements later.
        sql = sql.replace("set_config('search_path', '', false)",
                          "set_config('search_path', 'public', false)")
        # The test database belongs to whoever created it; etl_user may not exist.
        return re.sub(r'^ALTER [^;]* OWNER TO [^;]*;$', '', sql, flags=re.M)

    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    def run(chunk):
        sql = clean('\n'.join(chunk))
        if any(l.strip() and not l.strip().startswith('--') for l in sql.splitlines()):
            cur.execute(sql)

    with conn.cursor() as cur:
        pending, i = [], 0
        while i < len(lines):
            line = lines[i]
            if line.startswith('COPY ') and line.rstrip().endswith('FROM stdin;'):
                run(pending)
                pending = []
                data = []
                i += 1
                while i < len(lines) and lines[i] != '\\.':
                    data.append(lines[i])
                    i += 1
                cur.copy_expert(line.rstrip()[:-1], io.StringIO('\n'.join(data) + '\n'
                                                                if data else ''))
            elif not line.startswith('\\'):
                pending.append(line)
            i += 1
        run(pending)
        cur.execute(_BOOTSTRAP_SQL)
    conn.close()


@pytest.fixture(scope='session')
def database_url():
    if TEST_DB == os.environ.get('POSTGRES_DB', 'etl_db'):
        pytest.exit(f"refusing to run: RAYD_TEST_DB={TEST_DB!r} is the live database")
    import psycopg2
    admin = _admin_url()
    try:
        conn = psycopg2.connect(admin, connect_timeout=5)
    except Exception as exc:
        pytest.exit(f"no Postgres for the test suite at {admin}: {exc}\n"
                    f"Start the stack (dc up -d) or set RAYD_TEST_ADMIN_URL.")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{TEST_DB}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{TEST_DB}"')
    conn.close()
    url = _with_db(admin, TEST_DB)
    _load_schema(url)
    yield url
    if not os.environ.get('RAYD_TEST_KEEP_DB'):
        conn = psycopg2.connect(admin)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{TEST_DB}" WITH (FORCE)')
        conn.close()


class FakeUser:
    """Just enough of db.User for flask_login and user_has_page()."""
    is_active = True
    is_anonymous = False
    is_authenticated = True

    def __init__(self, role, uid=4242):
        self.role = role
        self.id = uid
        self.username = f'test-{role}'

    def get_id(self):
        return str(self.id)


@pytest.fixture(scope='session')
def app(database_url):
    from flask import Flask
    from flask_login import LoginManager
    from db import init_db
    from db_migrations import run_migrations

    sa_url = database_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
    flask_app = Flask('rayd_test', root_path=ROOT)
    flask_app.config.update(
        SQLALCHEMY_DATABASE_URI=sa_url,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SQLALCHEMY_ENGINE_OPTIONS=_engine_options(),
        TESTING=True, SECRET_KEY='rayd-test', WTF_CSRF_ENABLED=False,
    )
    init_db(flask_app)

    # As in production, so templates get csrf_token(); checks stay off for the
    # test client (WTF_CSRF_ENABLED=False).
    from flask_wtf.csrf import CSRFProtect
    CSRFProtect(flask_app)

    login = LoginManager(flask_app)

    @login.request_loader
    def _load(req):
        role = req.headers.get('X-Test-Role')
        return FakeUser(role) if role else None

    run_migrations(flask_app)

    from routes.ray7_console import ray7_bp
    flask_app.register_blueprint(ray7_bp)

    from db import db
    from sqlalchemy import text
    with flask_app.app_context():
        # The devices hl7kit's builders default to, so UNKNOWN_AETITLE fires only in
        # the tests that ask for an unknown one.
        db.session.execute(text("""
            INSERT INTO aetitle_modality_map (aetitle, modality)
            SELECT v.ae, v.mod FROM (VALUES ('CT64_RH', 'CT'), ('MR1', 'MR'), ('DR_RH', 'DX'))
                 AS v(ae, mod)
             WHERE NOT EXISTS (SELECT 1 FROM aetitle_modality_map m WHERE m.aetitle = v.ae)
        """))
        for t in CONFIG_TABLES:
            db.session.execute(text(f'DROP TABLE IF EXISTS _snap_{t}'))
            db.session.execute(text(f'CREATE TABLE _snap_{t} AS SELECT * FROM {t}'))
        db.session.commit()
    return flask_app


def _engine_options():
    """
    The engine options the product uses, so the suite runs under the same session
    settings production does. Read from db.py's helper when it exists, so a fix
    there is exercised here rather than re-implemented. (Not from app.py: importing
    it pulls in every blueprint and the scheduler.)
    """
    import db
    if hasattr(db, 'engine_options'):
        return db.engine_options()
    return {'pool_pre_ping': True}


def _reset_caches():
    import hl7_listener
    from utils import ray7, hl7_fieldmap, hl7_ingest
    for k in list(ray7._cache):
        ray7._cache[k] = (0.0, None)
    hl7_fieldmap.invalidate()
    hl7_fieldmap.invalidate_targets()
    hl7_ingest._PACS_APP.update({'at': 0.0, 'value': None})
    hl7_listener._fm_cache = None
    hl7_listener._fm_loaded_at = 0.0


@pytest.fixture(autouse=True)
def clean(request, app):
    from db import db
    from sqlalchemy import text
    with app.app_context():
        for t in DATA_TABLES:
            db.session.execute(text(f'DELETE FROM {t}'))
        for t in CONFIG_TABLES:
            db.session.execute(text(f'DELETE FROM {t}'))
            db.session.execute(text(f'INSERT INTO {t} SELECT * FROM _snap_{t}'))
        db.session.commit()
    _reset_caches()
    yield
    _reset_caches()


# ── The pipeline driver ───────────────────────────────────────────────────────

class Pipe:
    """
    Drives the real ingest path and reads back what landed.

    send() calls hl7_listener._process_message — the exact function a socket
    connection ends in — so everything below the framing is the production code.
    """

    def __init__(self, app):
        self.app = app
        self.peer = ('10.0.0.7', 51000)

    # writes
    def send(self, raw):
        import hl7_listener
        segments = [s.strip() for s in raw.replace('\r\n', '\r').replace('\n', '\r').split('\r')
                    if s.strip()]
        return hl7_listener._process_message(raw, segments, self.peer, self.app)

    def send_all(self, messages):
        return [self.send(m) for m in messages]

    def exec(self, sql, **params):
        from db import db
        from sqlalchemy import text
        with self.app.app_context():
            r = db.session.execute(text(sql), params)
            n = r.rowcount
            db.session.commit()
            return n

    # reads
    def q(self, sql, **params):
        # Commits: a "read" that calls hl7_surrogate_id() mints a key, and rolling it
        # back would make the next call mint a different one.
        from db import db
        from sqlalchemy import text
        with self.app.app_context():
            rows = [dict(r) for r in db.session.execute(text(sql), params).mappings().all()]
            db.session.commit()
            return rows

    def one(self, sql, **params):
        rows = self.q(sql, **params)
        return rows[0] if rows else None

    def scalar(self, sql, **params):
        row = self.one(sql, **params)
        return None if row is None else next(iter(row.values()))

    def state(self, acc):
        return self.one("SELECT * FROM ray7_study_state WHERE accession_number = :a", a=acc)

    def study(self, acc):
        return self.one("SELECT * FROM etl_didb_studies WHERE accession_number = :a", a=acc)

    def order(self, key):
        return self.one("SELECT * FROM etl_orders WHERE order_dbid = hl7_surrogate_id('order', :k)",
                        k=key)

    def patient(self, pid):
        return self.one("SELECT * FROM etl_patient_view WHERE id = :p", p=pid)

    def events(self, acc=None):
        if acc is None:
            return self.q("SELECT * FROM hl7_study_events ORDER BY id")
        return self.q("SELECT * FROM hl7_study_events WHERE accession_number = :a ORDER BY id", a=acc)

    def archive(self):
        return self.q("SELECT * FROM hl7_message_archive ORDER BY id")

    def findings(self, rule=None, acc=None, open_only=False):
        sql = "SELECT * FROM ray7_findings WHERE TRUE"
        if rule:
            sql += " AND rule_code = :r"
        if acc:
            sql += " AND accession_number = :a"
        if open_only:
            sql += " AND resolved_at IS NULL"
        return self.q(sql + " ORDER BY id", r=rule, a=acc)

    def codes(self, acc=None):
        """Sorted rule codes raised, optionally for one accession."""
        return sorted(f['rule_code'] for f in self.findings(acc=acc))

    def notes(self, rule=None, acc=None, open_only=False):
        """Study-level rows: RAY7 quality notes and workflow (absence) findings."""
        return [f for f in self.findings(rule=rule, acc=acc, open_only=open_only)
                if f['message_archive_id'] is None and f['accession_number'] is not None]

    def open_notes(self, acc=None):
        return sorted(f['rule_code'] for f in self.notes(acc=acc, open_only=True))

    # RAY7 layer 3: per-sender counters and the unknown-values registry
    def stat(self, metric, app=None):
        sql = "SELECT COALESCE(SUM(n), 0) AS n FROM ray7_interface_stats WHERE metric = :m"
        if app is not None:
            sql += " AND sending_app = :app"
        return int(self.scalar(sql, m=metric, app=app))

    def unknown(self, kind=None):
        sql = "SELECT kind, sending_app, value, n FROM ray7_unknown_values"
        if kind:
            sql += " WHERE kind = :k"
        return self.q(sql + " ORDER BY kind, value", k=kind)

    def held(self):
        return self.count('hl7_message_archive', "ray7_status = 'quarantined'")

    def count(self, table, where='TRUE', **params):
        return self.scalar(f"SELECT COUNT(*) FROM {table} WHERE {where}", **params)

    # configuration
    def profile(self, expected=(), enforced=()):
        self.exec("UPDATE ray7_ladder_profile SET expected = (rung = ANY(:e)), "
                  "enforce_order = (rung = ANY(:o))", e=list(expected), o=list(enforced))
        _reset_caches()

    def setting(self, key, value):
        self.exec("INSERT INTO settings (key, value) VALUES (:k, :v) "
                  "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", k=key, v=value)
        _reset_caches()

    def rule(self, code, modality='', **cols):
        sets = ', '.join(f"{c} = :{c}" for c in cols)
        n = self.exec(f"UPDATE ray7_rules SET {sets} WHERE rule_code = :code AND modality = :m",
                      code=code, m=modality, **cols)
        if not n:
            raise AssertionError(f'no ray7_rules row for {code!r}/{modality!r}')
        _reset_caches()

    # the other two entry points
    def sweep(self):
        from utils.ray7_sweep import run_sweep
        with self.app.app_context():
            return run_sweep()

    def replay(self, **kw):
        from utils.hl7_replay import replay
        return replay(self.app, **kw)

    def snapshot(self):
        """
        Everything derived from the archive, in a form two runs can be compared by.
        Surrogate IDs and wall-clock stamps are excluded — they legitimately differ.
        """
        state = self.q("""
            SELECT accession_number, placer_order_number, patient_id, modality, aetitle,
                   room_name, procedure_code, procedure_text, patient_class, patient_location,
                   ordered_at, scheduled_at, arrived_at, started_at, completed_at,
                   pacs_completed_at, reported_at, signed_prelim_at, signed_final_at,
                   cancelled_at, current_rank, is_closed, is_provisional, visit_number
              FROM ray7_study_state ORDER BY accession_number""")
        studies = self.q("""
            SELECT accession_number, storing_ae, study_date, study_time, insert_time,
                   study_description, procedure_code, study_modality, patient_class,
                   patient_location, order_status, study_status, study_has_report,
                   rep_final_timestamp, rep_final_signed_by, age_at_exam,
                   referring_physician_first_name, referring_physician_last_name
              FROM etl_didb_studies ORDER BY accession_number""")
        orders = self.q("""
            SELECT accession_number, patient_dbid, proc_id, proc_text, scheduled_datetime,
                   order_status, modality, has_study
              FROM etl_orders ORDER BY accession_number NULLS FIRST, proc_id""")
        events = self.q("""
            SELECT accession_number, placer_order_number, canonical_state, ladder_rank,
                   event_time FROM hl7_study_events
             ORDER BY accession_number NULLS FIRST, ladder_rank, event_time""")
        return {'state': state, 'studies': studies, 'orders': orders, 'events': events}


@pytest.fixture
def pipe(app):
    return Pipe(app)


@pytest.fixture
def client(app):
    return app.test_client()


# ── A live MLLP listener on a free port, for the socket tests ─────────────────

def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope='session')
def mllp_port(app):
    import hl7_listener
    port = _free_port()
    hl7_listener.start_mllp_listener(app, host='127.0.0.1', port=port)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.5).close()
            return port
        except OSError:
            time.sleep(0.05)
    pytest.exit('MLLP listener did not start')


@contextmanager
def app_ctx(app):
    with app.app_context():
        yield
