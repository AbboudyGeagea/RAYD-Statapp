"""
tests/conftest.py
────────────────────────────────────────────────────────────────
Shared fixtures for the LAUMC suite.

    python -m pytest tests            (from the repo root, a Postgres reachable)

EVERYTHING HERE RUNS AGAINST A REAL POSTGRES, in a throwaway database built from
init-db/schema.sql plus every migration, exactly as a fresh install does. The
listener is driven over a real socket (start_mllp_listener on a free port), because
on this branch the message handling lives inside the connection loop — the socket
IS the entry point, and testing anything below it would test something LAUMC never
runs.

Why this suite exists (2026-10-26): BRAYN and Dynamic Worklists are being brought
to LAUMC "without breaking anything". tests/test_listener_baseline.py pins down
what the listener does TODAY — the fill-only report upsert, orders, PACS
completions, ADT — so every later change is checked against it.

The database is never etl_db. The session refuses to start if RAYD_TEST_DB names
the configured POSTGRES_DB, so a mistyped variable cannot wipe live data.

Connection, in order of preference:
    RAYD_TEST_ADMIN_URL   postgresql://user:pass@host:port/postgres
    POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_HOST / POSTGRES_PORT
                          (defaults: etl_user / etl_pass / localhost / 5432)

oracledb is stubbed when it is not installed: db.py imports it at the top, but only
the Oracle ETL calls it, and no test here touches Oracle.
"""
import os
import re
import socket
import sys
import time
import types
from urllib.parse import urlparse, urlunparse

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT, TESTS):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    import oracledb  # noqa: F401
except ImportError:
    sys.modules['oracledb'] = types.ModuleType('oracledb')

TEST_DB = os.environ.get('RAYD_TEST_DB', 'rayd_pytest_laumc')

# Data the listener (and anything built on it) writes. Cleared before every test.
# Missing tables are skipped, so the list can name tables later migrations add.
# DELETE in foreign-key order (children first), not TRUNCATE: much faster on
# Docker Desktop volumes.
DATA_TABLES = [
    'hl7_oru_analysis', 'ai_nlp_cache', 'crn_events', 'crn_notifications',
    'hl7_oru_reports', 'hl7_orders', 'hl7_scn_studies', 'order_status_log',
    'etl_didb_studies', 'etl_patient_view', 'etl_orders',
    # BRAYN (migration 0133), children of the archive first
    'brayn_findings', 'hl7_study_events', 'hl7_patients', 'hl7_message_archive',
    'brayn_study_state', 'brayn_interface_stats', 'brayn_unknown_values',
]

# Seeded configuration, snapshotted after migrations and restored before every test.
CONFIG_TABLES = [
    'settings', 'aetitle_modality_map',
    'brayn_rules', 'hl7_status_map', 'hl7_result_status_map', 'brayn_ladder_profile',
    'hl7_field_mappings', 'hl7_field_targets',
]

# The bootstrap app.py runs before migrations (inline in create_app()).
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
    return urlunparse(urlparse(url)._replace(path='/' + dbname))


def _load_schema(dsn):
    """
    init-db/schema.sql through psycopg2, doing what psql would: skip meta-commands
    (\\restrict from pg_dump 14.19+) and feed each `COPY ... FROM stdin` block its
    data through copy_expert.
    """
    import io
    import psycopg2
    with open(os.path.join(ROOT, 'init-db', 'schema.sql'), encoding='utf-8') as f:
        lines = f.read().splitlines()

    def clean(sql):
        sql = sql.replace("set_config('search_path', '', false)",
                          "set_config('search_path', 'public', false)")
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
                pending, data = [], []
                i += 1
                while i < len(lines) and lines[i] != '\\.':
                    data.append(lines[i])
                    i += 1
                cur.copy_expert(line.rstrip()[:-1],
                                io.StringIO('\n'.join(data) + '\n' if data else ''))
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
                    f"Start a Postgres or set RAYD_TEST_ADMIN_URL.")
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


class _AllFeatures(dict):
    """A licence where every feature is on and every report number is licensed."""
    def get(self, key, default=None):
        return list(range(1, 100)) if key == 'reports' else True

    def __getattr__(self, key):
        return True


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

    flask_app = Flask('rayd_laumc_test', root_path=ROOT)
    flask_app.config.update(
        SQLALCHEMY_DATABASE_URI=database_url.replace('postgresql://', 'postgresql+psycopg2://', 1),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SQLALCHEMY_ENGINE_OPTIONS={'pool_pre_ping': True},
        TESTING=True, SECRET_KEY='rayd-test', WTF_CSRF_ENABLED=False,
    )
    init_db(flask_app)
    from flask_wtf.csrf import CSRFProtect
    CSRFProtect(flask_app)

    login = LoginManager(flask_app)

    @login.request_loader
    def _load(req):
        role = req.headers.get('X-Test-Role')
        return FakeUser(role, int(req.headers.get('X-Test-Uid', 4242))) if role else None

    run_migrations(flask_app)

    # Full pages include the real _header/_sidebar, which link to every blueprint;
    # only those under test are registered, so other links render as '#'.
    flask_app.url_build_error_handlers.append(lambda error, endpoint, values: '#')
    flask_app.add_url_rule('/', 'index', lambda: '')
    from db import user_has_page as _uhp
    flask_app.add_template_filter(lambda user, key: _uhp(user, key), 'user_has_page')

    @flask_app.context_processor
    def _shell_context():
        return {'license': _AllFeatures(), 'config': flask_app.config, 'ui_theme': 'dark',
                'user_favorites': [], 'demo_mode': False, 'demo_start': '', 'demo_end': '',
                'demo_user': '', 'pending_approvals_count': 0, 'reset_requests_count': 0}
    from routes.brayn_console import brayn_bp
    flask_app.register_blueprint(brayn_bp)

    from db import db
    from sqlalchemy import text
    with flask_app.app_context():
        global _DATA, _CONFIG
        exists = lambda t: db.session.execute(text("SELECT to_regclass(:t) IS NOT NULL"),
                                              {'t': f'public.{t}'}).scalar()
        _DATA = [t for t in DATA_TABLES if exists(t)]
        _CONFIG = [t for t in CONFIG_TABLES if exists(t)]
        for t in _CONFIG:
            db.session.execute(text(f'DROP TABLE IF EXISTS _snap_{t}'))
            db.session.execute(text(f'CREATE TABLE _snap_{t} AS SELECT * FROM {t}'))
        db.session.commit()
    return flask_app


_DATA, _CONFIG = [], []


def _reset_caches():
    import hl7_listener
    from utils import brayn, brayn_shadow, hl7_fieldmap, hl7_ingest
    for k in list(brayn._cache):
        brayn._cache[k] = (0.0, None)
    hl7_fieldmap.invalidate()
    hl7_fieldmap.invalidate_targets()
    hl7_ingest._PACS_APP.update({'at': 0.0, 'value': None})
    brayn_shadow.reset_switch()
    hl7_listener._fm_cache = None
    hl7_listener._fm_loaded_at = 0.0


@pytest.fixture(autouse=True)
def clean(request, app):
    from db import db
    from sqlalchemy import text
    with app.app_context():
        for t in _DATA:
            db.session.execute(text(f'DELETE FROM {t}'))
        for t in _CONFIG:
            db.session.execute(text(f'DELETE FROM {t}'))
            db.session.execute(text(f'INSERT INTO {t} SELECT * FROM _snap_{t}'))
        db.session.commit()
    _reset_caches()
    yield
    _reset_caches()


@pytest.fixture
def client(app):
    return app.test_client()


# ── the listener, over a real socket ──────────────────────────────────────────

def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


_REAL_START_WORKER = []


@pytest.fixture(scope='session')
def mllp_port(app):
    """The real listener on a free port. BRAYN's background worker is NOT started
    with it: tests screen explicitly (Pipe.screen) so what they assert is not a race
    against a thread. The brayn_worker fixture runs the real worker on purpose."""
    from hl7_listener import start_mllp_listener
    from utils import brayn_shadow
    _REAL_START_WORKER.append(brayn_shadow.start_worker)
    brayn_shadow.start_worker = lambda app: None
    port = _free_port()
    start_mllp_listener(app, host='127.0.0.1', port=port)
    for _ in range(50):
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return port
        except OSError:
            time.sleep(0.05)
    pytest.exit('the MLLP listener did not start')


@pytest.fixture
def brayn_worker(app, mllp_port):
    from utils import brayn_shadow
    thread = _REAL_START_WORKER[0](app)
    yield thread
    brayn_shadow.stop_worker()


class Pipe:
    """Sends messages through the real listener and reads back what landed."""

    def __init__(self, app, port):
        self.app, self.port = app, port

    def send(self, raw):
        """Send one framed message; return the ACK code (AA / AE).

        Returns only once the listener has finished with the connection: what it
        does after the ACK (BRAYN's archive) is done by the time a test looks."""
        import hl7kit as k
        with socket.create_connection(('127.0.0.1', self.port), timeout=10) as s:
            s.sendall(k.frame(raw))
            buf = b''
            while k.EB not in buf:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
            s.shutdown(socket.SHUT_WR)
            while s.recv(4096):
                pass
        ack = buf.decode('utf-8', 'replace')
        m = re.search(r'MSA\|([A-Z]{2})', ack)
        return m.group(1) if m else None

    def brayn(self, on=True):
        """Switch BRAYN on (or off), effective for the next message."""
        self.exec("INSERT INTO settings (key, value) VALUES ('brayn_enabled', :v) "
                  "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                  v='true' if on else 'false')
        _reset_caches()

    def screen(self):
        """Run BRAYN's worker pass once, here, instead of waiting for the thread."""
        from utils import brayn_shadow
        return brayn_shadow.process_pending(self.app)

    def _run(self, sql, params, fetch):
        from db import db
        from sqlalchemy import text
        with self.app.app_context():
            res = db.session.execute(text(sql), params)
            out = [dict(r) for r in res.mappings().all()] if fetch else None
            db.session.commit()
            return out

    def exec(self, sql, **params):
        self._run(sql, params, False)

    def q(self, sql, **params):
        return self._run(sql, params, True)

    def one(self, sql, **params):
        rows = self.q(sql, **params)
        return rows[0] if rows else None

    def scalar(self, sql, **params):
        row = self.one(sql, **params)
        return next(iter(row.values())) if row else None

    def count(self, table, where='TRUE', **params):
        return self.scalar(f'SELECT COUNT(*) FROM {table} WHERE {where}', **params)


@pytest.fixture
def pipe(app, mllp_port):
    return Pipe(app, mllp_port)
