"""
tests/conftest.py
────────────────────────────────────────────────────────────────
Shared fixtures. Everything runs against a REAL Postgres, in a throwaway database
built the way a fresh install builds it:

    init-db/schema.sql          through psql (a pg_dump: \\restrict, COPY blocks)
    hl7_oru_reports bootstrap   the CREATE TABLE app.py runs before the migrations
    migrations/*.sql            through db_migrations.run_migrations, the app's own runner

    python -m pytest tests      (from the repo root)

The database is never etl_db: the session refuses to start if RAYD_TEST_DB names
the configured POSTGRES_DB, so a mistyped variable cannot wipe live data.

Connection (the server must let this user CREATE DATABASE and CREATE ROLE — the
postgres image's POSTGRES_USER can):
    RAYD_TEST_ADMIN_URL   postgresql://user:pass@host:port/postgres
    else POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_HOST / POSTGRES_PORT
                          (defaults: etl_user / etl_pass / localhost / 5432 — the dev
                           stack's published port)
psql:
    RAYD_TEST_PSQL        command that runs psql against that server, e.g.
                          "docker exec -i rayd_db psql -U etl_user"; the database name
                          and "-f -" are appended. Default: psql on PATH, same URL.
"""
import os
import shlex
import subprocess
import sys
from urllib.parse import urlparse, urlunparse

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

TEST_DB = os.environ.get('RAYD_TEST_DB', 'rayd_pytest')

# Rows the tests write. Emptied before every test so none passes or fails on what an
# earlier one left. TRUNCATE, not DELETE: crn_events rejects DELETE (append-only
# trigger), and TRUNCATE does not fire row triggers. Users and worklists are deleted
# row by row instead, so the example worklists migration 0064 seeds stay.
DATA_TABLES = [
    'etl_didb_studies', 'etl_patient_view', 'etl_orders', 'etl_didb_serieses',
    'summary_storage_daily', 'hl7_oru_reports', 'hl7_oru_analysis', 'crn_notifications',
    'crn_events', 'crn_recipients', 'physician_alias_map', 'aetitle_modality_map',
    'user_page_permissions',
]


def _admin_url():
    url = os.environ.get('RAYD_TEST_ADMIN_URL')
    if url:
        return url
    return 'postgresql://{}:{}@{}:{}/postgres'.format(
        os.environ.get('POSTGRES_USER', 'etl_user'), os.environ.get('POSTGRES_PASSWORD', 'etl_pass'),
        os.environ.get('POSTGRES_HOST', 'localhost'), os.environ.get('POSTGRES_PORT', '5432'))


def _db_url(name):
    u = urlparse(_admin_url())
    return urlunparse(u._replace(path='/' + name))


def _psql(db_name, sql_path):
    prefix = os.environ.get('RAYD_TEST_PSQL')
    cmd = (shlex.split(prefix) + ['-d', db_name] if prefix else ['psql', _db_url(db_name)])
    with open(sql_path, 'rb') as fh:
        return subprocess.run(cmd + ['-q', '-v', 'ON_ERROR_STOP=0', '-f', '-'], stdin=fh,
                              capture_output=True, timeout=600)


@pytest.fixture(scope='session')
def database():
    """Builds TEST_DB from scratch once per session. Yields its SQLAlchemy URL."""
    import psycopg2
    if TEST_DB == os.environ.get('POSTGRES_DB', 'etl_db'):
        pytest.exit(f'RAYD_TEST_DB ({TEST_DB}) is the configured POSTGRES_DB — refusing to wipe it')
    admin = psycopg2.connect(_admin_url())
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{TEST_DB}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{TEST_DB}"')
    admin.close()

    done = _psql(TEST_DB, os.path.join(ROOT, 'init-db', 'schema.sql'))
    if done.returncode != 0:
        pytest.exit('psql could not load init-db/schema.sql: '
                    + done.stderr.decode(errors='replace')[:500])

    url = _db_url(TEST_DB).replace('postgresql://', 'postgresql+psycopg2://', 1)
    os.environ['RAYD_TEST_SQLALCHEMY_URL'] = url
    app = make_app(url)
    from sqlalchemy import text
    from db import db
    with app.app_context():
        # app.py creates this table before running the migrations (create_app).
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS hl7_oru_reports (
                id SERIAL PRIMARY KEY, procedure_code VARCHAR(100), procedure_name TEXT,
                modality VARCHAR(20), physician_id VARCHAR(100), patient_id VARCHAR(100),
                accession_number VARCHAR(100), report_text TEXT, impression_text TEXT,
                result_datetime TIMESTAMP, received_at TIMESTAMP DEFAULT NOW())
        """))
        db.session.commit()
    from db_migrations import run_migrations
    run_migrations(app)
    yield url


class _TestUser:
    """flask_login user loaded with plain SQL (the ORM model carries columns app.py
    adds at startup, which a migrations-only database does not have)."""
    is_authenticated = True
    is_active = True
    is_anonymous = False

    def __init__(self, uid, role):
        self.id, self.role = uid, role

    def get_id(self):
        return str(self.id)


def make_app(url):
    from flask import Flask
    from flask_login import LoginManager
    from db import db, init_db

    app = Flask(__name__, template_folder=os.path.join(ROOT, 'templates'))
    app.config.update(SQLALCHEMY_DATABASE_URI=url, SQLALCHEMY_TRACK_MODIFICATIONS=False,
                      SECRET_KEY='pytest', TESTING=True)
    init_db(app)
    lm = LoginManager()
    lm.init_app(app)

    @lm.request_loader
    def _load(req):
        from sqlalchemy import text
        uid = req.headers.get('X-Test-Uid')
        if not uid:
            return None
        row = db.session.execute(text('SELECT id, role FROM users WHERE id = :id'),
                                 {'id': int(uid)}).first()
        return _TestUser(row[0], row[1]) if row else None

    @lm.unauthorized_handler
    def _unauthorized():
        return 'login required', 401

    # The tests hold one app context for the whole session, and a request reuses it,
    # so Flask-Login's cached user (g._login_user) would outlive its request.
    @app.before_request
    def _forget_cached_user():
        from flask import g
        g.pop('_login_user', None)

    from routes.worklists import worklists_bp
    app.register_blueprint(worklists_bp)
    return app


@pytest.fixture(scope='session')
def app(database):
    app = make_app(database)
    ctx = app.app_context()
    ctx.push()
    yield app
    ctx.pop()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def clean(request):
    """Empty the data tables before every test that touches the database."""
    if 'database' not in request.fixturenames and 'app' not in request.fixturenames:
        yield
        return
    from sqlalchemy import text
    from db import db
    request.getfixturevalue('app')
    db.session.rollback()
    db.session.execute(text(f'TRUNCATE {", ".join(DATA_TABLES)} RESTART IDENTITY CASCADE'))
    db.session.execute(text("DELETE FROM worklists WHERE owner_id IS NOT NULL OR folder <> 'Examples'"))
    db.session.execute(text("DELETE FROM users"))
    db.session.execute(text("DELETE FROM settings WHERE key ~ '^(shift_|pc_)'"))
    db.session.commit()
    yield
    db.session.rollback()


def sql(statement, **params):
    """Run one statement on the app's own (owner) connection and commit."""
    from sqlalchemy import text
    from db import db
    res = db.session.execute(text(statement), params)
    rows = res.mappings().all() if res.returns_rows else None
    db.session.commit()
    return rows
