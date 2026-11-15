"""
utils/site_db.py — which database a request reads: RH's or SJH's (meeting of 2026-11-13,
item 13a: "Separate database for SJH, same web app", full database separation).

Each site with its own database:
    RH  (the default site)   the main database, POSTGRES_DB (etl_db), as before
    SJH                      POSTGRES_DB_SJH (e.g. etl_db_sjh), same server and account

A site gets a database by setting POSTGRES_DB_<CODE> in the app's environment. At startup
init_app() creates that database if missing, loads init-db/schema.sql into it, runs every
migration on it, and copies the user list into it. Without the variable nothing changes:
one database, RH only.

What lives where:
    main database    logins and access (MAIN_TABLES: users, user_sites, page and report
                     grants, sessions, the audit log, permission groups) and the licence:
                     one set of logins for both sites (item 13f)
    each database    its site's data (ETL, HL7, BRAYN), its AE mapping, its settings
                     (mail server, report start date, ...), its saved reports
    copy of users    each site database keeps a read-only copy of users (no password), so
                     its own tables (Report Composer, acknowledgements, CRN, ...) can point
                     at the account that acted, and owner names still join

How a request is routed. The session's get_bind (db.SiteSession) asks route(): a query on
a MAIN_TABLES model, or one given an explicit bind, goes where it says; everything else
(every text() query, every other model) goes to the database of the site the request
reads. That site is the one picked in the header dropdown (session['site_id']) when the
account may open it, else the default site when it may, else its first site. Outside a
request (ETL, listener, scheduler, CLI) everything reads the main database, unless a block
runs inside use_site(site_id).

Code that must read the main database whatever the site uses main_only (a view decorator)
or use_site(default_site_id()). Code that opens its own connection uses current_engine(),
never db.engine, which is always the main database.
"""
import contextlib
import functools
import logging
import os
import threading
from contextvars import ContextVar

logger = logging.getLogger("SITE_DB")

# Tables that exist once, in the main database, whatever site a request reads.
MAIN_TABLES = frozenset({
    'users', 'user_sites', 'user_page_permissions', 'report_access_control',
    'active_sessions', 'user_audit_log', 'permission_groups',
})

# Procedure codes are shared by both sites (meeting of 2026-11-13, item 13e): the main
# database holds them, every write goes there, and each site database keeps an identical
# copy (sync_shared_tables). Parents before children, the order they are copied in.
SHARED_TABLES = (
    'procedure_canonical_groups', 'procedure_duration_map', 'procedure_canonical_members',
    'procedure_duplicate_candidates', 'procedure_fuzzy_candidates', 'procedure_modality_conflicts',
)

_engines = {}                 # site_id -> Engine, for sites with their own database
_codes = {}                   # site_id -> site code
_forced = ContextVar('rayd_site_forced', default=None)
_ENV_KEY = 'rayd.site_id'
_RESOLVING = object()
_BOOT_LOCK_ID = 872951065     # next to db_migrations' lock id
_lock = threading.Lock()


# ── which site ────────────────────────────────────────────────────────────────

def default_site_id():
    from utils.site_resolver import default_site
    return default_site()


def database_site_ids():
    """Sites RAYD can serve: the default site (the main database) and every site whose own
    database is set up."""
    out = set(_engines)
    d = default_site_id()
    if d is not None:
        out.add(d)
    return out


def current_site_id():
    """The site this request (or use_site block) reads. Outside a request: the default."""
    forced = _forced.get()
    if forced is not None:
        return forced
    from flask import has_request_context, request
    if not has_request_context():
        return default_site_id()
    env = request.environ
    got = env.get(_ENV_KEY)
    if got is _RESOLVING:          # asked again while resolving (loading the user)
        return default_site_id()
    if got is None:
        env[_ENV_KEY] = _RESOLVING
        try:
            got = _resolve_request_site()
        finally:
            env[_ENV_KEY] = got if got is not None else default_site_id()
        got = env[_ENV_KEY]
    return got


def _resolve_request_site():
    from flask import session
    from flask_login import current_user
    default = default_site_id()
    try:
        if not getattr(current_user, 'is_authenticated', False):
            return default
        choices = selectable_site_ids(current_user)
    except Exception as e:
        logger.error(f"request site not resolved: {e}")
        return default
    wanted = session.get('site_id')
    if wanted in choices:
        return wanted
    if default in choices:
        return default
    return min(choices) if choices else default


def served_site_id():
    """The site whose devices and studies a report keeps: the site this request reads.
    Each database serves one site (RH's the default site, SJH's its own), so this is also
    what rayd_served_site_id() answers inside that database (migration 0153)."""
    return current_site_id()


def selectable_site_ids(user):
    """Sites the account may open now: granted to it, and with a database."""
    from utils.roles import granted_site_ids
    return set(granted_site_ids(user)) & database_site_ids()


def choose(site_id):
    """Pick the site the following requests read (the header dropdown). False when the
    account may not open it."""
    from flask import request, session
    from flask_login import current_user
    if site_id not in selectable_site_ids(current_user):
        return False
    session['site_id'] = site_id
    request.environ.pop(_ENV_KEY, None)
    from utils.roles import forget_access
    forget_access()
    return True


def site_code(site_id):
    if site_id in _codes:
        return _codes[site_id]
    from utils.roles import all_sites
    for sid, code, _name in all_sites():
        _codes.setdefault(sid, code)
    return _codes.get(site_id)


@contextlib.contextmanager
def use_site(site_id):
    """Read site_id's database inside the block (background jobs, tests, main_only)."""
    token = _forced.set(site_id)
    try:
        yield
    finally:
        _forced.reset(token)


def main_only(view):
    """A view that reads and writes the main database whatever site the request reads:
    User Management, the Activity Log, permission groups."""
    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        with use_site(default_site_id()):
            return view(*args, **kwargs)
    return wrapper


def shared_write(view):
    """A view that changes procedure codes (SHARED_TABLES): it writes the main database, and
    each site database's copy is brought in line as soon as it succeeds."""
    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        with use_site(default_site_id()):
            response = view(*args, **kwargs)
        status = response[1] if isinstance(response, tuple) and len(response) > 1             else getattr(response, 'status_code', 200)
        if isinstance(status, int) and status < 400:
            sync_shared_tables()
        return response
    return wrapper


# ── which engine ──────────────────────────────────────────────────────────────

def engine_for(site_id):
    """The engine of a site's database; the main engine for the default site."""
    from db import db
    return _engines.get(site_id) or db.engine


def current_engine():
    return engine_for(current_site_id())


def route(default_engine, mapper=None, clause=None):
    """db.SiteSession.get_bind: the engine a statement goes to (see the module notes)."""
    if not _engines:
        return default_engine
    if mapper is not None and _table_name(mapper) in MAIN_TABLES | set(SHARED_TABLES):
        return default_engine
    site = _routing_site()
    return _engines.get(site, default_engine) if site is not None else default_engine


def _routing_site():
    """The site whose database a statement goes to, or None for the main database. Outside
    a request and outside use_site() it is the main database without asking anything, and a
    statement made while the request's site is being worked out goes there too, so routing
    never needs the database to decide where to go."""
    forced = _forced.get()
    if forced is not None:
        return forced
    from flask import has_request_context, request
    if not has_request_context():
        return None
    got = request.environ.get(_ENV_KEY)
    if got is _RESOLVING:
        return None
    return got if got is not None else current_site_id()


def _table_name(mapper):
    try:
        import sqlalchemy as sa
        return sa.inspect(mapper).local_table.name
    except Exception:
        return getattr(getattr(mapper, '__table__', None), 'name', None)


# ── background work per site ──────────────────────────────────────────────────

def has_site_databases():
    """True when at least one site has its own database (no app context needed)."""
    return bool(_engines)


def each_site_id():
    """Every site with a database, the default site (main database) first."""
    d = default_site_id()
    return ([d] if d is not None else []) + sorted(s for s in _engines if s != d)


def run_per_site(name, job):
    """Run job(site_id) once per database: the main one as before, then each site database
    inside use_site(site_id), so db.session reads and writes that database. A failure on one
    site is logged and does not stop the others. Returns {site_id: job's result}."""
    from db import db
    out = {}
    for sid in each_site_id() or [None]:
        try:
            if sid is None or sid == default_site_id():
                out[sid] = job(sid)
            else:
                with use_site(sid):
                    out[sid] = job(sid)
        except Exception:
            logger.exception(f"{name}: failed for site {site_code(sid) if sid else 'main'}")
            try:
                db.session.rollback()
            except Exception:
                pass
        finally:
            try:
                db.session.remove()
            except Exception:
                pass
    return out


# ── setting up a site's database ──────────────────────────────────────────────

def configured_databases(main_url):
    """{site_code: URL} from POSTGRES_DB_<CODE>, same server and account as the main
    database. The default site never gets one (it is the main database)."""
    from sqlalchemy.engine import make_url
    from sqlalchemy import text
    from db import db
    out = {}
    try:
        rows = db.session.execute(text(
            "SELECT code FROM sites WHERE active AND NOT is_default ORDER BY id")).fetchall()
        db.session.rollback()
    except Exception as e:
        logger.error(f"sites not read: {e}")
        db.session.rollback()
        return out
    base = make_url(main_url)
    for (code,) in rows:
        name = (os.environ.get(f'POSTGRES_DB_{code}') or '').strip()
        if name:
            if name == base.database:
                logger.error(f"POSTGRES_DB_{code} names the main database; ignored")
                continue
            out[code] = base.set(database=name)
    return out


def init_app(app, databases=None):
    """Set up every site database (create, schema, migrations, user copy) and its engine.
    databases: {site_code: URL}, else read from POSTGRES_DB_<CODE>. A site whose database
    cannot be set up stays unavailable; the app still starts on the main database."""
    from sqlalchemy import create_engine, text
    from db import db
    with app.app_context():
        wanted = databases if databases is not None else \
            configured_databases(app.config['SQLALCHEMY_DATABASE_URI'])
        if not wanted:
            return {}
        ids = {code: sid for sid, code in db.session.execute(text(
            "SELECT id, code FROM sites WHERE active")).fetchall()}
        db.session.rollback()
        ready = {}
        for code, url in wanted.items():
            sid = ids.get(code)
            if sid is None:
                logger.error(f"site {code}: no such active site; database not used")
                continue
            try:
                _bootstrap(url, code)
                options = dict(app.config.get('SQLALCHEMY_ENGINE_OPTIONS') or {})
                options.setdefault('pool_pre_ping', True)
                engine = create_engine(url, **options)
                _install_hooks(app, engine)
                with _lock:
                    _engines[sid] = engine
                    _codes[sid] = code
                ready[code] = sid
                logger.info(f"site {code}: database {url.database} ready")
            except Exception:
                logger.exception(f"site {code}: database {getattr(url, 'database', url)} not set up")
        if ready:
            mirror_users()
            sync_shared_tables()
        return ready


def _bootstrap(url, code):
    """Create the database if missing, load the schema into an empty one, run the
    migrations, and record the site it serves (settings.served_site, which
    rayd_served_site_id() reads, migration 0153). Serialised across workers with an
    advisory lock on the main database."""
    from sqlalchemy import create_engine, text
    from db import db
    import db_bootstrap
    import db_migrations
    with db.engine.connect() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {'k': _BOOT_LOCK_ID})
        try:
            with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as ac:
                exists = ac.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"),
                                    {'n': url.database}).scalar()
                if not exists:
                    ac.execute(text(f'CREATE DATABASE "{_ident(url.database)}"'))
                    logger.info(f"database {url.database} created")
            engine = create_engine(url, pool_pre_ping=True)
            try:
                with engine.connect() as c:
                    empty = c.execute(text("SELECT to_regclass('public.users') IS NULL")).scalar()
                if empty:
                    db_bootstrap.load_schema(url.set(drivername='postgresql')
                                             .render_as_string(hide_password=False))
                    logger.info(f"database {url.database}: schema loaded")
                # Before the migrations: a migration may ask which site this database serves
                # (rayd_served_site_id(), e.g. 0154's CRN hospital name).
                with engine.begin() as c:
                    c.execute(text("INSERT INTO settings (key, value) VALUES ('served_site', :c) "
                                   "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {'c': code})
                db_migrations.run_migrations_on(engine)
                with engine.begin() as c:
                    # Access lives on the main database only: the rows the schema and the
                    # migrations seed here would only mislead (and a read that strayed here
                    # finds nothing, so it fails closed).
                    for table in ('user_page_permissions', 'report_access_control',
                                  'user_sites', 'active_sessions'):
                        c.execute(text(f"DELETE FROM {table}"))
            finally:
                engine.dispose()
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {'k': _BOOT_LOCK_ID})
            lock_conn.commit()


def _ident(name):
    if not name or '"' in name or '\x00' in name:
        raise ValueError(f"unusable database name {name!r}")
    return name


def _install_hooks(app, engine):
    """What init_db and create_app install on the main engine, minus RH-only (a site
    database holds its own site's rows; the rh views would hide them)."""
    try:
        from utils.brayn_shadow import install_clock
        install_clock(engine)
    except Exception:
        logger.exception("BRAYN clock hook not installed on a site database")
    try:
        from utils.query_monitor import init_query_monitor
        init_query_monitor(app, engine)
    except Exception:
        logger.exception("query monitor not installed on a site database")


# ── the copy of users in each site database ───────────────────────────────────

_USER_COLUMNS = ('id', 'username', 'role', 'status', 'full_name', 'email')


def mirror_users():
    """Copy the main database's users into every site database (no password: nobody signs
    in there). Accounts deleted on main are deleted there, unless that site's data still
    points at them, in which case they stay, renamed so a new account can take the name."""
    from sqlalchemy import text
    from db import db
    if not _engines:
        return
    try:
        with db.engine.connect() as c:
            rows = [dict(r._mapping) for r in c.execute(text(
                f"SELECT {', '.join(_USER_COLUMNS)} FROM users ORDER BY id"))]
    except Exception:
        logger.exception("users not read for the site databases")
        return
    ids = [r['id'] for r in rows] or [0]
    for sid, engine in list(_engines.items()):
        try:
            with engine.begin() as c:
                for gone in c.execute(text("SELECT id FROM users WHERE id <> ALL(:ids)"),
                                      {'ids': ids}).scalars().all():
                    try:
                        with c.begin_nested():
                            c.execute(text("DELETE FROM users WHERE id = :i"), {'i': gone})
                    except Exception:
                        c.execute(text("UPDATE users SET username = '~deleted-' || id, status = 'disabled' "
                                       "WHERE id = :i"), {'i': gone})
                # names freed on main may be held by another id here: move them aside first
                c.execute(text("""
                    UPDATE users s SET username = '~moved-' || s.id
                    FROM unnest(CAST(:ids AS int[]), CAST(:names AS text[])) AS m(id, username)
                    WHERE s.username = m.username AND s.id <> m.id"""),
                    {'ids': [r['id'] for r in rows], 'names': [r['username'] for r in rows]})
                for r in rows:
                    c.execute(text("""
                        INSERT INTO users (id, username, password_hash, role, status, full_name, email)
                        VALUES (:id, :username, '!', :role, :status, :full_name, :email)
                        ON CONFLICT (id) DO UPDATE SET
                            username = EXCLUDED.username, password_hash = '!', role = EXCLUDED.role,
                            status = EXCLUDED.status, full_name = EXCLUDED.full_name,
                            email = EXCLUDED.email"""), r)
        except Exception:
            logger.exception(f"users not copied to the database of site {sid}")


# ── the shared procedure codes in each site database ──────────────────────────

def sync_shared_tables():
    """Make every site database's copy of SHARED_TABLES identical to the main database's.
    Skipped where nothing changed (a checksum per table over the same columns on both
    sides); otherwise all of them are copied in one transaction (COPY with an explicit
    column list, so column order and types never matter). Returns {site_id: copied?}."""
    from db import db
    if not _engines:
        return {}
    out = {}
    try:
        main_cols = {t: _columns(db.engine, t) for t in SHARED_TABLES}
    except Exception:
        logger.exception("shared tables not read on the main database")
        return out
    for sid, engine in list(_engines.items()):
        try:
            cols = {}
            for t in SHARED_TABLES:
                there = set(_columns(engine, t))
                cols[t] = [c for c in main_cols[t] if c in there]
                if not cols[t]:
                    raise RuntimeError(f"{t} has no column in common")
            if all(_checksum(db.engine, t, cols[t]) == _checksum(engine, t, cols[t]) for t in SHARED_TABLES):
                out[sid] = False
                continue
            _copy_shared(db.engine, engine, cols)
            out[sid] = True
            logger.info(f"shared procedure tables copied to site {site_code(sid)}")
        except Exception:
            logger.exception(f"shared procedure tables not copied to site {sid}")
    return out


def _columns(engine, table):
    from sqlalchemy import text
    with engine.connect() as c:
        return [r[0] for r in c.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :t ORDER BY ordinal_position"""), {'t': table})]


def _quoted(cols):
    return ', '.join('"' + c.replace('"', '""') + '"' for c in cols)


def _checksum(engine, table, cols):
    from sqlalchemy import text
    with engine.connect() as c:
        return c.execute(text(f"""
            SELECT md5(COALESCE(string_agg(x, E'\n' ORDER BY x), ''))
            FROM (SELECT ROW({_quoted(cols)})::text AS x FROM public.{table}) q""")).scalar()


def _copy_shared(main_engine, site_engine, cols):
    import io
    data = {}
    src = main_engine.raw_connection()
    try:
        cur = src.cursor()
        for t in SHARED_TABLES:
            buf = io.StringIO()
            cur.copy_expert(f"COPY public.{t} ({_quoted(cols[t])}) TO STDOUT", buf)
            data[t] = buf.getvalue()
        src.rollback()
    finally:
        src.close()
    dst = site_engine.raw_connection()
    try:
        cur = dst.cursor()
        for t in reversed(SHARED_TABLES):               # children first
            cur.execute(f"DELETE FROM public.{t}")
        for t in SHARED_TABLES:                         # parents first
            cur.copy_expert(f"COPY public.{t} ({_quoted(cols[t])}) FROM STDIN", io.StringIO(data[t]))
            if 'id' in cols[t]:
                cur.execute(f"""SELECT setval(pg_get_serial_sequence('public.{t}', 'id'),
                                              COALESCE((SELECT MAX(id) FROM public.{t}), 0) + 1, false)
                                WHERE pg_get_serial_sequence('public.{t}', 'id') IS NOT NULL""")
        dst.commit()
    except Exception:
        dst.rollback()
        raise
    finally:
        dst.close()
