"""
db_migrations.py — runs SQL migration files from the migrations/ folder.

Each .sql file in migrations/ is run exactly once, tracked in the
schema_migrations table. Files are applied in alphabetical order,
so prefix them with a number: 0001_..., 0002_..., etc.

Usage:
    from db_migrations import run_migrations
    run_migrations(app)   # call inside app context after init_db()
"""

import os
import re
import logging
from sqlalchemy import text

logger = logging.getLogger("migrations")

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")


def run_migrations(app):
    from db import db

    with app.app_context():
        # Ensure tracking table exists
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name        VARCHAR(255) PRIMARY KEY,
                applied_at  TIMESTAMP DEFAULT NOW()
            )
        """))
        db.session.commit()

        # Get already-applied migrations
        applied = {
            row[0]
            for row in db.session.execute(text("SELECT name FROM schema_migrations")).fetchall()
        }

        # Find and sort pending migration files
        try:
            files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql"))
        except FileNotFoundError:
            logger.warning(f"[migrations] Directory not found: {MIGRATIONS_DIR}")
            return

        pending = [f for f in files if f not in applied]

        if not pending:
            logger.info("[migrations] All migrations already applied.")
            return

        for filename in pending:
            path = os.path.join(MIGRATIONS_DIR, filename)
            try:
                with open(path) as f:
                    raw = f.read()

                statements = _split_sql(raw)
                _run_statements(db, statements)

                with db.engine.connect() as conn:
                    conn.execute(
                        text("INSERT INTO schema_migrations (name) VALUES (:name)"),
                        {"name": filename}
                    )
                    conn.commit()

                logger.info(f"[migrations] Applied: {filename}")

            except Exception as e:
                logger.error(f"[migrations] FAILED: {filename} — {e}")


def _run_statements(db, statements):
    """
    Execute a migration's statements on a RAW DBAPI connection (psycopg2), not through
    SQLAlchemy's Connection.exec_driver_sql().

    Why: exec_driver_sql() hands psycopg2 a (possibly empty) parameter collection even
    when the migration passes none, which puts psycopg2's cursor.execute() into its
    %-substitution code path. Any literal '%' in the SQL — e.g. Postgres format()
    specifiers like %I / %L used inside `DO $$ ... EXECUTE format(...) ... $$;` blocks
    (migration 0050's RLS policy generator) — is then read as a substitution spec, and
    since no real parameters were supplied it fails with something like
    "immutabledict is not a sequence" instead of running the SQL. cursor.execute(sql)
    with NO second argument (the raw DBAPI path below) never scans for '%' at all, so
    format()-based DDL runs exactly as it would from psql.

    CONCURRENTLY statements (e.g. CREATE INDEX CONCURRENTLY) cannot run inside a
    transaction. The previous approach tried to flip isolation_level on the SAME
    connection mid-migration, which fails once that connection has already autobegun a
    transaction ("isolation_level may not be altered..."). Here each CONCURRENTLY
    statement gets its own dedicated raw connection with autocommit enabled, opened and
    closed just for that statement, leaving the main migration connection/transaction
    untouched.
    """
    main_conn = db.engine.raw_connection()
    try:
        cur = main_conn.cursor()
        for statement in statements:
            if "CONCURRENTLY" in statement.upper():
                conc_conn = db.engine.raw_connection()
                try:
                    conc_conn.autocommit = True
                    conc_conn.cursor().execute(statement)
                finally:
                    conc_conn.close()
            else:
                cur.execute(statement)
        main_conn.commit()
    except Exception:
        main_conn.rollback()
        raise
    finally:
        main_conn.close()


_DOLLAR_TAG_RE = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)?\$')


def _split_sql(sql):
    """
    Split a SQL file into individual statements on ';'.

    Dollar-quote aware: a ';' inside a dollar-quoted block ($$...$$ or $tag$...$tag$,
    as used by `DO $$ ... $$;` and `CREATE FUNCTION ... $body$ ... $body$;`) does NOT
    terminate a statement. Without this, a block like
        DO $$ BEGIN ... CREATE ROLE x LOGIN; ... END $$;
    is cut at the inner ';' and fails with "unterminated dollar-quoted string".

    Full-line comments and blank lines outside a block are skipped.
    """
    statements = []
    current = []
    in_dollar = None          # active dollar tag (e.g. '$$' or '$body$'), or None
    for line in sql.splitlines():
        stripped = line.strip()
        if in_dollar is None and (stripped.startswith("--") or stripped == ""):
            continue
        # Toggle dollar-quote state on each tag found on this line.
        for m in _DOLLAR_TAG_RE.finditer(line):
            tag = m.group(0)
            if in_dollar is None:
                in_dollar = tag           # opening a block
            elif in_dollar == tag:
                in_dollar = None          # closing the matching block
            # a different tag while inside a block is literal text — ignore
        current.append(line)
        if in_dollar is None and stripped.rstrip().endswith(";"):
            stmt = "\n".join(current).rstrip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            current = []
    # Catch any trailing statement without a semicolon
    if current:
        stmt = "\n".join(current).strip().rstrip(";").strip()
        if stmt:
            statements.append(stmt)
    return statements
