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

                with db.engine.connect() as conn:
                    for statement in statements:
                        if "CONCURRENTLY" in statement.upper():
                            # CREATE INDEX CONCURRENTLY cannot run inside a transaction
                            conn.execution_options(isolation_level="AUTOCOMMIT").exec_driver_sql(statement)
                        else:
                            # exec_driver_sql bypasses SQLAlchemy's :param parsing,
                            # which prevents false positives on JSON literals like :true/:false
                            conn.exec_driver_sql(statement)

                    conn.execute(
                        text("INSERT INTO schema_migrations (name) VALUES (:name)"),
                        {"name": filename}
                    )
                    conn.commit()

                logger.info(f"[migrations] Applied: {filename}")

            except Exception as e:
                logger.error(f"[migrations] FAILED: {filename} — {e}")


def _split_sql(sql):
    """Split a SQL file into individual statements, ignoring semicolons in comments."""
    statements = []
    current = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        current.append(line)
        if stripped.rstrip().endswith(";"):
            stmt = "\n".join(current).rstrip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            current = []
    # Catch any trailing statement without a semicolon
    if current:
        stmt = "\n".join(current).strip()
        if stmt:
            statements.append(stmt)
    return statements
