"""
Database Provisioner — creates per-system-type PostgreSQL databases.

All databases live on the same Postgres instance (rayd_db container).
Creates rayd_pacs, rayd_ris, rayd_lis, rayd_his as needed.
Records provisioning state in the main rayd database.
"""

import os
import logging
from datetime import datetime
from sqlalchemy import text, create_engine

try:
    from ETL_JOBS.system_type_registry import SYSTEM_TYPES, generate_ddl
except ImportError:
    from system_type_registry import SYSTEM_TYPES, generate_ddl

logger = logging.getLogger("DB_PROVISIONER")


def _get_pg_url(db_name=None):
    """Build PostgreSQL connection URL from environment."""
    user = os.getenv('POSTGRES_USER', 'etl_user')
    pw   = os.getenv('POSTGRES_PASSWORD', '')
    host = os.getenv('POSTGRES_HOST', 'db')
    port = os.getenv('POSTGRES_PORT', '5432')
    name = db_name or os.getenv('POSTGRES_DB', 'etl_db')
    return f"postgresql://{user}:{pw}@{host}:{port}/{name}"


def _ensure_registry_table(engine):
    """Create the system_type_databases tracking table in the main DB."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS system_type_databases (
                id              SERIAL PRIMARY KEY,
                system_type     VARCHAR(20) NOT NULL,
                db_name         VARCHAR(100) NOT NULL UNIQUE,
                connection_name VARCHAR(100),
                provisioned_at  TIMESTAMP DEFAULT NOW(),
                schema_version  INTEGER DEFAULT 1,
                is_active       BOOLEAN DEFAULT TRUE
            )
        """))


def get_provisioned_databases(engine):
    """Return list of provisioned databases from the main DB."""
    _ensure_registry_table(engine)
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT system_type, db_name, provisioned_at, schema_version, is_active "
            "FROM system_type_databases ORDER BY system_type"
        )).fetchall()
    return [dict(zip(['system_type', 'db_name', 'provisioned_at', 'schema_version', 'is_active'], r)) for r in rows]


def ensure_database(main_engine, system_type_key):
    """
    Ensure the database for a system type exists.
    Creates it if missing, applies DDL, records in registry.

    Args:
        main_engine: SQLAlchemy engine for the main rayd database
        system_type_key: 'PACS', 'RIS', 'LIS', or 'HIS'

    Returns:
        dict with {db_name, created, system_type}
    """
    st = SYSTEM_TYPES.get(system_type_key.upper())
    if not st:
        raise ValueError(f"Unknown system type: {system_type_key}")

    db_name = f"rayd_{st['db_name_suffix']}"
    _ensure_registry_table(main_engine)

    # Check if already provisioned
    with main_engine.connect() as conn:
        existing = conn.execute(
            text("SELECT id FROM system_type_databases WHERE db_name = :n"),
            {"n": db_name}
        ).fetchone()

    if existing:
        logger.info(f"Database {db_name} already provisioned.")
        return {"db_name": db_name, "created": False, "system_type": system_type_key}

    # Create the database (must connect to 'postgres' default DB, outside transaction)
    admin_url = _get_pg_url('postgres')
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    try:
        with admin_engine.connect() as conn:
            # Check if DB exists at the Postgres level
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": db_name}
            ).fetchone()

            if not exists:
                logger.info(f"Creating database: {db_name}")
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                logger.info(f"Database {db_name} created.")
    finally:
        admin_engine.dispose()

    # Apply DDL to the new database
    target_url = _get_pg_url(db_name)
    target_engine = create_engine(target_url)

    try:
        ddl_statements = generate_ddl(system_type_key)
        with target_engine.begin() as conn:
            for ddl in ddl_statements:
                conn.execute(text(ddl))
        logger.info(f"DDL applied to {db_name}: {len(ddl_statements)} tables created.")
    finally:
        target_engine.dispose()

    # Record in registry
    with main_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO system_type_databases (system_type, db_name)
            VALUES (:st, :dn)
            ON CONFLICT (db_name) DO NOTHING
        """), {"st": system_type_key.upper(), "dn": db_name})

    logger.info(f"Provisioned {db_name} for {system_type_key}.")
    return {"db_name": db_name, "created": True, "system_type": system_type_key}


def get_target_engine(system_type_key):
    """Get a SQLAlchemy engine for a system type's database."""
    st = SYSTEM_TYPES.get(system_type_key.upper())
    if not st:
        raise ValueError(f"Unknown system type: {system_type_key}")
    db_name = f"rayd_{st['db_name_suffix']}"
    return create_engine(_get_pg_url(db_name))
