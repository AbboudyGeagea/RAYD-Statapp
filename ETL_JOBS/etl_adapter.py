"""
Generic ETL Adapter — runs confirmed adapter_mappings.

For each confirmed mapping it:
  1. Opens the source DB connection (any supported type via schema_discovery)
  2. Reads each mapped table incrementally (or full-scan if no incremental_key)
  3. Applies column transforms (direct, date, timestamp, boolean_yn, string_truncate)
  4. Upserts rows into the target PostgreSQL table
  5. Records progress in adapter_etl_log

Entry points:
    run_all_adapters(app)        — called by APScheduler or manual trigger
    run_one_mapping(app, mapping_id)  — run a single mapping by ID
"""

import logging
from datetime import datetime, date

from sqlalchemy import text

logger = logging.getLogger("ETL_ADAPTER")

BATCH_SIZE = 500

# ── Type inference from Oracle/foreign type strings ───────────────────────────

_ORACLE_PG_MAP = {
    'NUMBER':    'NUMERIC',
    'INTEGER':   'INTEGER',
    'FLOAT':     'DOUBLE PRECISION',
    'VARCHAR2':  'TEXT',
    'NVARCHAR2': 'TEXT',
    'CHAR':      'TEXT',
    'NCHAR':     'TEXT',
    'CLOB':      'TEXT',
    'NCLOB':     'TEXT',
    'LONG':      'TEXT',
    'DATE':      'TIMESTAMP',
    'BLOB':      'BYTEA',
    'RAW':       'BYTEA',
}

def infer_pg_type(source_type):
    """Infer a safe PostgreSQL type from a foreign DB column type string."""
    t = (source_type or '').upper().split('(')[0].strip()
    if 'TIMESTAMP' in t:
        return 'TIMESTAMP'
    return _ORACLE_PG_MAP.get(t, 'TEXT')


# ── Transform helpers ─────────────────────────────────────────────────────────

def _apply_transform(value, transform):
    """Apply a single-column transform to a raw value from the source cursor."""
    if value is None:
        return None

    if transform == 'direct':
        # Coerce to str for types psycopg2 cannot handle natively
        if hasattr(value, 'read'):          # LOB object (Oracle)
            return value.read()
        return value

    if transform in ('date', 'timestamp'):
        if isinstance(value, (datetime, date)):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return str(value)

    if transform == 'boolean_yn':
        if isinstance(value, bool):
            return value
        return str(value).strip().upper() in ('Y', 'YES', '1', 'TRUE')

    if transform == 'string_truncate':
        s = str(value) if not hasattr(value, 'read') else value.read()
        return s[:65535] if s else s

    return value


# ── Watermark (incremental sync) ──────────────────────────────────────────────

def _get_watermark(pg_engine, mapping_id, target_table):
    """Return the last watermark value for a table, or None for full scan."""
    with pg_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT watermark_val FROM adapter_etl_log
            WHERE mapping_id = :mid AND target_table = :tbl AND status = 'done'
            ORDER BY finished_at DESC LIMIT 1
        """), {"mid": mapping_id, "tbl": target_table}).fetchone()
    return row[0] if row else None


def _log_start(pg_engine, mapping_id, target_table, watermark_col):
    """Insert a running log row and return its id."""
    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO adapter_etl_log
                (mapping_id, target_table, status, watermark_col, started_at)
            VALUES (:mid, :tbl, 'running', :wc, NOW())
            RETURNING id
        """), {"mid": mapping_id, "tbl": target_table, "wc": watermark_col}).fetchone()
    return row[0]


def _log_finish(pg_engine, log_id, rows_synced, watermark_val):
    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE adapter_etl_log
            SET status='done', finished_at=NOW(),
                rows_synced=:rs, watermark_val=:wv
            WHERE id=:id
        """), {"rs": rows_synced, "wv": str(watermark_val) if watermark_val is not None else None, "id": log_id})


def _log_error(pg_engine, log_id, error_msg):
    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE adapter_etl_log
            SET status='error', finished_at=NOW(), error_message=:em
            WHERE id=:id
        """), {"em": str(error_msg)[:2000], "id": log_id})


# ── Table sync ────────────────────────────────────────────────────────────────

def _ensure_target_table(pg_engine, tbl_def):
    """
    Create target table if it does not exist, using pg_type from mapping columns.
    Falls back to TEXT for any column missing pg_type.
    """
    target = tbl_def['target_table']
    cols   = tbl_def.get('columns', [])
    if not cols:
        return

    col_lines = []
    pk_col    = None

    for col in cols:
        pg_type = col.get('pg_type') or infer_pg_type(col.get('source_type', ''))
        col_lines.append(f"    {col['target']} {pg_type}")
        # First column with a non-null pg_type containing NOT NULL → candidate PK
        if pk_col is None and 'NOT NULL' in pg_type.upper():
            pk_col = col['target']

    inc_key_source = tbl_def.get('incremental_key')
    if inc_key_source:
        for col in cols:
            if col['source'] == inc_key_source:
                pk_col = col['target']
                break

    if pk_col:
        col_lines.append(f"    PRIMARY KEY ({pk_col})")

    ddl = f"CREATE TABLE IF NOT EXISTS {target} (\n" + ",\n".join(col_lines) + "\n);"
    with pg_engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info(f"Ensured table: {target}")


def _upsert_batch(pg_engine, target_table, col_defs, rows):
    """Upsert a batch of rows into the target PG table."""
    if not rows:
        return

    targets   = [c['target'] for c in col_defs]
    cols_sql  = ', '.join(targets)
    vals_sql  = ', '.join(f':{c}' for c in targets)

    # Determine PK for ON CONFLICT
    pk = col_defs[0]['target']  # first column assumed PK
    for c in col_defs:
        if 'NOT NULL' in (c.get('pg_type') or '').upper():
            pk = c['target']
            break

    set_sql = ', '.join(
        f"{c} = EXCLUDED.{c}" for c in targets if c != pk
    )
    on_conflict = f"ON CONFLICT ({pk}) DO UPDATE SET {set_sql}" if set_sql else f"ON CONFLICT ({pk}) DO NOTHING"

    upsert = text(f"""
        INSERT INTO {target_table} ({cols_sql})
        VALUES ({vals_sql})
        {on_conflict}
    """)

    with pg_engine.begin() as conn:
        for row in rows:
            params = {targets[i]: row[i] for i in range(len(targets))}
            conn.execute(upsert, params)


def _sync_table(src_conn, pg_engine, tbl_def, mapping_id):
    """Sync one source table to one PG target table. Returns rows synced."""
    source_table = tbl_def['source_table']
    target_table = tbl_def['target_table']
    col_defs     = tbl_def.get('columns', [])
    inc_key      = tbl_def.get('incremental_key')

    if not col_defs:
        logger.warning(f"Table {source_table} has no columns in mapping — skipped.")
        return 0

    # Ensure PG table exists
    _ensure_target_table(pg_engine, tbl_def)

    log_id = _log_start(pg_engine, mapping_id, target_table, inc_key)
    rows_synced  = 0
    last_wm_val  = None

    try:
        watermark = _get_watermark(pg_engine, mapping_id, target_table)
        src_cols  = ', '.join(c['source'] for c in col_defs)
        transforms = [c.get('transform', 'direct') for c in col_defs]

        if inc_key and watermark:
            query = f"SELECT {src_cols} FROM {source_table} WHERE {inc_key} > :wm ORDER BY {inc_key}"
            cursor = src_conn.cursor()
            cursor.execute(query, {"wm": watermark})
        else:
            query = f"SELECT {src_cols} FROM {source_table}"
            if inc_key:
                query += f" ORDER BY {inc_key}"
            cursor = src_conn.cursor()
            cursor.execute(query)

        while True:
            raw_rows = cursor.fetchmany(BATCH_SIZE)
            if not raw_rows:
                break

            transformed = []
            for raw_row in raw_rows:
                transformed_row = tuple(
                    _apply_transform(raw_row[i], transforms[i])
                    for i in range(len(col_defs))
                )
                transformed.append(transformed_row)
                if inc_key:
                    # Track the inc_key column position for watermark
                    inc_idx = next((j for j, c in enumerate(col_defs) if c['source'] == inc_key), None)
                    if inc_idx is not None:
                        last_wm_val = raw_row[inc_idx]

            _upsert_batch(pg_engine, target_table, col_defs, transformed)
            rows_synced += len(raw_rows)
            logger.info(f"  [{target_table}] synced {rows_synced} rows so far…")

        cursor.close()
        _log_finish(pg_engine, log_id, rows_synced, last_wm_val)
        logger.info(f"[{target_table}] done — {rows_synced} rows synced.")
        return rows_synced

    except Exception as e:
        _log_error(pg_engine, log_id, str(e))
        logger.error(f"[{target_table}] sync failed: {e}", exc_info=True)
        raise


# ── Public entry points ───────────────────────────────────────────────────────

def run_one_mapping(app, mapping_id):
    """
    Sync all tables for a single confirmed adapter mapping.
    Returns dict with per-table results.
    """
    with app.app_context():
        from db import db
        from ETL_JOBS.schema_discovery import _get_conn_row, _make_foreign_conn

        row = db.session.execute(
            text("SELECT * FROM adapter_mappings WHERE id = :id AND status = 'confirmed'"),
            {"id": mapping_id}
        ).mappings().fetchone()

        if not row:
            raise ValueError(f"Mapping {mapping_id} not found or not confirmed.")

        mapping_json    = row['mapping_json']
        connection_name = row['connection_name']

        conn_row               = _get_conn_row(db.engine, connection_name)
        _db_type, src_conn     = _make_foreign_conn(conn_row)

        results = {}
        try:
            for tbl_def in mapping_json.get('tables', []):
                target = tbl_def.get('target_table', '?')
                try:
                    n = _sync_table(src_conn, db.engine, tbl_def, mapping_id)
                    results[target] = {'rows': n, 'ok': True}
                except Exception as e:
                    results[target] = {'rows': 0, 'ok': False, 'error': str(e)}
        finally:
            src_conn.close()

        # Update last_etl_at on the mapping
        db.session.execute(
            text("UPDATE adapter_mappings SET last_etl_at = NOW() WHERE id = :id"),
            {"id": mapping_id}
        )
        db.session.commit()

        return results


def run_all_adapters(app):
    """
    Run ETL for all confirmed, enabled adapter mappings.
    Called by APScheduler. Errors on individual mappings are caught and logged.
    """
    logger.info("Adapter ETL: starting run for all confirmed mappings.")
    with app.app_context():
        from db import db
        rows = db.session.execute(text("""
            SELECT id, connection_name FROM adapter_mappings
            WHERE status = 'confirmed' AND etl_enabled = TRUE
            ORDER BY id
        """)).fetchall()

    for mapping_id, conn_name in rows:
        try:
            logger.info(f"Adapter ETL: running mapping {mapping_id} ({conn_name})")
            run_one_mapping(app, mapping_id)
        except Exception as e:
            logger.error(f"Adapter ETL: mapping {mapping_id} failed — {e}", exc_info=True)

    logger.info("Adapter ETL: all mappings processed.")
