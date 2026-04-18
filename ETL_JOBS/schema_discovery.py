"""
Schema Discovery — connects to any supported foreign DB (Oracle, PostgreSQL, MySQL, MSSQL),
extracts table/column metadata for a given schema owner, and saves as JSON.

Usage:
    from ETL_JOBS.schema_discovery import run_discovery
    result = run_discovery(pg_engine, connection_name='oracle_PACS', schema_owner='MEDISTORE')
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger("SCHEMA_DISCOVERY")

# ── Target schema we want to map TO ─────────────────────────────────────────
TARGET_SCHEMA = {
    "studies": {
        "table": "etl_didb_studies",
        "description": "Main radiology studies/exams table",
        "columns": {
            "study_db_uid":             {"type": "integer",   "required": True,  "description": "Unique study identifier (primary key / auto-increment)"},
            "patient_db_uid":           {"type": "integer",   "required": True,  "description": "Patient identifier (foreign key to patients)"},
            "study_instance_uid":       {"type": "string",    "required": False, "description": "DICOM Study Instance UID"},
            "accession_number":         {"type": "string",    "required": False, "description": "Accession number"},
            "study_id":                 {"type": "string",    "required": False, "description": "Study ID"},
            "storing_ae":               {"type": "string",    "required": False, "description": "AE title of the storing/sending device"},
            "study_date":               {"type": "date",      "required": True,  "description": "Date the study was performed"},
            "study_description":        {"type": "string",    "required": False, "description": "Study description / body part / protocol name"},
            "study_body_part":          {"type": "string",    "required": False, "description": "Body part examined"},
            "study_age":                {"type": "string",    "required": False, "description": "Patient age at time of study (raw string, e.g. '045Y')"},
            "age_at_exam":              {"type": "float",     "required": False, "description": "Computed age in years at time of exam"},
            "number_of_study_series":   {"type": "integer",   "required": False, "description": "Number of series in the study"},
            "number_of_study_images":   {"type": "integer",   "required": False, "description": "Total image count"},
            "study_status":             {"type": "string",    "required": False, "description": "Study read/report status (UNREAD, READ, REPORTED, etc.)"},
            "patient_class":            {"type": "string",    "required": False, "description": "Patient class (ER, IP, OP, Emergency, etc.)"},
            "patient_location":         {"type": "string",    "required": False, "description": "Patient location code (max 3 chars)"},
            "procedure_code":           {"type": "string",    "required": False, "description": "Procedure / exam code"},
            "referring_physician_first_name": {"type": "string", "required": False, "description": "Referring physician first name"},
            "referring_physician_last_name":  {"type": "string", "required": False, "description": "Referring physician last name"},
            "report_status":            {"type": "string",    "required": False, "description": "Report status"},
            "order_status":             {"type": "string",    "required": False, "description": "Order status"},
            "insert_time":              {"type": "timestamp", "required": True,  "description": "When the record was inserted — high-water mark for incremental sync"},
            "last_update":              {"type": "timestamp", "required": False, "description": "Last modification timestamp"},
            "reading_physician_first_name":  {"type": "string", "required": False, "description": "Reading radiologist first name"},
            "reading_physician_last_name":   {"type": "string", "required": False, "description": "Reading radiologist last name"},
            "signing_physician_first_name":  {"type": "string", "required": False, "description": "Signing radiologist first name"},
            "signing_physician_last_name":   {"type": "string", "required": False, "description": "Signing radiologist last name"},
            "study_has_report":         {"type": "boolean",   "required": False, "description": "Whether the study has a report"},
            "rep_prelim_timestamp":     {"type": "timestamp", "required": False, "description": "Preliminary report timestamp"},
            "rep_prelim_signed_by":     {"type": "string",    "required": False, "description": "Who signed the preliminary report"},
            "rep_final_signed_by":      {"type": "string",    "required": False, "description": "Who signed the final report"},
            "rep_final_timestamp":      {"type": "timestamp", "required": True,  "description": "When the final report was signed — key for TAT calculations"},
            "rep_has_addendum":         {"type": "boolean",   "required": False, "description": "Whether the report has an addendum"},
            "is_linked_study":          {"type": "boolean",   "required": False, "description": "Whether this is a linked/related study"},
        }
    },
    "patients": {
        "table": "etl_patient_view",
        "description": "Patient demographics",
        "columns": {
            "patient_db_uid": {"type": "integer", "required": True,  "description": "Unique patient identifier"},
            "id":             {"type": "string",  "required": False, "description": "Patient external / HIS ID"},
            "fallback_id":    {"type": "string",  "required": False, "description": "Fallback patient ID"},
            "birth_date":     {"type": "date",    "required": False, "description": "Patient date of birth"},
            "sex":            {"type": "string",  "required": False, "description": "Patient sex (M / F / O)"},
            "number_of_patient_studies": {"type": "integer", "required": False, "description": "Total study count"},
        }
    },
    "orders": {
        "table": "etl_orders",
        "description": "Radiology orders / scheduled procedures",
        "columns": {
            "study_db_uid":       {"type": "integer",   "required": True,  "description": "Study reference"},
            "proc_id":            {"type": "string",    "required": False, "description": "Procedure code"},
            "scheduled_datetime": {"type": "timestamp", "required": False, "description": "Scheduled exam date and time"},
            "order_status":       {"type": "string",    "required": False, "description": "Order status"},
            "insert_time":        {"type": "timestamp", "required": False, "description": "Order insert timestamp"},
        }
    },
}


# ── Driver availability ──────────────────────────────────────────────────────

DRIVER_REGISTRY = {
    'oracle': {
        'label': 'Oracle',       'pip': 'oracledb',
        'import': 'oracledb',    'color': '#f59e0b',
        'icon': 'bi-database-fill',
    },
    'postgres': {
        'label': 'PostgreSQL',   'pip': 'psycopg2-binary',
        'import': 'psycopg2',    'color': '#60a5fa',
        'icon': 'bi-database',
    },
    'mysql': {
        'label': 'MySQL',        'pip': 'pymysql',
        'import': 'pymysql',     'color': '#34d399',
        'icon': 'bi-database',
    },
    'mssql': {
        'label': 'SQL Server',   'pip': 'pyodbc',
        'import': 'pyodbc',      'color': '#a78bfa',
        'icon': 'bi-microsoft',
        'note': 'Also requires unixodbc system library',
    },
}


def check_drivers():
    """Return driver availability status for all supported DB types."""
    status = {}
    for key, info in DRIVER_REGISTRY.items():
        try:
            __import__(info['import'])
            status[key] = {**info, 'available': True}
        except ImportError:
            status[key] = {**info, 'available': False}
    return status


# ── Connection helpers ───────────────────────────────────────────────────────

def _get_conn_row(pg_engine, connection_name):
    from sqlalchemy import text
    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM db_params WHERE name = :n"),
            {"n": connection_name}
        ).mappings().fetchone()
    if not row:
        raise ValueError(f"No db_params entry for '{connection_name}'")
    return dict(row)


def _make_foreign_conn(row):
    """Open a raw DB-API connection to the foreign DB described by a db_params row."""
    from utils.crypto import decrypt
    db_type  = (row.get('db_type') or '').lower()
    host     = row.get('host') or ''
    port     = row.get('port')
    sid      = row.get('sid') or ''       # Oracle SID  /  database name for others
    username = row.get('username') or ''
    password = decrypt(row['password']) if row.get('password') else ''
    mode     = (row.get('mode') or '').upper()

    if 'oracle' in db_type:
        import oracledb
        dsn    = oracledb.makedsn(host, int(port or 1521), sid=sid)
        kwargs = {"user": username, "password": password, "dsn": dsn}
        if mode == 'SYSDBA':
            kwargs["mode"] = oracledb.SYSDBA
        return 'oracle', oracledb.connect(**kwargs)

    if 'postgres' in db_type or db_type in ('pg', 'postgresql'):
        import psycopg2
        return 'postgres', psycopg2.connect(
            host=host, port=int(port or 5432),
            database=sid, user=username, password=password
        )

    if 'mysql' in db_type:
        import pymysql
        return 'mysql', pymysql.connect(
            host=host, port=int(port or 3306),
            database=sid, user=username, password=password
        )

    if 'mssql' in db_type or 'sqlserver' in db_type:
        import pyodbc
        cs = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
              f"SERVER={host},{int(port or 1433)};DATABASE={sid};"
              f"UID={username};PWD={password}")
        return 'mssql', pyodbc.connect(cs, timeout=5)

    raise ValueError(f"Unsupported db_type: '{db_type}'")


def test_connection(pg_engine, connection_name):
    """Attempt to open and immediately close a connection. Returns (ok, message)."""
    try:
        row = _get_conn_row(pg_engine, connection_name)
        _, conn = _make_foreign_conn(row)
        conn.close()
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)


# ── Vendor-specific discovery queries ────────────────────────────────────────

def _discover_oracle(conn, schema_owner):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT c.table_name, c.column_name, c.data_type,
                   c.nullable, c.data_length, c.data_precision,
                   c.column_id, NVL(t.num_rows, -1) AS num_rows
            FROM   all_tab_columns c
            LEFT JOIN all_tables t
                   ON t.table_name = c.table_name AND t.owner = c.owner
            WHERE  c.owner = UPPER(:owner)
            ORDER  BY c.table_name, c.column_id
        """, {"owner": schema_owner.upper()})
        rows = cursor.fetchall()
    finally:
        cursor.close()

    tables = {}
    for table_name, col_name, data_type, nullable, data_length, data_precision, col_id, num_rows in rows:
        if table_name not in tables:
            tables[table_name] = {"name": table_name, "row_count": int(num_rows) if num_rows >= 0 else None, "columns": []}
        tables[table_name]["columns"].append({
            "name": col_name, "type": data_type,
            "nullable": nullable == 'Y',
            "length": int(data_length) if data_length else None,
            "precision": int(data_precision) if data_precision else None,
            "position": int(col_id),
        })
    return tables


def _discover_postgres(conn, schema_owner):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT table_name, column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision, ordinal_position
            FROM   information_schema.columns
            WHERE  table_schema = %s
            ORDER  BY table_name, ordinal_position
        """, (schema_owner,))
        rows = cursor.fetchall()
    finally:
        cursor.close()

    tables = {}
    for table_name, col_name, data_type, is_nullable, char_len, num_prec, ordinal in rows:
        if table_name not in tables:
            tables[table_name] = {"name": table_name, "row_count": None, "columns": []}
        tables[table_name]["columns"].append({
            "name": col_name, "type": data_type,
            "nullable": is_nullable == 'YES',
            "length": int(char_len) if char_len else None,
            "precision": int(num_prec) if num_prec else None,
            "position": int(ordinal),
        })
    return tables


def _discover_mysql(conn, schema_owner):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, ORDINAL_POSITION
            FROM   information_schema.COLUMNS
            WHERE  TABLE_SCHEMA = %s
            ORDER  BY TABLE_NAME, ORDINAL_POSITION
        """, (schema_owner,))
        rows = cursor.fetchall()
    finally:
        cursor.close()

    tables = {}
    for table_name, col_name, data_type, is_nullable, char_len, num_prec, ordinal in rows:
        if table_name not in tables:
            tables[table_name] = {"name": table_name, "row_count": None, "columns": []}
        tables[table_name]["columns"].append({
            "name": col_name, "type": data_type,
            "nullable": is_nullable == 'YES',
            "length": int(char_len) if char_len else None,
            "precision": int(num_prec) if num_prec else None,
            "position": int(ordinal),
        })
    return tables


def _discover_mssql(conn, schema_owner):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT t.name, c.name, tp.name,
                   CASE c.is_nullable WHEN 1 THEN 'YES' ELSE 'NO' END,
                   c.max_length, c.precision, c.column_id
            FROM   sys.tables t
            JOIN   sys.columns c  ON c.object_id = t.object_id
            JOIN   sys.types   tp ON tp.user_type_id = c.user_type_id
            WHERE  SCHEMA_NAME(t.schema_id) = ?
            ORDER  BY t.name, c.column_id
        """, schema_owner)
        rows = cursor.fetchall()
    finally:
        cursor.close()

    tables = {}
    for table_name, col_name, data_type, is_nullable, char_len, num_prec, col_id in rows:
        if table_name not in tables:
            tables[table_name] = {"name": table_name, "row_count": None, "columns": []}
        tables[table_name]["columns"].append({
            "name": col_name, "type": data_type,
            "nullable": is_nullable == 'YES',
            "length": int(char_len) if char_len else None,
            "precision": int(num_prec) if num_prec else None,
            "position": int(col_id),
        })
    return tables


# ── Public entry point ───────────────────────────────────────────────────────

def run_discovery(pg_engine, connection_name, schema_owner, output_dir=None):
    """
    Discover the schema of any configured foreign DB and save to JSON.
    Returns: dict with keys 'file', 'table_count', 'column_count', 'tables_summary'
    """
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema_dumps')
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Discovering schema: connection={connection_name}, owner={schema_owner}")

    row              = _get_conn_row(pg_engine, connection_name)
    db_type_key, conn = _make_foreign_conn(row)

    dispatch = {
        'oracle':   _discover_oracle,
        'postgres': _discover_postgres,
        'mysql':    _discover_mysql,
        'mssql':    _discover_mssql,
    }
    discover_fn = dispatch.get(db_type_key)
    if not discover_fn:
        conn.close()
        raise ValueError(f"No discovery handler for db_type '{db_type_key}'")

    try:
        tables = discover_fn(conn, schema_owner)
    finally:
        conn.close()

    if not tables:
        raise ValueError(f"No tables found for schema owner '{schema_owner}'.")

    tables_list = sorted(tables.values(), key=lambda t: t["name"])

    dump = {
        "connection_name": connection_name,
        "schema_owner":    schema_owner.upper(),
        "db_type":         db_type_key,
        "discovered_at":   datetime.now().isoformat(),
        "table_count":     len(tables_list),
        "column_count":    sum(len(t["columns"]) for t in tables_list),
        "target_schema":   TARGET_SCHEMA,
        "tables":          tables_list,
    }

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename  = f"{connection_name}_{schema_owner}_{timestamp}.json"
    filepath  = os.path.join(output_dir, filename)
    with open(filepath, 'w') as f:
        json.dump(dump, f, indent=2, default=str)

    logger.info(f"Schema dump saved: {filepath} ({len(tables_list)} tables)")

    return {
        "file":           filename,
        "filepath":       filepath,
        "table_count":    len(tables_list),
        "column_count":   dump["column_count"],
        "tables_summary": [
            {"name": t["name"], "columns": len(t["columns"]), "rows": t["row_count"]}
            for t in tables_list
        ],
    }
