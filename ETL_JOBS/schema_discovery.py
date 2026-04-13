"""
Schema Discovery — connects to a foreign Oracle DB (via db_params),
extracts all table/column metadata for a given schema owner,
and saves the result as a JSON file in ETL_JOBS/schema_dumps/.

Usage (from Flask route):
    from ETL_JOBS.schema_discovery import run_discovery
    result = run_discovery(connection_name='oracle_PACS', schema_owner='MEDISTORE')
"""

import os
import json
import logging
from datetime import datetime

import oracledb

logger = logging.getLogger("SCHEMA_DISCOVERY")

# ── Target schema we want to map TO ─────────────────────────────────────────
# This is what Claude reads to understand what columns we need.
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
            "insert_time":              {"type": "timestamp", "required": True,  "description": "When the record was inserted — used as high-water mark for incremental sync"},
            "last_update":              {"type": "timestamp", "required": False, "description": "Last modification timestamp"},
            "reading_physician_first_name":  {"type": "string", "required": False, "description": "Reading radiologist first name"},
            "reading_physician_last_name":   {"type": "string", "required": False, "description": "Reading radiologist last name"},
            "signing_physician_first_name":  {"type": "string", "required": False, "description": "Signing radiologist first name"},
            "signing_physician_last_name":   {"type": "string", "required": False, "description": "Signing radiologist last name"},
            "study_has_report":         {"type": "boolean",   "required": False, "description": "Whether the study has a report (Y/N or true/false)"},
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
            "patient_db_uid": {"type": "integer",   "required": True,  "description": "Unique patient identifier (primary key)"},
            "id":             {"type": "string",    "required": False, "description": "Patient external / HIS ID"},
            "fallback_id":    {"type": "string",    "required": False, "description": "Fallback patient ID used for matching"},
            "birth_date":     {"type": "date",      "required": False, "description": "Patient date of birth"},
            "sex":            {"type": "string",    "required": False, "description": "Patient sex (M / F / O)"},
            "number_of_patient_studies": {"type": "integer", "required": False, "description": "Total study count for this patient"},
        }
    },
    "orders": {
        "table": "etl_orders",
        "description": "Radiology orders / scheduled procedures",
        "columns": {
            "study_db_uid":       {"type": "integer",   "required": True,  "description": "Study reference (foreign key to studies)"},
            "proc_id":            {"type": "string",    "required": False, "description": "Procedure code"},
            "scheduled_datetime": {"type": "timestamp", "required": False, "description": "Scheduled exam date and time"},
            "order_status":       {"type": "string",    "required": False, "description": "Order status (CA=cancelled, CM=completed, etc.)"},
            "insert_time":        {"type": "timestamp", "required": False, "description": "Order insert timestamp"},
        }
    },
}


def _get_oracle_params(pg_engine, connection_name):
    """Read connection params from db_params table."""
    from sqlalchemy import text
    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT host, port, sid, username, password, mode FROM db_params WHERE name = :n"),
            {"n": connection_name}
        ).fetchone()
    if not row:
        raise ValueError(f"No db_params entry found for connection '{connection_name}'")
    return row


def _connect_oracle(params):
    from utils.crypto import decrypt
    host, port, sid, username, password, mode = params
    dsn = oracledb.makedsn(host, int(port or 1521), sid=sid)
    kwargs = {"user": username, "password": decrypt(password), "dsn": dsn}
    if mode and mode.upper() == 'SYSDBA':
        kwargs["mode"] = oracledb.SYSDBA
    return oracledb.connect(**kwargs)


def run_discovery(pg_engine, connection_name, schema_owner, output_dir=None):
    """
    Discover the schema of a foreign Oracle DB and save to JSON.
    Returns: dict with keys 'file', 'table_count', 'column_count', 'tables_summary'
    """
    if output_dir is None:
        base = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base, 'schema_dumps')
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Discovering schema: connection={connection_name}, owner={schema_owner}")

    params = _get_oracle_params(pg_engine, connection_name)
    conn   = _connect_oracle(params)
    cursor = conn.cursor()

    try:
        # ── Pull all columns for the schema owner ────────────────────────
        cursor.execute("""
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                c.nullable,
                c.data_length,
                c.data_precision,
                c.column_id,
                NVL(t.num_rows, -1) AS num_rows
            FROM all_tab_columns c
            LEFT JOIN all_tables t
                   ON t.table_name = c.table_name
                  AND t.owner      = c.owner
            WHERE c.owner = UPPER(:owner)
            ORDER BY c.table_name, c.column_id
        """, {"owner": schema_owner.upper()})

        rows = cursor.fetchall()

    finally:
        cursor.close()
        conn.close()

    if not rows:
        raise ValueError(f"No tables found for schema owner '{schema_owner}'. Check the owner name.")

    # ── Group by table ───────────────────────────────────────────────────
    tables = {}
    for table_name, col_name, data_type, nullable, data_length, data_precision, col_id, num_rows in rows:
        if table_name not in tables:
            tables[table_name] = {
                "name":       table_name,
                "row_count":  int(num_rows) if num_rows >= 0 else None,
                "columns":    []
            }
        tables[table_name]["columns"].append({
            "name":      col_name,
            "type":      data_type,
            "nullable":  nullable == 'Y',
            "length":    int(data_length) if data_length else None,
            "precision": int(data_precision) if data_precision else None,
            "position":  int(col_id),
        })

    tables_list = sorted(tables.values(), key=lambda t: t["name"])

    dump = {
        "connection_name":  connection_name,
        "schema_owner":     schema_owner.upper(),
        "discovered_at":    datetime.now().isoformat(),
        "table_count":      len(tables_list),
        "column_count":     sum(len(t["columns"]) for t in tables_list),
        "target_schema":    TARGET_SCHEMA,
        "tables":           tables_list,
    }

    timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename   = f"{connection_name}_{schema_owner}_{timestamp}.json"
    filepath   = os.path.join(output_dir, filename)

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
