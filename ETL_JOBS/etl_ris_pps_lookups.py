"""
ETL_JOBS/etl_ris_pps_lookups.py — small RIS reference/catalog tables that support PPS:
STATUS -> std_status_ris, PROCEDURE_PRIORITY -> std_procedure_priorities,
DICTATION -> std_dictations.

See migration 0064 for target schemas and design notes. All three: full pull, no date
filter (reference/catalog data), refresh-on-conflict (no RAYD-owned fields to protect).
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_STATUS_TABLE   = os.getenv("RAYD_RIS_STATUS_TABLE", "STATUS")
_PRIORITY_TABLE = os.getenv("RAYD_RIS_PRIORITY_TABLE", "PROCEDURE_PRIORITY")
_DICTATION_TABLE = os.getenv("RAYD_RIS_DICTATION_TABLE", "DICTATION")
_FETCH_BATCH = 2000

_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)


def _safe_date(val):
    if val is None:
        return None
    try:
        dt = val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
        if dt.year == 0 or not (_SAFE_DATE_MIN <= dt <= _SAFE_DATE_MAX):
            return None
        return dt
    except Exception:
        return None


def _safe_str(val, max_len=None):
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None   # binary content (e.g. a BLOB column) — not stored as text
    try:
        s = str(val).strip()
    except TypeError:
        # DICTATION.AUDIO turned out to be a BLOB, not the text path/reference migration
        # 0064 guessed at — oracledb's LOB wrapper for a BLOB column's __str__ returns raw
        # bytes, which is a TypeError (crashed the whole RIS_DICTATION_ETL run, 2026-07-26).
        # Same defensive fallback for any other column that turns out to be binary.
        return None
    if not s:
        return None
    return s[:max_len] if max_len else s


def _safe_bool(val):
    if val is None:
        return None
    return str(val).strip().upper() in ('Y', 'YES', 'TRUE', '1')


def _safe_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _log_job(pg_engine, job_name):
    """Open an etl_job_log row. Returns (log_id, start_time); log_id may be None."""
    start_time = datetime.now()
    log_id = None
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, 'RUNNING', :t, 0) RETURNING id"),
                {"n": job_name, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"{job_name} log error: {e}")
    return log_id, start_time


def _close_job(pg_engine, log_id, start_time, status, total, error_msg=None):
    if not log_id:
        return
    try:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        with pg_engine.connect() as conn:
            conn.execute(
                text("UPDATE etl_job_log SET status=:s, end_time=:et, records_processed=:r, "
                     "duration_seconds=:d, error_message=:e WHERE id=:id"),
                {"s": status, "et": end_time, "r": total, "d": round(duration, 2),
                 "e": error_msg, "id": log_id}
            )
            conn.commit()
    except Exception as le:
        logging.error(f"Failed to close job log {log_id}: {le}")


_UPSERT_STATUS_SQL = text("""
    INSERT INTO std_status_ris (
        status_key, name, description, end_point, hl7_code, dicom_status, type,
        source_last_updated, time_threshold_check, dragged_behavior, drag_linked,
        core_status_key, last_update
    ) VALUES (
        :status_key, :name, :description, :end_point, :hl7_code, :dicom_status, :type,
        :source_last_updated, :time_threshold_check, :dragged_behavior, :drag_linked,
        :core_status_key, :last_update
    )
    ON CONFLICT (status_key) DO UPDATE SET
        name = EXCLUDED.name, description = EXCLUDED.description, end_point = EXCLUDED.end_point,
        hl7_code = EXCLUDED.hl7_code, dicom_status = EXCLUDED.dicom_status, type = EXCLUDED.type,
        source_last_updated = EXCLUDED.source_last_updated,
        time_threshold_check = EXCLUDED.time_threshold_check,
        dragged_behavior = EXCLUDED.dragged_behavior, drag_linked = EXCLUDED.drag_linked,
        core_status_key = EXCLUDED.core_status_key, last_update = EXCLUDED.last_update
""")


def run_ris_status_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_STATUS_ETL")
    total, error_msg, status = 0, None, "SUCCESS"

    query = f"""
        SELECT STATUS_KEY, NAME, DESCRIPTION, END_POINT, HL7_CODE, DICOM_STATUS, TYPE,
               LAST_UPDATED, TIME_THRESHOLD_CHECK, DRAGGED_BEHAVIOR, DRAG_LINKED, CORE_STATUS
        FROM {_STATUS_TABLE}
    """
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Status ETL] 🚀 Starting ({_STATUS_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for (skey, name, desc, end_point, hl7, dicom, type_, last_upd,
                 time_thresh, dragged, drag_linked, core_status) in batch:
                if skey is None:
                    continue
                params.append({
                    "status_key": skey, "name": _safe_str(name), "description": _safe_str(desc),
                    "end_point": _safe_bool(end_point), "hl7_code": _safe_str(hl7),
                    "dicom_status": _safe_str(dicom), "type": _safe_str(type_, 16),
                    "source_last_updated": _safe_date(last_upd),
                    "time_threshold_check": _safe_bool(time_thresh),
                    "dragged_behavior": _safe_int(dragged), "drag_linked": _safe_int(drag_linked),
                    "core_status_key": _safe_int(core_status), "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_STATUS_SQL, params)
                total += len(params)
        print(f"[RIS Status ETL] ✅ Done — {total:,} statuses upserted")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Status ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg)
    return total


_UPSERT_PRIORITY_SQL = text("""
    INSERT INTO std_procedure_priorities (priority_key, code, description, active, source_last_updated, last_update)
    VALUES (:priority_key, :code, :description, :active, :source_last_updated, :last_update)
    ON CONFLICT (priority_key) DO UPDATE SET
        code = EXCLUDED.code, description = EXCLUDED.description, active = EXCLUDED.active,
        source_last_updated = EXCLUDED.source_last_updated, last_update = EXCLUDED.last_update
""")


def run_ris_procedure_priority_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_PRIORITY_ETL")
    total, error_msg, status = 0, None, "SUCCESS"

    query = f"SELECT PRIORITY_KEY, CODE, DESCRIPTION, ACTIVE, LAST_UPDATED FROM {_PRIORITY_TABLE}"
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Priority ETL] 🚀 Starting ({_PRIORITY_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for pkey, code, desc, active, last_upd in batch:
                if pkey is None:
                    continue
                params.append({
                    "priority_key": pkey, "code": _safe_str(code), "description": _safe_str(desc),
                    "active": _safe_bool(active), "source_last_updated": _safe_date(last_upd),
                    "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_PRIORITY_SQL, params)
                total += len(params)
        print(f"[RIS Priority ETL] ✅ Done — {total:,} priorities upserted")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Priority ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg)
    return total


_UPSERT_DICTATION_SQL = text("""
    INSERT INTO std_dictations (
        dictation_key, dictation_date, last_modified_date, audio, revision_count,
        dictated_by_resource_id_key, last_update
    ) VALUES (
        :dictation_key, :dictation_date, :last_modified_date, :audio, :revision_count,
        :dictated_by_resource_id_key, :last_update
    )
    ON CONFLICT (dictation_key) DO UPDATE SET
        dictation_date = EXCLUDED.dictation_date, last_modified_date = EXCLUDED.last_modified_date,
        audio = EXCLUDED.audio, revision_count = EXCLUDED.revision_count,
        dictated_by_resource_id_key = EXCLUDED.dictated_by_resource_id_key,
        last_update = EXCLUDED.last_update
""")


def run_ris_dictation_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_DICTATION_ETL")
    total, error_msg, status = 0, None, "SUCCESS"

    query = f"""
        SELECT DICTATION_KEY, DICTATION_DATE, LAST_MODIFIED_DATE, AUDIO, REVISION_COUNT,
               DICTATED_BY_RESOURCE_ID_KEY
        FROM {_DICTATION_TABLE}
    """
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Dictation ETL] 🚀 Starting ({_DICTATION_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for dkey, ddate, lastmod, audio, revcount, dictated_by in batch:
                if dkey is None:
                    continue
                params.append({
                    "dictation_key": dkey, "dictation_date": _safe_date(ddate),
                    "last_modified_date": _safe_date(lastmod), "audio": _safe_str(audio),
                    "revision_count": _safe_int(revcount), "dictated_by_resource_id_key": dictated_by,
                    "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_DICTATION_SQL, params)
                total += len(params)
        print(f"[RIS Dictation ETL] ✅ Done — {total:,} dictations upserted")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Dictation ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg)
    return total
