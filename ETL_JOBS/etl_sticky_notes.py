"""
ETL_JOBS/etl_sticky_notes.py — what the JCI urgent read measure needs from PACS (LAUMC).

Two steps, both once a night after the studies load. See migration 0143.

run_sticky_notes_etl    PACS DIDB_STUDY_DATA -> etl_sticky_notes: who wrote each sticky note
                        and when. One query on a table of about 52,000 rows. The note text
                        (DATA) and the author's display name are never read.
record_access_history   Postgres only: copies each recent study's lock and access values
                        from etl_didb_studies, which Phase 1 has just refreshed, into
                        study_access_history. PACS overwrites them, and so does Phase 1 the
                        next night.

Real notes are the rows with delete_indication = 'N' AND data_len > 1. Blank and deleted
ones are kept here so a later deletion in PACS reaches the row already copied.

Notes and history on studies RAYD does not hold are removed each run. That keeps SJH out
(operator, 2026-11-05: only RH) once the RH-only purge has removed its studies, along with
the devices the studies ETL leaves out. A note whose study is not loaded yet comes back the
next night, because the lookback pulls it again.
"""
import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from db import OracleConnector

_LOOKBACK_DAYS = 10     # same as the studies ETL, whose window this history follows
_FETCH_BATCH = 1000

_NOTES_QUERY = """
    SELECT d.DATA_DB_UID, d.STUDY_DB_UID, d.USER_NAME, d.DATE_CREATED,
           d.LAST_UPDATED_BY, d.LAST_UPDATE_TIME, d.DELETE_INDICATION, d.DATA_LEN
    FROM medistore.didb_study_data d
    WHERE d.TYPE = 1
      AND d.ATTRIBUTES = '113000'
      AND d.DATE_CREATED >= TO_DATE(:cutoff, 'YYYY-MM-DD')
"""

_RECENT_ONLY = """
      AND (d.DATE_CREATED >= TO_DATE(:lookback, 'YYYY-MM-DD')
           OR d.LAST_UPDATE_TIME >= TO_DATE(:lookback, 'YYYY-MM-DD'))
"""

_UPSERT_SQL = text("""
    INSERT INTO etl_sticky_notes (
        data_db_uid, study_db_uid, user_name, date_created,
        last_updated_by, last_update_time, delete_indication, data_len, last_update
    ) VALUES (
        :data_db_uid, :study_db_uid, :user_name, :date_created,
        :last_updated_by, :last_update_time, :delete_indication, :data_len, :last_update
    )
    ON CONFLICT (data_db_uid) DO UPDATE SET
        study_db_uid      = EXCLUDED.study_db_uid,
        user_name         = EXCLUDED.user_name,
        date_created      = EXCLUDED.date_created,
        last_updated_by   = EXCLUDED.last_updated_by,
        last_update_time  = EXCLUDED.last_update_time,
        delete_indication = EXCLUDED.delete_indication,
        data_len          = EXCLUDED.data_len,
        last_update       = EXCLUDED.last_update
""")

_REMOVE_OUT_OF_SCOPE_SQL = text("""
    DELETE FROM etl_sticky_notes n
    WHERE NOT EXISTS (SELECT 1 FROM etl_didb_studies s WHERE s.study_db_uid = n.study_db_uid)
""")

_REMOVE_HISTORY_OUT_OF_SCOPE_SQL = text("""
    DELETE FROM study_access_history h
    WHERE NOT EXISTS (SELECT 1 FROM etl_didb_studies s WHERE s.study_db_uid = h.study_db_uid)
""")

_HISTORY_SQL = text("""
    INSERT INTO study_access_history (
        study_db_uid, captured_at, study_locked, study_lock_time,
        rep_last_accessed_by, last_accessed_time
    )
    SELECT s.study_db_uid, :captured_at, s.study_locked, s.study_lock_time,
           s.rep_last_accessed_by, s.last_accessed_time
    FROM etl_didb_studies s
    WHERE s.study_date >= :since
    ON CONFLICT (study_db_uid, captured_at) DO NOTHING
""")


def run_sticky_notes_etl(pg_engine, oracle_source, go_live_date):
    job_name = "STICKY_NOTES_ETL"
    start_time = datetime.now()
    total = 0
    status = "RUNNING"
    error_msg = None
    log_id = None

    try:
        with pg_engine.connect() as conn:
            log_id = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            ).fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"Sticky Notes ETL log error: {e}")

    with pg_engine.connect() as conn:
        is_fresh_load = conn.execute(text("SELECT COUNT(*) FROM etl_sticky_notes")).scalar() == 0

    cutoff = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)
    params = {"cutoff": cutoff}
    query = _NOTES_QUERY
    if not is_fresh_load:
        query += _RECENT_ONLY
        params["lookback"] = (datetime.now() - timedelta(days=_LOOKBACK_DAYS)).strftime('%Y-%m-%d')

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    try:
        cursor.execute(query, params)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            now = datetime.now()
            rows = [{
                "data_db_uid": data_uid, "study_db_uid": study_uid,
                "user_name": user_name, "date_created": created,
                "last_updated_by": updated_by, "last_update_time": updated_at,
                "delete_indication": deleted, "data_len": data_len, "last_update": now,
            } for data_uid, study_uid, user_name, created, updated_by, updated_at, deleted, data_len in batch]
            with pg_engine.begin() as conn:
                conn.execute(_UPSERT_SQL, rows)
            total += len(rows)

        with pg_engine.begin() as conn:
            conn.execute(_REMOVE_OUT_OF_SCOPE_SQL)
        status = "SUCCESS"
        logging.info(f"Sticky Notes ETL complete: {total:,} rows")

    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logging.error(f"Sticky Notes ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, records_processed=:r, "
                             "duration_seconds=:d, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round((end_time - start_time).total_seconds(), 2),
                         "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update Sticky Notes log: {le}")

    return total


def record_access_history(pg_engine, captured_at=None):
    """One row per recent study, every run. No PACS query. Returns the rows added.
    History of a study RAYD no longer holds goes too, as with the notes above."""
    captured_at = captured_at or datetime.now()
    since = (captured_at - timedelta(days=_LOOKBACK_DAYS)).date()
    with pg_engine.begin() as conn:
        conn.execute(_REMOVE_HISTORY_OUT_OF_SCOPE_SQL)
        return conn.execute(_HISTORY_SQL, {"captured_at": captured_at, "since": since}).rowcount
