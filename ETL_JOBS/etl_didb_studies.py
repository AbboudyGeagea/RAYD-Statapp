import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

def run_studies_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    job_name = "STUDIES_ETL"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None
    processed_uids = []

    # 1. Start Log Entry
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"Log error: {e}")

    # Prepare Dates
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    try:
        # Get Max UID — determines FULL load vs INCREMENTAL load
        try:
            with pg_engine.connect() as conn:
                val = conn.execute(text(f"SELECT MAX(study_db_uid) FROM {pg_table}")).fetchone()[0]
                max_uid = val if val else 0
        except:
            max_uid = 0

        is_fresh_load = (max_uid == 0)

        if is_fresh_load:
            print(f"[Studies ETL] 🆕 Fresh load detected — pulling ALL studies since {gd_str}")
            logging.info(f"Studies ETL: fresh load, pulling all since {gd_str}")
        else:
            print(f"[Studies ETL] 🔄 Incremental load — max_uid={max_uid:,}, lookback={lookback_date}")
            logging.info(f"Studies ETL: incremental, max_uid={max_uid}, lookback={lookback_date}")

        col_names = [
            'study_db_uid', 'patient_db_uid', 'study_instance_uid', 'accession_number',
            'study_id', 'storing_ae', 'study_date', 'study_description',
            'study_body_part', 'study_age', 'age_at_exam', 'number_of_study_series',
            'number_of_study_images', 'study_status', 'patient_class', 'procedure_code',
            'referring_physician_first_name', 'referring_physician_mid_name',
            'referring_physician_last_name', 'report_status', 'order_status',
            'last_accessed_time', 'insert_time', 'last_update',
            'reading_physician_first_name', 'reading_physician_last_name', 'reading_physician_id',
            'signing_physician_first_name', 'signing_physician_last_name', 'signing_physician_id',
            'study_has_report', 'rep_prelim_timestamp', 'rep_prelim_signed_by',
            'rep_transcribed_by', 'rep_transcribed_timestamp',
            'rep_final_signed_by', 'rep_final_timestamp',
            'rep_addendum_by', 'rep_addendum_timestamp', 'rep_has_addendum',
            'is_linked_study', 'patient_location'
        ]

        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()

        # ── FRESH LOAD: pull everything since go_live_date, no UID filter ──
        # ── INCREMENTAL: pull new UIDs OR recently updated studies ──────────
        if is_fresh_load:
            query = """
                SELECT
                    s.STUDY_DB_UID, s.PATIENT_DB_UID, s.STUDY_INSTANCE_UID, s.ACCESSION_NUMBER,
                    s.STUDY_ID,
                    UPPER(TRIM(s.STORING_AE)),
                    s.STUDY_DATE,
                    CAST(SUBSTR(s.STUDY_DESCRIPTION, 1, 4000) AS VARCHAR2(4000)),
                    s.STUDY_BODY_PART, s.STUDY_AGE,
                    CASE
                        WHEN p.BIRTH_DATE IS NOT NULL AND s.STUDY_DATE IS NOT NULL
                        THEN FLOOR((s.STUDY_DATE - p.BIRTH_DATE) / 365.25)
                        ELSE NULL
                    END as age_at_exam,
                    s.NUMBER_OF_STUDY_SERIES, s.NUMBER_OF_STUDY_IMAGES, s.STUDY_STATUS,
                    s.PATIENT_CLASS, s.PROCEDURE_CODE, s.REFERRING_PHYSICIAN_FIRST_NAME,
                    s.REFERRING_PHYSICIAN_MID_NAME, s.REFERRING_PHYSICIAN_LAST_NAME,
                    s.REPORT_STATUS, s.ORDER_STATUS, s.LAST_ACCESS_TIME, s.INSERT_TIME,
                    CURRENT_TIMESTAMP,
                    s.READING_PHYSICIAN_FIRST_NAME, s.READING_PHYSICIAN_LAST_NAME, s.READING_PHYSICIAN_ID,
                    s.SIGNING_PHYSICIAN_FIRST_NAME, s.SIGNING_PHYSICIAN_LAST_NAME, s.SIGNING_PHYSICIAN_ID,
                    CASE WHEN s.STUDY_HAS_REPORT = 'Y' THEN 'true' ELSE 'false' END,
                    s.REP_PRELIM_TIMESTAMP, s.REP_PRELIM_SIGNED_BY,
                    s.REP_TRANSCRIBED_BY, s.REP_TRANSCRIBED_TIMESTAMP,
                    s.REP_FINAL_SIGNED_BY, s.REP_FINAL_TIMESTAMP,
                    s.REP_ADDENDUM_BY, s.REP_ADDENDUM_TIMESTAMP,
                    CASE WHEN s.REP_HAS_ADDENDUM = 'Y' THEN 'true' ELSE 'false' END,
                    CASE WHEN s.IS_LINKED_STUDY = 'Y' THEN 'true' ELSE 'false' END,
                    SUBSTR(s.PATIENT_LOCATION, 1, 3)
                FROM medistore.didb_studies s
                LEFT JOIN medistore.didb_patients_view p ON p.PATIENT_DB_UID = s.PATIENT_DB_UID
                WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
                ORDER BY s.STUDY_DB_UID
            """
            cursor.execute(query, {'gd': gd_str})

        else:
            query = """
                SELECT
                    s.STUDY_DB_UID, s.PATIENT_DB_UID, s.STUDY_INSTANCE_UID, s.ACCESSION_NUMBER,
                    s.STUDY_ID,
                    UPPER(TRIM(s.STORING_AE)),
                    s.STUDY_DATE,
                    CAST(SUBSTR(s.STUDY_DESCRIPTION, 1, 4000) AS VARCHAR2(4000)),
                    s.STUDY_BODY_PART, s.STUDY_AGE,
                    CASE
                        WHEN p.BIRTH_DATE IS NOT NULL AND s.STUDY_DATE IS NOT NULL
                        THEN FLOOR((s.STUDY_DATE - p.BIRTH_DATE) / 365.25)
                        ELSE NULL
                    END as age_at_exam,
                    s.NUMBER_OF_STUDY_SERIES, s.NUMBER_OF_STUDY_IMAGES, s.STUDY_STATUS,
                    s.PATIENT_CLASS, s.PROCEDURE_CODE, s.REFERRING_PHYSICIAN_FIRST_NAME,
                    s.REFERRING_PHYSICIAN_MID_NAME, s.REFERRING_PHYSICIAN_LAST_NAME,
                    s.REPORT_STATUS, s.ORDER_STATUS, s.LAST_ACCESS_TIME, s.INSERT_TIME,
                    CURRENT_TIMESTAMP,
                    s.READING_PHYSICIAN_FIRST_NAME, s.READING_PHYSICIAN_LAST_NAME, s.READING_PHYSICIAN_ID,
                    s.SIGNING_PHYSICIAN_FIRST_NAME, s.SIGNING_PHYSICIAN_LAST_NAME, s.SIGNING_PHYSICIAN_ID,
                    CASE WHEN s.STUDY_HAS_REPORT = 'Y' THEN 'true' ELSE 'false' END,
                    s.REP_PRELIM_TIMESTAMP, s.REP_PRELIM_SIGNED_BY,
                    s.REP_TRANSCRIBED_BY, s.REP_TRANSCRIBED_TIMESTAMP,
                    s.REP_FINAL_SIGNED_BY, s.REP_FINAL_TIMESTAMP,
                    s.REP_ADDENDUM_BY, s.REP_ADDENDUM_TIMESTAMP,
                    CASE WHEN s.REP_HAS_ADDENDUM = 'Y' THEN 'true' ELSE 'false' END,
                    CASE WHEN s.IS_LINKED_STUDY = 'Y' THEN 'true' ELSE 'false' END,
                    SUBSTR(s.PATIENT_LOCATION, 1, 3)
                FROM medistore.didb_studies s
                LEFT JOIN medistore.didb_patients_view p ON p.PATIENT_DB_UID = s.PATIENT_DB_UID
                WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
                AND (
                    s.STUDY_DB_UID > :max_id
                    OR s.STUDY_DATE >= TO_DATE(:lb, 'YYYY-MM-DD')
                )
                ORDER BY s.STUDY_DB_UID
            """
            cursor.execute(query, {'gd': gd_str, 'max_id': max_uid, 'lb': lookback_date})

        # ── Batch fetch & upsert ─────────────────────────────────────────────
        batch_num  = 0
        skipped_pk = 0
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break
            batch_num += 1

            # Guard: study_db_uid (index 0) must be a valid integer — Oracle
            # occasionally has corrupted PKs like "R3". Skip those rows.
            clean = []
            for row in batch:
                try:
                    int(row[0])
                    clean.append(row)
                except (TypeError, ValueError):
                    skipped_pk += 1
                    logging.warning(f"[Studies ETL] Skipping bad study_db_uid: {row[0]!r}")

            if clean:
                processed_uids.extend([row[0] for row in clean])
                chunked_upsert_func(pg_engine, pg_table, col_names, clean, 'study_db_uid')
                total_rows += len(clean)

            print(f"[Studies ETL] 📦 Batch {batch_num} — {total_rows:,} rows so far, {skipped_pk} bad PKs skipped")

        status = "SUCCESS"
        cursor.close()
        ora_conn.close()

    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logging.error(f"Studies ETL Failed: {error_msg}")

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rows_per_sec = round(total_rows / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total_rows,
                         "d": round(duration, 2), "rps": rows_per_sec,
                         "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update studies log: {le}")

    return total_rows, processed_uids
