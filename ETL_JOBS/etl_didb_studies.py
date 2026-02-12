import logging
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

def run_studies_etl(
    pg_engine,
    oracle_source,
    pg_table,
    chunked_upsert_func,
    go_live_date
):
    job_name = "STUDIES_ETL"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None
    
    # 1. INITIALIZE LOG
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text("""
                INSERT INTO etl_job_log (job_name, status, start_time, records_processed)
                VALUES (:name, :status, :start, 0)
                RETURNING id
            """), {"name": job_name, "status": status, "start": start_time})
            log_id = res.fetchone()[0]
            conn.commit() 
    except Exception as e:
        logging.error(f"Failed to initialize ETL log: {e}")

    # 2. CALCULATE INCREMENTAL FILTERS
    # We look for the highest UID we already have and set a 10-day date window
    max_uid = 0
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    try:
        with pg_engine.connect() as conn:
            # Check if table exists/has data to get the high watermark
            res = conn.execute(text(f"SELECT MAX(study_db_uid) FROM {pg_table}"))
            val = res.fetchone()[0]
            max_uid = val if val is not None else 0
    except Exception as e:
        logging.warning(f"Could not fetch max_uid, defaulting to 0: {e}")

    # 3. DEFINE COLUMNS
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

    # 4. INITIALIZE CONNECTIONS
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    try:
        # 5. DUAL-LOGIC QUERY
        # Logic: Pull if it's a NEW ID OR if it's a RECENT study (last 10 days)
        # We still respect the global Go Live Date for the absolute floor.
        query = """
            SELECT 
                s.STUDY_DB_UID, s.PATIENT_DB_UID, s.STUDY_INSTANCE_UID, s.ACCESSION_NUMBER, 
                s.STUDY_ID, s.STORING_AE, s.STUDY_DATE, 
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
        """

        cursor.execute(query, {'gd': gd_str, 'max_id': max_uid, 'lb': lookback_date})
        
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break

            chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'study_db_uid')
            total_rows += len(batch)
            
            if log_id and total_rows % 5000 == 0:
                with pg_engine.connect() as conn:
                    conn.execute(text("UPDATE etl_job_log SET records_processed = :r WHERE id = :id"), 
                                     {"r": total_rows, "id": log_id})
                    conn.commit()

        status = "SUCCESS"

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        logging.exception("STUDIES_ETL_ERROR")
        raise 

    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rows_per_sec = total_rows / duration if duration > 0 else 0

        if log_id:
            try:
                with pg_engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE etl_job_log 
                        SET status = :status, end_time = :end, records_processed = :rows,
                            rows_per_second = :rps, duration_seconds = :dur, error_message = :err
                        WHERE id = :id
                    """), {
                        "status": status, "end": end_time, "rows": total_rows,
                        "rps": rows_per_sec, "dur": duration, "err": error_msg, "id": log_id
                    })
                    conn.commit()
            except Exception as log_err:
                logging.error(f"Failed to finalize ETL log: {log_err}")

        cursor.close()
        ora_conn.close()

    return total_rows
