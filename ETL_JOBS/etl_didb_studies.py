import logging
import time
from datetime import datetime
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
    
    # 1. INITIALIZE LOG (Explicit Commit)
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text("""
                INSERT INTO etl_job_log (job_name, status, start_time, records_processed)
                VALUES (:name, :status, :start, 0)
                RETURNING id
            """), {"name": job_name, "status": status, "start": start_time})
            log_id = res.fetchone()[0]
            conn.commit() # Ensure the 'RUNNING' state is visible immediately
    except Exception as e:
        logging.error(f"Failed to initialize ETL log: {e}")

    # Define columns to match the SELECT order and the PG table schema
    col_names = [
        'study_db_uid', 'patient_db_uid', 'study_instance_uid', 'accession_number',
        'study_id', 'storing_ae', 'study_date', 'study_description',
        'study_body_part', 'study_age', 'age_at_exam', 'number_of_study_series',
        'number_of_study_images', 'study_status', 'patient_class', 'procedure_code',
        'referring_physician_first_name', 'referring_physician_mid_name',
        'referring_physician_last_name', 'report_status', 'order_status',
        'last_accessed_time', 'insert_time', 'last_update'
    ]

    # Initialize Oracle connection
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    try:
        # CLEANED QUERY: Removed VARCHAR2 casts on UIDs to use native BIGINT/NUMBER performance
        query = """
            SELECT 
                s.STUDY_DB_UID, 
                s.PATIENT_DB_UID,
                s.STUDY_INSTANCE_UID, 
                s.ACCESSION_NUMBER, 
                s.STUDY_ID, 
                s.STORING_AE,
                s.STUDY_DATE, 
                CAST(SUBSTR(s.STUDY_DESCRIPTION,1,4000) AS VARCHAR2(4000)),
                s.STUDY_BODY_PART, 
                s.STUDY_AGE,
                CASE 
                    WHEN p.BIRTH_DATE IS NOT NULL AND s.STUDY_DATE IS NOT NULL 
                    THEN FLOOR((s.STUDY_DATE - p.BIRTH_DATE) / 365.25) 
                    ELSE NULL 
                END,
                s.NUMBER_OF_STUDY_SERIES, 
                s.NUMBER_OF_STUDY_IMAGES, 
                s.STUDY_STATUS,
                s.PATIENT_CLASS, 
                s.PROCEDURE_CODE, 
                s.REFERRING_PHYSICIAN_FIRST_NAME,
                s.REFERRING_PHYSICIAN_MID_NAME, 
                s.REFERRING_PHYSICIAN_LAST_NAME,
                s.REPORT_STATUS, 
                s.ORDER_STATUS, 
                s.LAST_ACCESS_TIME, 
                s.INSERT_TIME,
                CURRENT_TIMESTAMP
            FROM medistore.didb_studies s
            LEFT JOIN medistore.didb_patients_view p ON p.PATIENT_DB_UID = s.PATIENT_DB_UID
            WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
        """

        # Format go_live_date properly for the Oracle bind variable
        gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)
        
        cursor.execute(query, {'gd': gd_str})
        
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break

            # batch now contains native Python integers for the UIDs
            chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'study_db_uid')
            total_rows += len(batch)
            
            # Update partial progress every 5000 rows
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
        raise # Re-raise so the orchestrator knows it failed

    finally:
        # FINAL LOG UPDATE
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rows_per_sec = total_rows / duration if duration > 0 else 0

        if log_id:
            try:
                with pg_engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE etl_job_log 
                        SET status = :status, 
                            end_time = :end, 
                            records_processed = :rows,
                            rows_per_second = :rps,
                            duration_seconds = :dur,
                            error_message = :err
                        WHERE id = :id
                    """), {
                        "status": status,
                        "end": end_time,
                        "rows": total_rows,
                        "rps": rows_per_sec,
                        "dur": duration,
                        "err": error_msg,
                        "id": log_id
                    })
                    conn.commit()
            except Exception as log_err:
                logging.error(f"Failed to finalize ETL log: {log_err}")

        cursor.close()
        ora_conn.close()

    return total_rows
