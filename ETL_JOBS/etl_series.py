import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

def run_series_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func):
    job_name = "SERIES_ETL"
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
        logging.error(f"Failed to initialize Series log: {e}")

    col_names = [
        'series_db_uid', 'study_db_uid', 'patient_db_uid', 'study_instance_uid', 
        'series_instance_uid', 'series_number', 'modality', 'number_of_series_images',
        'body_part_examined', 'protocol_name', 'series_description', 'series_icon_blob_len',
        'institution_name', 'station_name', 'manufacturer', 'institutional_department_name',
        'last_update'
    ]

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    try:
        with pg_engine.connect() as conn:
            valid_study_ids = [r[0] for r in conn.execute(text("SELECT study_db_uid FROM etl_didb_studies")).fetchall()]

        if not valid_study_ids:
            status = "SUCCESS"
            return 0

        for i in range(0, len(valid_study_ids), 1000):
            chunk = valid_study_ids[i:i+1000]
            binds = [f":id{j}" for j in range(len(chunk))]
            query = f"SELECT series_db_uid, study_db_uid, patient_db_uid, study_instance_uid, series_instance_uid, series_number, modality, number_of_series_images, body_part_examined, protocol_name, series_description, series_icon_blob_len, institution_name, station_name, manufacturer, institutional_department_name, CURRENT_TIMESTAMP FROM medistore.didb_serieses WHERE study_db_uid IN ({','.join(binds)})"
            
            params = dict(zip([b.strip(':') for b in binds], chunk))
            cursor.execute(query, params)
            
            while True:
                batch = cursor.fetchmany(1000)
                if not batch: break
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'series_db_uid')
                total_rows += len(batch)
                
            if log_id and total_rows % 5000 == 0:
                with pg_engine.connect() as conn:
                    conn.execute(text("UPDATE etl_job_log SET records_processed = :r WHERE id = :id"), {"r": total_rows, "id": log_id})
                    conn.commit()

        status = "SUCCESS"
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise
    finally:
        end_time = datetime.now()
        if log_id:
            with pg_engine.connect() as conn:
                conn.execute(text("UPDATE etl_job_log SET status=:status, end_time=:end, records_processed=:rows, error_message=:err WHERE id=:id"),
                             {"status": status, "end": end_time, "rows": total_rows, "err": error_msg, "id": log_id})
                conn.commit()
        cursor.close()
        ora_conn.close()
    return total_rows
