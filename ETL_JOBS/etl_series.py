import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

def run_series_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, study_uid_whitelist):
    job_name = "SERIES_ETL"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None
    
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) VALUES (:n, :s, :t, 0) RETURNING id"),
                               {"n": job_name, "s": status, "t": start_time})
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e: logging.error(f"Series log error: {e}")

    if not study_uid_whitelist:
        logging.info("No active studies to sync Series for.")
        status = "SUCCESS"
        return 0

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
        for i in range(0, len(study_uid_whitelist), 1000):
            chunk = study_uid_whitelist[i:i+1000]
            binds = [f":id{j}" for j in range(len(chunk))]
            query = f"""
                SELECT series_db_uid, study_db_uid, patient_db_uid, study_instance_uid, 
                series_instance_uid, series_number, modality, number_of_series_images,
                body_part_examined, protocol_name, series_description, series_icon_blob_len,
                institution_name, station_name, manufacturer, institutional_department_name,
                CURRENT_TIMESTAMP FROM medistore.didb_serieses 
                WHERE study_db_uid IN ({','.join(binds)})
            """
            params = dict(zip([b.strip(':') for b in binds], chunk))
            cursor.execute(query, params)
            
            while True:
                batch = cursor.fetchmany(1000)
                if not batch: break
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'series_db_uid')
                total_rows += len(batch)
        status = "SUCCESS"
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise
    finally:
        if log_id:
            with pg_engine.connect() as conn:
                conn.execute(text("UPDATE etl_job_log SET status=:s, end_time=NOW(), records_processed=:r, error_message=:e WHERE id=:id"),
                             {"s": status, "r": total_rows, "e": error_msg, "id": log_id})
                conn.commit()
        cursor.close(); ora_conn.close()
    return total_rows
