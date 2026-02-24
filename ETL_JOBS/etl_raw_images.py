import logging
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from db import OracleConnector
from etl_settings import ETL_GEAR

def run_raw_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, study_uid_whitelist):
    job_name = "RAW_IMAGES_ETL"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None
    
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) VALUES (:n,:s,:t,0) RETURNING id"),
                               {"n": job_name, "s": status, "t": start_time})
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e: logging.error(f"Images log error: {e}")

    if not study_uid_whitelist: return 0

    col_names = ['raw_image_db_uid', 'patient_db_uid', 'study_db_uid', 'series_db_uid', 'study_instance_uid', 'series_instance_uid', 'image_number', 'last_update']
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    cursor.arraysize = ETL_GEAR["oracle_prefetch"]

    try:
        with ThreadPoolExecutor(max_workers=ETL_GEAR["num_workers"]) as executor:
            for i in range(0, len(study_uid_whitelist), 1000):
                chunk = study_uid_whitelist[i:i+1000]
                binds = [f":id{j}" for j in range(len(chunk))]
                query = f"SELECT raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid, study_instance_uid, series_instance_uid, image_number, CURRENT_TIMESTAMP FROM medistore.didb_raw_images WHERE study_db_uid IN ({','.join(binds)})"
                params = dict(zip([b.strip(':') for b in binds], chunk))
                cursor.execute(query, params)
                
                while True:
                    batch = cursor.fetchmany(ETL_GEAR["batch_size"])
                    if not batch: break
                    executor.submit(chunked_upsert_func, pg_engine, pg_table, col_names, batch, 'raw_image_db_uid')
                    total_rows += len(batch)
        status = "SUCCESS"
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise
    finally:
        if log_id:
            with pg_engine.connect() as conn:
                conn.execute(text("UPDATE etl_job_log SET status=:s, end_time=NOW(), records_processed=:r, error_message=:m WHERE id=:id"),
                             {"s": status, "r": total_rows, "m": error_msg, "id": log_id})
                conn.commit()
        cursor.close(); ora_conn.close()
    return total_rows
