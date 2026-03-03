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
    
    # 1. Start Log
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) VALUES (:n,:s,:t,0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e: 
        logging.error(f"Images log error: {e}")

    # 2. Logic Wrap
    try:
        # Check whitelist inside the try so we can still hit the 'finally' update
        if not study_uid_whitelist:
            logging.info(f"{job_name}: No studies to process.")
            status = "SUCCESS"
        else:
            col_names = ['raw_image_db_uid', 'patient_db_uid', 'study_db_uid', 'series_db_uid', 'study_instance_uid', 'series_instance_uid', 'image_number', 'last_update']
            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor = ora_conn.cursor()
            cursor.arraysize = ETL_GEAR["oracle_prefetch"]

            # Use the ThreadPoolExecutor as before
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
            
            cursor.close()
            ora_conn.close()
            status = "SUCCESS"

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        logging.error(f"FATAL ERROR in {job_name}: {error_msg}")
        # Note: We don't 'raise' here if we want the master script to continue 
        # to the next job, but we'll leave it to your preference.

    finally:
        # 3. Final Log Update (Now guaranteed to run)
        if log_id:
            try:
                # Explicitly capture end_time to ensure accurate duration in dashboard
                end_time = datetime.now()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, records_processed=:r, error_message=:m WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total_rows, "m": error_msg, "id": log_id}
                    )
                    conn.commit()
                logging.info(f"{job_name} complete. Status: {status}, Rows: {total_rows}")
            except Exception as e:
                logging.error(f"Could not update {job_name} final log: {e}")

    return total_rows
