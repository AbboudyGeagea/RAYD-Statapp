import logging
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from db import OracleConnector
from etl_settings import ETL_GEAR  # Your external gear file

def run_raw_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func):
    job_name = "RAW_IMAGES_ETL"
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
        logging.error(f"Log initialization error: {e}")

    col_names = [
        'raw_image_db_uid', 'patient_db_uid', 'study_db_uid', 'series_db_uid', 
        'study_instance_uid', 'series_instance_uid', 'image_number', 'last_update'
    ]

    try:
        # 2. GET WHITELIST (Series already in Postgres)
        with pg_engine.connect() as conn:
            valid_series_ids = [r[0] for r in conn.execute(text("SELECT series_db_uid FROM etl_didb_serieses")).fetchall()]
        
        if not valid_series_ids:
            logging.info("No valid series IDs found to sync images for.")
            status = "SUCCESS"
            return 0

        # 3. CONFIGURE ORACLE WITH PREFETCH
        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()
        cursor.arraysize = ETL_GEAR["oracle_prefetch"] # Use the Gear settings

        # 4. PARALLEL EXECUTION VIA THREADPOOL
        # We use a pool to handle Postgres upserts while Oracle fetches the next chunk
        with ThreadPoolExecutor(max_workers=ETL_GEAR["num_workers"]) as executor:
            # Chunk the IN clause to avoid Oracle's 1000 limit
            for i in range(0, len(valid_series_ids), 1000):
                chunk = valid_series_ids[i:i+1000]
                binds = [f":id{j}" for j in range(len(chunk))]
                
                query = f"""
                    SELECT 
                        raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid, 
                        study_instance_uid, series_instance_uid, image_number, 
                        CURRENT_TIMESTAMP 
                    FROM medistore.didb_raw_images 
                    WHERE series_db_uid IN ({','.join(binds)})
                """
                
                bind_values = dict(zip([b.strip(':') for b in binds], chunk))
                cursor.execute(query, bind_values)

                while True:
                    batch = cursor.fetchmany(ETL_GEAR["batch_size"])
                    if not batch: 
                        break
                    
                    # Submit to thread pool for non-blocking Postgres write
                    executor.submit(chunked_upsert_func, pg_engine, pg_table, col_names, batch, 'raw_image_db_uid')
                    total_rows += len(batch)

                    # Update log every log_interval (e.g., 25k rows)
                    if total_rows % ETL_GEAR["log_interval"] == 0 and log_id:
                        _update_live_progress(pg_engine, log_id, total_rows)

        status = "SUCCESS"

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        logging.exception(f"🛑 {job_name} Failure")
        raise
    finally:
        # 5. FINAL LOG UPDATE
        if log_id:
            try:
                with pg_engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE etl_job_log 
                        SET status=:status, end_time=:end, records_processed=:rows, error_message=:err 
                        WHERE id=:id
                    """), {
                        "status": status, 
                        "end": datetime.now(), 
                        "rows": total_rows, 
                        "err": error_msg[:1000] if error_msg else None, 
                        "id": log_id
                    })
                    conn.commit()
            except Exception as e:
                logging.error(f"Final log update failed: {e}")
        
        if 'cursor' in locals(): cursor.close()
        if 'ora_conn' in locals(): ora_conn.close()

    return total_rows

def _update_live_progress(engine, log_id, rows):
    """Helper to update record count without finishing the job"""
    try:
        with engine.connect() as conn:
            conn.execute(text("UPDATE etl_job_log SET records_processed=:r WHERE id=:id"), 
                         {"r": rows, "id": log_id})
            conn.commit()
    except:
        pass
