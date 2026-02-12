import logging
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from db import OracleConnector
from etl_settings import ETL_GEAR

def run_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func):
    job_name = "IMAGE_LOCATIONS_ETL"
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
        'raw_images_db_uid', 'source_db_uid', 'file_system', 'image_size_kb', 
        'file_num', 'image_checksum', 'path_type', 'last_update'
    ]

    try:
        # 2. GET WHITELIST (Based on Raw Images already synced)
        with pg_engine.connect() as conn:
            valid_image_ids = [r[0] for r in conn.execute(text("SELECT raw_image_db_uid FROM etl_didb_raw_images")).fetchall()]

        if not valid_image_ids:
            logging.info("No valid raw image IDs found. Skipping locations sync.")
            status = "SUCCESS"
            return 0

        # 3. ORACLE CONNECTION WITH PREFETCH GEAR
        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()
        cursor.arraysize = ETL_GEAR["oracle_prefetch"]

        # 4. PARALLEL THREADED UPSERT
        with ThreadPoolExecutor(max_workers=ETL_GEAR["num_workers"]) as executor:
            for i in range(0, len(valid_image_ids), 1000):
                chunk = valid_image_ids[i:i+1000]
                binds = [f":id{j}" for j in range(len(chunk))]
                
                # Keep your Oracle-specific logic (ROUND for KB)
                query = f"""
                    SELECT 
                        RAW_IMAGE_DB_UID, SOURCE_DB_UID, FILE_SYSTEM, 
                        ROUND(IMAGE_SIZE/1024), FILE_NUM, IMAGE_CHECKSUM, 
                        PATH_TYPE, CURRENT_TIMESTAMP 
                    FROM medistore.didb_image_locations 
                    WHERE RAW_IMAGE_DB_UID IN ({','.join(binds)})
                """
                
                bind_dict = dict(zip([b.strip(':') for b in binds], chunk))
                cursor.execute(query, bind_dict)

                while True:
                    batch = cursor.fetchmany(ETL_GEAR["batch_size"])
                    if not batch: 
                        break
                    
                    # High-Gear Parallel Upsert
                    executor.submit(chunked_upsert_func, pg_engine, pg_table, col_names, batch, 'raw_images_db_uid')
                    total_rows += len(batch)

                    # Periodic Progress Log Update
                    if total_rows % ETL_GEAR["log_interval"] == 0 and log_id:
                        _update_progress(pg_engine, log_id, total_rows)

        status = "SUCCESS"

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        logging.exception(f"🛑 {job_name} Error")
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
                logging.error(f"Final log update error: {e}")

        if 'cursor' in locals(): cursor.close()
        if 'ora_conn' in locals(): ora_conn.close()

    return total_rows

def _update_progress(engine, log_id, count):
    """Silent progress updater for the UI/Log table"""
    try:
        with engine.connect() as conn:
            conn.execute(text("UPDATE etl_job_log SET records_processed=:r WHERE id=:id"), 
                         {"r": count, "id": log_id})
            conn.commit()
    except: pass
