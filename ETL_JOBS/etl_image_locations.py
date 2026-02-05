import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

def run_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func):
    job_name = "IMAGE_LOCATIONS_ETL"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None

    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text("""
                INSERT INTO etl_job_log (job_name, status, start_time, records_processed)
                VALUES (:name, :status, :start, 0)
                RETURNING id
            """), {"name": job_name, "status": status, "start": start_time})
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e: logging.error(f"Log error: {e}")

    col_names = ['raw_images_db_uid', 'source_db_uid', 'file_system', 'image_size_kb', 'file_num', 'image_checksum', 'path_type', 'last_update']

    try:
        with pg_engine.connect() as conn:
            valid_image_ids = [r[0] for r in conn.execute(text("SELECT raw_image_db_uid FROM etl_didb_raw_images")).fetchall()]

        if not valid_image_ids:
            status = "SUCCESS"
            return 0

        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()

        for i in range(0, len(valid_image_ids), 1000):
            chunk = valid_image_ids[i:i+1000]
            binds = [f":id{j}" for j in range(len(chunk))]
            query = f"SELECT RAW_IMAGE_DB_UID, SOURCE_DB_UID, FILE_SYSTEM, ROUND(IMAGE_SIZE/1024), FILE_NUM, IMAGE_CHECKSUM, PATH_TYPE, CURRENT_TIMESTAMP FROM medistore.didb_image_locations WHERE RAW_IMAGE_DB_UID IN ({','.join(binds)})"
            
            cursor.execute(query, dict(zip([b.strip(':') for b in binds], chunk)))
            while True:
                batch = cursor.fetchmany(5000)
                if not batch: break
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'raw_images_db_uid')
                total_rows += len(batch)
        
        status = "SUCCESS"
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        raise
    finally:
        if log_id:
            with pg_engine.connect() as conn:
                conn.execute(text("UPDATE etl_job_log SET status=:status, end_time=:end, records_processed=:rows, error_message=:err WHERE id=:id"),
                             {"status": status, "end": datetime.now(), "rows": total_rows, "err": error_msg, "id": log_id})
                conn.commit()
    return total_rows
