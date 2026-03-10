import logging
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import OracleConnector
from etl_settings import ETL_GEAR

logger = logging.getLogger("ETL_WORKER")


def run_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, study_uid_whitelist):
    job_name   = "IMAGE_LOCATIONS_ETL"
    start_time = datetime.now()
    total_rows = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    # ── 1. Open log entry ────────────────────────────────────────────────
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
        logger.error(f"[{job_name}] Log open error: {e}")

    # ── 2. ETL logic ─────────────────────────────────────────────────────
    try:
        if not study_uid_whitelist:
            logger.info(f"[{job_name}] Empty whitelist — skipping.")
            status = "SUCCESS"

        else:
            # Matches etl_image_locations table exactly:
            # raw_image_db_uid | file_system | image_size_kb | last_update
            col_names = [
                'raw_image_db_uid',
                'file_system',
                'image_size_kb',
            ]

            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor   = ora_conn.cursor()
            cursor.arraysize = ETL_GEAR["oracle_prefetch"]

            max_workers = ETL_GEAR.get("num_workers", 4)
            batch_size  = ETL_GEAR.get("batch_size", 5000)
            chunk_size  = 1000

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                for i in range(0, len(study_uid_whitelist), chunk_size):
                    chunk  = study_uid_whitelist[i : i + chunk_size]
                    binds  = [f":id{j}" for j in range(len(chunk))]
                    params = {f"id{j}": v for j, v in enumerate(chunk)}

                    # SELECT only the 4 columns that exist in pg etl_image_locations
                    # raw_image_db_uid, file_system, image_size_kb (KB), last_update
                    query = f"""
                        SELECT
                            il.raw_image_db_uid,
                            il.file_system,
                            il.image_size AS image_size_kb
                        FROM medistore.didb_image_locations il
                        WHERE il.raw_image_db_uid IN (
                            SELECT ri.raw_image_db_uid
                            FROM medistore.DIDB_RAW_IMAGES_TABLE ri
                            WHERE ri.study_db_uid IN ({','.join(binds)})
                        )
                    """

                    cursor.execute(query, params)

                    while True:
                        batch = cursor.fetchmany(batch_size)
                        if not batch:
                            break
                        futures.append(
                            executor.submit(
                                chunked_upsert_func,
                                pg_engine, pg_table, col_names, batch, 'raw_image_db_uid'
                            )
                        )
                        total_rows += len(batch)

                        # Backpressure — drain every 4× workers
                        if len(futures) >= max_workers * 4:
                            done = [f for f in futures if f.done()]
                            for f in done:
                                f.result()
                            futures = [f for f in futures if not f.done()]

                for f in as_completed(futures):
                    f.result()

            cursor.close()
            ora_conn.close()
            status = "SUCCESS"

    except Exception as exc:
        status    = "FAILED"
        error_msg = str(exc)
        logger.error(f"[{job_name}] Error: {error_msg}", exc_info=True)

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log "
                             "SET status=:s, end_time=:et, records_processed=:r, error_message=:m "
                             "WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total_rows, "m": error_msg, "id": log_id}
                    )
                    conn.commit()
                logger.info(f"[{job_name}] Finished — status={status}, rows={total_rows:,}")
            except Exception as e:
                logger.error(f"[{job_name}] Log close error: {e}")

    return total_rows
