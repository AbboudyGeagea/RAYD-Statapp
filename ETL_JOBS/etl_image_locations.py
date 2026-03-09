import logging
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from db import OracleConnector
from etl_settings import ETL_GEAR

def run_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, study_uid_whitelist):
    job_name   = "IMAGE_LOCATIONS_ETL"
    start_time = datetime.now()
    total_rows = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    # ── Log start ─────────────────────────────────────────────────────────────
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
        logging.error(f"Image Locations log error: {e}")

    # ── Main logic ────────────────────────────────────────────────────────────
    try:
        if not study_uid_whitelist:
            logging.info(f"{job_name}: No studies in whitelist. Skipping.")
            status = "SUCCESS"
        else:
            # 8 columns — matches PG table exactly
            col_names = [
                'raw_image_db_uid', 'source_db_uid', 'file_system',
                'image_size_kb', 'file_num', 'image_checksum',
                'path_type', 'last_update'
            ]

            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor   = ora_conn.cursor()
            cursor.arraysize = ETL_GEAR["oracle_prefetch"]

            batch_num = 0
            with ThreadPoolExecutor(max_workers=ETL_GEAR["num_workers"]) as executor:
                for i in range(0, len(study_uid_whitelist), 1000):
                    chunk = study_uid_whitelist[i:i + 1000]
                    binds = [f":id{j}" for j in range(len(chunk))]
                    params = dict(zip([b.strip(':') for b in binds], chunk))

                    # Fix 1: didb_raw_images (plural) — was didb_raw_image
                    # Fix 2: CURRENT_TIMESTAMP added to match last_update column
                    query = f"""
                        SELECT
                            loc.raw_image_db_uid,
                            loc.source_db_uid,
                            loc.file_system,
                            ROUND(loc.image_size / 1024),
                            loc.file_num,
                            loc.image_checksum,
                            loc.path_type,
                            CURRENT_TIMESTAMP
                        FROM medistore.didb_image_locations loc
                        WHERE loc.raw_image_db_uid IN (
                            SELECT raw_image_db_uid
                            FROM medistore.didb_raw_images
                            WHERE study_db_uid IN ({','.join(binds)})
                        )
                    """

                    cursor.execute(query, params)

                    while True:
                        batch = cursor.fetchmany(ETL_GEAR["batch_size"])
                        if not batch:
                            break
                        batch_num += 1
                        executor.submit(
                            chunked_upsert_func,
                            pg_engine, pg_table, col_names, batch, 'raw_image_db_uid'
                        )
                        total_rows += len(batch)
                        print(f"[Image Locations ETL] 📦 Batch {batch_num} — {total_rows:,} rows so far...")

            cursor.close()
            ora_conn.close()
            status = "SUCCESS"
            print(f"[Image Locations ETL] ✅ Done — {total_rows:,} rows")

    except Exception as exc:
        status    = "FAILED"
        error_msg = str(exc)
        logging.error(f"Error in {job_name}: {error_msg}")
        print(f"[Image Locations ETL] ❌ Failed: {error_msg}")
        # Not re-raising — allows master ETL to continue

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rps      = round(total_rows / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, error_message=:m WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total_rows,
                         "d": round(duration, 2), "rps": rps,
                         "m": error_msg, "id": log_id}
                    )
                    conn.commit()
                logging.info(f"{job_name} finished — status: {status}, rows: {total_rows}")
            except Exception as e:
                logging.error(f"Failed to update final log for {job_name}: {e}")

    return total_rows
