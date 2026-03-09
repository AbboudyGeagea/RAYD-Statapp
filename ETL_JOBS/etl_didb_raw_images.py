import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

def run_raw_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, study_uid_whitelist):
    job_name   = "RAW_IMAGES_ETL"
    start_time = datetime.now()
    total_rows = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

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
        logging.error(f"Raw Images log error: {e}")

    if not study_uid_whitelist:
        print("[Raw Images ETL] ⏭️  No study IDs in whitelist, skipping.")
        return 0

    col_names = [
        'raw_image_db_uid', 'patient_db_uid', 'study_db_uid', 'series_db_uid',
        'study_instance_uid', 'series_instance_uid', 'image_number', 'last_update'
    ]

    total_chunks = (len(study_uid_whitelist) + 999) // 1000
    print(f"[Raw Images ETL] 🚀 Starting — {len(study_uid_whitelist):,} study IDs across {total_chunks} chunks")

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        for chunk_num, i in enumerate(range(0, len(study_uid_whitelist), 1000), start=1):
            chunk  = study_uid_whitelist[i:i + 1000]
            binds  = [f":id{j}" for j in range(len(chunk))]
            params = dict(zip([b.strip(':') for b in binds], chunk))
            query  = f"""
                SELECT raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid,
                       study_instance_uid, series_instance_uid, image_number, CURRENT_TIMESTAMP
                FROM medistore.didb_raw_images
                WHERE study_db_uid IN ({','.join(binds)})
            """
            cursor.execute(query, params)

            batch_num = 0
            while True:
                batch = cursor.fetchmany(2000)
                if not batch:
                    break
                batch_num += 1
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'raw_image_db_uid')
                total_rows += len(batch)

                if total_rows % 10000 == 0:
                    print(f"[Raw Images ETL] 📦 Chunk {chunk_num}/{total_chunks} — {total_rows:,} rows so far...")
                    if log_id:
                        with pg_engine.connect() as conn:
                            conn.execute(
                                text("UPDATE etl_job_log SET records_processed = :r WHERE id = :id"),
                                {"r": total_rows, "id": log_id}
                            )
                            conn.commit()

        status = "SUCCESS"
        print(f"[Raw Images ETL] ✅ Done — {total_rows:,} rows")
        logging.info(f"Raw Images ETL complete: {total_rows:,} rows")

    except Exception as exc:
        status    = "FAILED"
        error_msg = str(exc)
        logging.error(f"Raw Images ETL error: {error_msg}")
        print(f"[Raw Images ETL] ❌ Failed: {error_msg}")
        raise

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rps      = round(total_rows / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:e, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, error_message=:m WHERE id=:id"),
                        {"s": status, "e": datetime.now(), "r": total_rows,
                         "d": round(duration, 2), "rps": rps,
                         "m": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update Raw Images log: {le}")
        cursor.close()
        ora_conn.close()

    return total_rows
