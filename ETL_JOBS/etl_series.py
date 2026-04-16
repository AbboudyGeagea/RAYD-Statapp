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
    
    # 1. Initial Log Entry
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e: 
        logging.error(f"Series log error: {e}")

    # 2. Handle Empty Whitelist specifically
    if not study_uid_whitelist:
        logging.info("No active studies to sync Series for.")
        status = "SUCCESS" # Mark as success even if 0 rows
        # We don't return here anymore; we let it fall through to 'finally' 
        # so the DB record is updated with the end_time.
    else:
        # --- Extraction Logic ---
        # Pre-load valid study IDs already in PG to guard against FK violations.
        # Oracle sometimes returns series for study_db_uid values that failed to
        # insert in Phase 1 (e.g. corrupt PKs like "R3" that were skipped).
        with pg_engine.connect() as _c:
            _rows = _c.execute(text("SELECT study_db_uid FROM etl_didb_studies")).fetchall()
        valid_study_ids = {r[0] for r in _rows}
        logging.info(f"Series ETL: {len(valid_study_ids):,} valid study IDs loaded from PG")

        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()
        try:
            col_names = [
                'series_db_uid', 'study_db_uid', 'patient_db_uid', 'study_instance_uid',
                'series_instance_uid', 'series_number', 'modality', 'number_of_series_images',
                'body_part_examined', 'protocol_name', 'series_description', 'series_icon_blob_len',
                'institution_name', 'station_name', 'manufacturer', 'institutional_department_name',
                'last_update'
            ]

            skipped_fk = 0
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
                    # study_db_uid is index 1 — skip rows whose parent study isn't in PG
                    clean = [r for r in batch if r[1] in valid_study_ids]
                    skipped_fk += len(batch) - len(clean)
                    if clean:
                        chunked_upsert_func(pg_engine, pg_table, col_names, clean, 'series_db_uid')
                        total_rows += len(clean)

            logging.info(f"Series ETL: {skipped_fk} rows skipped (orphan study FK)")
            status = "SUCCESS"
        except Exception as exc:
            status = "FAILED"
            error_msg = str(exc)
            logging.error(f"ETL Error in {job_name}: {error_msg}")
        finally:
            cursor.close()
            ora_conn.close()

    # 3. FINAL LOG UPDATE (Always executes)
    if log_id:
        try:
            end_time = datetime.now()
            # Calculate duration in seconds for internal logging if needed
            duration = (end_time - start_time).total_seconds()
            
            with pg_engine.connect() as conn:
                conn.execute(
                    text("""
                        UPDATE etl_job_log 
                        SET status=:s, 
                            end_time=:et, 
                            records_processed=:r, 
                            error_message=:e 
                        WHERE id=:id
                    """),
                    {
                        "s": status, 
                        "et": end_time, 
                        "r": total_rows, 
                        "e": error_msg, 
                        "id": log_id
                    }
                )
                conn.commit()
            logging.info(f"Job {job_name} finished: {status} ({total_rows} rows in {duration}s)")
        except Exception as e:
            logging.error(f"Failed to update final log: {e}")

    return total_rows
