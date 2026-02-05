import logging
import time
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
import cx_Oracle

def run_patients_etl(oracle_conn, pg_engine, logger, batch_size=1000):
    """
    Optimized Patient ETL: Only imports patients who exist in the 
    local 'etl_didb_studies' table.
    Now unified to use BIGINT (Python int) for all UID fields.
    """
    logger.info("🚀 Starting Patient ETL (Study-Linked Whitelist)")
    ora_cursor = None
    start_time = time.time()
    
    # Target columns in Postgres (mapping fallback_pid -> fallback_id)
    col_names = [
        'patient_db_uid', 'id', 'birth_date', 'sex',
        'number_of_patient_studies', 'number_of_patient_series', 
        'number_of_patient_images', 'mdl_patient_dbid', 'fallback_id'
    ]

    try:
        # 1. Fetch Whitelist from local Postgres
        with pg_engine.connect() as conn:
            # FIX: Keep patient_db_uid as an integer (r[0]), do not cast to str()
            query_whitelist = text("""
                SELECT DISTINCT patient_db_uid 
                FROM etl_didb_studies 
                WHERE patient_db_uid IS NOT NULL
            """)
            valid_patient_ids = [r[0] for r in conn.execute(query_whitelist).fetchall()]

        if not valid_patient_ids:
            logger.warning("⚠️ No studies found in Postgres. Patient sync skipped.")
            return 0

        logger.info(f"Whitelisting {len(valid_patient_ids)} unique patients from studies.")
        ora_cursor = oracle_conn.cursor()
        total_processed = 0

        # 2. Chunked Oracle Retrieval (Max 1000 IDs per IN clause)
        for i in range(0, len(valid_patient_ids), 1000):
            id_chunk = valid_patient_ids[i:i + 1000]
            bind_names = [f":id{j}" for j in range(len(id_chunk))]
            
            # Mapping Oracle's 'fallback_pid' to our schema's 'fallback_id'
            # Native selection of patient_db_uid as NUMBER (int)
            ora_query = f"""
                SELECT patient_db_uid, id, birth_date, sex,
                       number_of_patient_studies, number_of_patient_series, 
                       number_of_patient_images, mdl_patient_dbid, fallback_pid
                FROM medistore.didb_patients_view
                WHERE patient_db_uid IN ({','.join(bind_names)})
            """
            
            # Bind parameters: dictionary mapping names to integer IDs
            bind_params = dict(zip([b.strip(':') for b in bind_names], id_chunk))
            ora_cursor.execute(ora_query, bind_params)
            
            rows = ora_cursor.fetchall()
            if not rows:
                continue

            # 3. Prepare Batch for Upsert
            current_ts = datetime.now()
            mapped_rows = []
            for row in rows:
                d = dict(zip(col_names, row))
                d['last_update'] = current_ts
                # Ensure the UID is treated as a native int for Postgres BIGINT
                mapped_rows.append(d)

            # 4. Postgres Upsert (Insert or Update if patient metadata changed)
            upsert_stmt = text("""
                INSERT INTO etl_patient_view (
                    patient_db_uid, id, birth_date, sex, 
                    number_of_patient_studies, number_of_patient_series, 
                    number_of_patient_images, mdl_patient_dbid, fallback_id, last_update
                ) VALUES (
                    :patient_db_uid, :id, :birth_date, :sex, 
                    :number_of_patient_studies, :number_of_patient_series, 
                    :number_of_patient_images, :mdl_patient_dbid, :fallback_id, :last_update
                )
                ON CONFLICT (patient_db_uid) DO UPDATE SET
                    id = EXCLUDED.id,
                    birth_date = EXCLUDED.birth_date,
                    sex = EXCLUDED.sex,
                    number_of_patient_studies = EXCLUDED.number_of_patient_studies,
                    number_of_patient_series = EXCLUDED.number_of_patient_series,
                    number_of_patient_images = EXCLUDED.number_of_patient_images,
                    last_update = EXCLUDED.last_update;
            """)

            with pg_engine.begin() as pg_conn:
                pg_conn.execute(upsert_stmt, mapped_rows)
            
            total_processed += len(mapped_rows)

        duration = time.time() - start_time
        logger.info(f"✅ Patient ETL complete: {total_processed} records in {duration:.2f}s")
        return total_processed

    except Exception as e:
        logger.error(f"💥 Patient ETL failed: {str(e)}")
        raise
    finally:
        if ora_cursor:
            ora_cursor.close()
