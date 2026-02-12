import logging
import time
from datetime import datetime, date
from sqlalchemy import text
import cx_Oracle

def run_patients_etl(oracle_conn, pg_engine, logger, batch_size=1000):
    """
    Optimized Patient ETL: Standardized on BIGINT for UIDs.
    Updates gender and age_group to support report_23 analytics.
    """
    logger.info("🚀 Starting Patient ETL (Study-Linked Whitelist)")
    ora_cursor = None
    start_time = time.time()
    
    # Target columns in Postgres 
    # Added 'gender' (mapped from Oracle 'sex') and 'age_group'
    col_names = [
        'patient_db_uid', 'id', 'birth_date', 'gender',
        'number_of_patient_studies', 'number_of_patient_series', 
        'number_of_patient_images', 'mdl_patient_dbid', 'fallback_id', 'age_group'
    ]

    try:
        # 1. Fetch Whitelist from local Postgres (BigInt native)
        with pg_engine.connect() as conn:
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

        # 2. Chunked Oracle Retrieval
        for i in range(0, len(valid_patient_ids), 1000):
            id_chunk = valid_patient_ids[i:i + 1000]
            bind_names = [f":id{j}" for j in range(len(id_chunk))]
            
            # AGE_GROUP CALCULATION:
            # We calculate current age here so report_23 doesn't have to do it on the fly.
            ora_query = f"""
                SELECT patient_db_uid, id, birth_date, sex as gender,
                       number_of_patient_studies, number_of_patient_series, 
                       number_of_patient_images, mdl_patient_dbid, fallback_pid as fallback_id,
                       FLOOR(MONTHS_BETWEEN(SYSDATE, birth_date) / 12) as age_group
                FROM medistore.didb_patients_view
                WHERE patient_db_uid IN ({','.join(bind_names)})
            """
            
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
                # Standardize gender to 'M', 'F', or 'O' if needed
                if d['gender']:
                    d['gender'] = str(d['gender']).strip().upper()[:1]
                mapped_rows.append(d)

            # 4. Postgres Upsert 
            # Note: fallback_id is kept as TEXT, patient_db_uid is BIGINT.
            upsert_stmt = text("""
                INSERT INTO etl_patient_view (
                    patient_db_uid, id, birth_date, sex, 
                    number_of_patient_studies, number_of_patient_series, 
                    number_of_patient_images, mdl_patient_dbid, fallback_id, 
                    age_group, last_update
                ) VALUES (
                    :patient_db_uid, :id, :birth_date, :gender, 
                    :number_of_patient_studies, :number_of_patient_series, 
                    :number_of_patient_images, :mdl_patient_dbid, :fallback_id, 
                    :age_group, :last_update
                )
                ON CONFLICT (patient_db_uid) DO UPDATE SET
                    id = EXCLUDED.id,
                    birth_date = EXCLUDED.birth_date,
                    sex = EXCLUDED.sex,
                    age_group = EXCLUDED.age_group,
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
