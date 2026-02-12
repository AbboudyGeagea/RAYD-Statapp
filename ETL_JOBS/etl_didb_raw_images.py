import logging
from sqlalchemy import text
from db import OracleConnector

logger = logging.getLogger("ETL_MASTER")

def run_raw_images_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func):
    """
    Worker for Raw Images. 
    Synchronizes the core image metadata based on series in Postgres.
    """
    logger.info("🚀 Starting Raw Images ETL (Series Whitelist)")
    
    col_names = [
        'raw_image_db_uid', 'patient_db_uid', 'study_db_uid', 'series_db_uid',
        'study_instance_uid', 'series_instance_uid', 'image_number', 'last_update'
    ]

    total_processed = 0
    try:
        # 1. Fetch valid Series IDs from Postgres
        with pg_engine.connect() as conn:
            query_whitelist = text("SELECT series_db_uid FROM etl_didb_serieses")
            valid_series_ids = [r[0] for r in conn.execute(query_whitelist).fetchall()]

        if not valid_series_ids:
            logger.warning("⚠️ No Series found in Postgres. Raw Images sync skipped.")
            return 0

        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()

        # 2. Chunked Oracle Retrieval (Max 1000 IDs)
        for i in range(0, len(valid_series_ids), 1000):
            id_chunk = valid_series_ids[i:i + 1000]
            bind_names = [f":id{j}" for j in range(len(id_chunk))]

            query = f"""
                SELECT 
                    raw_image_db_uid, 
                    patient_db_uid, 
                    study_db_uid, 
                    series_db_uid,
                    study_instance_uid, 
                    series_instance_uid, 
                    image_number,
                    CURRENT_TIMESTAMP
                FROM medistore.didb_raw_images
                WHERE series_db_uid IN ({','.join(bind_names)})
            """
            
            bind_params = dict(zip([b.strip(':') for b in bind_names], id_chunk))
            cursor.execute(query, bind_params)
            
            while True:
                batch = cursor.fetchmany(5000)
                if not batch: 
                    break
                
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'raw_image_db_uid')
                total_processed += len(batch)

        cursor.close()
        ora_conn.close()
        logger.info(f"✅ Raw Images ETL complete: {total_processed} records.")
        return total_processed
        
    except Exception as e:
        logger.error(f"💥 Raw Images ETL failed: {str(e)}")
        raise e
