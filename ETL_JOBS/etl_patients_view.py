from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import insert
import cx_Oracle
import time


        
def run_patients_etl(oracle_conn, pg_engine, go_live_date_filter, logger, batch_size=1000):
    logger.info("Starting ETL for DIDB_PATIENTS_VIEW...")
    ora_cursor = None

    query = """
        SELECT patient_db_uid, id, birth_date, sex,
               number_of_patient_studies, number_of_patient_series, number_of_patient_images,
               mdl_patient_dbid, fallback_pid
        FROM medistore.didb_patients_view
    """

    col_names = [
        'patient_db_uid', 'id', 'birth_date', 'sex',
        'number_of_patient_studies', 'number_of_patient_series', 'number_of_patient_images',
        'mdl_patient_dbid', 'fallback_id', 'last_update'
    ]

    try:
        start_time = time.time()
        ora_cursor = oracle_conn.cursor()
        ora_cursor.execute(query, {'go_live_date': go_live_date_filter})
        rows = ora_cursor.fetchall()

        if not rows:
            logger.info("No new or updated patient records found in Oracle.")
            return

        logger.info(f"Fetched {len(rows)} rows from Oracle.")

        metadata = MetaData()
        metadata.reflect(bind=pg_engine)
        table = metadata.tables['etl_patient_view']

        stmt = insert(table).on_conflict_do_update(
            index_elements=['patient_db_uid'],
            set_={col: getattr(stmt.excluded, col) for col in col_names}
        )

        with pg_engine.begin() as connection:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                mapped_rows = [dict(zip(col_names[:-1], row)) for row in batch]
                for r in mapped_rows:
                    r['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                connection.execute(stmt, mapped_rows)
                logger.info(f"Inserted batch {i // batch_size + 1} ({len(mapped_rows)} rows)")

        duration = time.time() - start_time
        logger.info(f"ETL completed for patients: {len(rows)} records processed in {duration:.2f} seconds.")

    except cx_Oracle.Error as e:
        logger.error(f"Oracle ETL Error in patients job: {e}")
    except Exception as e:
        logger.error(f"PostgreSQL ETL Error in patients job: {e}")
    finally:
        if ora_cursor:
            ora_cursor.close()
