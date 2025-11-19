import time
import cx_Oracle
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import insert


def run_studies_etl(oracle_conn, pg_engine, go_live_date_filter, logger, batch_size=1500):
    logger.info("▶ Starting ETL for DIDB_STUDIES...")
    ora_cursor = None

    query = """
        SELECT study_db_uid, patient_db_uid, study_instance_uid, last_access_time,
               number_of_study_series, number_of_study_images, accession_number,
               study_body_part, study_date, insert_time, mdl_order_dbid,
               referring_physician_first_name, referring_physician_last_name, referring_physician_mid_name,
               study_status, study_description, storing_ae, patient_class, procedure_code,
               report_status, order_status, signing_physician_first_name, signing_physician_last_name,
               signing_physician_id, study_age, rep_transcribed_by, rep_transcribed_timestamp,
               rep_prelim_timestamp, rep_final_signed_by, rep_final_timestamp,
               rep_addendum_by, rep_addendum_timestamp, rep_has_addendum, study_locked
        FROM medistore.didb_studies
        WHERE study_date >= TO_DATE(:go_live_date, 'DD-MON-YY')
    """

    col_names = [
        'study_db_uid', 'patient_db_uid', 'study_instance_uid', 'last_accessed_time',
        'number_of_study_series', 'number_of_study_images', 'accession_number',
        'study_body_part', 'study_date', 'insert_time', 'mdl_order_dbid',
        'referring_physician_first_name', 'referring_physician_last_name', 'referring_physician_mid_name',
        'study_status', 'study_description', 'storing_ae', 'patient_class', 'procedure_code',
        'report_status', 'order_status', 'signing_physician_first_name', 'signing_physician_last_name',
        'signing_physician_id', 'study_age', 'rep_transcribed_by', 'rep_transcribed_timestamp',
        'rep_prelim_timestamp', 'rep_final_signed_by', 'rep_final_timestamp',
        'rep_addendum_by', 'rep_addendum_timestamp', 'rep_has_addendum', 'study_locked', 'last_update'
    ]

    try:
        start_time = time.time()
        ora_cursor = oracle_conn.cursor()
        ora_cursor.execute(query, {'go_live_date': go_live_date_filter})
        rows = ora_cursor.fetchall()

        if not rows:
            logger.info("✔ No new or updated study records found in Oracle.")
            return

        logger.info(f"Fetched {len(rows)} rows from Oracle.")

        metadata = MetaData()
        metadata.reflect(bind=pg_engine)
        table = metadata.tables['etl_didb_studies']

        stmt = insert(table).on_conflict_do_update(
            index_elements=['study_db_uid'],
            set_={col: getattr(stmt.excluded, col) for col in col_names}
        )

        with pg_engine.begin() as connection:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                mapped_rows = [dict(zip(col_names[:-1], row)) for row in batch]
                for r in mapped_rows:
                    r['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"▶ Inserting batch {i//batch_size+1} ({len(mapped_rows)} rows)")
                connection.execute(stmt, mapped_rows)
                logger.info(f"✔ Batch {i//batch_size+1} inserted successfully")

        duration = time.time() - start_time
        logger.info(f"✅ ETL completed for studies: {len(rows)} records processed in {duration:.2f}s")

    except cx_Oracle.Error as e:
        logger.error(f"❌ Oracle ETL Error in studies job: {e}")
    except Exception as e:
        logger.error(f"❌ PostgreSQL ETL Error in studies job: {e}")
    finally:
        if ora_cursor:
            ora_cursor.close()
