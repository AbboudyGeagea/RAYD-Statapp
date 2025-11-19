from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import insert
import cx_Oracle
import time
import traceback


        
def run_orders_etl(oracle_conn, pg_engine, go_live_date_filter, logger, batch_size=1000):
    logger.info("Starting ETL for MDB_ORDERS...")
    ora_cursor = None

    query = """
        SELECT order_dbid, patient_dbid, proc_id, proc_text,
               study_instance_uid, study_db_uid, has_study,
               scheduled_datetime, order_control, order_status,
               placer_field1, placer_field2
        FROM medlink.mdb_orders
        WHERE scheduled_datetime >= TO_DATE(:go_live_date, 'DD-MON-YY')
    """

    col_names = [
        'order_dbid', 'patient_dbid', 'proc_id', 'proc_text',
        'study_instance_uid', 'study_db_uid', 'has_study',
        'scheduled_datetime', 'order_control', 'order_status',
        'placer_field1', 'placer_field2', 'last_update'
    ]

    try:
        start_time = time.time()
        ora_cursor = oracle_conn.cursor()

        try:
            ora_cursor.execute(query, {'go_live_date': go_live_date_filter})
            rows = ora_cursor.fetchall()
        except cx_Oracle.Error as e:
            logger.warning("⚠ Query failed or scheduled_datetime might be NULL for all rows.")
            logger.warning(f"Oracle error: {e}")
            return

        if not rows:
            logger.warning("⚠ No new orders found or scheduled_datetime is NULL for all rows.")
            return

        logger.info(f"Fetched {len(rows)} rows from Oracle.")

        # Reflect PostgreSQL table
        metadata = MetaData()
        metadata.reflect(bind=pg_engine)
        table = metadata.tables['etl_orders']

        stmt = insert(table).on_conflict_do_update(
            index_elements=['order_dbid'],
            set_={col: getattr(stmt.excluded, col) for col in col_names}
        )

        # Chunked insert
        with pg_engine.begin() as connection:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                mapped_rows = [dict(zip(col_names[:-1], row)) for row in batch]
                for r in mapped_rows:
                    r['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                connection.execute(stmt, mapped_rows)
                logger.info(f"Inserted batch {i // batch_size + 1} ({len(mapped_rows)} rows)")

        duration = time.time() - start_time
        logger.info(f"ETL completed for orders: {len(rows)} records processed in {duration:.2f} seconds.")

    except cx_Oracle.Error as e:
        logger.error(f"Oracle ETL Error in orders job: {e}")
    except Exception as e:
        logger.error("PostgreSQL/General ETL Error in orders job:")
        logger.error(traceback.format_exc())
    finally:
        if ora_cursor:
            ora_cursor.close()
