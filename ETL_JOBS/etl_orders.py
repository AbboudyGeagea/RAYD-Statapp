import logging
from datetime import datetime
from db import OracleConnector

def run_orders_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    # Added 'last_update' to the column list
    col_names = [
        'order_dbid', 'patient_dbid', 'proc_id', 'proc_text', 
        'study_instance_uid', 'study_db_uid', 'has_study', 
        'scheduled_datetime', 'order_control', 'order_status', 
        'placer_field1', 'placer_field2', 'modality', 'body_part',
        'last_update'
    ]

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    # Improvements:
    # 1. Added CURRENT_TIMESTAMP to the SELECT for the Postgres last_update field.
    # 2. Kept your CASE logic for the boolean 'has_study'.
    query = """
        SELECT 
            CAST(ORDER_DBID AS VARCHAR2(100)), CAST(PATIENT_DBID AS VARCHAR2(100)), 
            PROC_ID, PROC_TEXT, STUDY_INSTANCE_UID, CAST(STUDY_DB_UID AS VARCHAR2(100)),
            CASE WHEN HAS_STUDY = 'Y' THEN 1 ELSE 0 END,
            SCHEDULED_DATETIME, ORDER_CONTROL, ORDER_STATUS, 
            PLACER_FIELD1, PLACER_FIELD2, MODALITY, BODY_PART,
            CURRENT_TIMESTAMP
        FROM MEDILINK.MDB_ORDERS
        WHERE SCHEDULED_DATETIME >= TO_DATE(:gd, 'YYYY-MM-DD')
    """
    
    try:
        logging.info(f"🚀 Starting Orders ETL for Date >= {go_live_date}")
        cursor.execute(query, {'gd': str(go_live_date)})
        
        total = 0
        while True:
            batch = cursor.fetchmany(1000)
            if not batch: 
                break
            
            # Using chunked_upsert_func to handle 'order_dbid' conflicts
            chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'order_dbid')
            total += len(batch)
            
            if total % 5000 == 0:
                logging.info(f"Synced {total} orders...")
                
    except Exception as e:
        logging.error(f"💥 Orders ETL Error: {str(e)}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        
    return total
