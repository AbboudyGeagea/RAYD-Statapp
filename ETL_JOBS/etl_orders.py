import logging
from datetime import datetime
from db import OracleConnector

def run_orders_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    # MATCHING YOUR PG STRUCTURE EXACTLY (13 Columns total)
    # Order here must match the SELECT order below
    col_names = [
        'order_dbid', 'patient_dbid', 'study_db_uid', 'visit_dbid',
        'study_instance_uid', 'proc_id', 'proc_text', 'scheduled_datetime',
        'order_status', 'modality', 'has_study', 'order_control', 
        'last_update'
    ]

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()

    # Query aligned to your new 13-column PG structure
    query = """
        SELECT 
            ORDER_DBID,            -- bigint
            PATIENT_DBID,          -- text
            STUDY_DB_UID,          -- text
            VISIT_DBID,            -- text
            STUDY_INSTANCE_UID,    -- text
            PROC_ID,               -- text
            PROC_TEXT,             -- text
            SCHEDULED_DATETIME,    -- timestamp
            ORDER_STATUS,          -- text
            MODALITY,              -- text
            CASE 
                WHEN HAS_STUDY = 'Y' THEN 'true' 
                ELSE 'false' 
            END as has_study,      -- boolean
            ORDER_CONTROL,         -- text
            CURRENT_TIMESTAMP      -- last_update (timestamp)
        FROM MEDILINK.MDB_ORDERS
        WHERE SCHEDULED_DATETIME >= TO_DATE(:gd, 'YYYY-MM-DD')
    """
    
    try:
        logging.info(f"🚀 Starting Orders ETL for Date >= {go_live_date}")
        
        # Date handling
        gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)
        
        cursor.execute(query, {'gd': gd_str})
        
        total = 0
        while True:
            batch = cursor.fetchmany(1000)
            if not batch: 
                break
            
            # Upsert into Postgres
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
