import os, sys, logging
from datetime import datetime

# Path Injection
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path: sys.path.append(parent_dir)

from app import create_app
from db import db, get_pg_engine, chunked_upsert, get_etl_cutoff_date, OracleConnector

try:
    from etl_didb_studies import run_studies_etl
    from etl_series import run_series_etl
    from etl_patients_view import run_patients_etl
    from etl_orders import run_orders_etl
    from etl_raw_images import run_raw_images_etl  
    from etl_image_locations import run_images_etl
    from etl_analytics_refresh import refresh_storage_summary 
except ImportError as e:
    print(f"❌ Import Error: {e}"); sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL_RUNNER")

def execute_sync():
    app = create_app()
    with app.app_context():
        engine = get_pg_engine()
        go_live = get_etl_cutoff_date()
        if not go_live: return

        logger.info(f"--- STARTING SYNC (Cutoff: {go_live}) ---")
        src = "PROD_ORACLE"

        try:
            # 1. Studies (Returns tuple: count, list of IDs)
            s_count, active_study_ids = run_studies_etl(engine, src, 'etl_didb_studies', chunked_upsert, go_live)
            logger.info(f"Studies Synced: {s_count}")

            # 2. Orders (Independent)
            run_orders_etl(engine, src, 'etl_orders', chunked_upsert, go_live)

            # 3. Series (Uses Study ID list)
            run_series_etl(engine, src, 'etl_didb_serieses', chunked_upsert, active_study_ids)

            # 4. Raw Images (Uses Study ID list)
            run_raw_images_etl(engine, src, 'etl_didb_raw_images', chunked_upsert, active_study_ids)

            # 5. Patients
            ora_conn = OracleConnector.get_connection(src)
            run_patients_etl(ora_conn, engine, logger)
            ora_conn.close()

            # 6. Image Locations (Uses Study ID list)
            run_images_etl(engine, src, 'etl_image_locations', chunked_upsert, active_study_ids)

            # 7. Analytics
            refresh_storage_summary() 
            logger.info("--- ALL SYNC TASKS COMPLETE ---")

        except Exception as e:
            logger.error(f"🛑 ETL MASTER FAILURE: {str(e)}")
            db.session.rollback()

if __name__ == "__main__":
    execute_sync()
