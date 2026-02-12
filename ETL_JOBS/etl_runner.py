import os
import sys
import logging
from datetime import datetime

# 1. PATH INJECTION: Ensure we can see db.py in the parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
if current_dir not in sys.path:
    sys.path.append(current_dir)

from app import create_app
from db import db, get_pg_engine, chunked_upsert, get_etl_cutoff_date, OracleConnector

# Filename-safe imports
try:
    from etl_didb_studies import run_studies_etl
    from etl_series import run_series_etl
    from etl_patients_view import run_patients_etl
    from etl_orders import run_orders_etl
    from etl_raw_images import run_raw_images_etl  
    from etl_image_locations import run_images_etl
    # NEW: Import the fixed analytics script
    from etl_analytics_refresh import refresh_storage_summary 
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL_RUNNER")

def execute_sync():
    app = create_app()
    with app.app_context():
        engine = get_pg_engine()
        go_live = get_etl_cutoff_date()
        
        if not go_live:
            logger.error("❌ No Go-Live date found in database.")
            return

        logger.info(f"--- STARTING SYNC (Cutoff: {go_live}) ---")
        src = "PROD_ORACLE"

        try:
            # 1. Studies (The Root)
            s_count = run_studies_etl(engine, src, 'etl_didb_studies', chunked_upsert, go_live)
            logger.info(f"Studies Synced: {s_count}")

            # 2. Orders
            o_count = run_orders_etl(engine, src, 'etl_orders', chunked_upsert, go_live)
            logger.info(f"Orders Synced: {o_count}")

            # 3. Series
            ser_count = run_series_etl(engine, src, 'etl_didb_serieses', chunked_upsert)
            logger.info(f"Series Synced: {ser_count}")

            # 4. Raw Images
            ri_count = run_raw_images_etl(engine, src, 'etl_didb_raw_images', chunked_upsert)
            logger.info(f"Raw Images Synced: {ri_count}")

            # 5. Patients
            ora_conn = OracleConnector.get_connection(src)
            p_count = run_patients_etl(ora_conn, engine, logger)
            ora_conn.close()
            logger.info(f"Patients Synced: {p_count}")

            # 6. Image Locations
            i_count = run_images_etl(engine, src, 'etl_image_locations', chunked_upsert)
            logger.info(f"Image Locations Synced: {i_count}")

            # --- NEW STEP: 7. STORAGE ANALYTICS ROLLUP ---
            # This recalculates everything since Go-Live to ensure the graph only grows
            logger.info("🚀 Starting Storage Analytics Rollup...")
            refresh_storage_summary() 
            logger.info("✅ Analytics Rollup Complete.")

            logger.info("--- ALL SYNC TASKS COMPLETE ---")

        except Exception as e:
            logger.error(f"🛑 ETL MASTER FAILURE: {str(e)}")
            db.session.rollback()

if __name__ == "__main__":
    execute_sync()
