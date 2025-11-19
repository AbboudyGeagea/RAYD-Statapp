from datetime import datetime
from flask import current_app
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text, create_engine

from db import db, get_go_live_date
from ETL_JOBS.etl_didb_studies import run_studies_etl
from ETL_JOBS.etl_patients_view import run_patients_etl
from ETL_JOBS.etl_orders import run_orders_etl

# =====================================================
# 🔹 Utility: Fetch Oracle connection string dynamically
# =====================================================
def get_oracle_connection_string(conn_name="oracle_PACS"):
    """
    Reads the Oracle connection string from the source_db_params table.
    Example: oracle+oracledb://user:xxxx@8.8.8.8:1521/?service_name=ORC
    """
    with db.engine.begin() as conn:
        result = conn.execute(
            text("SELECT conn_string FROM db_params WHERE name = :name"),
            {"name": conn_name}
        ).fetchone()

    if not result:
        raise ValueError(f"No Oracle connection found for name '{conn_name}' in source_db_params")

    return result[0]


# =====================================================
# 🔹 Run ETL for one system
# =====================================================
def run_etl_for_system(system_name: str, oracle_engine, pg_engine, logger):
    """
    Executes the ETL logic for a given system after checking go-live date.
    """
    go_live_date = get_go_live_date(system_name)
    if not go_live_date:
        logger.warning(f"No go-live date found for system: {system_name}")
        return

    go_live_date_str = go_live_date.strftime('%d-%b-%y')  # Format: DD-MON-YY
    logger.info(f"Running ETL for {system_name} starting from {go_live_date_str}")

    try:
        if system_name == 'etl_didb_studies':
            run_studies_etl(oracle_engine, pg_engine, go_live_date_str, logger)
        elif system_name == 'etl_patient_view':
            run_patients_etl(oracle_engine, pg_engine, go_live_date_str, logger)
        elif system_name == 'etl_orders':
            run_orders_etl(oracle_engine, pg_engine, go_live_date_str, logger)
        else:
            logger.warning(f"Unknown system name: {system_name}")
    except Exception as e:
        logger.error(f"ETL failed for {system_name}: {e}")
        raise


# =====================================================
# 🔹 Daily ETL Job
# =====================================================
def daily_etl_job():
    """
    Main scheduled ETL job that runs daily at 5:00 AM.
    Dynamically connects to Oracle before running ETL.
    """
    app = current_app._get_current_object()
    with app.app_context():
        logger = app.logger
        pg_engine = db.engine

        try:
            oracle_conn_str = get_oracle_connection_string("oracle_PACS")
            oracle_engine = create_engine(oracle_conn_str)
        except Exception as e:
            logger.error(f"❌ Failed to get Oracle connection string: {e}")
            return

        # Updated system names to match new table naming
        systems = ['etl_didb_studies', 'etl_patient_view', 'etl_orders']
        for system in systems:
            run_etl_for_system(system, oracle_engine, pg_engine, logger)


# =====================================================
# 🔹 Scheduler Setup
# =====================================================
scheduler = BackgroundScheduler()

def start_etl_scheduler(app):
    """
    Starts the ETL scheduler. Should be called once during app startup.
    """
    with app.app_context():
        if not scheduler.running:
            scheduler.add_job(daily_etl_job, 'cron', hour=5, minute=0)
            scheduler.start()
            app.logger.info("🕔 ETL Scheduler started and daily job scheduled at 5:00 AM.")
        else:
            app.logger.info("ETL Scheduler already running.")

