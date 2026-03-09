import os, sys, logging, gc
from datetime import datetime
from sqlalchemy import text

# Ensure paths are correct
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
if parent_dir not in sys.path: sys.path.insert(0, parent_dir)

import db as database_module
try:
    from ETL_JOBS.etl_settings import ETL_GEAR
except ImportError:
    from etl_settings import ETL_GEAR

# Worker imports
from etl_didb_studies       import run_studies_etl
from etl_orders             import run_orders_etl
from etl_series             import run_series_etl
from etl_didb_raw_images    import run_raw_images_etl
from etl_patients_view      import run_patients_etl
from etl_image_locations    import run_images_etl
from etl_analytics_refresh    import refresh_storage_summary   # ← Phase 4

logger = logging.getLogger("ETL_WORKER")


def execute_sync(app=None):
    """
    Entry point — handles both manual (App) and automatic (Standalone) triggers.
    """
    if app:
        logger.info("Manual trigger received from App.")
        engine = app.extensions['sqlalchemy'].db.engine
        _perform_migration(engine)
    else:
        logger.info("Standalone/Cron trigger started.")
        from sqlalchemy import create_engine
        uri = os.getenv('SQLALCHEMY_DATABASE_URI', 'postgresql://etl_user:SecureCrynBabe@localhost:5432/etl_db')
        engine = create_engine(uri)
        _perform_migration(engine)


def _perform_migration(engine):
    """The actual 4TB heavy lifting logic."""
    start_time = datetime.now()

    try:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_job_log (job_name, status, start_time) "
                "VALUES ('4TB_SYNC', 'RUNNING', now())"
            ))
            res     = conn.execute(text(
                "SELECT go_live_date FROM go_live_config ORDER BY id DESC LIMIT 1"
            )).fetchone()
            go_live = res[0] if res else '2000-01-01'

        logger.info(f"🚀 Starting 4TB Sync | Cutoff: {go_live}")
        src = "PROD_ORACLE"

        # ── PHASE 1: Studies (required for IDs) ──────────────────────────
        s_count, active_ids = run_studies_etl(
            engine, src, 'etl_didb_studies', database_module.chunked_upsert, go_live
        )

        # ── PHASE 2: Series + Raw Images (parallel), then Image Locations ─
        if active_ids:
            from concurrent.futures import ThreadPoolExecutor
            workers = ETL_GEAR.get("num_workers", 4)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                executor.submit(run_series_etl,     engine, src, 'etl_didb_serieses',   database_module.chunked_upsert, active_ids)
                executor.submit(run_raw_images_etl, engine, src, 'etl_didb_raw_images', database_module.chunked_upsert, active_ids)

            run_images_etl(engine, src, 'etl_image_locations', database_module.chunked_upsert, active_ids)

        # ── PHASE 3: Patients ─────────────────────────────────────────────
        ora_conn = database_module.OracleConnector.get_connection(sysdba=False)
        run_patients_etl(ora_conn, engine, logger)
        ora_conn.close()

        # ── PHASE 4: Storage Summary Rollup ───────────────────────────────
        # Must run after Phase 2 so etl_didb_raw_images + etl_image_locations
        # are fully populated before we aggregate.
        logger.info("📦 Starting Phase 4: Storage Summary Rollup ...")
        refresh_storage_summary()
        logger.info("✅ Phase 4 complete.")

        # ── Mark overall sync as SUCCESS ──────────────────────────────────
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE etl_job_log SET status='SUCCESS', end_time=now() "
                "WHERE status='RUNNING' AND job_name='4TB_SYNC'"
            ))

        logger.info("✅ Sync Complete.")

    except Exception as e:
        logger.error(f"🛑 Migration Error: {e}")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE etl_job_log SET status='FAILED', error_message=:msg "
                     "WHERE status='RUNNING'"),
                {"msg": str(e)}
            )
        raise

    finally:
        gc.collect()
