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
from etl_didb_studies      import run_studies_etl
from etl_series            import run_series_etl
from etl_didb_raw_images   import run_raw_images_etl
from etl_image_locations   import run_images_etl
from etl_patients_view     import run_patients_etl
from etl_orders            import run_orders_etl
from etl_analytics_refresh import refresh_storage_summary

logger = logging.getLogger("ETL_WORKER")


def execute_sync(app=None):
    if app:
        logger.info("Manual trigger received from App.")
        engine = app.extensions['sqlalchemy'].engine
        _perform_migration(engine)
    else:
        logger.info("Standalone/Cron trigger started.")
        from sqlalchemy import create_engine
        uri = os.getenv('SQLALCHEMY_DATABASE_URI', 'postgresql://etl_user:Rayd_Secure_2026@localhost:5432/etl_db')
        engine = create_engine(uri)
        _perform_migration(engine)


def _perform_migration(engine):
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

        # ── PHASE 1: Studies ──────────────────────────────────────────────
        logger.info("📋 Phase 1: Studies")
        s_count, active_ids = run_studies_etl(
            engine, src, 'etl_didb_studies', database_module.chunked_upsert, go_live
        )
        logger.info(f"✅ Phase 1 done — {s_count:,} studies, {len(active_ids):,} active IDs")

        if not active_ids:
            logger.warning("No active study IDs — skipping phases 2-6.")
        else:
            # ── PHASE 2: Series ───────────────────────────────────────────
            logger.info("📋 Phase 2: Series")
            run_series_etl(engine, src, 'etl_didb_serieses', database_module.chunked_upsert, active_ids)
            logger.info("✅ Phase 2 done")

            # ── PHASE 3: Raw Images ───────────────────────────────────────
            logger.info("📋 Phase 3: Raw Images")
            run_raw_images_etl(engine, src, 'etl_didb_raw_images', database_module.chunked_upsert, active_ids)
            logger.info("✅ Phase 3 done")

            # ── PHASE 4: Image Locations (FK depends on raw images) ───────
            logger.info("📋 Phase 4: Image Locations")
            run_images_etl(engine, src, 'etl_image_locations', database_module.chunked_upsert, active_ids)
            logger.info("✅ Phase 4 done")

        # ── PHASE 5: Patients ─────────────────────────────────────────────
        logger.info("📋 Phase 5: Patients")
        ora_conn = database_module.OracleConnector.get_connection(sysdba=False)
        run_patients_etl(ora_conn, engine, logger)
        ora_conn.close()
        logger.info("✅ Phase 5 done")

        # ── PHASE 6: Orders ───────────────────────────────────────────────
        logger.info("📋 Phase 6: Orders")
        run_orders_etl(engine, src, 'etl_orders', database_module.chunked_upsert, go_live)
        logger.info("✅ Phase 6 done")

        # ── PHASE 7: Storage Summary (all tables must be populated first) ─
        logger.info("📋 Phase 7: Storage Summary Rollup")
        refresh_storage_summary()
        logger.info("✅ Phase 7 done")

        # ── Mark overall sync SUCCESS ─────────────────────────────────────
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE etl_job_log SET status='SUCCESS', end_time=now() "
                "WHERE status='RUNNING' AND job_name='4TB_SYNC'"
            ))
        logger.info("✅ 4TB Sync Complete.")

    except Exception as e:
        logger.error(f"🛑 Migration Error: {e}", exc_info=True)
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE etl_job_log SET status='FAILED', error_message=:msg "
                     "WHERE status='RUNNING'"),
                {"msg": str(e)}
            )
        raise

    finally:
        gc.collect()


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from app import create_app
    app = create_app()
    with app.app_context():
        execute_sync(app)
