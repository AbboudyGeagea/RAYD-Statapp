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
        uri = os.getenv('SQLALCHEMY_DATABASE_URI')
        if not uri:
            logger.error("SQLALCHEMY_DATABASE_URI not set — cannot run standalone ETL.")
            return
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

        # ── PHASE 8: Auto-sync lookup tables ─────────────────────────────
        logger.info("📋 Phase 8: Syncing AE mappings & procedure codes")
        _sync_lookup_tables(engine)
        logger.info("✅ Phase 8 done")

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


def _sync_lookup_tables(engine):
    """Auto-populate aetitle_modality_map and procedure_duration_map from ETL data.
    Uses ON CONFLICT DO NOTHING — never overwrites manually configured values."""
    with engine.begin() as conn:

        # Enable trigram extension for fuzzy matching
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

        # 1. AE → Modality: pick the most frequent modality per AE from series data
        conn.execute(text("""
            INSERT INTO aetitle_modality_map (aetitle, modality, daily_capacity_minutes)
            SELECT storing_ae, modality, 480
            FROM (
                SELECT
                    s.storing_ae,
                    ser.modality,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.storing_ae
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM etl_didb_studies s
                JOIN etl_didb_serieses ser ON ser.study_db_uid = s.study_db_uid
                WHERE s.storing_ae IS NOT NULL
                  AND TRIM(s.storing_ae) != ''
                  AND ser.modality IS NOT NULL
                  AND TRIM(ser.modality) != ''
                GROUP BY s.storing_ae, ser.modality
            ) ranked
            WHERE rn = 1
            ON CONFLICT (aetitle) DO NOTHING
        """))

        # 2. Default weekly schedule for any new AEs (uses daily_capacity_minutes from map)
        conn.execute(text("""
            INSERT INTO device_weekly_schedule (aetitle, day_of_week, std_opening_minutes)
            SELECT m.aetitle, d.day_of_week, COALESCE(m.daily_capacity_minutes, 480)
            FROM aetitle_modality_map m
            CROSS JOIN generate_series(0, 6) AS d(day_of_week)
            ON CONFLICT (aetitle, day_of_week) DO NOTHING
        """))

        # 3. Procedure codes: distinct from orders, default 15 min / 1.0 RVU
        conn.execute(text("""
            INSERT INTO procedure_duration_map (procedure_code, duration_minutes, rvu_value)
            SELECT DISTINCT TRIM(proc_id), 15, 1.0
            FROM etl_orders
            WHERE proc_id IS NOT NULL AND TRIM(proc_id) != ''
            ON CONFLICT (procedure_code) DO NOTHING
        """))

        # 4. Auto-learn procedure → modality from historical data
        #    Only fills NULL modality — never overwrites manual assignments
        #    Strategy A: exact match via etl_orders → etl_didb_studies (study_db_uid join)
        conn.execute(text("""
            UPDATE procedure_duration_map p
            SET modality = sub.modality
            FROM (
                SELECT
                    TRIM(o.proc_id) AS procedure_code,
                    MODE() WITHIN GROUP (ORDER BY s.study_modality) AS modality
                FROM etl_orders o
                JOIN etl_didb_studies s
                    ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
                WHERE o.proc_id IS NOT NULL
                  AND s.study_modality IS NOT NULL
                  AND TRIM(s.study_modality) != ''
                GROUP BY TRIM(o.proc_id)
            ) sub
            WHERE p.procedure_code = sub.procedure_code
              AND p.modality IS NULL
        """))

        #    Strategy B: exact match directly from etl_didb_studies.procedure_code
        conn.execute(text("""
            UPDATE procedure_duration_map p
            SET modality = sub.modality
            FROM (
                SELECT
                    TRIM(s.procedure_code) AS procedure_code,
                    MODE() WITHIN GROUP (ORDER BY s.study_modality) AS modality
                FROM etl_didb_studies s
                WHERE s.procedure_code IS NOT NULL
                  AND TRIM(s.procedure_code) != ''
                  AND s.study_modality IS NOT NULL
                  AND TRIM(s.study_modality) != ''
                GROUP BY TRIM(s.procedure_code)
            ) sub
            WHERE p.procedure_code = sub.procedure_code
              AND p.modality IS NULL
        """))

        #    Strategy C: fuzzy match (>=0.9 similarity) on procedure code
        #    Catches codes that differ by spacing, dashes, minor typos
        conn.execute(text("""
            UPDATE procedure_duration_map p
            SET modality = sub.modality
            FROM (
                SELECT
                    p2.procedure_code,
                    (
                        SELECT MODE() WITHIN GROUP (ORDER BY s.study_modality)
                        FROM etl_didb_studies s
                        WHERE similarity(UPPER(TRIM(p2.procedure_code)), UPPER(TRIM(s.procedure_code))) >= 0.9
                          AND s.study_modality IS NOT NULL
                          AND TRIM(s.study_modality) != ''
                    ) AS modality
                FROM procedure_duration_map p2
                WHERE p2.modality IS NULL
            ) sub
            WHERE p.procedure_code = sub.procedure_code
              AND sub.modality IS NOT NULL
              AND p.modality IS NULL
        """))

        #    Strategy D: fuzzy match (>=0.9 similarity) on procedure description
        #    Matches proc_text from orders against study_description
        conn.execute(text("""
            UPDATE procedure_duration_map p
            SET modality = sub.modality
            FROM (
                SELECT
                    p2.procedure_code,
                    (
                        SELECT MODE() WITHIN GROUP (ORDER BY s.study_modality)
                        FROM etl_orders o
                        JOIN etl_didb_studies s
                            ON s.study_db_uid::TEXT = o.study_db_uid::TEXT
                        WHERE TRIM(o.proc_id) = p2.procedure_code
                          AND o.proc_text IS NOT NULL AND TRIM(o.proc_text) != ''
                          AND s.study_description IS NOT NULL
                          AND similarity(UPPER(o.proc_text), UPPER(s.study_description)) >= 0.9
                          AND s.study_modality IS NOT NULL
                          AND TRIM(s.study_modality) != ''
                    ) AS modality
                FROM procedure_duration_map p2
                WHERE p2.modality IS NULL
            ) sub
            WHERE p.procedure_code = sub.procedure_code
              AND sub.modality IS NOT NULL
              AND p.modality IS NULL
        """))

        #    Fuzzy candidates (70-89%): store for human review, do NOT auto-fill
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS procedure_fuzzy_candidates (
                id              SERIAL PRIMARY KEY,
                procedure_code  VARCHAR UNIQUE,
                suggested_modality VARCHAR(20),
                match_score     NUMERIC(4,3),
                matched_via     VARCHAR(20),
                detected_at     TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("TRUNCATE procedure_fuzzy_candidates"))
        conn.execute(text("""
            INSERT INTO procedure_fuzzy_candidates (procedure_code, suggested_modality, match_score, matched_via)
            SELECT DISTINCT ON (p.procedure_code)
                p.procedure_code,
                s.study_modality,
                MAX(similarity(UPPER(TRIM(p.procedure_code)), UPPER(TRIM(s.procedure_code)))) AS match_score,
                'code'
            FROM procedure_duration_map p
            JOIN etl_didb_studies s
                ON similarity(UPPER(TRIM(p.procedure_code)), UPPER(TRIM(s.procedure_code))) >= 0.7
               AND similarity(UPPER(TRIM(p.procedure_code)), UPPER(TRIM(s.procedure_code))) < 0.9
            WHERE p.modality IS NULL
              AND s.study_modality IS NOT NULL
              AND TRIM(s.study_modality) != ''
            GROUP BY p.procedure_code, s.study_modality
            ORDER BY p.procedure_code, match_score DESC
            ON CONFLICT (procedure_code) DO UPDATE SET
                suggested_modality = EXCLUDED.suggested_modality,
                match_score        = EXCLUDED.match_score,
                detected_at        = NOW()
        """))

        #    Strategy E: match via AE title modality as last resort
        conn.execute(text("""
            UPDATE procedure_duration_map p
            SET modality = sub.modality
            FROM (
                SELECT
                    TRIM(s.procedure_code) AS procedure_code,
                    MODE() WITHIN GROUP (ORDER BY am.modality) AS modality
                FROM etl_didb_studies s
                JOIN aetitle_modality_map am ON am.aetitle = s.storing_ae
                WHERE s.procedure_code IS NOT NULL
                  AND TRIM(s.procedure_code) != ''
                  AND am.modality IS NOT NULL
                GROUP BY TRIM(s.procedure_code)
            ) sub
            WHERE p.procedure_code = sub.procedure_code
              AND p.modality IS NULL
        """))

        # 5. Flag inconsistent procedure→modality mappings
        #    (procedures that appear on 2+ different modalities)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS procedure_modality_conflicts (
                id              SERIAL PRIMARY KEY,
                procedure_code  VARCHAR UNIQUE,
                modalities      TEXT,
                sample_count    INTEGER,
                detected_at     TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("TRUNCATE procedure_modality_conflicts"))
        conn.execute(text("""
            INSERT INTO procedure_modality_conflicts (procedure_code, modalities, sample_count)
            SELECT
                TRIM(s.procedure_code),
                STRING_AGG(DISTINCT UPPER(TRIM(s.study_modality)), ', ' ORDER BY UPPER(TRIM(s.study_modality))),
                COUNT(*)
            FROM etl_didb_studies s
            WHERE s.procedure_code IS NOT NULL
              AND TRIM(s.procedure_code) != ''
              AND s.study_modality IS NOT NULL
              AND TRIM(s.study_modality) != ''
            GROUP BY TRIM(s.procedure_code)
            HAVING COUNT(DISTINCT UPPER(TRIM(s.study_modality))) > 1
        """))


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from app import create_app
    app = create_app()
    with app.app_context():
        execute_sync(app)
