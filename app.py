import sys
import os
import logging
import oracledb
from datetime import date, datetime
from hl7_listener import start_mllp_listener
# 1. ORACLE ALIAS (Must be before other imports)
sys.modules["cx_Oracle"] = oracledb

from flask import (
    Flask, request, redirect, url_for,
    session as flask_session, jsonify, render_template, abort
)
from flask_login import (
    LoginManager, logout_user,
    current_user, login_required
)

from dotenv import load_dotenv
from sqlalchemy.sql import text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import pytz

# ---------------------------------------------------------
# LOAD ENV & PROJECT MODULES
# ---------------------------------------------------------
load_dotenv()

from config import config as app_config

from db import (
    db,
    init_db,
    User,
    ReportTemplate,
    ReportDimension,
    ReportAccessControl,
    GoLiveDate,
)
from routes.registry import register_blueprints

# Ensure ETL_JOBS workers are importable from anywhere
_etl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ETL_JOBS')
if _etl_path not in sys.path:
    sys.path.insert(0, _etl_path)

logger = logging.getLogger("APP")

# ---------------------------------------------------------
# EMPTY DB DETECTION
# ---------------------------------------------------------
CRITICAL_TABLES = [
    'etl_didb_studies',
    'etl_patient_view',
    'etl_orders',
    'etl_didb_raw_images',
    'etl_image_locations',
]

def is_db_empty():
    """
    Returns True if ANY of the critical ETL tables has zero rows.
    This covers a fresh environment or a partially failed previous sync.
    """
    try:
        for table in CRITICAL_TABLES:
            result = db.session.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            ).scalar()
            if result == 0:
                logger.warning(f"[Startup Check] Table '{table}' is empty — triggering initial ETL.")
                return True
        return False
    except Exception as e:
        logger.error(f"[Startup Check] Could not check tables: {e}")
        return False


def trigger_initial_etl(app):
    """
    Runs the full ETL in a background thread with app context.
    Called once on startup if any critical table is empty.
    """
    import threading

    def _run():
        with app.app_context():
            try:
                from ETL_JOBS.etl_runner import execute_sync
                logger.info("🚀 [Startup ETL] Empty DB detected — starting initial sync ...")
                execute_sync(app)
                logger.info("✅ [Startup ETL] Initial sync complete.")
            except Exception as e:
                logger.error(f"❌ [Startup ETL] Failed: {e}", exc_info=True)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    logger.info("🔄 [Startup ETL] ETL thread launched in background.")


# ---------------------------------------------------------
# CREATE APP
# ---------------------------------------------------------
def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY")
    if not app.secret_key:
        raise RuntimeError("SECRET_KEY environment variable is required")

    # --- LOAD FEATURE FLAGS FROM config.py ---
    app.config.from_object(app_config)

    # --- EXPOSE CONFIG TO ALL JINJA TEMPLATES ---
    @app.context_processor
    def inject_config():
        import json
        from sqlalchemy import text as _text
        theme     = getattr(current_user, 'ui_theme', 'dark') if current_user.is_authenticated else 'dark'
        favorites = json.loads(getattr(current_user, 'favorites', '[]') or '[]') if current_user.is_authenticated else []

        # Demo mode settings
        demo_mode = False
        demo_start = ''
        demo_end   = ''
        demo_user  = ''
        try:
            from db import db as _db
            rows = _db.session.execute(
                _text("SELECT key, value FROM settings WHERE key IN ('demo_mode','demo_start','demo_end','demo_user')")
            ).fetchall()
            d = {r[0]: r[1] for r in rows}
            demo_mode  = d.get('demo_mode', 'false').lower() == 'true'
            demo_start = d.get('demo_start', '')
            demo_end   = d.get('demo_end', '')
            demo_user  = d.get('demo_user', '')
        except Exception:
            pass

        # In demo mode override all feature flags to True
        cfg = app.config
        if demo_mode:
            cfg = dict(app.config)
            cfg['LIVE_FEED_ENABLED']       = True
            cfg['BITNET_ENABLED']          = True
            cfg['PATIENT_PORTAL_ENABLED']  = True

        oracle_configured = False
        try:
            from db import db as _db
            row = _db.session.execute(
                _text("SELECT 1 FROM db_params WHERE name ILIKE '%oracle%' LIMIT 1")
            ).fetchone()
            oracle_configured = row is not None
        except Exception:
            pass

        return {
            "config":             cfg,
            "ui_theme":           theme,
            "user_favorites":     favorites,
            "demo_mode":          demo_mode,
            "demo_start":         demo_start,
            "demo_end":           demo_end,
            "demo_user":          demo_user,
            "oracle_configured":  oracle_configured,
        }

    # --- JINJA FILTER: user_has_page ---
    from db import user_has_page as _user_has_page
    @app.template_filter('user_has_page')
    def jinja_user_has_page(user, page_key):
        # During demo mode, the designated demo user gets full access
        try:
            from sqlalchemy.sql import text as _t
            from db import db as _db
            rows = _db.session.execute(
                _t("SELECT key, value FROM settings WHERE key IN ('demo_mode','demo_user')")
            ).fetchall()
            d = {r[0]: r[1] for r in rows}
            if d.get('demo_mode', 'false').lower() == 'true':
                demo_u = d.get('demo_user', '')
                if demo_u and getattr(user, 'username', '') == demo_u:
                    return True
        except Exception:
            pass
        return _user_has_page(user, page_key)

    # --- DB CONFIG ---
    user     = os.environ.get('POSTGRES_USER',     'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', '')
    host     = os.environ.get('POSTGRES_HOST',     'localhost')
    port     = os.environ.get('POSTGRES_PORT',     '5432')
    dbname   = os.environ.get('POSTGRES_DB',       'etl_db')

    app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', 'true').lower() == 'true'

    init_db(app)

    # --- DB MIGRATIONS ---
    from db_migrations import run_migrations
    run_migrations(app)

    # --- LOGIN MANAGER ---
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # --- ROUTES ---
    register_blueprints(app)

    # --- AUTH CHECKS ---
    @app.before_request
    def check_auth():
        if request.path.startswith('/static/') or (
            request.endpoint and (
                request.endpoint.startswith('auth.') or
                request.endpoint.startswith('portal.') or
                request.endpoint == 'static'
            )
        ):
            return
        if not current_user.is_authenticated:
            if request.endpoint != 'auth.login':
                return redirect(url_for('auth.login'))

        # License expiry — admins can still log in to update the license
        if current_user.is_authenticated and current_user.role != 'admin':
            from routes.registry import check_license_limit
            ok, msg = check_license_limit(app, 'expired')
            if not ok:
                from flask import flash as _flash
                _flash(msg, 'danger')
                logout_user()
                return redirect(url_for('auth.login'))

    # --- ROOT REDIRECT ---
    @app.route('/')
    def index():
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if current_user.role == 'admin':
            return redirect(url_for('admin.admin_dashboard'))
        return redirect(url_for('viewer.viewer_dashboard'))

    # --- MIGRATE: add new columns / tables if missing ---
    with app.app_context():
        try:
            db.session.execute(text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS ui_theme VARCHAR DEFAULT 'dark'"
            ))
            db.session.execute(text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS favorites TEXT DEFAULT '[]'"
            ))
            db.session.execute(text(
                "ALTER TABLE aetitle_modality_map ADD COLUMN IF NOT EXISTS room_name VARCHAR(100)"
            ))
            db.session.execute(text(
                "ALTER TABLE procedure_duration_map ADD COLUMN IF NOT EXISTS modality VARCHAR(20)"
            ))
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS procedure_modality_conflicts (
                    id              SERIAL PRIMARY KEY,
                    procedure_code  VARCHAR UNIQUE,
                    modalities      TEXT,
                    sample_count    INTEGER,
                    detected_at     TIMESTAMP DEFAULT NOW()
                )
            """))
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS procedure_fuzzy_candidates (
                    id              SERIAL PRIMARY KEY,
                    procedure_code  VARCHAR UNIQUE,
                    suggested_modality VARCHAR(20),
                    match_score     NUMERIC(4,3),
                    matched_via     VARCHAR(20),
                    detected_at     TIMESTAMP DEFAULT NOW()
                )
            """))
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS procedure_canonical_groups (
                    id                 SERIAL PRIMARY KEY,
                    canonical_name     VARCHAR(300),
                    approved           BOOLEAN DEFAULT FALSE,
                    approved_by        VARCHAR(100),
                    approved_at        TIMESTAMP,
                    detected_at        TIMESTAMP DEFAULT NOW(),
                    source             VARCHAR(20) DEFAULT 'human',
                    cluster_confidence NUMERIC(4,3)
                )
            """))
            db.session.execute(text(
                "ALTER TABLE procedure_canonical_groups "
                "ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'human'"
            ))
            db.session.execute(text(
                "ALTER TABLE procedure_canonical_groups "
                "ADD COLUMN IF NOT EXISTS cluster_confidence NUMERIC(4,3)"
            ))
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS procedure_canonical_members (
                    procedure_code  VARCHAR PRIMARY KEY,
                    group_id        INTEGER REFERENCES procedure_canonical_groups(id) ON DELETE CASCADE,
                    similarity_score NUMERIC(4,3),
                    member_approved BOOLEAN DEFAULT NULL,
                    added_at        TIMESTAMP DEFAULT NOW()
                )
            """))
            db.session.execute(text(
                "ALTER TABLE procedure_canonical_members "
                "ADD COLUMN IF NOT EXISTS member_approved BOOLEAN DEFAULT NULL"
            ))
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS procedure_duplicate_candidates (
                    id              SERIAL PRIMARY KEY,
                    code_a          VARCHAR,
                    code_b          VARCHAR,
                    code_similarity NUMERIC(4,3),
                    desc_similarity NUMERIC(4,3),
                    desc_a          TEXT,
                    desc_b          TEXT,
                    status          VARCHAR(10) DEFAULT 'pending',
                    group_id        INTEGER REFERENCES procedure_canonical_groups(id) ON DELETE SET NULL,
                    reviewed_at     TIMESTAMP,
                    detected_at     TIMESTAMP DEFAULT NOW(),
                    UNIQUE(code_a, code_b)
                )
            """))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] columns: {e}")

    with app.app_context():
        try:
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS hl7_oru_reports (
                    id               SERIAL PRIMARY KEY,
                    procedure_code   VARCHAR(100),
                    procedure_name   TEXT,
                    modality         VARCHAR(20),
                    physician_id     VARCHAR(100),
                    patient_id       VARCHAR(100),
                    accession_number VARCHAR(100),
                    report_text      TEXT,
                    impression_text  TEXT,
                    result_datetime  TIMESTAMP,
                    received_at      TIMESTAMP DEFAULT NOW()
                )
            """))
            db.session.execute(text(
                "ALTER TABLE hl7_oru_reports ADD COLUMN IF NOT EXISTS patient_id VARCHAR(100)"
            ))
            db.session.execute(text(
                "ALTER TABLE hl7_oru_reports ADD COLUMN IF NOT EXISTS accession_number VARCHAR(100)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_oru_received ON hl7_oru_reports (received_at DESC)"
            ))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] hl7_oru_reports: {e}")

    with app.app_context():
        try:
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS ai_nlp_cache (
                    id              SERIAL PRIMARY KEY,
                    source_id       INTEGER NOT NULL REFERENCES hl7_oru_reports(id) ON DELETE CASCADE,
                    classification  VARCHAR(20),
                    keywords        JSONB,
                    cluster_id      INTEGER,
                    cluster_label   TEXT,
                    severity_score  NUMERIC(3,1),
                    processed_at    TIMESTAMP DEFAULT NOW(),
                    UNIQUE (source_id)
                )
            """))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_nlp_classification ON ai_nlp_cache (classification)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_nlp_cluster ON ai_nlp_cache (cluster_id)"
            ))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] ai_nlp_cache: {e}")

    # --- MIGRATION: hl7_scn_studies (real-time completed studies) ---
    with app.app_context():
        try:
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS hl7_scn_studies (
                    id               SERIAL PRIMARY KEY,
                    accession_number VARCHAR(100),
                    patient_id       VARCHAR(100),
                    patient_name     TEXT,
                    procedure_code   VARCHAR(100),
                    procedure_text   TEXT,
                    modality         VARCHAR(20),
                    storing_ae       VARCHAR(100),
                    patient_class    VARCHAR(50),
                    study_datetime   TIMESTAMP,
                    order_status     VARCHAR(20) DEFAULT 'CM',
                    received_at      TIMESTAMP DEFAULT NOW(),
                    UNIQUE (accession_number)
                )
            """))
            db.session.execute(text(
                "ALTER TABLE hl7_scn_studies ADD COLUMN IF NOT EXISTS patient_class VARCHAR(50)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_scn_study_dt ON hl7_scn_studies (study_datetime DESC)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_scn_modality ON hl7_scn_studies (modality)"
            ))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] hl7_scn_studies: {e}")

    # --- MIGRATION: pacs_done_at on hl7_orders ---
    with app.app_context():
        try:
            db.session.execute(text(
                "ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS pacs_done_at TIMESTAMP"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_hl7_orders_pacs_done_at ON hl7_orders (pacs_done_at)"
            ))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] hl7_orders.pacs_done_at: {e}")

    # --- MIGRATION: patient_class + patient_location on hl7_orders ---
    with app.app_context():
        try:
            db.session.execute(text(
                "ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS patient_class VARCHAR(50)"
            ))
            db.session.execute(text(
                "ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS patient_location VARCHAR(100)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_hl7_orders_patient_class ON hl7_orders (patient_class)"
            ))
            db.session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_hl7_orders_patient_location ON hl7_orders (patient_location)"
            ))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] hl7_orders.patient_class/location: {e}")

    # --- MIGRATION: patient portal password_hash column ---
    with app.app_context():
        try:
            db.session.execute(text("""
                ALTER TABLE patient_portal_users
                ADD COLUMN IF NOT EXISTS password_hash VARCHAR(256)
            """))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] patient_portal_users.password_hash: {e}")

    # --- MIGRATION: encrypt db_params passwords ---
    with app.app_context():
        try:
            from utils.crypto import encrypt, decrypt
            from cryptography.fernet import InvalidToken
            rows = db.session.execute(
                text("SELECT id, password FROM db_params WHERE password IS NOT NULL AND password != ''")
            ).fetchall()
            for row_id, pwd in rows:
                # Test if already encrypted by trying to decrypt
                try:
                    from cryptography.fernet import Fernet
                    import base64, hashlib
                    secret = os.environ.get('SECRET_KEY', '')
                    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
                    Fernet(key).decrypt(pwd.encode())
                    # Already encrypted — skip
                except Exception:
                    # Not encrypted — encrypt it now
                    db.session.execute(
                        text("UPDATE db_params SET password = :p WHERE id = :id"),
                        {"p": encrypt(pwd), "id": row_id}
                    )
                    logger.info(f"[Migration] Encrypted password for db_params id={row_id}")
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] db_params encryption: {e}")

    # --- STARTUP: AUTO-TRIGGER ETL IF DB IS EMPTY ---
    with app.app_context():
        # Skip ETL entirely when demo mode is active (no Oracle available)
        demo_mode = False
        try:
            demo_row = db.session.execute(
                text("SELECT value FROM settings WHERE key = 'demo_mode'")
            ).fetchone()
            demo_mode = demo_row and demo_row[0].lower() == 'true'
        except Exception:
            pass

        # Also skip if no Oracle source is configured in db_params
        has_oracle = False
        try:
            ora_row = db.session.execute(
                text("SELECT 1 FROM db_params WHERE db_type ILIKE '%oracle%' LIMIT 1")
            ).fetchone()
            has_oracle = ora_row is not None
        except Exception:
            pass

        if demo_mode:
            logger.info("⏸  [Startup Check] Demo mode — skipping ETL.")
        elif not has_oracle:
            logger.info("⏸  [Startup Check] No Oracle source configured — skipping ETL.")
        elif is_db_empty():
            trigger_initial_etl(app)
        else:
            logger.info("✅ [Startup Check] All critical tables have data — skipping initial ETL.")

    # --- SCHEDULER (5:00 AM AUTO-SYNC) ---
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Beirut"))

    def scheduled_etl():
        with app.app_context():
            # Skip ETL when demo mode is active
            try:
                demo_row = db.session.execute(
                    text("SELECT value FROM settings WHERE key = 'demo_mode'")
                ).fetchone()
                if demo_row and demo_row[0].lower() == 'true':
                    logger.info("⏸  [Scheduled ETL] Skipped — demo mode is active.")
                    return
            except Exception:
                pass
            try:
                ora_row = db.session.execute(
                    text("SELECT 1 FROM db_params WHERE db_type ILIKE '%oracle%' LIMIT 1")
                ).fetchone()
                if not ora_row:
                    logger.info("⏸  [Scheduled ETL] Skipped — no Oracle source configured.")
                    return
            except Exception:
                pass
            from ETL_JOBS.etl_runner import execute_sync
            logger.info(f"⏰ [5:00 AM] Scheduled ETL Start: {datetime.now()}")
            execute_sync(app)

    scheduler.add_job(
        func=scheduled_etl,
        trigger=CronTrigger(hour=5, minute=0),
        id='daily_etl_sync',
        name='Sync Data from Oracle',
        replace_existing=True
    )

    def scheduled_analytics():
        with app.app_context():
            try:
                from ETL_JOBS.daily_analytics import run as run_analytics
                logger.info(f"⏰ [5:30 AM] Daily analytics snapshot starting: {datetime.now()}")
                run_analytics(engine=db.engine)
                logger.info("✅ [5:30 AM] Daily analytics snapshot complete.")
            except Exception as e:
                logger.error(f"🛑 [5:30 AM] Daily analytics failed: {e}", exc_info=True)

    scheduler.add_job(
        func=scheduled_analytics,
        trigger=CronTrigger(hour=5, minute=30),
        id='daily_analytics_snapshot',
        name='Daily Analytics Snapshot',
        replace_existing=True
    )


    # Only start scheduler and HL7 listener when running as server, not manual ETL
    manual_mode = len(sys.argv) > 1 and sys.argv[1] == '-m'
    if not manual_mode:
        scheduler.start()
        start_mllp_listener(app, host='0.0.0.0', port=6661)

    return app

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------
if __name__ == '__main__':
    app = create_app()

    # MANUAL TRIGGER: python app.py -m
    if len(sys.argv) > 1 and sys.argv[1] == '-m':
        with app.app_context():
            try:
                from ETL_JOBS.etl_runner import execute_sync
                print("🚀 Manual ETL Trigger Detected...")
                execute_sync(app)
                print("✅ Manual Sync Finished.")
            except Exception as e:
                print(f"❌ Manual Sync Failed: {e}")
    else:
        app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
