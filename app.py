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
    app.secret_key = os.getenv("SECRET_KEY", "P@ssw0rd123!")

    # --- LOAD FEATURE FLAGS FROM config.py ---
    app.config.from_object(app_config)

    # --- EXPOSE CONFIG TO ALL JINJA TEMPLATES ---
    @app.context_processor
    def inject_config():
        import json
        theme     = getattr(current_user, 'ui_theme', 'dark') if current_user.is_authenticated else 'dark'
        favorites = json.loads(getattr(current_user, 'favorites', '[]') or '[]') if current_user.is_authenticated else []
        return {"config": app.config, "ui_theme": theme, "user_favorites": favorites}

    # --- DB CONFIG ---
    user     = os.environ.get('POSTGRES_USER',     'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', 'SecureCrynBabe')
    host     = os.environ.get('POSTGRES_HOST',     'localhost')
    port     = os.environ.get('POSTGRES_PORT',     '5432')
    dbname   = os.environ.get('POSTGRES_DB',       'etl_db')

    app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    init_db(app)

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
                request.endpoint == 'static'
            )
        ):
            return
        if not current_user.is_authenticated:
            if request.endpoint != 'auth.login':
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
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"[Migration] columns: {e}")

    # --- STARTUP: AUTO-TRIGGER ETL IF DB IS EMPTY ---
    with app.app_context():
        if is_db_empty():
            trigger_initial_etl(app)
        else:
            logger.info("✅ [Startup Check] All critical tables have data — skipping initial ETL.")

    # --- SCHEDULER (5:00 AM AUTO-SYNC) ---
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Beirut"))

    def scheduled_etl():
        with app.app_context():
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

    start_mllp_listener(app, host='0.0.0.0', port=6661)
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)
