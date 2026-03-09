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

from db import (
    db,
    init_db,
    User,
    ReportTemplate,
    ReportDimension,
    ReportAccessControl,
    GoLiveDate
)
from routes.registry import register_blueprints

# ---------------------------------------------------------
# CREATE APP
# ---------------------------------------------------------
def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "P@ssw0rd123!")

    # --- DB CONFIG ---
    user = os.environ.get('POSTGRES_USER', 'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', 'Rayd_Secure_2026')
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    port = os.environ.get('POSTGRES_PORT', '5432')
    dbname = os.environ.get('POSTGRES_DB', 'etl_db')

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
        if request.path.startswith('/static/') or (request.endpoint and (request.endpoint.startswith('auth.') or request.endpoint == 'static')):
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

    # --- SCHEDULER (5:00 AM AUTO-SYNC) ---
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Beirut"))
    
    def scheduled_etl():
        # This helper ensures the 4TB job runs in the app bubble
        with app.app_context():
            from ETL_JOBS.etl_runner import execute_sync
            print(f"⏰ [5:00 AM] Scheduled ETL Start: {datetime.now()}")
            execute_sync(app)

    # Add cron job for 5:00 AM
    scheduler.add_job(
        func=scheduled_etl,
        trigger=CronTrigger(hour=5, minute=0),
        id='daily_etl_sync',
        name='Sync Data from Oracle',
        replace_existing=True
    )
    scheduler.start()

    return app

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------
if __name__ == '__main__':
    app = create_app()

    # MANUAL TRIGGER CHECK
    # Usage: python app.py -m
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
