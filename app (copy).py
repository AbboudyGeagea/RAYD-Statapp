# app.py
import os
import logging
from datetime import date
from flask import Flask, request, redirect, url_for, session as flask_session, jsonify
from flask_login import LoginManager, logout_user, current_user, login_required
from dotenv import load_dotenv
import psycopg2

# Load env vars early
load_dotenv()

# ---------------------------
# Import ALL project modules here
# (Safe because none of them should run DB queries at import time)
# ---------------------------
from db import db, init_db, ActiveSession, User
from routes import register_blueprints
try:
    from ETL_JOBS.etl_processor import start_etl_scheduler
except ImportError:
    start_etl_scheduler = None


# ---------------------------------------------------------
# CONFIG CLASS
# ---------------------------------------------------------
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'default_strong_secret_key_please_change')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GO_LIVE_DATE = date(2023, 1, 1)
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'application.log')
    ETL_JOB_INTERVAL_SECONDS = int(os.environ.get('ETL_JOB_INTERVAL_SECONDS', 3600))
    SQLALCHEMY_DATABASE_URI = "" 


# ---------------------------------------------------------
# Function to load the REAL DB URI from db_params
# ---------------------------------------------------------
def get_db_uri_from_db():
    """
    Uses raw psycopg2 to fetch the final connection string from db_params.
    Runs BEFORE Flask-SQLAlchemy is initialized.
    """

    # fallback credentials from environment variables
    user = os.environ.get('POSTGRES_USER', 'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', '$ecureC3ynbabe')
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    port = os.environ.get('POSTGRES_PORT', '5432')
    dbname = os.environ.get('POSTGRES_DB', 'etl_db')

    fallback_dsn = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    sqlalchemy_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        with psycopg2.connect(fallback_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT conn_string FROM db_params WHERE name = %s AND db_type = %s LIMIT 1",
                    ('etl_db', 'postgres')
                )
                result = cur.fetchone()
                if result and result[0]:
                    print("🔵 Loaded connection URI from db_params table.")
                    return result[0]
    except Exception as e:
        print(f"⚠️ Could not load DB URI from db_params. Falling back. Error: {e}")

    return sqlalchemy_uri


# ---------------------------------------------------------
# CREATE APP
# ---------------------------------------------------------
def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # load final DB URI BEFORE initializing SQLAlchemy
    db_uri = get_db_uri_from_db()
    app.config['SQLALCHEMY_DATABASE_URI'] = db_uri

    # Initialize SQLAlchemy
    init_db(app)

    # Setup LoginManager
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # Register all blueprints
    register_blueprints(app)

    # -----------------------------------------------------
    # ACTIVE SESSION VALIDATION
    # -----------------------------------------------------
    @app.before_request
    def validate_active_session():

        if request.path.startswith('/static/'):
            return None

        if request.endpoint and (
            'auth.login' in request.endpoint or 
            'auth.logout' in request.endpoint
        ):
            return None

        # If flask-login says NOT authenticated → redirect
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        sess_uuid = flask_session.get('session_uuid')
        uid = flask_session.get('user_id')

        # If session variables missing → logout
        if not sess_uuid or not uid:
            logout_user()
            flask_session.clear()
            return redirect(url_for('auth.login'))

        active = ActiveSession.query.get(sess_uuid)

        # session mismatch or expired
        if not active or int(active.user_id) != int(current_user.id):
            logout_user()
            flask_session.clear()

            if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'status': 'error', 'message': 'Session expired. Login again.'}), 401

            return redirect(url_for('auth.login'))

    # -----------------------------------------------------
    # LOGGING SETUP
    # -----------------------------------------------------
    logging.basicConfig(
        level=getattr(logging, app.config['LOG_LEVEL'].upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        filename=app.config['LOG_FILE'],
        filemode='a'
    )

    return app


# ---------------------------------------------------------
# RUN APP DIRECTLY
# ---------------------------------------------------------
if __name__ == '__main__':
    app = create_app()

    if start_etl_scheduler:
        try:
            start_etl_scheduler(app)
        except Exception as e:
            app.logger.error(f"ETL scheduler failed: {e}")

    app.run(host='0.0.0.0', port=8080, debug=True)
