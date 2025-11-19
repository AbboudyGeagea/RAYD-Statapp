# app.py
import os
import logging
from datetime import date, datetime

from flask import (
    Flask, request, redirect, url_for,
    session as flask_session, jsonify, render_template, abort
)
from flask_login import (
    LoginManager, logout_user,
    current_user, login_required
)

from dotenv import load_dotenv
import psycopg2
from sqlalchemy.sql import text

# ---------------------------------------------------------
# LOAD ENV
# ---------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------
# IMPORT PROJECT MODULES
# ---------------------------------------------------------
from db import (
    db,
    init_db,
    ActiveSession,
    User,
    ReportTemplate,
    ReportDimension,
    ReportAccessControl,
    GoLiveDate
)

from routes import register_blueprints

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'P@ssw0rd123!')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'application.log')
    SQLALCHEMY_DATABASE_URI = ""  # loaded dynamically


# ---------------------------------------------------------
# DB URI LOADER
# ---------------------------------------------------------
def get_db_uri_from_db():
    user = os.environ.get('POSTGRES_USER', 'etl_user')
    password = os.environ.get('POSTGRES_PASSWORD', '$ecureC3ynbabe')
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    port = os.environ.get('POSTGRES_PORT', '5432')
    dbname = os.environ.get('POSTGRES_DB', 'etl_db')

    fallback_dsn = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    fallback_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        with psycopg2.connect(fallback_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT conn_string
                    FROM db_params
                    WHERE name = %s AND db_type = %s
                    LIMIT 1
                """, ('etl_db', 'postgres'))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
    except Exception:
        pass

    return fallback_uri


# ---------------------------------------------------------
# CREATE APP
# ---------------------------------------------------------
def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri_from_db()
    init_db(app)

    # -----------------------------------------------------
    # LOGIN MANAGER
    # -----------------------------------------------------
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # -----------------------------------------------------
    # BLUEPRINTS
    # -----------------------------------------------------
    register_blueprints(app)

    # -----------------------------------------------------
    # SESSION VALIDATION
    # -----------------------------------------------------
    @app.before_request
    def validate_active_session():
        if request.path.startswith('/static/'):
            return

        if request.endpoint and request.endpoint.startswith('auth.'):
            return

        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        sess_uuid = flask_session.get('session_uuid')
        if not sess_uuid:
            logout_user()
            flask_session.clear()
            return redirect(url_for('auth.login'))

        active = ActiveSession.query.get(sess_uuid)
        if not active or active.user_id != current_user.id:
            logout_user()
            flask_session.clear()
            return redirect(url_for('auth.login'))

    # -----------------------------------------------------
    # REPORT VIEW (PLAIN ID, DB-GATED)
    # -----------------------------------------------------
    @app.route('/report/<int:report_id>', methods=['GET', 'POST'])
    @login_required
    def view_report(report_id):

        # 🔒 Access control
        if current_user.role != 'admin':
            allowed = ReportAccessControl.query.filter_by(
                user_id=current_user.id,
                report_template_id=report_id,
                is_enabled=True
            ).first()
            if not allowed:
                abort(403, description="This report is not enabled for your account.")

        report = ReportTemplate.query.filter_by(
            report_id=report_id,
            is_base=True
        ).first_or_404()

        dimensions = (
            ReportDimension.query
            .filter_by(report_id=report_id)
            .order_by(ReportDimension.sort_order)
            .all()
        )

        return render_template(
            "report_page.html",
            report=report,
            dimensions=dimensions
        )

    return app


# ---------------------------------------------------------
# RUN
# ---------------------------------------------------------
if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8080, debug=True)

