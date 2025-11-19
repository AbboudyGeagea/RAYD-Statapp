import os
import datetime
import logging
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask import has_app_context
import oracledb
import pandas as pd

db = SQLAlchemy()

# ----------------------------
# LOGGER CONFIGURATION
# ----------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ----------------------------
# New ORM Models for Mapping
# ----------------------------
class AETitleModalityMap(db.Model):
    __tablename__ = 'aetitle_modality_map'
    id = db.Column(db.Integer, primary_key=True)
    aetitle = db.Column(db.String, nullable=False)
    modality = db.Column(db.String, nullable=False)

class ProcedureDurationMap(db.Model):
    __tablename__ = 'procedure_duration_map'
    id = db.Column(db.Integer, primary_key=True)
    procedure_code = db.Column(db.String, nullable=False)
    duration_minutes = db.Column(db.Integer, nullable=False)

# ----------------------------
# USER MODEL
# ----------------------------
class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(50), nullable=False, default='viewer')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f"<User id={self.id} username={self.username} role={self.role}>"

# ----------------------------
# REPORT TEMPLATE & ACCESS
# ----------------------------
class ReportTemplate(db.Model):
    __tablename__ = 'report_template'
    report_id = db.Column(db.Integer, primary_key=True)
    report_name = db.Column(db.String(255), nullable=False)
    long_description = db.Column(db.Text)
    report_sql_query = db.Column(db.Text)
    required_parameters = db.Column(db.Text)
    created_by_user_id = db.Column(db.Integer)
    creation_date = db.Column(db.DateTime)
    visualization_type = db.Column(db.String(50), nullable=False)
    is_base = db.Column(db.Boolean, default=False, nullable=False)

class ReportAccessControl(db.Model):
    __tablename__ = 'report_access_control'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    report_template_id = db.Column(db.Integer, db.ForeignKey('report_template.report_id'), nullable=False)
    is_enabled = db.Column(db.Boolean, default=False, nullable=False)

    user = db.relationship('User', backref=db.backref('report_access', lazy=True))
    report = db.relationship('ReportTemplate', backref=db.backref('access_control', lazy=True))

# ----------------------------
# Active Sessions (server-side session tracking)
# ----------------------------
class ActiveSession(db.Model):
    __tablename__ = 'active_sessions'
    session_id = db.Column(db.String(128), primary_key=True)   # UUID stored in client session
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    role = db.Column(db.String(50), nullable=False)
    ip_address = db.Column(db.String(45), nullable=False)      # IPv4 or IPv6
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    user = db.relationship('User', backref=db.backref('active_sessions', lazy=True))

# ----------------------------
# DB PARAMETERS MODEL (SOURCE + TARGET)
# ----------------------------
class DBParams(db.Model):
    __tablename__ = 'db_params'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    db_role = db.Column(db.String(50), nullable=False)        # 'source' or 'target'
    db_type = db.Column(db.String(50), nullable=False)        # 'postgres' or 'oracle'
    conn_string = db.Column(db.String(1024))                  # SQLAlchemy conn string (Postgres)
    host = db.Column(db.String(255))
    username = db.Column(db.String(255))
    password = db.Column(db.String(255))
    port = db.Column(db.Integer)
    sid = db.Column(db.String(50))
    mode = db.Column(db.String(50))                           # Oracle: SYSDBA / normal
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.datetime.utcnow,
                           onupdate=datetime.datetime.utcnow)

# ----------------------------
# GO-LIVE CONFIG
# ----------------------------
class GoLiveDate(db.Model):
    __tablename__ = 'go_live_config'
    id = db.Column(db.Integer, primary_key=True)
    go_live_date = db.Column(db.Date)

def get_go_live_date():
    record = GoLiveDate.query.first()
    if record and record.go_live_date:
        logger.info(f"Go-live date: {record.go_live_date}")
        return record.go_live_date
    return None

# ----------------------------
# Saved Report - For end-users saving reports
# ----------------------------
class SavedReport(db.Model):
    __tablename__ = 'saved_reports'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    owner_user_id = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    base_report_id = db.Column(db.Integer, db.ForeignKey('report_template.report_id', ondelete='CASCADE'), nullable=False)
    is_public = db.Column(db.Boolean, nullable=False, default=False)
    filter_json = db.Column(db.JSON, nullable=False, default={})
    generated_sql = db.Column(db.Text)  # optional cached final SQL for fast execution
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    owner = db.relationship('User', backref=db.backref('saved_reports', lazy=True))
    base_report = db.relationship('ReportTemplate', backref=db.backref('saved_variants', lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "owner_user_id": self.owner_user_id,
            "base_report_id": self.base_report_id,
            "is_public": self.is_public,
            "filter_json": self.filter_json,
            "generated_sql": self.generated_sql,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }



# ... (User, ReportTemplate classes)

class ReportDimension(db.Model):
    __tablename__ = 'report_dimension'

    dimension_id = db.Column(db.Integer, primary_key=True)
    report_id = db.Column(
        db.Integer,
        db.ForeignKey('report_template.report_id', ondelete='CASCADE'),
        nullable=False
    )
    dimension_name = db.Column(db.String(255), nullable=False)
    source_table = db.Column(db.String(255))
    source_column = db.Column(db.String(255))
    sql_type = db.Column(db.String(50))
    operator = db.Column(db.String(50))
    ui_type = db.Column(db.String(50))
    domain_table = db.Column(db.String(255), nullable=True)
    # Changed to nullable=True in ORM to match "no required field" intent
    required = db.Column(db.Boolean, default=False) 
    sort_order = db.Column(db.Integer, default=0)
    fact_alias = db.Column(db.String(10), nullable=True)

# ----------------------------
# Report Derivative - For dynamic filtering per dimension
# ----------------------------
class ReportDerivative(db.Model):
    __tablename__ = 'report_derivative'

    derivative_id = db.Column(db.Integer, primary_key=True)
    
    report_id = db.Column(
        'report_id', 
        db.Integer,
        db.ForeignKey('report_template.report_id', ondelete='CASCADE'),
        nullable=False
    )
    
    dimension_id = db.Column(
        db.Integer,
        db.ForeignKey('report_dimension.dimension_id', ondelete='CASCADE'),
        nullable=False
    )

    sql_fragment = db.Column(db.Text, nullable=False)
    operator = db.Column(db.String(20), nullable=True)  # =, <>, IN, LIKE, etc.
    description = db.Column(db.Text, nullable=True)
    sort_order = db.Column(db.Integer, default=0)

    report = db.relationship('ReportTemplate', backref=db.backref('derivatives', lazy=True))
    dimension = db.relationship('ReportDimension', backref=db.backref('derivatives', lazy=True))

# ----------------------------
# ORACLE CONNECTOR — FIXED FOR ETL
# ----------------------------
class OracleConnector:

    @staticmethod
    def get_connection(source_name: str):
        if not has_app_context():
            raise RuntimeError(
                f"OracleConnector.get_connection('{source_name}') called outside application context. "
                f"Wrap ETL in:   with app.app_context():"
            )

        params = DBParams.query.filter_by(name=source_name, db_type='oracle').first()
        if not params:
            logger.error(f"No Oracle DB params found for {source_name}")
            return None

        dsn = f"{params.host}:{params.port}/{params.sid}"

        try:
            mode = oracledb.SYSDBA if (params.mode and params.mode.upper() == 'SYSDBA') else None
            conn = oracledb.connect(
                user=params.username,
                password=params.password,
                dsn=dsn,
                mode=mode
            )
            logger.info(f"✅ Oracle connection established to {source_name}")
            return conn

        except Exception as e:
            logger.error(f"❌ Failed Oracle connection to {source_name}: {e}")
            return None

    @staticmethod
    def fetch_dataframe(source_name: str, query: str) -> pd.DataFrame:
        try:
            conn = OracleConnector.get_connection(source_name)
        except RuntimeError as e:
            logger.error(str(e))
            return pd.DataFrame()

        if not conn:
            return pd.DataFrame()

        try:
            df = pd.read_sql(query, conn)
            logger.info(f"✅ Query returned {len(df)} rows from {source_name}")
            return df
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            return pd.DataFrame()


# ----------------------------
# INIT DB + DEFAULT ADMIN
# ----------------------------
def init_db(app):
    db.init_app(app)
    with app.app_context():
        db.create_all()

        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', role='admin')
            admin.set_password('admin')
            db.session.add(admin)
            db.session.commit()
            logger.info("Default admin user created (admin/admin)")

# Backward compatibility
SourceDBParams = DBParams

