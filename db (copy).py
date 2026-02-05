import os
import cx_Oracle
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from flask_login import UserMixin
# ----------------------------------------------------------------
# DB INIT
# ----------------------------------------------------------------

db = SQLAlchemy()

# ----------------------------------------------------------------
# 1. CONNECTION / ETL UTILITIES
# ----------------------------------------------------------------

class OracleConnector:
    """
    Handles connections to the source Oracle database.
    Automatically selects the source based on db_params table:
        db_role='source' OR db_type='oracle'
    Only uses existing columns: host, username, password, port, sid
    """

    @staticmethod
    def get_connection(sysdba=False):
        from db import DBParams

        # automatically select the first Oracle source
        params = DBParams.query.filter(
            (DBParams.name.ilike('%oracle%'))  # fallback if you don't have db_type column
        ).first()

        if not params:
            raise Exception("No Oracle source found in db_params.")

        dsn = cx_Oracle.makedsn(params.host, params.port, sid=params.sid)

        connect_kwargs = {
            "user": params.username,
            "password": params.password,
            "dsn": dsn
        }

        if sysdba:
            connect_kwargs["mode"] = cx_Oracle.SYSDBA

        return cx_Oracle.connect(**connect_kwargs)


def init_db(app):
    """Initializes the database extension with the Flask app."""
    db.init_app(app)


def get_pg_engine():
    """Returns the raw SQLAlchemy engine for ETL scripts."""
    return db.engine


# ---- GO-LIVE DATE (SINGLE SOURCE OF TRUTH) ----------------------

def get_etl_cutoff_date():
    try:
        result = db.session.execute(
            text("SELECT go_live_date FROM go_live_config ORDER BY id DESC LIMIT 1")
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def get_go_live_date():
    return get_etl_cutoff_date()


# ---- GENERIC UPSERT ---------------------------------------------

def chunked_upsert(engine, table_name, col_names, data, constraint_col):
    if not data:
        return

    cols_str = ", ".join(col_names)
    placeholders = ", ".join([f":{col}" for col in col_names])
    update_cols = [col for col in col_names if col != constraint_col]
    update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

    query = text(f"""
        INSERT INTO {table_name} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT ({constraint_col})
        DO UPDATE SET {update_stmt}
    """)

    with engine.begin() as conn:
        dict_data = [dict(zip(col_names, row)) for row in data]
        conn.execute(query, dict_data)


# ----------------------------------------------------------------
# 2. CORE / AUTH MODELS
# ----------------------------------------------------------------

class DBParams(db.Model):
    __tablename__ = 'db_params'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    host = db.Column(db.String(100), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(50), nullable=False)
    password = db.Column(db.String(100), nullable=False)
    sid = db.Column(db.String(50), nullable=False)


class User(db.Model,UserMixin):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String, unique=True, nullable=False)
    password_hash = db.Column(db.String, nullable=False)
    role = db.Column(db.String, default='viewer')


class ETLJobLog(db.Model):
    __tablename__ = 'etl_job_log'
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(100))
    status = db.Column(db.String(50))
    records_processed = db.Column(db.Integer)
    start_time = db.Column(db.DateTime, default=func.now())
    end_time = db.Column(db.DateTime)
    error_message = db.Column(db.Text)


# ----------------------------------------------------------------
# 3. REPORT BUILDER MODELS
# ----------------------------------------------------------------

class ReportTemplate(db.Model):
    __tablename__ = 'report_template'
    report_id = db.Column(db.Integer, primary_key=True)
    report_name = db.Column(db.String(255), unique=True)
    report_sql_query = db.Column(db.Text)
    visualization_type = db.Column(db.String(50))
    is_base = db.Column(db.Boolean, default=True)

class ReportDimension(db.Model):
    __tablename__ = 'report_dimension'
    dimension_id = db.Column(db.Integer, primary_key=True)
    report_id = db.Column(db.Integer, db.ForeignKey('report_template.report_id'))
    dimension_name = db.Column(db.String(255))
    source_table = db.Column(db.String(255))
    source_column = db.Column(db.String(255))
    operator = db.Column(db.String(50))
    ui_type = db.Column(db.String(50))


class ReportAccessControl(db.Model):
    __tablename__ = 'report_access_control'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    report_template_id = db.Column(db.Integer, db.ForeignKey('report_template.report_id'))
    is_enabled = db.Column(db.Boolean, default=True)


class SavedReport(db.Model):
    __tablename__ = 'saved_reports'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))
    owner_user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    base_report_id = db.Column(db.Integer, db.ForeignKey('report_template.report_id'))
    filter_json = db.Column(JSONB)


# ----------------------------------------------------------------
# 4. IMAGING / CLINICAL MODELS (ETL OUTPUT)
# ----------------------------------------------------------------

class GoLiveDate(db.Model):
    __tablename__ = 'go_live_config'
    id = db.Column(db.Integer, primary_key=True)
    go_live_date = db.Column(db.Date)


class Patient(db.Model):
    __tablename__ = 'etl_patient_view'
    patient_db_uid = db.Column(db.BigInteger, primary_key=True)
    patient_id = db.Column(db.String)
    fallback_id = db.Column(db.String)
    birth_date = db.Column(db.Date)
    sex = db.Column(db.String(1))
    age_group = db.Column(db.String(50))
    patient_class = db.Column(db.String(50))


class Study(db.Model):
    __tablename__ = 'etl_didb_studies'
    study_db_uid = db.Column(db.BigInteger, primary_key=True)
    patient_db_uid = db.Column(db.BigInteger, index=True)
    accession_number = db.Column(db.String)
    study_date = db.Column(db.Date)
    storing_ae = db.Column(db.String)
    last_update = db.Column(db.DateTime)


class Series(db.Model):
    __tablename__ = 'etl_didb_serieses'
    series_db_uid = db.Column(db.BigInteger, primary_key=True)
    study_db_uid = db.Column(db.BigInteger, index=True)
    modality = db.Column(db.String)
    last_update = db.Column(db.DateTime)


class ImageLocation(db.Model):
    __tablename__ = 'etl_image_locations'
    raw_images_db_uid = db.Column(db.BigInteger, primary_key=True)
    source_db_uid = db.Column(db.BigInteger)
    file_system = db.Column(db.String)
    image_size_kb = db.Column(db.Integer)
    last_update = db.Column(db.DateTime)


class Order(db.Model):
    __tablename__ = 'etl_orders'
    order_dbid = db.Column(db.String(100), primary_key=True)
    patient_dbid = db.Column(db.String(100))
    proc_id = db.Column(db.String(100))
    proc_text = db.Column(db.String)
    scheduled_datetime = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)


#------------------------------------------------
# Mapping Section
#------------------------------------------------

class ProcedureDurationMap(db.Model):
    __tablename__ = 'procedure_duration_map'
    id = db.Column(db.Integer, primary_key=True)
    procedure_code = db.Column(db.String, unique=True, nullable=False)
    duration_minutes = db.Column(db.Integer, nullable=False)


class AETitleModalityMap(db.Model):
    __tablename__ = 'aetitle_modality_map'
    id = db.Column(db.Integer, primary_key=True)
    aetitle = db.Column(db.String, nullable=False, index=True)
    modality = db.Column(db.String, nullable=False)


# ----------------------------------------------------------------
# 5. BACKWARD-COMPAT ALIASES (DO NOT REMOVE)
# ----------------------------------------------------------------

def get_report_data(sql_query, params=None):
    """Centralized helper to execute report SQL safely"""
    if not sql_query:
        return []

    params = params or {}
    result = db.session.execute(text(sql_query), params)
    return result.mappings().all()


# Aliases
SourceDBParams = DBParams

