import os
import sys
import logging
import oracledb

# 1. ORACLE MODERNIZATION
sys.modules["cx_Oracle"] = oracledb 

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from sqlalchemy import text, BigInteger, ForeignKey, Numeric, Boolean, Integer, String, DateTime, Date, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

db = SQLAlchemy()

# ----------------------------------------------------------------
# 2. UTILITIES & ETL HELPERS
# ----------------------------------------------------------------

class OracleConnector:
    @staticmethod
    def get_connection(sysdba=False):
        from utils.crypto import decrypt
        params = DBParams.query.filter(DBParams.name.ilike('%oracle%')).first()
        if not params:
            raise Exception("No Oracle configuration found in db_params table.")

        dsn = oracledb.makedsn(params.host, params.port, sid=params.sid)
        connect_kwargs = {
            "user": params.username,
            "password": decrypt(params.password),
            "dsn": dsn
        }
        
        if sysdba or (params.mode and params.mode.upper() == 'SYSDBA'):
            connect_kwargs["mode"] = oracledb.SYSDBA
            
        return oracledb.connect(**connect_kwargs)

def init_db(app):
    db.init_app(app)

def get_pg_engine():
    return db.engine

def get_etl_cutoff_date():
    try:
        result = db.session.execute(text("SELECT go_live_date FROM go_live_config ORDER BY id DESC LIMIT 1")).fetchone()
        return result[0] if result else None
    except Exception: return None

def get_go_live_date():
    return get_etl_cutoff_date()

def etl_analytics_refresh():
    try:
        db.session.execute(text("SELECT refresh_analytics_summary();"))
        db.session.commit()
    except Exception: db.session.rollback()

def chunked_upsert(engine, table_name, col_names, data, constraint_col):
    """
    Bulk upsert with automatic type-safe fallback.

    Fast path: insert the whole batch in one statement.
    If PostgreSQL rejects the batch (e.g. "R3" in a bigint column), fall back
    to row-by-row insertion.  On each individual row failure the offending
    non-PK numeric column is set to NULL and the insert is retried once.
    If it still fails, the row is skipped and a warning is logged.

    Numeric columns are discovered once per (engine, table) pair from the PG
    information_schema so no per-ETL-file knowledge is required.
    """
    if not data:
        return

    cols_str   = ", ".join(col_names)
    placeholders = ", ".join([f":{col}" for col in col_names])
    update_cols  = [col for col in col_names if col != constraint_col]
    update_stmt  = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    upsert_sql   = text(
        f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) "
        f"ON CONFLICT ({constraint_col}) DO UPDATE SET {update_stmt}"
    )

    dict_data = [dict(zip(col_names, row)) for row in data]

    # ── Fast path ─────────────────────────────────────────────────────────────
    try:
        with engine.begin() as conn:
            conn.execute(upsert_sql, dict_data)
        return
    except Exception:
        pass  # fall through to row-by-row

    # ── Discover numeric columns from PG catalog (cached per table) ───────────
    _cache = getattr(chunked_upsert, '_numeric_cache', {})
    if table_name not in _cache:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = :t
                      AND data_type IN ('bigint','integer','smallint','numeric','double precision','real')
                """), {"t": table_name}).fetchall()
            _cache[table_name] = {r[0] for r in rows}
        except Exception:
            _cache[table_name] = set()
        chunked_upsert._numeric_cache = _cache
    numeric_cols = _cache[table_name]

    def _sanitize(row_dict):
        """NULL out any non-PK numeric value that can't be cast to a number."""
        sanitized = dict(row_dict)
        for col, val in sanitized.items():
            if col == constraint_col or col not in numeric_cols or val is None:
                continue
            try:
                float(val)          # catches int, float, Decimal, numeric strings
            except (TypeError, ValueError):
                logging.warning(
                    f"[chunked_upsert] {table_name}.{col}: "
                    f"non-numeric value {val!r} → NULL"
                )
                sanitized[col] = None
        return sanitized

    # ── Row-by-row fallback ───────────────────────────────────────────────────
    skipped = 0
    for row_dict in dict_data:
        # First try: original values
        try:
            with engine.begin() as conn:
                conn.execute(upsert_sql, [row_dict])
            continue
        except Exception:
            pass

        # Second try: sanitized (NULL out bad numeric columns)
        sanitized = _sanitize(row_dict)
        try:
            with engine.begin() as conn:
                conn.execute(upsert_sql, [sanitized])
        except Exception as e:
            skipped += 1
            pk_val = row_dict.get(constraint_col, '?')
            logging.warning(
                f"[chunked_upsert] {table_name}: skipping row "
                f"(pk={pk_val!r}) — {e}"
            )

    if skipped:
        logging.warning(
            f"[chunked_upsert] {table_name}: {skipped} rows permanently skipped"
        )

# ----------------------------------------------------------------
# 3. CORE & AUTH MODELS
# ----------------------------------------------------------------

class User(db.Model, UserMixin):
    __tablename__ = 'users'
    id = db.Column(Integer, primary_key=True)
    username = db.Column(String, unique=True, nullable=False)
    password_hash = db.Column(String, nullable=False)
    role = db.Column(String)
    ui_theme = db.Column(String, server_default='dark')
    favorites = db.Column(Text, server_default='[]')  # JSON array of report_ids

class active_sessions(db.Model):
    __tablename__ = 'active_sessions'
    session_id = db.Column(String, primary_key=True)
    user_id = db.Column(Integer, ForeignKey('users.id'))
    role = db.Column(String)
    ip_address = db.Column(String)
    login_time = db.Column(DateTime, server_default=func.now())
    created_at = db.Column(DateTime, server_default=func.now())

class DBParams(db.Model):
    __tablename__ = 'db_params'
    id = db.Column(Integer, primary_key=True)
    name = db.Column(String(100), unique=True, nullable=False)
    db_role = db.Column(String(50))
    db_type = db.Column(String(50))
    conn_string = db.Column(Text)
    host = db.Column(String(100))
    username = db.Column(String(50))
    password = db.Column(String(100))
    port = db.Column(Integer)
    sid = db.Column(String(50))      # Oracle SID  /  database name for PG·MySQL·MSSQL
    mode = db.Column(String(50))
    owner = db.Column(String(100))   # schema owner (e.g. MEDISTORE)
    created_at = db.Column(DateTime, server_default=func.now())
    updated_at = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

class GoLiveDate(db.Model):
    __tablename__ = 'go_live_config'
    id = db.Column(Integer, primary_key=True)
    go_live_date = db.Column(Date)

class ETLJobLog(db.Model):
    __tablename__ = 'etl_job_log'
    id = db.Column(Integer, primary_key=True)
    job_name = db.Column(Text)
    status = db.Column(Text, server_default='RUNNING')
    start_time = db.Column(DateTime, server_default=func.now())
    end_time = db.Column(DateTime)
    records_processed = db.Column(Integer, default=0)
    null_alerts = db.Column(Integer, default=0)
    rows_per_second = db.Column(Numeric(10,2))
    error_message = db.Column(Text)
    duration_seconds = db.Column(Numeric(10,2))

# ----------------------------------------------------------------
# 4. REPORTING ENGINE
# ----------------------------------------------------------------

class ReportTemplate(db.Model):
    __tablename__ = 'report_template'
    report_id = db.Column(Integer, primary_key=True)
    report_name = db.Column(String(255), unique=True)
    long_description = db.Column(Text)
    report_sql_query = db.Column(Text)
    required_parameters = db.Column(Text)
    created_by_user_id = db.Column(Integer)
    creation_date = db.Column(DateTime)
    visualization_type = db.Column(String(50))
    is_base = db.Column(Boolean, default=True)

class ReportDimension(db.Model):
    __tablename__ = 'report_dimension'
    dimension_id = db.Column(Integer, primary_key=True)
    report_id = db.Column(Integer, ForeignKey('report_template.report_id'))
    dimension_name = db.Column(String(255))
    source_table = db.Column(String(255))
    source_column = db.Column(String(255))
    sql_type = db.Column(String(50))
    operator = db.Column(String(50))
    ui_type = db.Column(String(50))
    domain_table = db.Column(String(255))
    required = db.Column(Boolean, default=True)
    sort_order = db.Column(Integer, default=0)
    fact_alias = db.Column(String(10))

class report_derivative(db.Model):
    __tablename__ = 'report_derivative'
    derivative_id = db.Column(Integer, primary_key=True)
    report_id = db.Column(Integer, ForeignKey('report_template.report_id'))
    dimension_id = db.Column(Integer, ForeignKey('report_dimension.dimension_id'))
    sql_fragment = db.Column(Text)
    operator = db.Column(String(50))
    description = db.Column(Text)
    sort_order = db.Column(Integer, default=0)

class ReportAccessControl(db.Model):
    __tablename__ = 'report_access_control'
    id = db.Column(Integer, primary_key=True)
    user_id = db.Column(Integer, ForeignKey('users.id'))
    is_enabled = db.Column(Boolean, default=True)
    report_template_id = db.Column(Integer, ForeignKey('report_template.report_id'))

class UserPagePermission(db.Model):
    __tablename__ = 'user_page_permissions'
    id = db.Column(Integer, primary_key=True)
    user_id = db.Column(Integer, ForeignKey('users.id'), nullable=False)
    page_key = db.Column(String(50), nullable=False)   # e.g. 'live_feed', 'hl7_orders'
    is_enabled = db.Column(Boolean, default=True)

class SchedulingEntry(db.Model):
    __tablename__ = 'scheduling_entries'
    id = db.Column(Integer, primary_key=True)
    first_name = db.Column(Text, nullable=False)
    middle_name = db.Column(Text, nullable=False)
    last_name = db.Column(Text, nullable=False)
    date_of_birth = db.Column(Date, nullable=False)
    referring_physician = db.Column(Text, nullable=False)
    patient_class = db.Column(String(10), nullable=False)
    procedure_datetime = db.Column(DateTime, nullable=False)
    modality_type = db.Column(String(50), nullable=False)
    procedures = db.Column(JSONB, nullable=False, server_default='[]')
    third_party_approvals = db.Column(JSONB, nullable=False, server_default='[]')
    created_at = db.Column(DateTime, server_default=func.now())
    updated_at = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

ALL_FEATURE_KEYS = [
    'live_feed', 'hl7_orders', 'report_ai', 'bitnet', 'oru', 'mapping', 'patient_portal', 'scheduling',
    'financial',
]

def user_has_page(user, page_key):
    if user.role == 'admin':
        return True
    perm = UserPagePermission.query.filter_by(user_id=user.id, page_key=page_key).first()
    return perm.is_enabled if perm is not None else False

class OruReport(db.Model):
    __tablename__ = 'hl7_oru_reports'
    id               = db.Column(Integer, primary_key=True)
    procedure_code   = db.Column(String(100))
    procedure_name   = db.Column(Text)
    modality         = db.Column(String(20))
    physician_id     = db.Column(String(100))
    patient_id       = db.Column(String(100))
    accession_number = db.Column(String(100))
    report_text      = db.Column(Text)
    impression_text  = db.Column(Text)
    result_datetime  = db.Column(DateTime)
    received_at      = db.Column(DateTime, server_default=func.now())

class SavedReport(db.Model):
    __tablename__ = 'saved_reports'
    id = db.Column(Integer, primary_key=True)
    name = db.Column(String(255))
    owner_user_id = db.Column(Integer, ForeignKey('users.id'))
    base_report_id = db.Column(Integer, ForeignKey('report_template.report_id'))
    is_public = db.Column(Boolean, default=False)
    filter_json = db.Column(JSONB, server_default='{}')
    generated_sql = db.Column(Text)
    created_at = db.Column(DateTime, server_default=func.now())
    updated_at = db.Column(DateTime, server_default=func.now())

# ----------------------------------------------------------------
# 5. CLINICAL ETL TABLES
# ----------------------------------------------------------------

class etl_patient_view(db.Model):
    __tablename__ = 'etl_patient_view'
    patient_db_uid = db.Column(BigInteger, primary_key=True)
    id = db.Column(Text)
    birth_date = db.Column(Date)
    sex = db.Column(Text)
    number_of_patient_studies = db.Column(Integer)
    gender = db.Column(String(50))

class etl_didb_studies(db.Model):
    __tablename__ = 'etl_didb_studies'
    study_db_uid = db.Column(BigInteger, primary_key=True)
    patient_db_uid = db.Column(BigInteger)
    study_instance_uid = db.Column(Text)
    accession_number = db.Column(Text)
    storing_ae = db.Column(Text)
    study_date = db.Column(Date)
    procedure_code = db.Column(Text)
    last_update = db.Column(DateTime, server_default=func.now())
    study_modality = db.Column(String(50))

class etl_didb_serieses(db.Model):
    __tablename__ = 'etl_didb_serieses'
    series_db_uid = db.Column(BigInteger, primary_key=True)
    study_db_uid = db.Column(BigInteger, ForeignKey('etl_didb_studies.study_db_uid'))
    modality = db.Column(Text)
    last_update = db.Column(DateTime, server_default=func.now())

class etl_didb_raw_images(db.Model):
    __tablename__ = 'etl_didb_raw_images'
    raw_image_db_uid = db.Column(BigInteger, primary_key=True)
    patient_db_uid = db.Column(BigInteger)
    study_db_uid = db.Column(BigInteger, ForeignKey('etl_didb_studies.study_db_uid'))
    series_db_uid = db.Column(BigInteger, ForeignKey('etl_didb_serieses.series_db_uid'))
    study_instance_uid = db.Column(String(255))
    last_update = db.Column(DateTime, server_default=func.now())

class etl_image_locations(db.Model):
    __tablename__ = 'etl_image_locations'
    raw_image_db_uid = db.Column(BigInteger, ForeignKey('etl_didb_raw_images.raw_image_db_uid'), primary_key=True)
    file_system = db.Column(Text)
    image_size_kb = db.Column(Integer)
    last_update = db.Column(DateTime, server_default=func.now())

class etl_orders(db.Model):
    __tablename__ = 'etl_orders'
    order_dbid = db.Column(BigInteger, primary_key=True)
    patient_dbid = db.Column(Text)
    study_db_uid = db.Column(BigInteger)
    proc_id = db.Column(Text)
    proc_text = db.Column(Text)
    scheduled_datetime = db.Column(DateTime)
    last_update = db.Column(DateTime, server_default=func.now())

# ----------------------------------------------------------------
# 6. MAPPING & STORAGE TABLES
# ----------------------------------------------------------------

class procedure_duration_map(db.Model):
    __tablename__ = 'procedure_duration_map'
    id = db.Column(Integer, primary_key=True)
    procedure_code = db.Column(String, unique=True)
    duration_minutes = db.Column(Integer)
    rvu_value = db.Column(Numeric(10,2), default=0.0)
    modality = db.Column(String(20))

class aetitle_modality_map(db.Model):
    __tablename__ = 'aetitle_modality_map'
    id = db.Column(Integer, primary_key=True)
    aetitle = db.Column(String, unique=True)
    modality = db.Column(String)
    room_name = db.Column(String(100))
    daily_capacity_minutes = db.Column(Integer, default=480)
    weekly_schedules = relationship("device_weekly_schedule", back_populates="device")
    exceptions = relationship("device_exceptions", back_populates="device")

class device_weekly_schedule(db.Model):
    __tablename__ = 'device_weekly_schedule'
    aetitle = db.Column(String(50), ForeignKey('aetitle_modality_map.aetitle'), primary_key=True)
    day_of_week = db.Column(Integer, primary_key=True)
    std_opening_minutes = db.Column(Integer, default=720)
    device = relationship("aetitle_modality_map", back_populates="weekly_schedules")

class device_exceptions(db.Model):
    __tablename__ = 'device_exceptions'
    id = db.Column(Integer, primary_key=True)
    aetitle = db.Column(String(50), ForeignKey('aetitle_modality_map.aetitle'))
    exception_date = db.Column(Date)
    actual_opening_minutes = db.Column(Integer)
    reason = db.Column(String(255))
    device = relationship("aetitle_modality_map", back_populates="exceptions")

class summary_storage_daily(db.Model):
    __tablename__ = 'summary_storage_daily'
    id = db.Column(Integer, primary_key=True)
    study_date = db.Column(Date, index=True)
    storing_ae = db.Column(String(100))
    modality = db.Column(String(50))
    procedure_code = db.Column(String(255))
    total_gb = db.Column(Numeric(12, 4), default=0)
    study_count = db.Column(Integer, default=0)

# ----------------------------------------------------------------
# 7. PATIENT PORTAL TABLES  ← NEW
# ----------------------------------------------------------------

class AiFeedback(db.Model):
    """Thumbs up/down on AI chat responses — reviewed weekly by admin."""
    __tablename__ = 'ai_feedback'
    id           = db.Column(Integer, primary_key=True)
    question     = db.Column(Text, nullable=False)
    response     = db.Column(Text, nullable=False)
    vote         = db.Column(String(10), nullable=False)   # 'up' or 'down'
    user_id      = db.Column(Integer, ForeignKey('users.id'))
    reviewed     = db.Column(Boolean, default=False)
    created_at   = db.Column(DateTime, server_default=func.now())

class AiCorrection(db.Model):
    """Admin-taught corrections — injected as few-shot examples in AI prompts."""
    __tablename__ = 'ai_corrections'
    id               = db.Column(Integer, primary_key=True)
    keywords         = db.Column(Text, nullable=False)         # comma-separated trigger words
    correct_answer   = db.Column(Text, nullable=False)
    example_question = db.Column(Text)                         # optional: the original bad question
    created_by       = db.Column(String(100))
    is_active        = db.Column(Boolean, default=True)
    created_at       = db.Column(DateTime, server_default=func.now())


class PatientPortalUser(db.Model):
    """
    One record per patient MRN.
    Upserted every time an ORM arrives for that patient.
    Passwords are hashed. password_plain kept temporarily for migration,
    new logins use password_hash.
    """
    __tablename__ = 'patient_portal_users'
    id               = db.Column(Integer, primary_key=True)
    mrn              = db.Column(String(50), nullable=False)
    full_name        = db.Column(String(200))
    phone            = db.Column(String(30))
    accession_number = db.Column(String(100))
    username         = db.Column(String(50), unique=True, nullable=False)  # = MRN
    password_plain   = db.Column(String(20))                               # deprecated — migrate then drop
    password_hash    = db.Column(String(256))
    is_active        = db.Column(Boolean, default=True)
    last_login       = db.Column(DateTime)
    whatsapp_sent    = db.Column(Boolean, default=False)
    whatsapp_sent_at = db.Column(DateTime)
    created_at       = db.Column(DateTime, server_default=func.now())
    updated_at       = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<PatientPortalUser mrn={self.mrn} name={self.full_name}>"


class PortalConfig(db.Model):
    """
    Key-value store for per-site portal settings.
    Editable via /admin/portal/config without redeploy.
    """
    __tablename__ = 'portal_config'
    id           = db.Column(Integer, primary_key=True)
    config_key   = db.Column(String(100), unique=True, nullable=False)
    config_value = db.Column(Text)
    description  = db.Column(String(255))
    updated_at   = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<PortalConfig {self.config_key}>"

# ----------------------------------------------------------------
# ----------------------------------------------------------------
# 8. FINANCIAL CONFIGURATION TABLES
# ----------------------------------------------------------------

class FinancialConfig(db.Model):
    __tablename__ = 'financial_config'
    id          = db.Column(Integer, primary_key=True)
    entity_type = db.Column(String(20), nullable=False)
    entity_id   = db.Column(Text)
    usd_per_rvu = db.Column(Numeric(8, 4), nullable=False)
    notes       = db.Column(Text)
    created_at  = db.Column(DateTime, server_default=func.now())
    updated_at  = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

class FinancialAuditLog(db.Model):
    __tablename__ = 'financial_audit_log'
    id          = db.Column(Integer, primary_key=True)
    user_id     = db.Column(Integer)
    user_name   = db.Column(Text)
    action      = db.Column(Text, nullable=False)
    entity_type = db.Column(Text)
    entity_id   = db.Column(Text)
    old_value   = db.Column(Numeric(8, 4))
    new_value   = db.Column(Numeric(8, 4))
    ip_address  = db.Column(Text)
    created_at  = db.Column(DateTime, server_default=func.now())

class TechFlagAck(db.Model):
    __tablename__        = 'tech_flag_acknowledgements'
    id                   = db.Column(Integer, primary_key=True)
    accession_number     = db.Column(Text, nullable=False)
    flag_date            = db.Column(Date, nullable=False)
    flags                = db.Column(ARRAY(Text), nullable=False, server_default='{}')
    note                 = db.Column(Text)
    acknowledged_by_id   = db.Column(Integer, ForeignKey('users.id', ondelete='SET NULL'))
    acknowledged_by_name = db.Column(Text, nullable=False)
    acknowledged_at      = db.Column(DateTime, nullable=False, server_default=func.now())
    __table_args__       = (db.UniqueConstraint('accession_number', 'flag_date', name='uq_tfa_accession_date'),)

# ----------------------------------------------------------------
# 9. ALIASES (KEEPS CONTROLLERS HAPPY)
# ----------------------------------------------------------------
ActiveSession        = active_sessions
AETitleModalityMap   = aetitle_modality_map
ProcedureDurationMap = procedure_duration_map
DeviceException      = device_exceptions
DeviceWeeklySchedule = device_weekly_schedule
EtlDidbStudy         = etl_didb_studies
Patient              = etl_patient_view
SummaryStorageDaily  = summary_storage_daily
