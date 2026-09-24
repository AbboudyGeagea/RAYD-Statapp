"""
db.py
-----
SQLAlchemy setup, ORM models, and shared DB helpers.

Exports used across the application:
  db                — the SQLAlchemy extension instance (init via init_db(app))
  OracleConnector   — DISABLED on this branch, see the banner below
  get_pg_engine()   — returns db.engine (PostgreSQL)
  get_etl_cutoff_date() / get_go_live_date() — earliest date we hold HL7 data for
  chunked_upsert()  — bulk upsert with automatic row-by-row fallback on type errors

╔══════════════════════════════════════════════════════════════════════════════╗
║ HL7 DISTRIBUTION BRANCH — NO ORACLE                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Philips does not permit direct database access to the PACS Oracle schema, so ║
║ this branch takes all clinical data as HL7 v2 over MLLP instead. The Oracle  ║
║ driver is NOT installed here: `oracledb` is out of requirements.txt and the  ║
║ Instant Client is out of the Dockerfile and install.sh.                      ║
║                                                                              ║
║ That is deliberate. Commented-out Python still ships a working driver, and   ║
║ the whole point of the cutover is that the capability is absent, not merely  ║
║ unused. So the import, the cx_Oracle shim and the thick-mode initialiser are ║
║ removed rather than commented — they would raise ImportError at startup.     ║
║                                                                              ║
║ OracleConnector is kept as a loud stub so that any caller we missed fails    ║
║ with an explanatory error instead of an AttributeError three frames deep.    ║
║ The original implementation is in git history on the LAUMC branch.           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import os
import sys
import logging
import time
from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from sqlalchemy import text, BigInteger, ForeignKey, Numeric, Boolean, Integer, String, DateTime, Date, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

logger = logging.getLogger("db")


db = SQLAlchemy()

# ----------------------------------------------------------------
# 2. UTILITIES & ETL HELPERS
# ----------------------------------------------------------------

class OracleConnector:
    """
    Disabled on the HL7 distribution branch — see the banner at the top of this file.

    Kept as a loud stub rather than deleted so that any caller we missed fails with
    an explanatory message instead of an AttributeError three frames deep. There is
    no Oracle driver installed here to connect with.
    """

    _MESSAGE = (
        "Oracle access is disabled on the HL7 distribution branch. This install "
        "receives all clinical data as HL7 v2 over MLLP; python-oracledb is not "
        "installed and no PACS credentials are stored. If you reached this from a "
        "report or an admin screen, that code path still assumes the Oracle ETL and "
        "needs to be routed through the HL7 projector instead."
    )

    @staticmethod
    def get_connection(oracle_source=None, sysdba=False):
        raise RuntimeError(OracleConnector._MESSAGE)

def init_db(app):
    db.init_app(app)

def get_pg_engine():
    return db.engine

# get_etl_cutoff_date() is called on most report page loads to seed the default
# start date of the date picker. MIN() over an indexed column is an index scan, but
# there is no reason to pay for it on every request: the value only moves when the
# very oldest record we hold changes, which in practice means once, when the first
# message arrives.
_CUTOFF_CACHE = {"value": None, "at": 0.0}
_CUTOFF_TTL = 600  # seconds


def get_etl_cutoff_date(force_refresh=False):
    """
    Return the earliest date this install holds data for, or None if it holds none.

    HL7 BRANCH: there is no go-live date here. The Oracle ETL needed one because it
    had to be told how far back to pull from a PACS database containing years of
    history it should ignore; an HL7 feed has no history to pull, so the installer
    no longer asks for one and `go_live_config` is left empty.

    The honest replacement is the floor of the data we actually have. Deriving it
    from `etl_didb_studies.study_date` — rather than from the raw hl7_* tables —
    means the date picker opens on the earliest date a report can genuinely display,
    which is the question the caller is really asking. Before the projector has run
    this returns None and each report falls back to its own hardcoded default, the
    same as it always did when `go_live_config` was unset.

    Every report reaches the go-live date through this one helper, so none of their
    queries needed to change.
    """
    now = time.time()
    if not force_refresh and _CUTOFF_CACHE["value"] is not None \
            and (now - _CUTOFF_CACHE["at"]) < _CUTOFF_TTL:
        return _CUTOFF_CACHE["value"]

    try:
        result = db.session.execute(
            text("SELECT MIN(study_date) FROM etl_didb_studies")
        ).fetchone()
        value = result[0] if result else None
    except Exception:
        logger.exception("Could not derive the data-floor date from etl_didb_studies")
        return None

    # Only cache a real answer. Caching None would pin an empty install to its
    # hardcoded fallbacks for TTL seconds after the first data actually lands.
    if value is not None:
        _CUTOFF_CACHE["value"] = value
        _CUTOFF_CACHE["at"] = now
    return value


def get_go_live_date():
    return get_etl_cutoff_date()

def etl_analytics_refresh():
    """Trigger the analytics summary stored procedure. Called by APScheduler."""
    try:
        db.session.execute(text("SELECT refresh_analytics_summary();"))
        db.session.commit()
    except Exception:
        logger.exception("analytics refresh stored procedure failed")
        db.session.rollback()

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

class PermissionGroup(db.Model):
    """
    A named set of capabilities. Users inherit from their group;
    individual overrides on User.permission_overrides can grant or deny
    on top of the group default.
    """
    __tablename__ = 'permission_groups'
    id          = db.Column(Integer, primary_key=True)
    name        = db.Column(String(100), unique=True, nullable=False)
    description = db.Column(Text, default='')
    permissions = db.Column(JSONB, nullable=False, server_default='{}')
    created_at  = db.Column(DateTime, server_default=func.now())

    members = db.relationship('User', backref='group', lazy='select',
                              foreign_keys='User.group_id')


class User(db.Model, UserMixin):
    __tablename__ = 'users'
    id                   = db.Column(Integer, primary_key=True)
    username             = db.Column(String, unique=True, nullable=False)
    password_hash        = db.Column(String, nullable=False)
    role                 = db.Column(String)
    ui_theme             = db.Column(String, server_default='dark')
    favorites            = db.Column(Text, server_default='[]')
    # profile fields
    full_name            = db.Column(String(200))
    email                = db.Column(String(200))
    phone                = db.Column(String(50))
    department           = db.Column(String(100))
    notes                = db.Column(Text)
    # lifecycle fields
    status               = db.Column(String(20), server_default='active')   # active | pending | disabled
    created_by           = db.Column(Integer, ForeignKey('users.id'))
    created_at           = db.Column(DateTime, server_default=func.now())
    last_login           = db.Column(DateTime)
    must_change_password      = db.Column(Boolean, server_default='false')
    password_reset_requested  = db.Column(Boolean, server_default='false')
    # group-based permissions
    group_id             = db.Column(Integer, ForeignKey('permission_groups.id'), nullable=True)
    permission_overrides = db.Column(JSONB, server_default='{}')

    @property
    def is_active(self):
        return self.status == 'active'

    @property
    def display_name(self):
        return self.full_name or self.username

class UserAuditLog(db.Model):
    __tablename__ = 'user_audit_log'
    id              = db.Column(Integer, primary_key=True)
    actor_user_id   = db.Column(Integer, ForeignKey('users.id'))
    target_user_id  = db.Column(Integer, ForeignKey('users.id'))
    action          = db.Column(String(50), nullable=False)
    event_category  = db.Column(String(20))   # auth | user_mgmt | report | etl | ai | config
    resource_type   = db.Column(String(50))   # e.g. report_22, super_report, er_dashboard
    detail          = db.Column(JSONB)
    ip_address      = db.Column(String(45))
    created_at      = db.Column(DateTime, server_default=func.now())

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

class CDLog(db.Model):
    __tablename__ = 'cd_burn_log'
    id = db.Column(Integer, primary_key=True)
    event_type = db.Column(String(50), server_default='cd_burned')
    timestamp = db.Column(DateTime, nullable=False)
    burn_mode = db.Column(String(50))
    burn_location = db.Column(String(100))

    patient_id = db.Column(String(50))
    patient_name = db.Column(String(255))
    patient_dob = db.Column(Date)

    studies = db.Column(JSONB, server_default='[]')

    copies_count = db.Column(Integer, default=1)
    disc_format = db.Column(String(20))
    disc_size_mb = db.Column(Numeric(10,2))
    disc_label = db.Column(String(255))
    burn_duration_seconds = db.Column(Integer)

    status = db.Column(String(20), server_default='success')
    error_message = db.Column(Text)

    operator_id = db.Column(String(100))
    facility_code = db.Column(String(50))
    app_version = db.Column(String(20))

    orthanc_validated = db.Column(Boolean, server_default='false')
    orthanc_validation_result = db.Column(JSONB)
    orthanc_validated_at = db.Column(DateTime)

    created_at = db.Column(DateTime, server_default=func.now())
    updated_at = db.Column(DateTime, server_default=func.now())

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
    aetitle = db.Column(String(50), nullable=True)
    procedures = db.Column(JSONB, nullable=False, server_default='[]')
    third_party_approvals = db.Column(JSONB, nullable=False, server_default='[]')
    cancelled = db.Column(Boolean, nullable=False, server_default='false')
    cancelled_at = db.Column(DateTime, nullable=True)
    cancelled_by = db.Column(String(100), nullable=True)
    created_at = db.Column(DateTime, server_default=func.now())
    updated_at = db.Column(DateTime, server_default=func.now(), onupdate=func.now())

ALL_FEATURE_KEYS = [
    # HL7 branch core features
    'live_feed', 'hl7_orders', 'report_ai', 'oru', 'mapping',
    'financial', 'cd_print', 'referring_intel', 'custom_reports',
]

# HL7 BRANCH ROLES (replaces old: viewer, viewer2, tec, finance, secretary)
#
# SU (Super User)
#   - R&D level, zero restrictions, full access to everything
#
# Implementation
#   - ATH employees implementing the system
#   - Backend config: HL7→DB mappings, modalities, procedures, CSV imports
#
# Administrator
#   - Daily radiology manager / chief radiologist
#   - All reports, user management, referring contacts, system admin
#
# User
#   - Read-only reports (administrator assigns which reports)
#   - Limited to features admin grants
#
ROLE_PAGE_DEFAULTS = {
    'su':               set(ALL_FEATURE_KEYS),  # Full access
    'implementation':   {'mapping', 'live_feed', 'hl7_orders', 'custom_reports'},  # Backend config + data ingestion
    'administrator':    {'live_feed', 'hl7_orders', 'report_ai', 'oru', 'mapping', 'financial', 'referring_intel', 'custom_reports'},  # All reports + user mgmt
    'user':             {'hl7_orders', 'oru', 'report_ai'},  # Admin assigns which reports
}

def user_has_page(user, page_key):
    # SU has unrestricted access
    if user.role == 'su':
        return True
    # Admin-level access (backward compat: old 'admin' role becomes 'administrator')
    if user.role == 'administrator':
        return True
    # For other roles, check explicit permission or fall back to role default
    perm = UserPagePermission.query.filter_by(user_id=user.id, page_key=page_key).first()
    if perm is not None:
        return perm.is_enabled
    # No explicit record — fall back to the role's default so users approved
    # before this table existed (or before a new key was added) aren't locked out.
    return page_key in ROLE_PAGE_DEFAULTS.get(user.role, set())

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
    base_report_id = db.Column(Integer, ForeignKey('report_template.report_id'), nullable=True)
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
    procedure_name = db.Column(Text)
    duration_minutes = db.Column(Integer)
    clinical_rvu  = db.Column(Numeric(10,2), default=1.0)
    technical_rvu = db.Column(Numeric(10,2), default=1.0)
    modality = db.Column(String(20))

class PhysicianAliasMap(db.Model):
    __tablename__ = 'physician_alias_map'
    alias          = db.Column(db.Text, primary_key=True)
    canonical_name = db.Column(db.Text, nullable=False)
    dismissed      = db.Column(db.Boolean, nullable=False, default=False)
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)


class aetitle_modality_map(db.Model):
    __tablename__ = 'aetitle_modality_map'
    id = db.Column(Integer, primary_key=True)
    aetitle = db.Column(String, unique=True)
    modality = db.Column(String)
    room_name = db.Column(String(100))
    description = db.Column(db.Text)
    daily_capacity_minutes = db.Column(Integer, default=480)
    display_aetitle = db.Column(String(100))
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
AuditLog             = UserAuditLog
AETitleModalityMap   = aetitle_modality_map
ProcedureDurationMap = procedure_duration_map
DeviceException      = device_exceptions
DeviceWeeklySchedule = device_weekly_schedule
EtlDidbStudy         = etl_didb_studies
Patient              = etl_patient_view
SummaryStorageDaily  = summary_storage_daily
