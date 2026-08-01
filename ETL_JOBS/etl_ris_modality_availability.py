"""
ETL_JOBS/etl_ris_modality_availability.py — RIS MODALITY_AVAIL_EXCEPTION ->
std_modality_exceptions, RIS SCHEDULE_TEMPLATE_ITEM -> std_schedule_template_items
(LAUMC).

See migration 0067 for target schemas and design notes. Both are RIS-authoritative
counterparts to RAYD's existing manually-editable device_exceptions /
device_weekly_schedule — built as new, separate, NOT-editable-from-RAYD tables (no admin
route is built for either), not merged into the existing ones.

std_modality_exceptions resolves modality_key to a real aetitle via a live MODALITY
join, ready to use as-is. std_schedule_template_items was captured raw/faithful but NOT
attributable to a specific device — until run_schedule_template_device_link below
(migration 0107, confirmed 2026-08-01 against a real SCHEDULE_TEMPLATE_ITEM sample): the
chain SCHEDULE_TEMPLATE_ITEM.schedule_template_version_key -> std_schedule_template_versions
(default_version only) -> schedule_template_key -> aetitle_modality_map.ris_schedule_template_key
now resolves it. Pure Postgres, no Oracle needed — must run AFTER this module's own
run_ris_schedule_template_items_etl (populates the item rows) AND
ETL_JOBS/etl_ris_modality_schedule.py's run_ris_schedule_template_version_etl (populates
the version table) in the same pass. Called from Phase 18 in etl_runner.py.

Full pull, no date filter — reference/scheduling data, not high volume.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_MODALITY_EXCEPTION_TABLE = os.getenv("RAYD_RIS_MODALITY_EXCEPTION_TABLE", "MODALITY_AVAILABLE_EXCEPTION")
_SCHEDULE_TEMPLATE_ITEM_TABLE = os.getenv("RAYD_RIS_SCHEDULE_TEMPLATE_ITEM_TABLE", "SCHEDULE_TEMPLATE_ITEM")
_MODALITY_TABLE = os.getenv("RAYD_RIS_MODALITY_TABLE", "MODALITY")
_SCHEDULE_SCHEME_TABLE = os.getenv("RAYD_RIS_SCHEDULE_SCHEME_TABLE", "SCHEDULE_SCHEME")
_AVAILABILITY_INDICATOR_TABLE = os.getenv("RAYD_RIS_AVAILABILITY_INDICATOR_TABLE", "AVAILABILITY_INDICATOR")
_FETCH_BATCH = 2000

_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)


def _safe_date(val):
    if val is None:
        return None
    try:
        dt = val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
        if dt.year == 0 or not (_SAFE_DATE_MIN <= dt <= _SAFE_DATE_MAX):
            return None
        return dt
    except Exception:
        return None


def _safe_str(val, max_len=None):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _safe_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_bool(val):
    if val is None:
        return None
    return str(val).strip().upper() in ('Y', 'YES', 'TRUE', '1')


def _time_of_day(val):
    """Extract just the time-of-day portion from an Oracle DATE/TIMESTAMP value —
    FROM_TIME/TO_TIME store full timestamps but the date portion is an arbitrary/anchor
    date (confirmed against a real SCHEDULE_TEMPLATE_ITEM export, 2026-08-01: rows for the
    same recurring weekly slot carry different anchor dates); only HH:MM:SS matters for a
    recurring weekly schedule."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, str):
        s = val.strip()
        for fmt in ('%Y-%m-%d %H:%M:%S', '%H:%M:%S'):
            try:
                return datetime.strptime(s, fmt).time()
            except ValueError:
                continue
    return None


def _log_job(pg_engine, job_name):
    start_time = datetime.now()
    log_id = None
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, 'RUNNING', :t, 0) RETURNING id"),
                {"n": job_name, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"{job_name} log error: {e}")
    return log_id, start_time


def _close_job(pg_engine, log_id, start_time, status, total, error_msg=None, skipped=0):
    if not log_id:
        return
    try:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        with pg_engine.connect() as conn:
            conn.execute(
                text("UPDATE etl_job_log SET status=:s, end_time=:et, records_processed=:r, "
                     "duration_seconds=:d, null_alerts=:na, error_message=:e WHERE id=:id"),
                {"s": status, "et": end_time, "r": total, "d": round(duration, 2),
                 "na": skipped, "e": error_msg, "id": log_id}
            )
            conn.commit()
    except Exception as le:
        logging.error(f"Failed to close job log {log_id}: {le}")


_UPSERT_EXCEPTION_SQL = text("""
    INSERT INTO std_modality_exceptions (
        modality_avail_exception_key, from_date, to_date, reason,
        exception_created_person_key, exception_created_date, modality_key, aetitle,
        availability_indicator_key, priority, source_last_updated, last_update
    ) VALUES (
        :modality_avail_exception_key, :from_date, :to_date, :reason,
        :exception_created_person_key, :exception_created_date, :modality_key, :aetitle,
        :availability_indicator_key, :priority, :source_last_updated, :last_update
    )
    ON CONFLICT (modality_avail_exception_key) DO UPDATE SET
        from_date = EXCLUDED.from_date, to_date = EXCLUDED.to_date, reason = EXCLUDED.reason,
        exception_created_person_key = EXCLUDED.exception_created_person_key,
        exception_created_date = EXCLUDED.exception_created_date,
        modality_key = EXCLUDED.modality_key, aetitle = EXCLUDED.aetitle,
        availability_indicator_key = EXCLUDED.availability_indicator_key,
        priority = EXCLUDED.priority, source_last_updated = EXCLUDED.source_last_updated,
        last_update = EXCLUDED.last_update
""")


def run_ris_modality_exceptions_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_MODALITY_EXCEPTIONS_ETL")
    total, skipped, error_msg, status = 0, 0, None, "SUCCESS"
    seen_keys = []

    query = f"""
        SELECT
            e.MODALITY_AVAIL_EXCEPTION_KEY, e.FROM_DATE, e.TO_DATE, e.REASON,
            e.EXCEPTION_CREATED_PERSON_KEY, e.EXCEPTION_CREATED_DATE, e.MODALITY_KEY,
            m.AE_TITLE, e.AVAILABILITY_INDICATOR_KEY, e.PRIORITY, e.LAST_UPDATED
        FROM {_MODALITY_EXCEPTION_TABLE} e
        LEFT JOIN {_MODALITY_TABLE} m ON m.MODALITY_KEY = e.MODALITY_KEY
    """
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Modality Exceptions ETL] 🚀 Starting ({_MODALITY_EXCEPTION_TABLE} ⋈ {_MODALITY_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for (key, from_date, to_date, reason, created_by, created_date,
                 modality_key, ae_title, avail_key, priority, last_updated) in batch:
                if key is None:
                    skipped += 1
                    continue
                seen_keys.append(key)
                params.append({
                    "modality_avail_exception_key": key, "from_date": _safe_date(from_date),
                    "to_date": _safe_date(to_date), "reason": _safe_str(reason),
                    "exception_created_person_key": created_by,
                    "exception_created_date": _safe_date(created_date),
                    "modality_key": modality_key, "aetitle": _safe_str(ae_title),
                    "availability_indicator_key": avail_key, "priority": _safe_str(priority),
                    "source_last_updated": _safe_date(last_updated), "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_EXCEPTION_SQL, params)
                total += len(params)

        # Full-refresh sync: exceptions get resolved/removed in the RIS, not just added —
        # a row Oracle no longer returns must not linger in Postgres forever making a
        # device look unavailable after the exception has actually cleared. Safe because
        # this is a full pull, no date filter. Skipped when Oracle returned zero rows at
        # all (more likely a transient/connectivity issue than a genuinely empty table).
        if seen_keys:
            with pg_engine.begin() as conn:
                conn.execute(text(
                    "DELETE FROM std_modality_exceptions WHERE modality_avail_exception_key != ALL(:keys)"
                ), {"keys": seen_keys})

        print(f"[RIS Modality Exceptions ETL] ✅ Done — {total:,} exceptions upserted, {skipped} skipped")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Modality Exceptions ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg, skipped)
    return total


_UPSERT_SCHEDULE_ITEM_SQL = text("""
    INSERT INTO std_schedule_template_items (
        schedule_template_item_key, day_of_week, from_time, to_time,
        from_time_of_day, to_time_of_day,
        availability_indicator_key, schedule_scheme_key, schedule_template_version_key,
        source_last_updated, last_update
    ) VALUES (
        :schedule_template_item_key, :day_of_week, :from_time, :to_time,
        :from_time_of_day, :to_time_of_day,
        :availability_indicator_key, :schedule_scheme_key, :schedule_template_version_key,
        :source_last_updated, :last_update
    )
    ON CONFLICT (schedule_template_item_key) DO UPDATE SET
        day_of_week = EXCLUDED.day_of_week, from_time = EXCLUDED.from_time,
        to_time = EXCLUDED.to_time,
        from_time_of_day = EXCLUDED.from_time_of_day, to_time_of_day = EXCLUDED.to_time_of_day,
        availability_indicator_key = EXCLUDED.availability_indicator_key,
        schedule_scheme_key = EXCLUDED.schedule_scheme_key,
        schedule_template_version_key = EXCLUDED.schedule_template_version_key,
        source_last_updated = EXCLUDED.source_last_updated, last_update = EXCLUDED.last_update
""")


def run_ris_schedule_template_items_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_SCHEDULE_TEMPLATE_ITEMS_ETL")
    total, skipped, error_msg, status = 0, 0, None, "SUCCESS"
    seen_keys = []

    query = f"""
        SELECT SCHEDULE_TEMPLATE_ITEM_KEY, DAY_OF_WEEK, FROM_TIME, TO_TIME,
               AVAILABILITY_INDICATOR_KEY, SCHEDULE_SCHEME_KEY, SCHEDULE_TEMPLATE_VERSION_KEY,
               LAST_UPDATED
        FROM {_SCHEDULE_TEMPLATE_ITEM_TABLE}
    """
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Schedule Template Items ETL] 🚀 Starting ({_SCHEDULE_TEMPLATE_ITEM_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for (key, dow, from_time, to_time, avail_key, scheme_key,
                 version_key, last_updated) in batch:
                if key is None:
                    skipped += 1
                    continue
                seen_keys.append(key)
                params.append({
                    "schedule_template_item_key": key, "day_of_week": _safe_int(dow),
                    "from_time": _safe_str(from_time), "to_time": _safe_str(to_time),
                    "from_time_of_day": _time_of_day(from_time), "to_time_of_day": _time_of_day(to_time),
                    "availability_indicator_key": avail_key, "schedule_scheme_key": scheme_key,
                    "schedule_template_version_key": version_key,
                    "source_last_updated": _safe_date(last_updated), "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SCHEDULE_ITEM_SQL, params)
                total += len(params)

        # Full-refresh sync: schedules "change almost weekly" (operator instruction,
        # 2026-08-01) — a row Oracle no longer returns must not linger in Postgres forever
        # counting toward availability. Safe because this is a full pull, no date filter.
        # Skipped when Oracle returned zero rows at all (more likely a transient/
        # connectivity issue than a genuinely empty table).
        if seen_keys:
            with pg_engine.begin() as conn:
                conn.execute(text(
                    "DELETE FROM std_schedule_template_items WHERE schedule_template_item_key != ALL(:keys)"
                ), {"keys": seen_keys})

        print(f"[RIS Schedule Template Items ETL] ✅ Done — {total:,} items upserted, {skipped} skipped")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Schedule Template Items ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg, skipped)
    return total


_UPSERT_SCHEME_SQL = text("""
    INSERT INTO std_schedule_schemes (
        schedule_scheme_key, code, description, default_flag, active,
        source_last_updated, last_update
    ) VALUES (
        :schedule_scheme_key, :code, :description, :default_flag, :active,
        :source_last_updated, :last_update
    )
    ON CONFLICT (schedule_scheme_key) DO UPDATE SET
        code = EXCLUDED.code, description = EXCLUDED.description,
        default_flag = EXCLUDED.default_flag, active = EXCLUDED.active,
        source_last_updated = EXCLUDED.source_last_updated, last_update = EXCLUDED.last_update
""")


def run_ris_schedule_schemes_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_SCHEDULE_SCHEMES_ETL")
    total, skipped, error_msg, status = 0, 0, None, "SUCCESS"

    query = f"SELECT SCHEDULE_SCHEME_KEY, CODE, DESCRIPTION, DEFAULT_FLAG, ACTIVE, LAST_UPDATED FROM {_SCHEDULE_SCHEME_TABLE}"
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Schedule Schemes ETL] 🚀 Starting ({_SCHEDULE_SCHEME_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for key, code, desc, default_flag, active, last_upd in batch:
                if key is None:
                    skipped += 1
                    continue
                params.append({
                    "schedule_scheme_key": key, "code": _safe_str(code), "description": _safe_str(desc),
                    "default_flag": _safe_bool(default_flag), "active": _safe_bool(active),
                    "source_last_updated": _safe_date(last_upd), "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SCHEME_SQL, params)
                total += len(params)
        print(f"[RIS Schedule Schemes ETL] ✅ Done — {total:,} schemes upserted")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Schedule Schemes ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg, skipped)
    return total


_UPSERT_AVAIL_INDICATOR_SQL = text("""
    INSERT INTO std_availability_indicators (
        availability_indicator_key, code, description, color, alternate_color,
        allow_days_in_advance, allow_n_next_days, default_search, source_last_updated,
        last_update
    ) VALUES (
        :availability_indicator_key, :code, :description, :color, :alternate_color,
        :allow_days_in_advance, :allow_n_next_days, :default_search, :source_last_updated,
        :last_update
    )
    ON CONFLICT (availability_indicator_key) DO UPDATE SET
        code = EXCLUDED.code, description = EXCLUDED.description, color = EXCLUDED.color,
        alternate_color = EXCLUDED.alternate_color,
        allow_days_in_advance = EXCLUDED.allow_days_in_advance,
        allow_n_next_days = EXCLUDED.allow_n_next_days, default_search = EXCLUDED.default_search,
        source_last_updated = EXCLUDED.source_last_updated, last_update = EXCLUDED.last_update
""")


def run_ris_availability_indicators_etl(pg_engine, oracle_source):
    log_id, start_time = _log_job(pg_engine, "RIS_AVAILABILITY_INDICATORS_ETL")
    total, skipped, error_msg, status = 0, 0, None, "SUCCESS"

    query = f"""
        SELECT AVAILABILITY_INDICATOR_KEY, CODE, DESCRIPTION, COLOR, ALTERNATE_COLOR,
               ALLOW_DAYS_IN_ADVANCE, ALLOW_N_NEXT_DAYS, DEFAULT_SEARCH, LAST_UPDATED
        FROM {_AVAILABILITY_INDICATOR_TABLE}
    """
    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor = ora_conn.cursor()
    try:
        print(f"[RIS Availability Indicators ETL] 🚀 Starting ({_AVAILABILITY_INDICATOR_TABLE})")
        cursor.execute(query)
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for (key, code, desc, color, alt_color, days_advance, n_next_days,
                 default_search, last_upd) in batch:
                if key is None:
                    skipped += 1
                    continue
                params.append({
                    "availability_indicator_key": key, "code": _safe_str(code),
                    "description": _safe_str(desc), "color": _safe_str(color),
                    "alternate_color": _safe_str(alt_color),
                    "allow_days_in_advance": _safe_int(days_advance),
                    "allow_n_next_days": _safe_int(n_next_days),
                    "default_search": _safe_bool(default_search),
                    "source_last_updated": _safe_date(last_upd), "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_AVAIL_INDICATOR_SQL, params)
                total += len(params)
        print(f"[RIS Availability Indicators ETL] ✅ Done — {total:,} indicators upserted")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"RIS Availability Indicators ETL error: {error_msg}")
        raise
    finally:
        cursor.close()
        ora_conn.close()
        _close_job(pg_engine, log_id, start_time, status, total, error_msg, skipped)
    return total


# ── Device attribution for std_schedule_template_items (Phase 18) ────────────────────────
# Pure Postgres — no Oracle connection needed, everything it reads is already imported.

_LINK_SCHEDULE_ITEM_DEVICE_SQL = text("""
    UPDATE std_schedule_template_items sti
    SET aetitle = am.aetitle
    FROM std_schedule_template_versions stv
    JOIN aetitle_modality_map am ON am.ris_schedule_template_key = stv.schedule_template_key
    WHERE sti.schedule_template_version_key = stv.schedule_template_version_key
      AND stv.default_version = TRUE
      AND am.aetitle IS NOT NULL
""")


def run_schedule_template_device_link(pg_engine):
    """
    Resolves std_schedule_template_items.aetitle via schedule_template_version_key ->
    std_schedule_template_versions (default_version only) -> schedule_template_key ->
    aetitle_modality_map.ris_schedule_template_key. Unconditional overwrite — this table is
    a pure RIS mirror, never manually edited (migration 0067), so there's no manual value to
    protect and no ambiguity to flag: aetitle_modality_map.ris_schedule_template_key is only
    ever set when unambiguous (etl_ris_modality_schedule.py), so at most one device can match
    per schedule_template_key here.

    Must run after run_ris_schedule_template_items_etl (this module), and after
    ETL_JOBS/etl_ris_modality_schedule.py's run_ris_schedule_template_version_etl and
    run_ris_schedule_template_etl — enforced by call order in etl_runner.py's Phase 18, not
    by this function.
    """
    log_id, start_time = _log_job(pg_engine, "SCHEDULE_TEMPLATE_DEVICE_LINK")
    linked, error_msg, status = 0, None, "SUCCESS"

    try:
        print("[Schedule Template Device Link] 🚀 Starting")
        with pg_engine.begin() as conn:
            r = conn.execute(_LINK_SCHEDULE_ITEM_DEVICE_SQL)
            linked = r.rowcount
        print(f"[Schedule Template Device Link] ✅ Done — {linked:,} schedule-item rows linked to a device")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"Schedule Template Device Link error: {error_msg}")
        raise
    finally:
        _close_job(pg_engine, log_id, start_time, status, linked, error_msg)
    return linked


# ── Simple weekly Available/Unavailable minutes per device (Phase 18) ────────────────────
# Pure Postgres — everything it reads (std_schedule_template_items, now resolved to a
# device) is already imported by the two steps above in the same phase.

_TRUNCATE_WEEKLY_AVAILABILITY_SQL = text("TRUNCATE std_device_weekly_availability")

# Interval sweep: std_schedule_template_items carries overlapping/superseded rows for the
# same device/day (confirmed against a real RH-CT64 export, 2026-08-01 — an old single
# block sits alongside a newer split covering the same hours). This resolves it properly
# instead of naively summing every "Available" row (which would double-count):
#   1. breakpoints  — every distinct from/to time-of-day boundary for a device/day
#   2. intervals    — consecutive breakpoints paired into micro-intervals
#   3. winning      — per micro-interval, the row with the MOST RECENT source_last_updated
#                     that fully covers it wins (ties broken by item key, higher = newer)
#   4. sum the winning micro-intervals whose availability_indicator_key = 1 ("Available",
#      confirmed against the real AVAILABILITY_INDICATOR export) — everything else
#      (Unavailable/Closed/Reserved-for-IP/...) contributes 0, per operator instruction to
#      keep this a plain Available-vs-everything-else binary.
# RIS day_of_week (0=Sun..6=Sat, confirmed) is converted to RAYD's convention
# (0=Mon..6=Sun, matches device_weekly_schedule) via (day_of_week + 6) % 7.
# IMPORTANT: GROUP BY (not a WHERE filter before it) — a device/day that's fully closed
# still gets an explicit row with available_minutes=0, so a downstream LEFT JOIN can tell
# "this device is closed today" apart from "no schedule data exists for this device at
# all" (which should fall back to the old estimate chain instead of reading as 0).
_COMPUTE_WEEKLY_AVAILABILITY_SQL = text("""
    WITH resolved_items AS (
        SELECT aetitle, day_of_week, from_time_of_day, to_time_of_day,
               availability_indicator_key, source_last_updated, schedule_template_item_key
        FROM std_schedule_template_items
        WHERE aetitle IS NOT NULL
          AND from_time_of_day IS NOT NULL AND to_time_of_day IS NOT NULL
          AND to_time_of_day > from_time_of_day
    ),
    breakpoints AS (
        SELECT aetitle, day_of_week, from_time_of_day AS t FROM resolved_items
        UNION
        SELECT aetitle, day_of_week, to_time_of_day AS t FROM resolved_items
    ),
    intervals AS (
        SELECT aetitle, day_of_week, t AS t_start,
               LEAD(t) OVER (PARTITION BY aetitle, day_of_week ORDER BY t) AS t_end
        FROM breakpoints
    ),
    intervals_clean AS (
        SELECT * FROM intervals WHERE t_end IS NOT NULL AND t_end > t_start
    ),
    winning AS (
        SELECT ic.aetitle, ic.day_of_week, ic.t_start, ic.t_end, w.availability_indicator_key
        FROM intervals_clean ic
        CROSS JOIN LATERAL (
            SELECT ri.availability_indicator_key
            FROM resolved_items ri
            WHERE ri.aetitle = ic.aetitle
              AND ri.day_of_week = ic.day_of_week
              AND ri.from_time_of_day <= ic.t_start
              AND ri.to_time_of_day >= ic.t_end
            ORDER BY ri.source_last_updated DESC NULLS LAST, ri.schedule_template_item_key DESC
            LIMIT 1
        ) w
    )
    INSERT INTO std_device_weekly_availability (aetitle, day_of_week, available_minutes, last_update)
    SELECT aetitle,
           (day_of_week + 6) % 7 AS rayd_day_of_week,
           SUM(CASE WHEN availability_indicator_key = 1
                    THEN EXTRACT(EPOCH FROM (t_end - t_start)) / 60
                    ELSE 0 END)::INT AS available_minutes,
           NOW()
    FROM winning
    GROUP BY aetitle, day_of_week
""")


def run_device_weekly_availability_etl(pg_engine):
    """
    Computes std_device_weekly_availability — simple Available-minutes-per-weekday per
    device (RAYD day-of-week convention) — from std_schedule_template_items, resolving
    overlapping/superseded rows via an interval sweep keyed on source_last_updated (most
    recent write wins per overlapping time slot).

    Full TRUNCATE + rebuild every pass, not upsert — operator instruction (2026-08-01):
    availability "changes almost weekly," this must never carry a stale row forward.

    Pure Postgres, no Oracle connection. Must run after run_ris_schedule_template_items_etl
    and run_schedule_template_device_link (both this module, same phase) — enforced by call
    order in etl_runner.py's Phase 18, not by this function.
    """
    log_id, start_time = _log_job(pg_engine, "DEVICE_WEEKLY_AVAILABILITY_ETL")
    total, error_msg, status = 0, None, "SUCCESS"

    try:
        print("[Device Weekly Availability ETL] 🚀 Starting")
        with pg_engine.begin() as conn:
            conn.execute(_TRUNCATE_WEEKLY_AVAILABILITY_SQL)
            r = conn.execute(_COMPUTE_WEEKLY_AVAILABILITY_SQL)
            total = r.rowcount
        print(f"[Device Weekly Availability ETL] ✅ Done — {total:,} device/day rows computed")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"Device Weekly Availability ETL error: {error_msg}")
        raise
    finally:
        _close_job(pg_engine, log_id, start_time, status, total, error_msg)
    return total
