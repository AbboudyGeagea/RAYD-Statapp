"""
ETL_JOBS/etl_pacs_audit.py — first opens from the PACS audit trail (Report 36 TAT, CHN).

Ported from LAUMC (same file there), keeping only the two kinds CHN needs. With every sync,
after the studies load, copies from AUDITING.AUDIT_LOG (Carestream's audit, PACS Oracle)
into etl_pacs_audit_events (migration 0075):
  open   images displayed ("Instances Used / Access", Vue PACS Client or Vue Motion)
  lock   the study locked for reading in the workstation (tags set LOCKED)
A row with no login is not kept. Report views ("Retrieve-Report") are not opens.

The audit is large, so every read goes through its EVENT_DATE index, one day at a time, with
the dates sent as text: a Python datetime binds as TIMESTAMP, and a DATE column compared with
a TIMESTAMP loses its index. The hint names LAUMC's index; Oracle ignores a hint whose index
does not exist. Only these columns are read: never the patient's name or ID, never the
description text (Oracle reads it only to find LOCKED).

Each run reads from the end of the last copy, less a day (a row committed late, a clock
change), up to now; rows already copied are left as they are. The first run starts at
settings pacs_audit_since, else the go-live date, and copies at most _MAX_DAYS_PER_RUN days,
so the backfill spreads over several syncs. Progress is saved after every day, so a run that
dies keeps what it copied.

Without read access to AUDITING.AUDIT_LOG the query fails, this step is recorded as failed
and the rest of the sync goes on; Report 36 then shows TAT as not measured.
"""
import logging
from datetime import date, datetime, time, timedelta

from sqlalchemy import text

from db import OracleConnector

SINCE_KEY = 'pacs_audit_since'
FROM_KEY = 'pacs_audit_copied_from'
UNTIL_KEY = 'pacs_audit_copied_until'

_OVERLAP = timedelta(days=1)
_MAX_DAYS_PER_RUN = 92
_FETCH_BATCH = 2000
_FMT = '%Y-%m-%d %H:%M:%S'

AUDIT_QUERY = """
    SELECT /*+ INDEX(a AUDIT_EVENT_DATE_INX) */
           a.LOG_DBID, a.EVENT_DATE,
           CASE WHEN a.ACTION = 'Access' THEN 'open' ELSE 'lock' END,
           LOWER(TRIM(a.USER_NAME)),
           a.SOURCE, a.LOCATION,
           TRIM(a.STUDY_INSTANCE_UID), TRIM(a.STUDY_ACCESSION_NO)
    FROM auditing.audit_log a
    WHERE a.EVENT_DATE >= TO_DATE(:since, 'YYYY-MM-DD HH24:MI:SS')
      AND a.EVENT_DATE <  TO_DATE(:until, 'YYYY-MM-DD HH24:MI:SS')
      AND a.CATEGORY = 'PHI Access'
      AND a.SUB_CATEGORY = 'Instances Used'
      AND a.STUDY_INSTANCE_UID IS NOT NULL
      AND a.USER_NAME IS NOT NULL
      AND (a.ACTION = 'Access'
           OR (a.ACTION = 'Modify' AND a.SOURCE = 'Vue PACS Client'
               AND INSTR(a.DESCRIPTION, 'Value: "LOCKED"') > 0))
"""

_INSERT_SQL = text("""
    INSERT INTO etl_pacs_audit_events (log_dbid, event_at, kind, user_name, source, location,
                                       study_instance_uid, accession_number)
    VALUES (:log_dbid, :event_at, :kind, :user_name, :source, :location,
            :study_instance_uid, :accession_number)
    ON CONFLICT (log_dbid) DO NOTHING
""")

_SET_SQL = text("""
    INSERT INTO settings (key, value) VALUES (:k, :v)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
""")


def _setting(conn, key):
    return conn.execute(text("SELECT value FROM settings WHERE key = :k"), {'k': key}).scalar()


def _as_date(value):
    try:
        return date.fromisoformat(str(value).strip()[:10]) if value else None
    except ValueError:
        return None


def _as_ts(value):
    try:
        return datetime.strptime(str(value).strip(), _FMT) if value else None
    except ValueError:
        return None


def coverage(conn):
    """(first moment, last moment) the copy holds without a gap, or (None, None)."""
    start, end = _as_date(_setting(conn, FROM_KEY)), _as_ts(_setting(conn, UNTIL_KEY))
    if start is None or end is None:
        return None, None
    return datetime.combine(start, time.min), end


def _plan(pg_engine, now, go_live):
    """(start, end, fresh): what this run reads. Fresh = the copy starts over at `since`."""
    with pg_engine.connect() as conn:
        since = _as_date(_setting(conn, SINCE_KEY)) or _as_date(go_live) or now.date()
        copied_from, copied_until = _as_date(_setting(conn, FROM_KEY)), _as_ts(_setting(conn, UNTIL_KEY))
    fresh = copied_from is None or copied_until is None or since < copied_from
    start = datetime.combine(since, time.min) if fresh else copied_until - _OVERLAP
    end = min(now, start + timedelta(days=_MAX_DAYS_PER_RUN))
    return start, end, fresh


def _row(r):
    log_dbid, event_at, kind, user_name, source, location, uid, accession = r
    return {'log_dbid': int(log_dbid), 'event_at': event_at, 'kind': kind, 'user_name': user_name,
            'source': source, 'location': location, 'study_instance_uid': uid,
            'accession_number': accession}


def run_pacs_audit_etl(pg_engine, oracle_source, go_live=None, now=None):
    """Copies the opens and locks not yet copied. Returns the rows read from PACS."""
    job_name = "PACS_AUDIT_OPENS_ETL"
    now = (now or datetime.now()).replace(microsecond=0)
    start_time = datetime.now()
    total, status, error_msg, log_id = 0, "RUNNING", None, None
    try:
        with pg_engine.begin() as conn:
            log_id = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}).scalar()
    except Exception as e:
        logging.error(f"PACS audit ETL log error: {e}")

    start, end, fresh = _plan(pg_engine, now, go_live)
    ora_conn = cursor = None
    try:
        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()
        day = start
        while day < end:
            until = min(day + timedelta(days=1), end)
            cursor.execute(AUDIT_QUERY, {'since': day.strftime(_FMT), 'until': until.strftime(_FMT)})
            rows = []
            while True:
                batch = cursor.fetchmany(_FETCH_BATCH)
                if not batch:
                    break
                rows.extend(_row(r) for r in batch if r[3])     # no login: not kept
            with pg_engine.begin() as conn:
                if rows:
                    conn.execute(_INSERT_SQL, rows)
                if fresh:
                    conn.execute(_SET_SQL, {'k': FROM_KEY, 'v': start.date().isoformat()})
                    fresh = False
                conn.execute(_SET_SQL, {'k': UNTIL_KEY, 'v': until.strftime(_FMT)})
            total += len(rows)
            day = until
        status = "SUCCESS"
        logging.info(f"PACS audit opens ETL complete: {total:,} rows, {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M}")
    except Exception as e:
        status, error_msg = "FAILED", str(e)
        logging.error(f"PACS audit opens ETL error: {error_msg}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if ora_conn is not None:
            ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                with pg_engine.begin() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, records_processed=:r, "
                             "duration_seconds=:d, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round((end_time - start_time).total_seconds(), 2), "e": error_msg, "id": log_id})
            except Exception as le:
                logging.error(f"Failed to update PACS audit ETL log: {le}")
    return total
