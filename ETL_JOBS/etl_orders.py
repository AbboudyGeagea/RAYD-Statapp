import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

# Earliest and latest safe dates Oracle can handle reliably
_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)

def _safe_date(val):
    """
    Sanitize a date/datetime value coming from Oracle.
    Returns None for anything corrupt, zero-year, or out of range.
    """
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
    """Strip and truncate strings, return None for empty."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s

def _clean_row(row):
    """
    Sanitize a single Oracle row before upsert.
    Columns by index (matches col_names order):
      0  order_dbid
      1  patient_dbid
      2  study_db_uid
      3  visit_dbid
      4  study_instance_uid
      5  proc_id
      6  proc_text
      7  scheduled_datetime   ← corrupt dates live here
      8  order_status
      9  modality
      10 has_study
      11 order_control
      12 last_update
    """
    return (
        row[0],                        # order_dbid        — bigint, must not be None
        _safe_str(row[1]),             # patient_dbid
        _safe_str(row[2]),             # study_db_uid
        _safe_str(row[3]),             # visit_dbid
        _safe_str(row[4]),             # study_instance_uid
        _safe_str(row[5]),             # proc_id
        _safe_str(row[6], 4000),       # proc_text         — truncate to be safe
        _safe_date(row[7]),            # scheduled_datetime ← sanitized
        _safe_str(row[8]),             # order_status
        _safe_str(row[9]),             # modality
        row[10],                       # has_study         — already 'true'/'false'
        _safe_str(row[11]),            # order_control
        _safe_date(row[12]),           # last_update
    )

def run_orders_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    job_name  = "ORDERS_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    col_names = [
        'order_dbid', 'patient_dbid', 'study_db_uid', 'visit_dbid',
        'study_instance_uid', 'proc_id', 'proc_text', 'scheduled_datetime',
        'order_status', 'modality', 'has_study', 'order_control',
        'last_update'
    ]

    # ── Log start ────────────────────────────────────────────────────────────
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"Orders ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    # IMPORTANT: Do NOT filter on SCHEDULED_DATETIME in Oracle at all.
    # EXTRACT() and TO_DATE() both raise ORA-01841 on rows with corrupt
    # zero-year dates — the error fires before Oracle can skip the row.
    # We pull all orders using a safe WHERE on ORDER_DBID (the PK),
    # then apply the go_live_date filter in Python after _safe_date() runs.
    query = """
        SELECT
            ORDER_DBID,
            PATIENT_DBID,
            STUDY_DB_UID,
            VISIT_DBID,
            STUDY_INSTANCE_UID,
            PROC_ID,
            PROC_TEXT,
            SCHEDULED_DATETIME,
            ORDER_STATUS,
            MODALITY,
            CASE WHEN HAS_STUDY = 'Y' THEN 'true' ELSE 'false' END AS has_study,
            ORDER_CONTROL,
            CURRENT_TIMESTAMP
        FROM MEDILINK.MDB_ORDERS
        WHERE ORDER_DBID IS NOT NULL
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        go_live_cutoff = datetime.strptime(gd_str, '%Y-%m-%d').date()
        logging.info(f"Orders ETL starting — cutoff: {gd_str}")
        print(f"[Orders ETL] 🚀 Starting — cutoff: {gd_str}")

        cursor.execute(query)

        batch_num = 0
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break

            batch_num += 1

            # Sanitize every row before touching Postgres
            # Also apply go_live_date filter here since we skip it in Oracle
            clean_batch = []
            for row in batch:
                if row[0] is None:          # order_dbid is PK, must exist
                    skipped += 1
                    continue
                cleaned = _clean_row(row)
                sched = cleaned[7]          # scheduled_datetime after sanitization
                if sched is not None and sched.date() < go_live_cutoff:
                    skipped += 1
                    continue
                clean_batch.append(cleaned)

            if clean_batch:
                chunked_upsert_func(pg_engine, pg_table, col_names, clean_batch, 'order_dbid')
                total += len(clean_batch)

            print(f"[Orders ETL] 📦 Batch {batch_num} — {total:,} rows inserted, {skipped} skipped")

        status = "SUCCESS"
        print(f"[Orders ETL] ✅ Done — {total:,} rows | {skipped} skipped (corrupt/null dates)")
        logging.info(f"Orders ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"Orders ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()

        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rps      = round(total / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, null_alerts=:na, "
                             "error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "rps": rps,
                         "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update orders log: {le}")

    return total
