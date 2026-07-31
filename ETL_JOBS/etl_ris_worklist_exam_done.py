"""
ETL_JOBS/etl_ris_worklist_exam_done.py — RIS WORKLIST_STATUS_HISTORY (STATUS_KEY=100,
"Exam Done") ⋈ SITE_WORKLIST -> std_worklist_exam_done (LAUMC).

See migration 0090 for the target table and full design notes. Same table/pattern as
ETL_JOBS/etl_ris_worklist_arrivals.py (status_key=60, "Arrived") -- WORKLIST_STATUS_HISTORY
confirmed usable 2026-07-31 once the WORKLIST_STATUS_HISTORY_KEY NULL issue was worked
around there (watermark on MAX(timestamp), upsert on the (site_worklist_key, timestamp)
pair, not a source-side key). status_key=100 = "Exam Done" per the STATUS_KEY lifecycle
decoded against std_status_ris (see project memory / docs/LAUMC_DATA_REQUEST.md).

Gives a real RIS "exam done" timestamp per worklist entry -- built for the PACS-vs-RIS
TAT anchor comparison (report_25's new tab): etl_didb_studies.insert_time is a PACS-side
ingestion proxy, this is the RIS's own status transition instead.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

_STATUS_HISTORY_TABLE = "WORKLIST_STATUS_HISTORY"
_WORKLIST_TABLE        = "SITE_WORKLIST"
_EXAM_DONE_STATUS_KEY  = 100  # "Exam Done" -- see std_status_ris / decoded STATUS_KEY lifecycle

_FETCH_BATCH = 2000


def _safe_date(val):
    if val is None:
        return None
    try:
        return val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


_UPSERT_SQL = text("""
    INSERT INTO std_worklist_exam_done (
        site_worklist_key, pps_key, exam_done_at, last_update
    ) VALUES (
        :site_worklist_key, :pps_key, :exam_done_at, :last_update
    )
    ON CONFLICT (site_worklist_key, exam_done_at) DO UPDATE SET
        pps_key      = EXCLUDED.pps_key,
        last_update  = EXCLUDED.last_update
""")


def run_ris_worklist_exam_done_etl(pg_engine, oracle_source):
    job_name   = "RIS_WORKLIST_EXAM_DONE_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

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
        logging.error(f"RIS Worklist Exam Done ETL log error: {e}")

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text(
                "SELECT MAX(exam_done_at) FROM std_worklist_exam_done"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Worklist Exam Done ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    is_fresh_load = watermark is None
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    base_query = f"""
        SELECT wsh.SITE_WORKLIST_KEY, sw.PPS_KEY, wsh.STATUS_TIME
        FROM {_STATUS_HISTORY_TABLE} wsh
        JOIN {_WORKLIST_TABLE} sw ON sw.SITE_WORKLIST_KEY = wsh.SITE_WORKLIST_KEY
        WHERE wsh.STATUS_KEY = :exam_done_key
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        params = {"exam_done_key": _EXAM_DONE_STATUS_KEY}
        if is_fresh_load:
            logging.info("RIS Worklist Exam Done ETL starting — fresh load")
            print(f"[RIS Worklist Exam Done ETL] 🚀 Starting ({_STATUS_HISTORY_TABLE} ⋈ {_WORKLIST_TABLE}), fresh load")
            cursor.execute(base_query, params)
        else:
            logging.info(f"RIS Worklist Exam Done ETL starting — incremental, watermark={watermark}, lookback={lookback_date}")
            print(f"[RIS Worklist Exam Done ETL] 🚀 Starting — incremental, watermark={watermark}, lookback={lookback_date}")
            params["watermark"] = watermark
            params["lb"] = lookback_date
            cursor.execute(
                base_query + " AND (wsh.STATUS_TIME > :watermark OR wsh.STATUS_TIME >= TO_DATE(:lb, 'YYYY-MM-DD'))",
                params
            )

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            rows = []
            for (site_worklist_key, pps_key, status_time) in batch:
                exam_done_at = _safe_date(status_time)
                if site_worklist_key is None or exam_done_at is None:
                    skipped += 1
                    continue
                rows.append({
                    "site_worklist_key": site_worklist_key,
                    "pps_key": pps_key,
                    "exam_done_at": exam_done_at,
                    "last_update": datetime.now(),
                })
            if rows:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, rows)
                total += len(rows)

        print(f"[RIS Worklist Exam Done ETL] ✅ {total:,} exam-done events upserted, {skipped} skipped (no key)")
        status = "SUCCESS"
        logging.info(f"RIS Worklist Exam Done ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Worklist Exam Done ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "null_alerts=:na, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Worklist Exam Done log: {le}")

    return total
