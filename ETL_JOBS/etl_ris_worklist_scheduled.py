"""
ETL_JOBS/etl_ris_worklist_scheduled.py — RIS WORKLIST_STATUS_HISTORY (STATUS_KEY=40,
"Scheduled") ⋈ SITE_WORKLIST -> std_worklist_scheduled (LAUMC).

See migration 0092 for the target table and full design notes. Same table/pattern as
ETL_JOBS/etl_ris_worklist_arrivals.py (status_key=60) and
ETL_JOBS/etl_ris_worklist_exam_done.py (status_key=100) -- third status transition off
the same WORKLIST_STATUS_HISTORY audit log. status_key=40 = "Scheduled" per the
STATUS_KEY lifecycle decoded against std_status_ris.

Feeds the redefined "Patient Wait Time" (Scheduled -> Arrived, operator instruction
2026-07-31) -- report_25's new/renamed TAT-pipeline tab.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

_STATUS_HISTORY_TABLE = "WORKLIST_STATUS_HISTORY"
_WORKLIST_TABLE        = "SITE_WORKLIST"
_SCHEDULED_STATUS_KEY  = 40  # "Scheduled" -- see std_status_ris / decoded STATUS_KEY lifecycle

_FETCH_BATCH = 2000


def _safe_date(val):
    if val is None:
        return None
    try:
        return val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


_UPSERT_SQL = text("""
    INSERT INTO std_worklist_scheduled (
        site_worklist_key, pps_key, scheduled_at, last_update
    ) VALUES (
        :site_worklist_key, :pps_key, :scheduled_at, :last_update
    )
    ON CONFLICT (site_worklist_key, scheduled_at) DO UPDATE SET
        pps_key      = EXCLUDED.pps_key,
        last_update  = EXCLUDED.last_update
""")


def run_ris_worklist_scheduled_etl(pg_engine, oracle_source):
    job_name   = "RIS_WORKLIST_SCHEDULED_ETL"
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
        logging.error(f"RIS Worklist Scheduled ETL log error: {e}")

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text(
                "SELECT MAX(scheduled_at) FROM std_worklist_scheduled"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Worklist Scheduled ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    is_fresh_load = watermark is None
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    base_query = f"""
        SELECT wsh.SITE_WORKLIST_KEY, sw.PPS_KEY, wsh.STATUS_TIME
        FROM {_STATUS_HISTORY_TABLE} wsh
        JOIN {_WORKLIST_TABLE} sw ON sw.SITE_WORKLIST_KEY = wsh.SITE_WORKLIST_KEY
        WHERE wsh.STATUS_KEY = :scheduled_key
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        params = {"scheduled_key": _SCHEDULED_STATUS_KEY}
        if is_fresh_load:
            logging.info("RIS Worklist Scheduled ETL starting — fresh load")
            print(f"[RIS Worklist Scheduled ETL] 🚀 Starting ({_STATUS_HISTORY_TABLE} ⋈ {_WORKLIST_TABLE}), fresh load")
            cursor.execute(base_query, params)
        else:
            logging.info(f"RIS Worklist Scheduled ETL starting — incremental, watermark={watermark}, lookback={lookback_date}")
            print(f"[RIS Worklist Scheduled ETL] 🚀 Starting — incremental, watermark={watermark}, lookback={lookback_date}")
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
                scheduled_at = _safe_date(status_time)
                if site_worklist_key is None or scheduled_at is None:
                    skipped += 1
                    continue
                rows.append({
                    "site_worklist_key": site_worklist_key,
                    "pps_key": pps_key,
                    "scheduled_at": scheduled_at,
                    "last_update": datetime.now(),
                })
            if rows:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, rows)
                total += len(rows)

        print(f"[RIS Worklist Scheduled ETL] ✅ {total:,} scheduled events upserted, {skipped} skipped (no key)")
        status = "SUCCESS"
        logging.info(f"RIS Worklist Scheduled ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Worklist Scheduled ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Worklist Scheduled log: {le}")

    return total
