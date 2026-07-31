"""
ETL_JOBS/etl_ris_worklist_arrivals.py — RIS WORKLIST_STATUS_HISTORY (STATUS_KEY=60,
"Arrived") ⋈ SITE_WORKLIST -> std_worklist_arrivals (LAUMC).

See migration 0088 for the target table and full design notes. Short version: real
patient-arrival timestamps, sourced from RIS's own per-transition audit log
(WORKLIST_STATUS_HISTORY), not the HL7 ORM feed (hl7_orders.arrived_at, migration
0022) -- that one is empty because R2I isn't flowing live orders yet. status_key=60
confirmed as "Arrived" against std_status_ris (operator, 2026-07-31).

Incremental watermark: MAX(worklist_status_history_key) already loaded.
WORKLIST_STATUS_HISTORY_KEY is that table's own PK and, like PPS_KEY, is monotonic
with insert order here -- built incremental from the start this time (unlike PPS,
which had to be fixed after the fact), since this is a transition-log table that will
only grow.
"""
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_STATUS_HISTORY_TABLE = "WORKLIST_STATUS_HISTORY"
_WORKLIST_TABLE        = "SITE_WORKLIST"
_ARRIVED_STATUS_KEY    = 60  # "Arrived" -- confirmed against std_status_ris, 2026-07-31

_FETCH_BATCH = 2000


def _safe_date(val):
    if val is None:
        return None
    try:
        return val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


_UPSERT_SQL = text("""
    INSERT INTO std_worklist_arrivals (
        worklist_status_history_key, site_worklist_key, pps_key, arrived_at, last_update
    ) VALUES (
        :worklist_status_history_key, :site_worklist_key, :pps_key, :arrived_at, :last_update
    )
    ON CONFLICT (worklist_status_history_key) DO UPDATE SET
        site_worklist_key = EXCLUDED.site_worklist_key,
        pps_key            = EXCLUDED.pps_key,
        arrived_at          = EXCLUDED.arrived_at,
        last_update          = EXCLUDED.last_update
""")


def run_ris_worklist_arrivals_etl(pg_engine, oracle_source):
    job_name   = "RIS_WORKLIST_ARRIVALS_ETL"
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
        logging.error(f"RIS Worklist Arrivals ETL log error: {e}")

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text(
                "SELECT MAX(worklist_status_history_key) FROM std_worklist_arrivals"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Worklist Arrivals ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    is_fresh_load = not watermark

    base_query = f"""
        SELECT wsh.WORKLIST_STATUS_HISTORY_KEY, wsh.SITE_WORKLIST_KEY, sw.PPS_KEY, wsh.STATUS_TIME
        FROM {_STATUS_HISTORY_TABLE} wsh
        JOIN {_WORKLIST_TABLE} sw ON sw.SITE_WORKLIST_KEY = wsh.SITE_WORKLIST_KEY
        WHERE wsh.STATUS_KEY = :arrived_key
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        params = {"arrived_key": _ARRIVED_STATUS_KEY}
        if is_fresh_load:
            logging.info("RIS Worklist Arrivals ETL starting — fresh load")
            print(f"[RIS Worklist Arrivals ETL] 🚀 Starting ({_STATUS_HISTORY_TABLE} ⋈ {_WORKLIST_TABLE}), fresh load")
            cursor.execute(base_query, params)
        else:
            logging.info(f"RIS Worklist Arrivals ETL starting — incremental, watermark={watermark}")
            print(f"[RIS Worklist Arrivals ETL] 🚀 Starting — incremental, watermark={watermark}")
            params["watermark"] = watermark
            cursor.execute(base_query + " AND wsh.WORKLIST_STATUS_HISTORY_KEY > :watermark", params)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            rows = []
            for (wsh_key, site_worklist_key, pps_key, status_time) in batch:
                if wsh_key is None:
                    skipped += 1
                    continue
                rows.append({
                    "worklist_status_history_key": wsh_key,
                    "site_worklist_key": site_worklist_key,
                    "pps_key": pps_key,
                    "arrived_at": _safe_date(status_time),
                    "last_update": datetime.now(),
                })
            if rows:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, rows)
                total += len(rows)

        print(f"[RIS Worklist Arrivals ETL] ✅ {total:,} arrivals upserted, {skipped} skipped (no key)")
        status = "SUCCESS"
        logging.info(f"RIS Worklist Arrivals ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Worklist Arrivals ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Worklist Arrivals log: {le}")

    return total
