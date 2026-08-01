"""
ETL_JOBS/etl_ris_worklist_arrivals.py — RIS WORKLIST_STATUS_HISTORY (STATUS_KEY=60,
"Arrived") ⋈ SITE_WORKLIST -> std_worklist_arrivals (LAUMC).

See migrations 0088/0089 for the target table and full design notes. Short version:
real patient-arrival timestamps, sourced from RIS's own per-transition audit log
(WORKLIST_STATUS_HISTORY), not the HL7 ORM feed (hl7_orders.arrived_at, migration
0022) -- that one is empty because R2I isn't flowing live orders yet. status_key=60
confirmed as "Arrived" against std_status_ris (operator, 2026-07-31).

BUG FIXED 2026-07-31: originally watermarked on MAX(worklist_status_history_key) and
used that column as the upsert conflict target. Confirmed against real data (operator
sample) that WORKLIST_STATUS_HISTORY_KEY is NULL on every single row despite the
name -- the first live run matched 627,491 rows and skipped all of them. Migration
0089 dropped that column/PK entirely; this now watermarks on MAX(arrived_at) (with a
lookback, same pattern as etl_ris_reports.py/etl_ris_pps.py) and upserts on
(site_worklist_key, arrived_at) -- a worklist entry can genuinely have multiple
Arrived transitions (confirmed in the same sample: the same SITE_WORKLIST_KEY twice
with different timestamps), so the pair is the real natural key, not either column
alone.

sps_id (migration 0099, added 2026-08-01) -- SITE_WORKLIST.SPS_ID, the real accession
number. Report 35's flagged-exam accession display was falling back to a synthetic
'WL#<pps_key>' placeholder far more often than expected, because a completed exam per
RIS doesn't always have a matching PACS study (etl_didb_studies) yet -- SPS_ID gives a
real accession straight from the worklist row itself, no PACS match required.

sps_id backfill (2026-08-01): adding the column doesn't retroactively populate it --
the incremental watermark (MAX(arrived_at) + 10-day lookback) means a normal run only
ever touches recent rows, so years of existing history would keep showing the WL#
placeholder forever without a one-time full pass. Set RAYD_FORCE_FULL_ARRIVALS_BACKFILL=1
for exactly one run to force is_fresh_load regardless of the watermark -- every row gets
re-upserted (harmless, same ON CONFLICT DO UPDATE path as always) so sps_id gets filled
in everywhere, then unset the env var so normal incremental runs resume.
"""
import logging
import os
from datetime import datetime, timedelta
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
        site_worklist_key, pps_key, arrived_at, sps_id, last_update
    ) VALUES (
        :site_worklist_key, :pps_key, :arrived_at, :sps_id, :last_update
    )
    ON CONFLICT (site_worklist_key, arrived_at) DO UPDATE SET
        pps_key      = EXCLUDED.pps_key,
        sps_id       = EXCLUDED.sps_id,
        last_update  = EXCLUDED.last_update
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
                "SELECT MAX(arrived_at) FROM std_worklist_arrivals"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Worklist Arrivals ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    _force_full_backfill = os.getenv('RAYD_FORCE_FULL_ARRIVALS_BACKFILL', '').strip() == '1'
    is_fresh_load = watermark is None or _force_full_backfill
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    # SPS_ID (migration 0099) -- the real accession number, "same value as the PACS
    # accession number across ALL tables" (docs/LAUMC_RIS_TABLES.md). Report 35 uses it
    # as its primary accession fallback ahead of the synthetic WL#<pps_key> placeholder.
    base_query = f"""
        SELECT wsh.SITE_WORKLIST_KEY, sw.PPS_KEY, wsh.STATUS_TIME, sw.SPS_ID
        FROM {_STATUS_HISTORY_TABLE} wsh
        JOIN {_WORKLIST_TABLE} sw ON sw.SITE_WORKLIST_KEY = wsh.SITE_WORKLIST_KEY
        WHERE wsh.STATUS_KEY = :arrived_key
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        params = {"arrived_key": _ARRIVED_STATUS_KEY}
        if is_fresh_load:
            mode_label = "forced full backfill (RAYD_FORCE_FULL_ARRIVALS_BACKFILL=1)" if _force_full_backfill else "fresh load"
            logging.info(f"RIS Worklist Arrivals ETL starting — {mode_label}")
            print(f"[RIS Worklist Arrivals ETL] 🚀 Starting ({_STATUS_HISTORY_TABLE} ⋈ {_WORKLIST_TABLE}), {mode_label}")
            cursor.execute(base_query, params)
        else:
            logging.info(f"RIS Worklist Arrivals ETL starting — incremental, watermark={watermark}, lookback={lookback_date}")
            print(f"[RIS Worklist Arrivals ETL] 🚀 Starting — incremental, watermark={watermark}, lookback={lookback_date}")
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
            for (site_worklist_key, pps_key, status_time, sps_id) in batch:
                arrived_at = _safe_date(status_time)
                if site_worklist_key is None or arrived_at is None:
                    skipped += 1
                    continue
                rows.append({
                    "site_worklist_key": site_worklist_key,
                    "pps_key": pps_key,
                    "arrived_at": arrived_at,
                    "sps_id": str(sps_id).strip() if sps_id else None,
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
