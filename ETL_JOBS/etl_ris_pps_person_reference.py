"""
ETL_JOBS/etl_ris_pps_person_reference.py — RIS PPS_PERSON_REFERENCE -> std_pps_person_reference
(LAUMC), the working replacement for std_pps.primary_tech_person_key.

See migration 0097 for the target table and full design notes. Short version:
std_pps.primary_tech_person_key (migration 0065, PPS.PRIMARY_TECH_PERSON_KEY) was
confirmed 100% NULL against production (live query, 2026-08-01) -- a dead column.
PPS_PERSON_REFERENCE, joined by PPS_KEY, gives a RESOURCE_ID_KEY that resolves to a
real person (confirmed ~99% populated, 12,209/12,301 recent PPS rows).

*** THIS TABLE IS NOT TECHNOLOGIST-ONLY *** — a single PPS can carry several
PPS_PERSON_REFERENCE rows (technologist, but also receptionist/nurse/radiologist/etc.,
all under the same broad PERSON_REFERENCE_TYPE_KEY catch-all). Consumers must filter by
joining RESOURCE_ID_KEY to std_resources_ris.role_code = 'TEC' at query time — several
different person_reference_type_key values were all found to resolve to genuine
technologists, while the single largest/most common type-key value is itself a mixed
bucket including other roles. Do NOT filter on person_reference_type_key alone.

No timestamp/date column exists on the Oracle source table at all (confirmed via
all_tab_columns) -- unlike etl_ris_pps.py's watermark on START_DATETIME, this is a pure
key-range pull: watermark is MAX(pps_person_reference_key) already loaded, same pattern
etl_ris_pps.py already uses for MAX(pps_key). No go_live_date cutoff either, since there
is no date column to apply one to.

pps_person_reference_key is used directly as the upsert conflict target/PK -- confirmed
real, populated, and presumably unique via all_tab_columns, no evidence of the same
NULL-PK problem migration 0089 found on WORKLIST_STATUS_HISTORY_KEY.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_PERSON_REFERENCE_TABLE = os.getenv("RAYD_RIS_PPS_PERSON_REFERENCE_TABLE", "PPS_PERSON_REFERENCE")
_FETCH_BATCH = 1000


_UPSERT_PERSON_REFERENCE_SQL = text("""
    INSERT INTO std_pps_person_reference (
        pps_person_reference_key, pps_key, person_reference_type_key,
        sequence_id, resource_id_key, display_sort_order, last_update
    ) VALUES (
        :pps_person_reference_key, :pps_key, :person_reference_type_key,
        :sequence_id, :resource_id_key, :display_sort_order, :last_update
    )
    ON CONFLICT (pps_person_reference_key) DO UPDATE SET
        pps_key                    = EXCLUDED.pps_key,
        person_reference_type_key  = EXCLUDED.person_reference_type_key,
        sequence_id                = EXCLUDED.sequence_id,
        resource_id_key            = EXCLUDED.resource_id_key,
        display_sort_order         = EXCLUDED.display_sort_order,
        last_update                = EXCLUDED.last_update
""")


def run_ris_pps_person_reference_etl(pg_engine, oracle_source):
    job_name   = "RIS_PPS_PERSON_REFERENCE_ETL"
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
        logging.error(f"RIS PPS Person Reference ETL log error: {e}")

    # Incremental watermark: MAX(pps_person_reference_key) already loaded, same
    # MAX(pps_key) pattern etl_ris_pps.py already uses -- there is no date column on
    # this table at all, so a plain key comparison is the only option here (not just
    # the cheaper one).
    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(
                text("SELECT MAX(pps_person_reference_key) FROM std_pps_person_reference")
            ).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS PPS Person Reference ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    is_fresh_load = not watermark

    query = f"""
        SELECT
            PPS_PERSON_REFERENCE_KEY, PPS_KEY, PERSON_REFERENCE_TYPE_KEY,
            SEQUENCE_ID, RESOURCE_ID_KEY, DISPLAY_SORT_ORDER
        FROM {_PERSON_REFERENCE_TABLE}
        {"" if is_fresh_load else "WHERE PPS_PERSON_REFERENCE_KEY > :watermark"}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        mode_str = "fresh load, full unfiltered pull" if is_fresh_load else f"incremental, watermark pps_person_reference_key>{watermark}"
        logging.info(f"RIS PPS Person Reference ETL starting — {mode_str}")
        print(f"[RIS PPS Person Reference ETL] 🚀 Starting ({_PERSON_REFERENCE_TABLE}) — {mode_str}")

        params = {}
        if not is_fresh_load:
            params["watermark"] = watermark
        cursor.execute(query, params)

        batch_num = 0
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            batch_num += 1

            params = []
            for row in batch:
                (pps_person_reference_key, pps_key, person_reference_type_key,
                 sequence_id, resource_id_key, display_sort_order) = row

                if pps_person_reference_key is None:
                    skipped += 1
                    continue

                params.append({
                    "pps_person_reference_key": pps_person_reference_key,
                    "pps_key": pps_key,
                    "person_reference_type_key": person_reference_type_key,
                    "sequence_id": sequence_id,
                    "resource_id_key": resource_id_key,
                    "display_sort_order": display_sort_order,
                    "last_update": datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_PERSON_REFERENCE_SQL, params)
                total += len(params)

            if batch_num % 50 == 0:
                print(f"[RIS PPS Person Reference ETL] 📦 {total:,} rows loaded")

        status = "SUCCESS"
        print(f"[RIS PPS Person Reference ETL] ✅ Done — {total:,} rows, {skipped} skipped (no key)")
        logging.info(f"RIS PPS Person Reference ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS PPS Person Reference ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS PPS Person Reference log: {le}")

    return total
