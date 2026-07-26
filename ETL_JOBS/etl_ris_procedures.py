"""
ETL_JOBS/etl_ris_procedures.py — RIS SPS_CODE -> procedure_duration_map (LAUMC).

Per migration 0053 / docs/LAUMC_RIS_TABLES.md: the RIS procedure catalog loads into the
EXISTING procedure_duration_map (not a separate std_procedure_codes table), so the
financial dashboard, capacity ladder, report 27, and the mapping tab keep joining on
procedure_code unchanged — this is also the same procedure_code value etl_orders.py
already pulls via the identical SPS_CODE join (see ETL_JOBS/etl_orders.py), so once this
has run, those existing joins start matching immediately.

Column map (vendor-confirmed):
    CODE          -> procedure_code   (join key — e.g. J17G-01)
    DESCRIPTION   -> procedure_name
    DURATION      -> duration_minutes (NOT NULL on the PG side; falls back to 15 when
                      the source value is missing, matching the same 15-minute default
                      routes/capacity_ladder.py already applies elsewhere)
    ACTIVE        -> active
    SPS_CODE_KEY  -> ris_sps_code_key (source back-reference)

NOT pulled yet: BODY_PART_KEY / LATERALITY_KEY / CODING_SCHEME_KEY. Each needs its own
Oracle lookup table (BODY_PART, etc.) resolving key -> text, none of which have been
provided yet — procedure_duration_map.body_part (migration 0053) stays NULL until that
lookup exists. Not blocking: procedure_code/name/duration/active are what every current
report join actually needs.

IMPORT POLICY (vendor-confirmed): FILL-ONLY. INSERT ... ON CONFLICT (procedure_code) DO
NOTHING — never overwrites a manually-edited row (clinical_rvu/technical_rvu are
RAYD-owned), and never deletes.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_SPS_CODE_TABLE   = os.getenv("RAYD_RIS_SPS_CODE_TABLE", "SPS_CODE")
_DEFAULT_DURATION = 15  # matches capacity_ladder.py's own COALESCE(pm.duration_minutes, 15)

_UPSERT_SQL = text("""
    INSERT INTO procedure_duration_map
        (procedure_code, procedure_name, duration_minutes, active, ris_sps_code_key)
    VALUES
        (:procedure_code, :procedure_name, :duration_minutes, :active, :ris_sps_code_key)
    ON CONFLICT (procedure_code) DO NOTHING
""")


def run_ris_procedures_etl(pg_engine, oracle_source):
    job_name   = "RIS_PROCEDURES_ETL"
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
        logging.error(f"RIS Procedures ETL log error: {e}")

    query = f"""
        SELECT SPS_CODE_KEY, CODE, DESCRIPTION, DURATION, ACTIVE
        FROM {_SPS_CODE_TABLE}
        WHERE CODE IS NOT NULL
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Procedures ETL starting")
        print(f"[RIS Procedures ETL] 🚀 Starting ({_SPS_CODE_TABLE})")

        cursor.execute(query)
        rows = cursor.fetchall()

        params = []
        for sps_code_key, code, description, duration, active_flag in rows:
            code = str(code).strip() if code is not None else None
            if not code:
                skipped += 1
                continue

            try:
                duration_minutes = int(duration) if duration is not None else _DEFAULT_DURATION
                if duration_minutes <= 0:
                    duration_minutes = _DEFAULT_DURATION
            except (TypeError, ValueError):
                duration_minutes = _DEFAULT_DURATION

            active = True
            if active_flag is not None:
                active = str(active_flag).strip().upper() in ('Y', 'YES', 'TRUE', '1')

            params.append({
                "procedure_code":   code,
                "procedure_name":   str(description).strip() if description else None,
                "duration_minutes": duration_minutes,
                "active":           active,
                "ris_sps_code_key": sps_code_key,
            })

        if params:
            with pg_engine.begin() as conn:
                conn.execute(_UPSERT_SQL, params)
            total = len(params)

        status = "SUCCESS"
        print(f"[RIS Procedures ETL] ✅ Done — {total:,} procedure codes seen (fill-only upsert), {skipped} skipped (no CODE)")
        logging.info(f"RIS Procedures ETL complete: {total:,} codes, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Procedures ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                duration_sec = (end_time - start_time).total_seconds()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "null_alerts=:na, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration_sec, 2), "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Procedures log: {le}")

    return total
