"""
ETL_JOBS/etl_ris_modality_schedule.py — RIS MODALITY_SCHEDULE -> procedure_duration_map.modality (LAUMC).

MODALITY_SCHEDULE is the RIS scheduling bridge table linking a procedure (SPS_CODE_KEY) to
the modality/device(s) it can be scheduled on (MODALITY_KEY), one row per (schedule template,
modality) pairing. It's the missing link that resolves procedure_duration_map.modality for
LAUMC: etl_ris_procedures.py already loads SPS_CODE -> procedure_duration_map (with
ris_sps_code_key), and etl_ris_modality.py already loads MODALITY -> aetitle_modality_map
(with ris_modality_key, resolved to a modality string like CT/MR/US) — but neither writes
procedure_duration_map.modality itself. At other sites that column is auto-learned from PACS
study history (etl_runner.py Phase 8, Strategies A-E); LAUMC disables that PACS auto-fill
(RAYD_ETL_LOOKUP_FROM_PACS=false in .env) since procedure/modality truth lives in the RIS
instead — this script is LAUMC's RIS-sourced equivalent of that gap.

A single SPS_CODE_KEY can appear against multiple MODALITY_KEY rows (a procedure schedulable
on more than one device — usually several devices of the same modality type, e.g. two CT
scanners). Resolved MODALITY_KEY -> modality string per SPS_CODE_KEY is aggregated with the
same MODE() WITHIN GROUP majority-vote used by every other auto-fill strategy in this codebase
(etl_runner.py Phase 8 Strategies A/B/E, Phase 2b's study_modality backfill) — picks the single
most common modality per procedure. PRIORITY (always 1 in observed samples, no documented
meaning beyond that) is not used as a tiebreaker; frequency across schedule/template rows is.

IMPORT POLICY: fill-only, matching every other auto-fill strategy in this codebase —
`WHERE procedure_duration_map.modality IS NULL` — never overwrites a manually-set or
already-resolved modality.

ORDERING: must run AFTER etl_ris_modality.py and etl_ris_procedures.py in the same pass —
this script joins through the ris_modality_key / ris_sps_code_key back-references those two
scripts populate. Enforced by call order in etl_runner.py's Phase 10, not by this module.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_MODALITY_SCHEDULE_TABLE = os.getenv("RAYD_RIS_MODALITY_SCHEDULE_TABLE", "MODALITY_SCHEDULE")

_STAGE_DDL = text("""
    CREATE TABLE IF NOT EXISTS ris_modality_schedule_stage (
        sps_code_key BIGINT,
        modality_key BIGINT
    )
""")

_UPDATE_SQL = text("""
    UPDATE procedure_duration_map p
    SET modality = sub.modality
    FROM (
        SELECT stg.sps_code_key,
               MODE() WITHIN GROUP (ORDER BY am.modality) AS modality
        FROM ris_modality_schedule_stage stg
        JOIN aetitle_modality_map am ON am.ris_modality_key = stg.modality_key
        WHERE am.modality IS NOT NULL AND am.modality != 'SR'
        GROUP BY stg.sps_code_key
    ) sub
    WHERE p.ris_sps_code_key = sub.sps_code_key
      AND p.modality IS NULL
""")


def run_ris_modality_schedule_etl(pg_engine, oracle_source):
    job_name   = "RIS_MODALITY_SCHEDULE_ETL"
    start_time = datetime.now()
    total      = 0
    mapped     = 0
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
        logging.error(f"RIS Modality Schedule ETL log error: {e}")

    query = f"""
        SELECT DISTINCT SPS_CODE_KEY, MODALITY_KEY
        FROM {_MODALITY_SCHEDULE_TABLE}
        WHERE SPS_CODE_KEY IS NOT NULL AND MODALITY_KEY IS NOT NULL
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Modality Schedule ETL starting")
        print(f"[RIS Modality Schedule ETL] 🚀 Starting ({_MODALITY_SCHEDULE_TABLE})")

        cursor.execute(query)
        rows = cursor.fetchall()

        params = [
            {"sps_code_key": sps_code_key, "modality_key": modality_key}
            for sps_code_key, modality_key in rows
            if sps_code_key is not None and modality_key is not None
        ]
        total = len(params)

        with pg_engine.begin() as conn:
            conn.execute(_STAGE_DDL)
            conn.execute(text("TRUNCATE ris_modality_schedule_stage"))
            if params:
                conn.execute(text(
                    "INSERT INTO ris_modality_schedule_stage (sps_code_key, modality_key) "
                    "VALUES (:sps_code_key, :modality_key)"
                ), params)
            r = conn.execute(_UPDATE_SQL)
            mapped = r.rowcount

        status = "SUCCESS"
        print(f"[RIS Modality Schedule ETL] ✅ Done — {total:,} schedule pairs seen, "
              f"{mapped:,} procedures newly mapped to a modality")
        logging.info(f"RIS Modality Schedule ETL complete: {total:,} pairs, {mapped:,} mapped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Modality Schedule ETL error: {error_msg}")
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
                             "error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": mapped,
                         "d": round(duration, 2), "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Modality Schedule log: {le}")

    return mapped
