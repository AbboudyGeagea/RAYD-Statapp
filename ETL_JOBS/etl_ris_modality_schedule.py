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

STAKES: procedure_duration_map.modality feeds report_ai.py's utilization estimate (first
in its COALESCE chain, ahead of aetitle_modality_map/study_modality), and the table is
joined throughout report_25/27/31/34/35, capacity_ladder.py and financial_dashboard.py —
a wrong value here doesn't just miscategorize one row, it quietly skews TAT/RVU/utilization
reporting sitewide. So this does NOT silently pick a modality when a procedure's schedule
rows disagree:

  - A single SPS_CODE_KEY commonly appears against several MODALITY_KEY rows (a procedure
    schedulable on more than one device). If every one of those devices resolves to the
    SAME modality string (the common case — multiple devices of one modality type, e.g. two
    CT scanners), that's unambiguous and gets auto-filled.
  - If they resolve to GENUINELY DIFFERENT modality strings, that's a real conflict, not
    noise to vote away. It's written to `procedure_modality_conflicts` — the same review
    table routes/mapping_controller.py's mapping tab already surfaces for PACS-history
    conflicts (a `source` column keeps the two origins distinct so neither writer's
    refresh clobbers the other's rows) — for a human to resolve, exactly like every other
    ambiguous-mapping case in this codebase (see also procedure_fuzzy_candidates).

IMPORT POLICY: fill-only for the unambiguous case — `WHERE procedure_duration_map.modality
IS NULL` — never overwrites a manually-set or already-resolved modality. Never auto-applies
for the ambiguous case.

NOTE: this table's SCHEDULE_TEMPLATE_KEY / MODALITY_SCHEDULE_GROUP_KEY columns look like they
may also resolve the open/closing-time gap noted in ETL_JOBS/etl_ris_modality_availability.py
(std_schedule_template_items "captured raw — not yet attributable to a specific device") —
NOT attempted here pending the SCHEDULE_TEMPLATE table schema (not yet seen).

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

# Same review table routes/mapping_controller.py's mapping tab already reads for PACS-
# history conflicts (etl_runner.py's _sync_lookup_tables) — `source` keeps the two
# origins from clobbering each other's rows (each writer only touches its own source).
_CONFLICTS_DDL = text("""
    CREATE TABLE IF NOT EXISTS procedure_modality_conflicts (
        id              SERIAL PRIMARY KEY,
        procedure_code  VARCHAR UNIQUE,
        modalities      TEXT,
        sample_count    INTEGER,
        detected_at     TIMESTAMP DEFAULT NOW()
    )
""")
_CONFLICTS_SOURCE_COLUMN_DDL = text("""
    ALTER TABLE procedure_modality_conflicts
        ADD COLUMN IF NOT EXISTS source VARCHAR(30) NOT NULL DEFAULT 'pacs_history'
""")

# Unambiguous case: every schedule-linked device for this procedure resolves to the same
# modality string -> safe to fill.
_UPDATE_UNAMBIGUOUS_SQL = text("""
    WITH resolved AS (
        SELECT stg.sps_code_key, array_agg(DISTINCT am.modality) AS modalities
        FROM ris_modality_schedule_stage stg
        JOIN aetitle_modality_map am ON am.ris_modality_key = stg.modality_key
        WHERE am.modality IS NOT NULL AND am.modality != 'SR'
        GROUP BY stg.sps_code_key
    )
    UPDATE procedure_duration_map p
    SET modality = resolved.modalities[1]
    FROM resolved
    WHERE p.ris_sps_code_key = resolved.sps_code_key
      AND p.modality IS NULL
      AND array_length(resolved.modalities, 1) = 1
""")

# Ambiguous case: 2+ distinct modality strings for the same procedure -> flag, don't guess.
_UPSERT_CONFLICTS_SQL = text("""
    WITH resolved AS (
        SELECT stg.sps_code_key,
               array_agg(DISTINCT am.modality ORDER BY am.modality) AS modalities,
               COUNT(*) AS sample_count
        FROM ris_modality_schedule_stage stg
        JOIN aetitle_modality_map am ON am.ris_modality_key = stg.modality_key
        WHERE am.modality IS NOT NULL AND am.modality != 'SR'
        GROUP BY stg.sps_code_key
        HAVING COUNT(DISTINCT am.modality) > 1
    )
    INSERT INTO procedure_modality_conflicts (procedure_code, modalities, sample_count, source)
    SELECT p.procedure_code, array_to_string(resolved.modalities, ', '), resolved.sample_count,
           'ris_modality_schedule'
    FROM resolved
    JOIN procedure_duration_map p ON p.ris_sps_code_key = resolved.sps_code_key
    ON CONFLICT (procedure_code) DO UPDATE SET
        modalities   = EXCLUDED.modalities,
        sample_count = EXCLUDED.sample_count,
        source       = EXCLUDED.source,
        detected_at  = NOW()
""")


def run_ris_modality_schedule_etl(pg_engine, oracle_source):
    job_name   = "RIS_MODALITY_SCHEDULE_ETL"
    start_time = datetime.now()
    total      = 0
    mapped     = 0
    flagged    = 0
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

            conn.execute(_CONFLICTS_DDL)
            conn.execute(_CONFLICTS_SOURCE_COLUMN_DDL)
            # Only this writer's rows — a PACS-history refresh (Phase 8, disabled at
            # LAUMC but harmless to coexist with) must not be able to wipe these either.
            conn.execute(text(
                "DELETE FROM procedure_modality_conflicts WHERE source = 'ris_modality_schedule'"
            ))

            r = conn.execute(_UPDATE_UNAMBIGUOUS_SQL)
            mapped = r.rowcount

            r2 = conn.execute(_UPSERT_CONFLICTS_SQL)
            flagged = r2.rowcount

        status = "SUCCESS"
        print(f"[RIS Modality Schedule ETL] ✅ Done — {total:,} schedule pairs seen, "
              f"{mapped:,} procedures unambiguously mapped, "
              f"{flagged:,} flagged with conflicting modalities for review")
        logging.info(
            f"RIS Modality Schedule ETL complete: {total:,} pairs, {mapped:,} mapped, "
            f"{flagged:,} flagged"
        )

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
                             "null_alerts=:na, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": mapped, "na": flagged,
                         "d": round(duration, 2), "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Modality Schedule log: {le}")

    return mapped
