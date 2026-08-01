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

Also imports two small reference tables tied to the same MODALITY_SCHEDULE rows (see
run_ris_schedule_template_etl below):
  - SCHEDULE_TEMPLATE -> std_schedule_templates. Names are literal device/room names
    (RH-CT64, RH-MRI_3T...) for device templates (RESOURCE_TEMPLATE_FLAG='N'), and
    physician names for resource/physician availability templates (FLAG='Y' — NOT
    devices, never resolved onto aetitle_modality_map). Each MODALITY_SCHEDULE row pairs
    exactly one SCHEDULE_TEMPLATE_KEY with exactly one MODALITY_KEY (confirmed against
    the vendor export), so device-type templates resolve unambiguously to an aetitle —
    written to aetitle_modality_map.ris_schedule_template_key (migration 0106), same
    back-reference convention as ris_modality_key/ris_sps_code_key (migration 0053).
    Ambiguous cases (a template key resolving to more than one distinct aetitle) are
    left NULL rather than guessed, consistent with the modality-conflict handling above.
  - MODALITY_SCHEDULE_GROUP -> std_modality_schedule_groups. A scheduling group/site
    discriminator (Radiology/NonRad/Default/SJH/Vascular Lab) — SJH and Vascular Lab
    match the ORG_STRUCTURE site split in docs/LAUMC_RIS_TABLES.md (org 5320=SJH, org
    5120=VASC). Reference data only, nothing joins to it yet.

run_ris_schedule_template_version_etl (below) imports SCHEDULE_TEMPLATE_VERSION ->
std_schedule_template_versions (migration 0107) — the version bridge confirmed against a
real SCHEDULE_TEMPLATE_ITEM sample (2026-08-01): SCHEDULE_TEMPLATE_KEY is one-to-many to
SCHEDULE_TEMPLATE_VERSION_KEY, DEFAULT_VERSION='Y' marks the currently-active one, and
SCHEDULE_TEMPLATE_ITEM.SCHEDULE_TEMPLATE_VERSION_KEY is what actually carries the day/time
rows. Device attribution for std_schedule_template_items itself (the item -> version ->
template -> aetitle chain) lives in etl_ris_modality_availability.py's
run_schedule_template_device_link, called from the new Phase 18 — it needs
std_schedule_template_items already populated (Phase 15) as well as this table and
aetitle_modality_map.ris_schedule_template_key (this file's other function, Phase 10), so
it can't live here without forcing an awkward phase-ordering dependency the other way.

ORDERING: must run AFTER etl_ris_modality.py and etl_ris_procedures.py in the same pass —
this script joins through the ris_modality_key / ris_sps_code_key back-references those two
scripts populate. Enforced by call order in etl_runner.py's Phase 10, not by this module.
run_ris_schedule_template_etl must run AFTER run_ris_modality_schedule_etl in the same pass
too — it reuses the stage table that populates, now extended with schedule_template_key.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_MODALITY_SCHEDULE_TABLE       = os.getenv("RAYD_RIS_MODALITY_SCHEDULE_TABLE", "MODALITY_SCHEDULE")
_SCHEDULE_TEMPLATE_TABLE       = os.getenv("RAYD_RIS_SCHEDULE_TEMPLATE_TABLE", "SCHEDULE_TEMPLATE")
_MODALITY_SCHEDULE_GROUP_TABLE = os.getenv("RAYD_RIS_MODALITY_SCHEDULE_GROUP_TABLE", "MODALITY_SCHEDULE_GROUP")

_STAGE_DDL = text("""
    CREATE TABLE IF NOT EXISTS ris_modality_schedule_stage (
        sps_code_key BIGINT,
        modality_key BIGINT,
        schedule_template_key BIGINT
    )
""")
_STAGE_TEMPLATE_COLUMN_DDL = text("""
    ALTER TABLE ris_modality_schedule_stage
        ADD COLUMN IF NOT EXISTS schedule_template_key BIGINT
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
        SELECT DISTINCT SPS_CODE_KEY, MODALITY_KEY, SCHEDULE_TEMPLATE_KEY
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
            {"sps_code_key": sps_code_key, "modality_key": modality_key,
             "schedule_template_key": schedule_template_key}
            for sps_code_key, modality_key, schedule_template_key in rows
            if sps_code_key is not None and modality_key is not None
        ]
        total = len(params)

        with pg_engine.begin() as conn:
            conn.execute(_STAGE_DDL)
            conn.execute(_STAGE_TEMPLATE_COLUMN_DDL)
            conn.execute(text("TRUNCATE ris_modality_schedule_stage"))
            if params:
                conn.execute(text(
                    "INSERT INTO ris_modality_schedule_stage "
                    "(sps_code_key, modality_key, schedule_template_key) "
                    "VALUES (:sps_code_key, :modality_key, :schedule_template_key)"
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


# ── SCHEDULE_TEMPLATE + MODALITY_SCHEDULE_GROUP (reference tables + device link) ──────────

_TEMPLATE_DDL = text("""
    CREATE TABLE IF NOT EXISTS std_schedule_templates (
        schedule_template_key   BIGINT PRIMARY KEY,
        name                    TEXT,
        description             TEXT,
        resource_template_flag  BOOLEAN,
        source_last_updated     TIMESTAMP,
        last_update             TIMESTAMP NOT NULL DEFAULT NOW()
    )
""")
_GROUP_DDL = text("""
    CREATE TABLE IF NOT EXISTS std_modality_schedule_groups (
        modality_schedule_group_key  BIGINT PRIMARY KEY,
        name                         TEXT,
        source_last_updated          TIMESTAMP,
        last_update                  TIMESTAMP NOT NULL DEFAULT NOW()
    )
""")
_RIS_SCHEDULE_TEMPLATE_KEY_COLUMN_DDL = text("""
    ALTER TABLE aetitle_modality_map
        ADD COLUMN IF NOT EXISTS ris_schedule_template_key BIGINT
""")

_UPSERT_TEMPLATE_SQL = text("""
    INSERT INTO std_schedule_templates
        (schedule_template_key, name, description, resource_template_flag, source_last_updated, last_update)
    VALUES
        (:schedule_template_key, :name, :description, :resource_template_flag, :source_last_updated, NOW())
    ON CONFLICT (schedule_template_key) DO UPDATE SET
        name = EXCLUDED.name, description = EXCLUDED.description,
        resource_template_flag = EXCLUDED.resource_template_flag,
        source_last_updated = EXCLUDED.source_last_updated, last_update = NOW()
""")

_UPSERT_GROUP_SQL = text("""
    INSERT INTO std_modality_schedule_groups
        (modality_schedule_group_key, name, source_last_updated, last_update)
    VALUES
        (:modality_schedule_group_key, :name, :source_last_updated, NOW())
    ON CONFLICT (modality_schedule_group_key) DO UPDATE SET
        name = EXCLUDED.name, source_last_updated = EXCLUDED.source_last_updated, last_update = NOW()
""")

# Device attribution: only when a schedule_template_key resolves to exactly one distinct
# aetitle across its (usually single) modality_key pairing in MODALITY_SCHEDULE — same
# "don't guess on ambiguity" discipline as the procedure/modality fill above. Physician/
# resource templates (RESOURCE_TEMPLATE_FLAG='Y') never have a MODALITY_KEY pairing, so
# they simply never match here and stay NULL, as intended — no explicit flag filter needed.
_UPDATE_DEVICE_TEMPLATE_LINK_SQL = text("""
    WITH resolved AS (
        SELECT stg.schedule_template_key, array_agg(DISTINCT am.aetitle) AS aetitles
        FROM ris_modality_schedule_stage stg
        JOIN aetitle_modality_map am ON am.ris_modality_key = stg.modality_key
        WHERE stg.schedule_template_key IS NOT NULL
        GROUP BY stg.schedule_template_key
        HAVING COUNT(DISTINCT am.aetitle) = 1
    )
    UPDATE aetitle_modality_map am
    SET ris_schedule_template_key = resolved.schedule_template_key
    FROM resolved
    WHERE am.aetitle = resolved.aetitles[1]
""")


def _flag_to_bool(val):
    if val is None:
        return None
    return str(val).strip().upper() in ('Y', 'YES', 'TRUE', '1')


def run_ris_schedule_template_etl(pg_engine, oracle_source):
    """
    Imports SCHEDULE_TEMPLATE -> std_schedule_templates and MODALITY_SCHEDULE_GROUP ->
    std_modality_schedule_groups (small reference tables, full reload each pass), then
    resolves aetitle_modality_map.ris_schedule_template_key for device-type templates
    using the ris_modality_schedule_stage table run_ris_modality_schedule_etl already
    populated this pass — MUST run after it.
    """
    job_name   = "RIS_SCHEDULE_TEMPLATE_ETL"
    start_time = datetime.now()
    templates  = 0
    groups     = 0
    linked     = 0
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
        logging.error(f"RIS Schedule Template ETL log error: {e}")

    template_query = f"""
        SELECT SCHEDULE_TEMPLATE_KEY, NAME, DESCRIPTION, RESOURCE_TEMPLATE_FLAG, LAST_UPDATED
        FROM {_SCHEDULE_TEMPLATE_TABLE}
    """
    # Column name for the group's label is unconfirmed (assumed NAME, matching
    # SCHEDULE_TEMPLATE's own convention) — this table is 5 reference rows, cheap to
    # fix if the real column turns out to be DESCRIPTION or similar.
    group_query = f"""
        SELECT MODALITY_SCHEDULE_GROUP_KEY, NAME, LAST_UPDATED
        FROM {_MODALITY_SCHEDULE_GROUP_TABLE}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Schedule Template ETL starting")
        print(f"[RIS Schedule Template ETL] 🚀 Starting ({_SCHEDULE_TEMPLATE_TABLE} + {_MODALITY_SCHEDULE_GROUP_TABLE})")

        cursor.execute(template_query)
        template_rows = cursor.fetchall()
        template_params = [
            {
                "schedule_template_key": key,
                "name": str(name).strip() if name else None,
                "description": str(desc).strip() if desc else None,
                "resource_template_flag": _flag_to_bool(flag),
                "source_last_updated": last_updated,
            }
            for key, name, desc, flag, last_updated in template_rows
            if key is not None
        ]
        templates = len(template_params)

        cursor.execute(group_query)
        group_rows = cursor.fetchall()
        group_params = [
            {
                "modality_schedule_group_key": key,
                "name": str(name).strip() if name else None,
                "source_last_updated": last_updated,
            }
            for key, name, last_updated in group_rows
            if key is not None
        ]
        groups = len(group_params)

        with pg_engine.begin() as conn:
            conn.execute(_TEMPLATE_DDL)
            conn.execute(_GROUP_DDL)
            conn.execute(_RIS_SCHEDULE_TEMPLATE_KEY_COLUMN_DDL)

            if template_params:
                conn.execute(_UPSERT_TEMPLATE_SQL, template_params)
            if group_params:
                conn.execute(_UPSERT_GROUP_SQL, group_params)

            r = conn.execute(_UPDATE_DEVICE_TEMPLATE_LINK_SQL)
            linked = r.rowcount

        status = "SUCCESS"
        print(f"[RIS Schedule Template ETL] ✅ Done — {templates:,} templates, {groups:,} groups, "
              f"{linked:,} devices linked to their schedule template")
        logging.info(
            f"RIS Schedule Template ETL complete: {templates:,} templates, {groups:,} groups, "
            f"{linked:,} device links"
        )

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Schedule Template ETL error: {error_msg}")
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
                        {"s": status, "et": end_time, "r": templates + groups,
                         "d": round(duration, 2), "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Schedule Template log: {le}")

    return linked


# ── SCHEDULE_TEMPLATE_VERSION (the version bridge — Phase 18 uses this) ───────────────────

_SCHEDULE_TEMPLATE_VERSION_TABLE = os.getenv("RAYD_RIS_SCHEDULE_TEMPLATE_VERSION_TABLE", "SCHEDULE_TEMPLATE_VERSION")

_VERSION_DDL = text("""
    CREATE TABLE IF NOT EXISTS std_schedule_template_versions (
        schedule_template_version_key  BIGINT PRIMARY KEY,
        schedule_template_key          BIGINT,
        version                        TEXT,
        description                    TEXT,
        default_version                BOOLEAN,
        source_last_updated            TIMESTAMP,
        last_update                    TIMESTAMP NOT NULL DEFAULT NOW()
    )
""")

_UPSERT_VERSION_SQL = text("""
    INSERT INTO std_schedule_template_versions
        (schedule_template_version_key, schedule_template_key, version, description,
         default_version, source_last_updated, last_update)
    VALUES
        (:schedule_template_version_key, :schedule_template_key, :version, :description,
         :default_version, :source_last_updated, NOW())
    ON CONFLICT (schedule_template_version_key) DO UPDATE SET
        schedule_template_key = EXCLUDED.schedule_template_key,
        version = EXCLUDED.version, description = EXCLUDED.description,
        default_version = EXCLUDED.default_version,
        source_last_updated = EXCLUDED.source_last_updated, last_update = NOW()
""")


def run_ris_schedule_template_version_etl(pg_engine, oracle_source):
    """
    Imports SCHEDULE_TEMPLATE_VERSION -> std_schedule_template_versions (small reference
    table, full reload each pass). This is the version bridge — see module docstring.
    Device attribution for std_schedule_template_items itself happens separately in
    etl_ris_modality_availability.py's run_schedule_template_device_link (Phase 18), which
    must run after this.
    """
    job_name   = "RIS_SCHEDULE_TEMPLATE_VERSION_ETL"
    start_time = datetime.now()
    total      = 0
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
        logging.error(f"RIS Schedule Template Version ETL log error: {e}")

    query = f"""
        SELECT SCHEDULE_TEMPLATE_VERSION_KEY, SCHEDULE_TEMPLATE_KEY, VERSION, DESCRIPTION,
               DEFAULT_VERSION, LAST_UPDATED
        FROM {_SCHEDULE_TEMPLATE_VERSION_TABLE}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Schedule Template Version ETL starting")
        print(f"[RIS Schedule Template Version ETL] 🚀 Starting ({_SCHEDULE_TEMPLATE_VERSION_TABLE})")

        cursor.execute(query)
        rows = cursor.fetchall()

        params = [
            {
                "schedule_template_version_key": key,
                "schedule_template_key": template_key,
                "version": str(version).strip() if version else None,
                "description": str(desc).strip() if desc else None,
                "default_version": _flag_to_bool(default_flag),
                "source_last_updated": last_updated,
            }
            for key, template_key, version, desc, default_flag, last_updated in rows
            if key is not None
        ]
        total = len(params)

        with pg_engine.begin() as conn:
            conn.execute(_VERSION_DDL)
            if params:
                conn.execute(_UPSERT_VERSION_SQL, params)

        status = "SUCCESS"
        print(f"[RIS Schedule Template Version ETL] ✅ Done — {total:,} versions upserted")
        logging.info(f"RIS Schedule Template Version ETL complete: {total:,} versions")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Schedule Template Version ETL error: {error_msg}")
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
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Schedule Template Version log: {le}")

    return total
