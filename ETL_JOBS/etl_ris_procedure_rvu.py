"""
ETL_JOBS/etl_ris_procedure_rvu.py — RIS PROCEDURE_CODE RVUs -> procedure_duration_map (LAUMC).

Customers asked to see the real clinical RVU next to the technical one in Report 25.
Both columns have existed since migration 0045, but both were seeded from the same
legacy rvu_value and default to 1.0, so they showed the same number twice. The real
values live in the RIS.

SOURCE (discovered 2026-09-18 against the live RIS):
    CSHRIS.PROCEDURE_CODE.PROF_RVU  -> procedure_duration_map.clinical_rvu
    CSHRIS.PROCEDURE_CODE.TECH_RVU  -> procedure_duration_map.technical_rvu

JOIN: SPS_CODE has no link to PROCEDURE_CODE, and PROCEDURE_CODE.DEFAULT_SPS_CODE_KEY is
NULL on most rows, so the bridge is SPS_PLAN (SPS_CODE_KEY <-> PROCEDURE_CODE_KEY,
USE_AS_DEFAULT). RAYD side joins on procedure_duration_map.ris_sps_code_key (migration
0053, populated by etl_ris_procedures.py) rather than matching CODE strings.

    SPS_PLAN ⋈ PROCEDURE_CODE  ON PROCEDURE_CODE_KEY, WHERE USE_AS_DEFAULT = 'Y'
    -> ris_sps_code_key

FAN-OUT: measured 587 SPS codes mapping 1:1 and exactly ONE mapping to 2 procedure
codes even with USE_AS_DEFAULT = 'Y'. A plain join would therefore emit two conflicting
RVUs for that one code and the last write would win at random. ROW_NUMBER picks the most
recently updated row, deterministically. (Same class of bug as the OR-join that inflated
Report 27's CM bar by 9.8% — a 1-in-587 fan-out is still nondeterminism.)

VALUE PARSING: PROF_RVU/TECH_RVU are VARCHAR2(192), not NUMBER. A live check found no
non-numeric junk, but they are parsed defensively anyway — a bad value is skipped, never
written as 0 and never fatal.

IMPORT POLICY (operator decision 2026-09-18): the RIS wins wherever it carries a value
> 0, overwriting manual edits. Rows where the RIS has 0, NULL or unparseable keep
whatever RAYD already holds.

This deliberately INVERTS the fill-only policy that etl_ris_procedures.py applies to the
same table (documented in docs/LAUMC_RIS_TABLES.md as "RAYD-owned RVUs preserved").
That policy assumed RVUs were only ever entered by hand in RAYD; now that the RIS is the
source of truth for them, fill-only would freeze the first value imported and never pick
up a repricing. Note the consequence: hand-edits to clinical_rvu/technical_rvu in the
mapping tab are NOT durable for procedures the RIS prices — they survive only until the
next run.

The "> 0" guard is what keeps this safe. Measured coverage:
    PROF_RVU  populated > 0 on 586 / 597 procedure codes (max 42.09)
    TECH_RVU  populated > 0 on  60 / 597 procedure codes (max  2.59)
Writing TECH_RVU unconditionally would zero the technical RVU on the other 537 and
flatten the Device RVU column plus every efficiency chart in Report 25.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_SPS_PLAN_TABLE  = os.getenv("RAYD_RIS_SPS_PLAN_TABLE", "SPS_PLAN")
_PROC_CODE_TABLE = os.getenv("RAYD_RIS_PROCEDURE_CODE_TABLE", "PROCEDURE_CODE")

# Only touches rows the RIS actually prices, and only the two RVU columns — never
# creates a procedure_duration_map row (that is etl_ris_procedures.py's job) and never
# touches duration_minutes, procedure_name or active.
_UPDATE_SQL = text("""
    UPDATE procedure_duration_map
    SET clinical_rvu  = COALESCE(:clinical_rvu,  clinical_rvu),
        technical_rvu = COALESCE(:technical_rvu, technical_rvu)
    WHERE ris_sps_code_key = :ris_sps_code_key
""")


def _safe_rvu(val):
    """VARCHAR2 -> float, or None for anything that is not a usable positive number.

    None means 'leave whatever RAYD already has' (the COALESCE in _UPDATE_SQL), which is
    what makes a sparse or dirty source column non-destructive.
    """
    if val is None:
        return None
    try:
        num = float(str(val).strip())
    except (TypeError, ValueError):
        return None
    return num if num > 0 else None


def run_ris_procedure_rvu_etl(pg_engine, oracle_source):
    job_name   = "RIS_PROCEDURE_RVU_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None
    clin_n     = 0
    tech_n     = 0

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
        logging.error(f"RIS Procedure RVU ETL log error: {e}")

    # rn = 1 collapses the one SPS code that maps to two procedure codes. Ordering by
    # LAST_UPDATED DESC then PROCEDURE_CODE_KEY DESC makes the choice stable across runs
    # even if both rows share a timestamp.
    query = f"""
        SELECT SPS_CODE_KEY, PROF_RVU, TECH_RVU
        FROM (
            SELECT sp.SPS_CODE_KEY,
                   pc.PROF_RVU,
                   pc.TECH_RVU,
                   ROW_NUMBER() OVER (
                       PARTITION BY sp.SPS_CODE_KEY
                       ORDER BY pc.LAST_UPDATED DESC, pc.PROCEDURE_CODE_KEY DESC
                   ) AS rn
            FROM {_SPS_PLAN_TABLE} sp
            JOIN {_PROC_CODE_TABLE} pc
              ON pc.PROCEDURE_CODE_KEY = sp.PROCEDURE_CODE_KEY
            WHERE sp.USE_AS_DEFAULT = 'Y'
              AND NVL(sp.ACTIVE, 'Y') = 'Y'
        )
        WHERE rn = 1
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info("RIS Procedure RVU ETL starting")
        print(f"[RIS Procedure RVU ETL] 🚀 Starting ({_SPS_PLAN_TABLE} ⋈ {_PROC_CODE_TABLE})")

        cursor.execute(query)
        rows = cursor.fetchall()

        params = []
        for sps_code_key, prof_rvu, tech_rvu in rows:
            if sps_code_key is None:
                skipped += 1
                continue

            clinical  = _safe_rvu(prof_rvu)
            technical = _safe_rvu(tech_rvu)

            # Nothing usable on either side — skip rather than issue a no-op UPDATE.
            if clinical is None and technical is None:
                skipped += 1
                continue

            clin_n += 1 if clinical  is not None else 0
            tech_n += 1 if technical is not None else 0

            params.append({
                "ris_sps_code_key": int(sps_code_key),
                "clinical_rvu":     clinical,
                "technical_rvu":    technical,
            })

        if params:
            with pg_engine.begin() as conn:
                conn.execute(_UPDATE_SQL, params)
            total = len(params)

        status = "SUCCESS"
        print(f"[RIS Procedure RVU ETL] ✅ Done — {total:,} procedures priced "
              f"({clin_n:,} clinical, {tech_n:,} technical), {skipped} skipped (no usable RVU)")
        logging.info(f"RIS Procedure RVU ETL complete: {total:,} priced "
                     f"({clin_n} clinical, {tech_n} technical), {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Procedure RVU ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Procedure RVU log: {le}")

    return total
