"""
ETL_JOBS/etl_fix_original_ae.py
---------------------------------
One-time backfill: re-fetch ORIGINAL_STORING_AE from Oracle for every study
already in etl_didb_studies and update the PostgreSQL column where the value
differs from what was previously stored (old STORING_AE / corrupted AE title).

Run once after migration 0052_rename_storing_ae_to_original_storing_ae.sql.
Safe to re-run — only rows where Oracle ORIGINAL_STORING_AE != current PG value
are touched.

Invoke via:
    docker compose exec rayd-app python app.py -fix-ae
"""
import os
import sys
import logging
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from sqlalchemy import text
from db import OracleConnector

logger = logging.getLogger("ETL_FIX_ORIGINAL_AE")

ORACLE_FETCH_SIZE = 5_000
PG_INSERT_BATCH   = 1_000


def run_fix_original_ae(pg_engine, go_live_date):
    """
    Must be called within a Flask app context (OracleConnector uses db session
    to read credentials from db_params).

    pg_engine   — SQLAlchemy engine connected to the PostgreSQL rayd_db
    go_live_date — datetime.date or 'YYYY-MM-DD' string; Oracle query lower bound
    """
    start_time = datetime.now()
    gd_str = (
        go_live_date.strftime('%Y-%m-%d')
        if hasattr(go_live_date, 'strftime')
        else str(go_live_date)
    )

    log_id       = None
    total_updated = 0
    status       = "RUNNING"
    error_msg    = None

    # ── Log job start ─────────────────────────────────────────────────────
    try:
        with pg_engine.connect() as conn:
            res = conn.execute(
                text(
                    "INSERT INTO etl_job_log "
                    "(job_name, status, start_time, records_processed) "
                    "VALUES (:n, :s, :t, 0) RETURNING id"
                ),
                {"n": "FIX_ORIGINAL_AE", "s": status, "t": start_time},
            )
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logger.warning(f"[Fix AE] Could not write start log: {e}")

    try:
        # ── Step 1: stream ORIGINAL_STORING_AE from Oracle ────────────────
        logger.info(f"[Fix AE] Querying Oracle ORIGINAL_STORING_AE since {gd_str}")
        print(f"[Fix AE] Connecting to Oracle ...")

        ora_conn = OracleConnector.get_connection()
        cursor   = ora_conn.cursor()
        cursor.execute(
            """
            SELECT STUDY_DB_UID,
                   UPPER(TRIM(ORIGINAL_STORING_AE))
            FROM   medistore.didb_studies
            WHERE  STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
              AND  ORIGINAL_STORING_AE IS NOT NULL
              AND  TRIM(ORIGINAL_STORING_AE) != ' '
            """,
            {"gd": gd_str},
        )

        oracle_data: dict[int, str] = {}
        while True:
            rows = cursor.fetchmany(ORACLE_FETCH_SIZE)
            if not rows:
                break
            for uid, ae in rows:
                oracle_data[uid] = ae
            if len(oracle_data) % 50_000 == 0:
                print(f"[Fix AE]   ... {len(oracle_data):,} Oracle rows fetched")

        cursor.close()
        ora_conn.close()

        oracle_count = len(oracle_data)
        logger.info(f"[Fix AE] Oracle returned {oracle_count:,} rows")
        print(f"[Fix AE] {oracle_count:,} rows fetched from Oracle")

        if not oracle_data:
            print("[Fix AE] Oracle returned no rows — nothing to update.")
            status = "SUCCESS"
            return 0

        # ── Step 2: bulk-stage Oracle values in a PG temp table ──────────
        # Then let PostgreSQL do the mismatch comparison natively —
        # more efficient than comparing in Python for large datasets.
        print(f"[Fix AE] Staging {oracle_count:,} rows into PostgreSQL temp table ...")
        logger.info("[Fix AE] Creating staging temp table")

        with pg_engine.begin() as conn:
            conn.execute(text("""
                CREATE TEMP TABLE _fix_ae_stage (
                    study_db_uid BIGINT PRIMARY KEY,
                    new_ae       TEXT
                ) ON COMMIT DROP
            """))

            items = list(oracle_data.items())
            for i in range(0, len(items), PG_INSERT_BATCH):
                chunk = items[i : i + PG_INSERT_BATCH]
                conn.execute(
                    text(
                        "INSERT INTO _fix_ae_stage (study_db_uid, new_ae) "
                        "VALUES (:uid, :ae)"
                    ),
                    [{"uid": int(uid), "ae": ae} for uid, ae in chunk],
                )
                if (i + PG_INSERT_BATCH) % 20_000 == 0:
                    print(f"[Fix AE]   ... staged {i + PG_INSERT_BATCH:,} rows")

            # ── Step 3: single UPDATE JOIN — only update mismatches ───────
            logger.info("[Fix AE] Executing UPDATE from staging table")
            print("[Fix AE] Running UPDATE — comparing Oracle vs PostgreSQL values ...")

            result = conn.execute(text("""
                UPDATE etl_didb_studies s
                SET    original_storing_ae = t.new_ae
                FROM   _fix_ae_stage t
                WHERE  s.study_db_uid = t.study_db_uid
                  AND  COALESCE(s.original_storing_ae, '') != COALESCE(t.new_ae, '')
            """))
            total_updated = result.rowcount

        logger.info(f"[Fix AE] Done — {total_updated:,} rows updated")
        print(f"[Fix AE] ✅ Complete — {total_updated:,} rows corrected "
              f"(out of {oracle_count:,} Oracle rows checked)")
        status = "SUCCESS"

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logger.error(f"[Fix AE] Failed: {e}")
        print(f"[Fix AE] ❌ Failed: {e}")
        raise

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                with pg_engine.connect() as conn:
                    conn.execute(
                        text(
                            "UPDATE etl_job_log "
                            "SET status=:s, end_time=:et, records_processed=:r, "
                            "    duration_seconds=:d, error_message=:e "
                            "WHERE id=:id"
                        ),
                        {
                            "s":   status,
                            "et":  datetime.now(),
                            "r":   total_updated,
                            "d":   round(duration, 2),
                            "e":   error_msg,
                            "id":  log_id,
                        },
                    )
                    conn.commit()
            except Exception as le:
                logger.warning(f"[Fix AE] Could not write end log: {le}")

    return total_updated
