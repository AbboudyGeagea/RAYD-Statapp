"""
ETL_JOBS/etl_ris_reports.py
---------------------------
Phase 9 (LAUMC dual-source): pull finalized radiology reports from the RIS
`REPORT` table into PostgreSQL `hl7_oru_reports`.

Why this exists
    At LAUMC the live HL7 ORU stream that PACS emits is ENCRYPTED, so it cannot
    feed NLP / the critical-findings log / full-text search. The RIS keeps the
    same reports as clean plain text in REPORT.DOCUMENT_PLAIN_TEXT. This job makes
    that plain text the authoritative body of hl7_oru_reports, while the live ORU
    listener (hl7_listener.py) keeps filling the same rows in real time for anything
    the batch hasn't caught up to yet. Both converge on ONE row per accession thanks
    to the unique index added in migration 0054.

Source columns (RIS REPORT — see docs/LAUMC_RIS_TABLES.md):
    REPORTED_ACC_NUMBER          -> accession_number  (= SPS_ID = PACS accession)
    DOCUMENT_PLAIN_TEXT          -> report_text       (full labelled body; CLOB)
      "IMPRESSION:" section of ^ -> impression_text   (extracted in Python)
    APPROVED_DATE (final sign)   -> result_datetime   (COALESCE w/ LAST_MODIFIED_DATE)
    APPROVED_BY_RESOURCE_ID_KEY  -> physician_id       (RIS resource key, as text)
    IS_MAX_VERSION = current     -> filter (current version only; amendments overwrite)
    LAST_MODIFIED_DATE           -> incremental watermark (future optimisation)

site_id / patient_id / modality are NOT in REPORT; they are enriched after upsert by
matching accession_number to the already-loaded PACS study (etl_didb_studies +
etl_patient_view), exactly as migration 0049 describes for ORU rows.

Config (env):
    RAYD_RIS_REPORT_TABLE   fully-qualified RIS table/synonym (default: REPORT)
The RIS connection itself is a db_params row selected by name (default 'oracle_RIS',
override with RAYD_RIS_SOURCE) — see etl_runner Phase 9.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

logger = logging.getLogger("ETL_WORKER")

_RIS_REPORT_TABLE = os.getenv("RAYD_RIS_REPORT_TABLE", "REPORT")
_FETCH_BATCH = 500

# ── hl7_oru_reports upsert ───────────────────────────────────────────────────
# RIS plain text is authoritative -> it always wins on report_text/impression_text.
# result_datetime / physician_id only FILL when the live ORU hasn't already set them.
_UPSERT_SQL = text("""
    INSERT INTO hl7_oru_reports
        (accession_number, report_text, impression_text, result_datetime,
         physician_id, received_at, report_source)
    VALUES
        (:accession_number, :report_text, :impression_text, :result_datetime,
         :physician_id, :received_at, 'ris')
    ON CONFLICT (accession_number) DO UPDATE SET
        report_text     = EXCLUDED.report_text,
        impression_text = EXCLUDED.impression_text,
        result_datetime = COALESCE(EXCLUDED.result_datetime, hl7_oru_reports.result_datetime),
        physician_id    = COALESCE(EXCLUDED.physician_id,    hl7_oru_reports.physician_id),
        received_at     = EXCLUDED.received_at,
        report_source   = 'ris'
""")

# Enrich site_id / patient_id / modality from the matching PACS study by accession.
# Fill-only (COALESCE) so a value already resolved by ORU enrichment is kept. SR studies
# are excluded per the project-wide SR rule. Safe no-op when studies aren't loaded yet.
_ENRICH_SQL = text("""
    UPDATE hl7_oru_reports o
    SET site_id    = COALESCE(o.site_id,    s.site_id),
        patient_id = COALESCE(o.patient_id, pv.patient_id),
        modality   = COALESCE(o.modality,   s.study_modality)
    FROM etl_didb_studies s
    LEFT JOIN etl_patient_view pv ON pv.patient_db_uid = s.patient_db_uid
    WHERE o.accession_number = s.accession_number
      AND COALESCE(s.study_modality, '') != 'SR'
      AND o.report_source = 'ris'
      AND (o.site_id IS NULL OR o.patient_id IS NULL OR o.modality IS NULL)
""")


def _extract_impression(body):
    """Return the text after an 'IMPRESSION' heading, or None. Per vendor (Qr4) the RIS
    plain text carries labelled sections (INDICATION / TECHNIQUE / FINDINGS / IMPRESSION)."""
    if not body:
        return None
    upper = body.upper()
    idx = upper.find("IMPRESSION")
    if idx == -1:
        return None
    seg = body[idx:]
    colon = seg.find(":")
    seg = seg[colon + 1:] if colon != -1 else seg[len("IMPRESSION"):]
    return seg.strip() or None


def run_ris_reports_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    """Pull current-version RIS reports since go_live and upsert into hl7_oru_reports.
    `pg_table` is 'hl7_oru_reports'; `chunked_upsert_func` is unused (custom upsert)."""
    job_name   = "RIS_REPORTS_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

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
        logging.error(f"RIS Reports ETL log error: {e}")

    # IS_MAX_VERSION type varies (NUMBER 1/0 or CHAR 'Y'/'N'); TO_CHAR handles both.
    query = f"""
        SELECT
            REPORTED_ACC_NUMBER,
            DOCUMENT_PLAIN_TEXT,
            COALESCE(APPROVED_DATE, LAST_MODIFIED_DATE, REPORT_TIME),
            APPROVED_BY_RESOURCE_ID_KEY
        FROM {_RIS_REPORT_TABLE}
        WHERE TO_CHAR(IS_MAX_VERSION) IN ('1', 'Y', 'y', 'T', 'TRUE')
          AND REPORTED_ACC_NUMBER  IS NOT NULL
          AND DOCUMENT_PLAIN_TEXT  IS NOT NULL
          AND COALESCE(APPROVED_DATE, LAST_MODIFIED_DATE) >= TO_DATE(:cutoff, 'YYYY-MM-DD')
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    # Fetch CLOB (DOCUMENT_PLAIN_TEXT) directly as str instead of a LOB locator.
    try:
        ora_conn.fetch_lobs = False
    except Exception:
        pass
    cursor = ora_conn.cursor()

    try:
        logger.info(f"[RIS Reports] 🚀 Pulling current-version reports since {gd_str} from {_RIS_REPORT_TABLE}")
        cursor.execute(query, {"cutoff": gd_str})

        now = datetime.now()
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break

            params = []
            for acc, body, result_dt, phys in batch:
                acc = (str(acc).strip() if acc is not None else None)
                if not acc or not body:
                    skipped += 1
                    continue
                params.append({
                    "accession_number": acc,
                    "report_text":      str(body),
                    "impression_text":  _extract_impression(str(body)),
                    "result_datetime":  result_dt,
                    "physician_id":     (str(phys).strip() if phys is not None else None),
                    "received_at":      now,
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)
                if (total // _FETCH_BATCH) % 5 == 0:
                    logger.info(f"[RIS Reports] 📦 {total:,} reports upserted")

        # Enrich site/patient/modality from the loaded PACS studies (by accession).
        with pg_engine.begin() as conn:
            enr = conn.execute(_ENRICH_SQL)
        logger.info(f"[RIS Reports] 🔗 site/patient/modality enriched on {enr.rowcount:,} rows")

        status = "SUCCESS"
        logger.info(f"[RIS Reports] ✅ Done — {total:,} reports upserted, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Reports ETL error: {error_msg}")
        raise

    finally:
        cursor.close()
        ora_conn.close()
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rps      = round(total / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, null_alerts=:na, "
                             "error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "rps": rps,
                         "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Reports log: {le}")

    return total
