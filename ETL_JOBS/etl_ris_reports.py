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
    LAST_MODIFIED_DATE           -> incremental watermark

Incremental load: an amended report gets a brand-new REPORT_KEY/version rather than an
in-place update (docs/LAUMC_RIS_TABLES.md, Qr2), so a plain "max already-loaded key"
watermark isn't reliable here -- REPORT_KEY isn't exposed by this query at all, and even
if it were, an amendment's new row can sort anywhere relative to prior keys. Instead this
tracks MAX(result_datetime) already loaded from RIS and, like etl_didb_studies.py, adds a
lookback window so a report that gets amended/re-approved after this job last saw it is
still picked up even though its underlying study is older than the watermark.

site_id / patient_id / modality are NOT in REPORT; they are enriched after upsert by
matching accession_number to the already-loaded PACS study (etl_didb_studies +
etl_patient_view), exactly as migration 0049 describes for ORU rows.

Config (env):
    RAYD_RIS_REPORT_TABLE   fully-qualified RIS table/synonym (default: REPORT)
The RIS connection itself is a db_params row selected by name (default 'ris',
override with RAYD_RIS_SOURCE) — see etl_runner Phase 9.
"""
import os
import logging
from datetime import datetime, timedelta
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
# Fill-only (COALESCE) so a value already resolved by ORU enrichment is kept. Safe no-op
# when studies aren't loaded yet.
#
# patient_id source fixed 2026-07-27: previously etl_patient_view.id — a PACS-internal
# column, confirmed by the operator to surface placeholder values (e.g. "PIX_xxxxx"),
# not the real hospital MRN. The correct source is RIS's own std_patient_ids (migration
# 0060, PATIENT_ID_LIST) — confirmed against real data that is_primary reliably marks
# exactly one authoritative row per patient regardless of what that ID's value looks
# like. Reached via etl_orders.patient_dbid (= RIS patient_person_key, by existing
# design — see ETL_JOBS/etl_orders.py), matched on accession_number directly (etl_orders
# already carries its own accession_number column, no need to go through
# etl_didb_studies for this specific join). Existing wrong values already loaded are
# corrected separately by migrations/0081 — this only changes the source for reports
# enriched from here on.
#
# Modality source fixed 2026-09-04: an accession commonly has ONLY an SR-classified
# row in etl_didb_studies (the auto-generated structured-report object — see the
# project-wide SR rule), with no companion row for the real acquisition. The previous
# version filtered such rows out of the join entirely (`s.study_modality != 'SR'` in
# the WHERE), which also blocked site_id/patient_id enrichment on the same accession
# and left modality permanently NULL — confirmed against live data: 63k+ of ~75k
# blank-modality RIS reports were stuck this way. Fix: pick ONE study row per
# accession via LATERAL (preferring a non-SR row when both exist, so we never lose
# real data to ambiguity), then resolve modality through aetitle_modality_map keyed
# on that row's storing_ae — same AE/station regardless of whether PACS also emitted
# an SR object for it — per the project-wide "prefer aetitle_modality_map over
# study_modality" convention. Raw study_modality is only the last-resort fallback,
# and NULLIF'd against 'SR' so that literal value can never land in the column.
_ENRICH_SQL = text("""
    UPDATE hl7_oru_reports o
    SET site_id    = COALESCE(o.site_id,    s.site_id),
        patient_id = COALESCE(o.patient_id, pid.patient_id),
        modality   = COALESCE(o.modality,   m.modality, NULLIF(s.study_modality, 'SR'))
    FROM LATERAL (
        SELECT *
        FROM etl_didb_studies s0
        WHERE s0.accession_number = o.accession_number
        ORDER BY (COALESCE(s0.study_modality, '') = 'SR')
        LIMIT 1
    ) s
    LEFT JOIN LATERAL (
        SELECT modality
        FROM aetitle_modality_map
        WHERE aetitle = s.storing_ae
        LIMIT 1
    ) m ON true
    LEFT JOIN LATERAL (
        SELECT eo.patient_dbid
        FROM etl_orders eo
        WHERE eo.accession_number = s.accession_number
          AND eo.patient_dbid ~ '^[0-9]+$'
        ORDER BY eo.last_update DESC NULLS LAST
        LIMIT 1
    ) eo ON true
    LEFT JOIN LATERAL (
        SELECT patient_id
        FROM std_patient_ids
        WHERE patient_person_key = eo.patient_dbid::bigint
          AND UPPER(is_primary) = 'Y'
        LIMIT 1
    ) pid ON true
    WHERE o.report_source = 'ris'
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

    # Incremental watermark: newest result_datetime already loaded from RIS. None on a
    # fresh install (table empty / no report_source='ris' rows yet) -> full pull since
    # go_live_date, same as before.
    watermark = None
    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text(
                "SELECT MAX(result_datetime) FROM hl7_oru_reports WHERE report_source = 'ris'"
            )).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Reports ETL: could not read watermark, falling back to full pull: {e}")

    is_fresh_load = watermark is None
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    # IS_MAX_VERSION type varies (NUMBER 1/0 or CHAR 'Y'/'N'); TO_CHAR handles both.
    base_query = f"""
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
        if is_fresh_load:
            logger.info(f"[RIS Reports] 🆕 Fresh load — pulling ALL reports since {gd_str} from {_RIS_REPORT_TABLE}")
            cursor.execute(base_query, {"cutoff": gd_str})
        else:
            logger.info(
                f"[RIS Reports] 🔄 Incremental load — watermark={watermark}, "
                f"lookback={lookback_date}, table={_RIS_REPORT_TABLE}"
            )
            cursor.execute(
                base_query + """
                  AND (
                      COALESCE(APPROVED_DATE, LAST_MODIFIED_DATE, REPORT_TIME) > :watermark
                      OR COALESCE(APPROVED_DATE, LAST_MODIFIED_DATE, REPORT_TIME) >= TO_DATE(:lb, 'YYYY-MM-DD')
                  )
                """,
                {"cutoff": gd_str, "watermark": watermark, "lb": lookback_date}
            )

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
