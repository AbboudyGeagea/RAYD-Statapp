"""
ETL_JOBS/etl_orders.py — Orders, sourced from the RIS (LAUMC), not PACS.

WHY: LAUMC's PACS-side MEDILINK.MDB_ORDERS has messier data than the RIS. Per an explicit
operator decision, etl_orders is now populated from the RIS worklist instead. The table
name and every column consumers already read (order_dbid, patient_dbid, study_db_uid,
proc_id, proc_text, scheduled_datetime, order_status, modality, has_study, order_control,
visit_dbid, study_instance_uid) are UNCHANGED — this is a source swap, not a rewrite of
the 11+ report files that join on this table. Two columns were added (migration 0057:
accession_number, linked_id) purely to support the enrichment pass below.

GRAIN: etl_orders' existing columns (modality, proc_id, scheduled_datetime) are per-EXAM
attributes, which is SITE_WORKLIST grain (1 row per SPS), not ORDERS grain (1 row per
order header, which can bundle several exams). So SITE_WORKLIST drives this query, LEFT
JOINed to ORDERS (site filter + header context) and SPS_CODE (the real procedure code
text, which SITE_WORKLIST only carries as a key).

    SITE_WORKLIST  ⋈ ORDERS     ON  order_key           (site issuer filter lives here)
    SITE_WORKLIST  ⋈ SPS_CODE   ON  sps_code_key         (procedure_duration_map join key)

SITE FILTER (mandatory, vendor-confirmed): issuer_of_placer_order_number IN
('SAP_PROD','SAP_SJH') — the RIS holds other sites; this is both site scope and junk
filter.

order_status TRANSLATION: RIS status codes (~45, collapsing to canonical stages via
migrations/0047_worklist_status_map.sql) are translated to the short PACS-style codes
several report files already hardcode comparisons against ('CA' for cancelled/
discontinued; 'CM' for anything at exam_done or later in the report chain). Anything
earlier in the pipeline (requested/scheduled/arrived/in_progress) passes through as its
raw canonical stage name — strictly more informative than PACS ever provided, and never
collides with 'CA'/'CM'. An unmapped status_key (a brand-new RIS status RAYD hasn't seen)
resolves to NULL rather than silently guessing — surfaced, not hidden, per 0047's own
documented policy.

STUDY_DB_UID ENRICHMENT: the RIS doesn't carry PACS's study_db_uid, so it can't be
resolved at extract time — the RIS<->PACS study join is a separate, POST-load pass (this
mirrors the site's own documented "extract independently, one idempotent enrichment
pass" ETL design; see docs/LAUMC_NEXT_SESSION.md). Two-stage, both idempotent (only
touch rows where study_db_uid IS NULL, safe to re-run):
  1. Primary: SITE_WORKLIST.sps_id = etl_didb_studies.accession_number (vendor-confirmed
     direct join for the common single-SPS-per-study case).
  2. Fallback: when several SPS rows share a linked_id (e.g. CT abdomen + pelvis
     scheduled together) and only SOME of them match an accession directly, the
     unmatched siblings inherit whichever match their linked_id group already found.
     This is a best-effort approximation — the exact PACS-side grouping column
     (WORKITEM_DB_UID, confirmed to exist alongside IS_LINKED_STUDY on
     medistore.didb_studies) isn't wired into the PACS extract yet, which would let this
     be exact instead of inferred. Fine for now; tighten later if it proves inaccurate.

PATIENT IDENTITY: patient_dbid is the RIS's own patient_person_key (operator decision —
PACS patient identity is intentionally NOT reconciled here).
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

# Earliest and latest safe dates Oracle can handle reliably
_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)

_ORDERS_TABLE   = os.getenv("RAYD_RIS_ORDERS_TABLE", "ORDERS")
_WORKLIST_TABLE = os.getenv("RAYD_RIS_WORKLIST_TABLE", "SITE_WORKLIST")
_SPS_CODE_TABLE = os.getenv("RAYD_RIS_SPS_CODE_TABLE", "SPS_CODE")

_FETCH_BATCH = 1000


def _safe_date(val):
    """Sanitize a date/datetime value coming from Oracle. None for anything corrupt/out of range."""
    if val is None:
        return None
    try:
        dt = val if isinstance(val, datetime) else datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S')
        if dt.year == 0 or not (_SAFE_DATE_MIN <= dt <= _SAFE_DATE_MAX):
            return None
        return dt
    except Exception:
        return None


def _safe_str(val, max_len=None):
    """Strip and truncate strings, return None for empty."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _load_status_map(pg_engine):
    """Preload RIS status_key -> {stage, is_cancel} from worklist_status_map (migration
    0047). Small table (~40 rows); loaded once per run, same pattern as the valid-ID
    whitelists other ETL jobs preload from Postgres before the Oracle loop. Returns an
    empty dict (never raises) if the table is missing, so a missing lookup degrades
    order_status to NULL instead of crashing the whole job."""
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT status_key, stage, is_cancel FROM worklist_status_map"
            )).fetchall()
        return {r[0]: {"stage": r[1], "is_cancel": r[2]} for r in rows}
    except Exception as e:
        logging.warning(f"Orders ETL: could not load worklist_status_map ({e}) — order_status will be NULL")
        return {}


_CM_STAGES = {"exam_done", "dictated", "prelim", "signed", "approved"}


def _translate_order_status(status_key, status_map):
    """RIS status_key -> short PACS-style code, per the module docstring's rule table."""
    if status_key is None:
        return None
    info = status_map.get(status_key)
    if not info:
        return None
    if info["is_cancel"] or info["stage"] == "discontinued":
        return "CA"
    if info["stage"] in _CM_STAGES:
        return "CM"
    return info["stage"]


# Enrichment pass — see module docstring. Two UPDATEs, both idempotent (guarded by
# study_db_uid IS NULL), run once after the main upsert commits.
_ENRICH_PRIMARY_SQL = text("""
    UPDATE etl_orders eo
    SET study_db_uid        = s.study_db_uid,
        study_instance_uid  = s.study_instance_uid,
        has_study           = TRUE
    FROM etl_didb_studies s
    WHERE eo.accession_number = s.accession_number
      AND eo.study_db_uid IS NULL
      AND eo.accession_number IS NOT NULL
""")

_ENRICH_LINKED_FALLBACK_SQL = text("""
    UPDATE etl_orders eo
    SET study_db_uid        = sib.study_db_uid,
        study_instance_uid  = sib.study_instance_uid,
        has_study           = TRUE
    FROM (
        SELECT DISTINCT ON (linked_id) linked_id, study_db_uid, study_instance_uid
        FROM etl_orders
        WHERE linked_id IS NOT NULL AND study_db_uid IS NOT NULL
        ORDER BY linked_id, order_dbid
    ) sib
    WHERE eo.linked_id = sib.linked_id
      AND eo.study_db_uid IS NULL
""")


def run_orders_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date):
    job_name   = "ORDERS_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    col_names = [
        'order_dbid', 'patient_dbid', 'study_db_uid', 'visit_dbid',
        'study_instance_uid', 'proc_id', 'proc_text', 'scheduled_datetime',
        'order_status', 'modality', 'has_study', 'order_control',
        'accession_number', 'linked_id', 'last_update'
    ]

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
        logging.error(f"Orders ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)
    status_map = _load_status_map(pg_engine)

    query = f"""
        SELECT
            w.SITE_WORKLIST_KEY,
            w.PATIENT_PERSON_KEY,
            w.VISIT_KEY,
            w.SPS_ID,
            COALESCE(sc.CODE, TO_CHAR(w.SPS_CODE_KEY))       AS PROC_CODE,
            COALESCE(sc.DESCRIPTION, w.DESCRIPTION)          AS PROC_TEXT,
            w.SCHEDULED_DATE,
            w.STATUS_KEY,
            w.MODALITY_TYPE,
            o.ISSUER_OF_PLACER_ORDER_NUMBER,
            w.LINKED_ID,
            w.LAST_UPDATE_DATE
        FROM {_WORKLIST_TABLE} w
        JOIN {_ORDERS_TABLE} o     ON o.ORDER_KEY = w.ORDER_KEY
        LEFT JOIN {_SPS_CODE_TABLE} sc ON sc.SPS_CODE_KEY = w.SPS_CODE_KEY
        WHERE o.ISSUER_OF_PLACER_ORDER_NUMBER IN ('SAP_PROD','SAP_SJH')
          AND o.CREATED_ON_DATE >= TO_DATE(:cutoff, 'YYYY-MM-DD')
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info(f"Orders ETL (RIS) starting — cutoff: {gd_str}")
        print(f"[Orders ETL] 🚀 Starting (RIS SITE_WORKLIST ⋈ ORDERS ⋈ SPS_CODE) — cutoff: {gd_str}")

        cursor.execute(query, {"cutoff": gd_str})

        batch_num = 0
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            batch_num += 1

            clean_batch = []
            for row in batch:
                (site_worklist_key, patient_person_key, visit_key, sps_id,
                 proc_code, proc_text, scheduled_date, status_key,
                 modality_type, issuer, linked_id, last_update_date) = row

                if site_worklist_key is None:
                    skipped += 1
                    continue

                clean_batch.append((
                    site_worklist_key,                        # order_dbid (PK)
                    _safe_str(patient_person_key),             # patient_dbid — RIS identity, by design
                    None,                                       # study_db_uid — resolved by enrichment pass
                    _safe_str(visit_key),                       # visit_dbid
                    None,                                       # study_instance_uid — resolved by enrichment pass
                    _safe_str(proc_code),                       # proc_id — matches procedure_duration_map.procedure_code
                    _safe_str(proc_text, 4000),                 # proc_text
                    _safe_date(scheduled_date),                 # scheduled_datetime
                    _translate_order_status(status_key, status_map),  # order_status
                    _safe_str(modality_type),                   # modality
                    False,                                       # has_study — set TRUE by enrichment pass
                    _safe_str(issuer),                          # order_control — raw RIS site issuer (traceability)
                    _safe_str(sps_id),                          # accession_number — enrichment join key
                    linked_id,                                  # linked_id — enrichment fallback key
                    _safe_date(last_update_date) or datetime.now(),  # last_update
                ))

            if clean_batch:
                chunked_upsert_func(pg_engine, pg_table, col_names, clean_batch, 'order_dbid')
                total += len(clean_batch)

            if batch_num % 50 == 0:
                print(f"[Orders ETL] 📦 {total:,} rows loaded{f', {skipped} skipped' if skipped else ''}")

        # Enrichment pass — resolve study_db_uid now that both sides are loaded.
        with pg_engine.begin() as conn:
            primary = conn.execute(_ENRICH_PRIMARY_SQL)
        with pg_engine.begin() as conn:
            fallback = conn.execute(_ENRICH_LINKED_FALLBACK_SQL)
        print(f"[Orders ETL] 🔗 study_db_uid resolved on {primary.rowcount:,} rows directly, "
              f"{fallback.rowcount:,} more via linked_id fallback")

        status = "SUCCESS"
        print(f"[Orders ETL] ✅ Done — {total:,} rows | {skipped} skipped (null PK)")
        logging.info(f"Orders ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"Orders ETL error: {error_msg}")
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
                logging.error(f"Failed to update orders log: {le}")

    return total
