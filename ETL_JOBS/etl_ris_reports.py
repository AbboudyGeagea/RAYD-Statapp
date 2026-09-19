"""
ETL_JOBS/etl_ris_reports.py — RIS REPORT -> std_reports (LAUMC).

See migration 0119 for the target schema and the full "why this table exists" note.
Short version: Report 36's KPI Detailed Reading needs Exam Done -> Signed 1 -> Approved,
and two of those three legs exist nowhere else. PACS REP_FINAL_* is 100% empty at RH,
SITE_WORKLIST.APPROVED_DATE is NULL on all 43,566 approved exams, and
worklist_status_history is empty. The RIS REPORT table carries the whole chain:
VERIFIED1_DATE ("Signed 1") 116,519 rows and APPROVED_DATE 64,766 over Jan-Aug 2026,
with a VERIFIED1 -> APPROVED median of 18.7h against a p90 of 69.7h.

Column map (docs/LAUMC_RIS_TABLES.md, vendor-resolved 2026-07-07):
    REPORT_KEY + VERSION        -> (report_key, version)   PK; one row per version
    IS_MAX_VERSION              -> is_max_version          filter for the live report
    REPORTED_ACC_NUMBER         -> reported_acc_number     = accession = SPS_ID
    VERIFIED1/2/3_DATE          -> verified1/2/3_date      = Signed 1 / 2 / 3
    APPROVED_DATE               -> approved_date           = Approved
    *_BY_RESOURCE_ID_KEY        -> same                    -> std_resources_ris

IMPORT POLICY (vendor answer Qr3): pull all date/status/people columns RAW. No
PACS<->RIS status interpretation happens here -- the report layer decides what a
timestamp means. Refresh-on-conflict: every column is RIS-owned, there are no
RAYD-authored fields on this table to protect.

NOT PULLED: DOCUMENT_PLAIN_TEXT / DOCUMENT_TEXT / DOCUMENT / PDF_DOCUMENT. Qr4 confirms
DOCUMENT_PLAIN_TEXT is the eventual NLP/CRN feed, but this loader is the TAT chain only
-- a multi-KB text column on every one of ~134k versions would multiply the table for a
consumer that does not exist yet. Adding it later is additive.

WATERMARK: LAST_MODIFIED_DATE, not MAX(report_key). REPORT_KEY is NOT monotonic with
load order -- an amended report gets a new sequence (Qr2, and see the watermark comment
in etl_ris_pps.py which calls this file out by name). A key-based watermark would
silently skip amendments forever. The lookback below covers rows whose LAST_MODIFIED_DATE
is backdated relative to when they became visible.
"""
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

_REPORT_TABLE = os.getenv("RAYD_RIS_REPORT_TABLE", "REPORT")
_FETCH_BATCH  = 2000

# Re-pull anything modified in the last N days on every incremental run. Cheap
# insurance against clock skew and backdated edits; the upsert makes it idempotent.
_LOOKBACK_DAYS = 3

_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)


def _safe_date(val):
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
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    try:
        s = str(val).strip()
    except TypeError:
        # Same defensive path as etl_ris_pps_lookups.py: a column that turns out to be
        # a LOB raises rather than stringifying.
        return None
    if not s:
        return None
    return s[:max_len] if max_len else s


def _safe_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_num(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_bool(val):
    """IS_MAX_VERSION's type is not documented — accept Y/N, T/F, 1/0 and booleans."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).strip().upper() in ('Y', 'YES', 'TRUE', 'T', '1')


_UPSERT_SQL = text("""
    INSERT INTO std_reports (
        report_key, version, is_max_version,
        reported_acc_number, body_key, cr_message_key, report_template_key,
        version_status_key, finalization_state, interpretation_type_key, addendum,
        draft_date, wet_read_date, transcription_date,
        verified1_date, verified2_date, verified3_date, approved_date,
        reviewed_date, returned_date, report_time, report_created_date, last_modified_date,
        reported_by, transcribed_by_resource_id_key,
        verified1_by_resource_id_key, verified2_by_resource_id_key,
        verified3_by_resource_id_key, approved_by_resource_id_key,
        created_by_resource_id_key, last_modified_resource_id_key,
        signed_behalf_resource_id_key, wet_read_by_resource_id_key,
        reviewed_by_resource_id_key, returned_by_resource_id_key,
        draft_by_resource_id_key, report_to,
        character_count, word_count, line_count, total_lines_in_document,
        minutes_of_editing_for_session,
        source_last_updated, last_update
    ) VALUES (
        :report_key, :version, :is_max_version,
        :reported_acc_number, :body_key, :cr_message_key, :report_template_key,
        :version_status_key, :finalization_state, :interpretation_type_key, :addendum,
        :draft_date, :wet_read_date, :transcription_date,
        :verified1_date, :verified2_date, :verified3_date, :approved_date,
        :reviewed_date, :returned_date, :report_time, :report_created_date, :last_modified_date,
        :reported_by, :transcribed_by_resource_id_key,
        :verified1_by_resource_id_key, :verified2_by_resource_id_key,
        :verified3_by_resource_id_key, :approved_by_resource_id_key,
        :created_by_resource_id_key, :last_modified_resource_id_key,
        :signed_behalf_resource_id_key, :wet_read_by_resource_id_key,
        :reviewed_by_resource_id_key, :returned_by_resource_id_key,
        :draft_by_resource_id_key, :report_to,
        :character_count, :word_count, :line_count, :total_lines_in_document,
        :minutes_of_editing_for_session,
        :source_last_updated, :last_update
    )
    ON CONFLICT (report_key, version) DO UPDATE SET
        is_max_version = EXCLUDED.is_max_version,
        reported_acc_number = EXCLUDED.reported_acc_number,
        body_key = EXCLUDED.body_key, cr_message_key = EXCLUDED.cr_message_key,
        report_template_key = EXCLUDED.report_template_key,
        version_status_key = EXCLUDED.version_status_key,
        finalization_state = EXCLUDED.finalization_state,
        interpretation_type_key = EXCLUDED.interpretation_type_key,
        addendum = EXCLUDED.addendum,
        draft_date = EXCLUDED.draft_date, wet_read_date = EXCLUDED.wet_read_date,
        transcription_date = EXCLUDED.transcription_date,
        verified1_date = EXCLUDED.verified1_date, verified2_date = EXCLUDED.verified2_date,
        verified3_date = EXCLUDED.verified3_date, approved_date = EXCLUDED.approved_date,
        reviewed_date = EXCLUDED.reviewed_date, returned_date = EXCLUDED.returned_date,
        report_time = EXCLUDED.report_time, report_created_date = EXCLUDED.report_created_date,
        last_modified_date = EXCLUDED.last_modified_date,
        reported_by = EXCLUDED.reported_by,
        transcribed_by_resource_id_key = EXCLUDED.transcribed_by_resource_id_key,
        verified1_by_resource_id_key = EXCLUDED.verified1_by_resource_id_key,
        verified2_by_resource_id_key = EXCLUDED.verified2_by_resource_id_key,
        verified3_by_resource_id_key = EXCLUDED.verified3_by_resource_id_key,
        approved_by_resource_id_key = EXCLUDED.approved_by_resource_id_key,
        created_by_resource_id_key = EXCLUDED.created_by_resource_id_key,
        last_modified_resource_id_key = EXCLUDED.last_modified_resource_id_key,
        signed_behalf_resource_id_key = EXCLUDED.signed_behalf_resource_id_key,
        wet_read_by_resource_id_key = EXCLUDED.wet_read_by_resource_id_key,
        reviewed_by_resource_id_key = EXCLUDED.reviewed_by_resource_id_key,
        returned_by_resource_id_key = EXCLUDED.returned_by_resource_id_key,
        draft_by_resource_id_key = EXCLUDED.draft_by_resource_id_key,
        report_to = EXCLUDED.report_to,
        character_count = EXCLUDED.character_count, word_count = EXCLUDED.word_count,
        line_count = EXCLUDED.line_count,
        total_lines_in_document = EXCLUDED.total_lines_in_document,
        minutes_of_editing_for_session = EXCLUDED.minutes_of_editing_for_session,
        source_last_updated = EXCLUDED.source_last_updated,
        last_update = EXCLUDED.last_update
""")

# Site enrichment. REPORT carries no org/site column of its own, so site is inherited
# from the exam via the accession. etl_orders.accession_number is the same SPS_ID value
# REPORTED_ACC_NUMBER holds, and etl_orders.site_id is already resolved by the orders
# ETL. Max-version rows only (Qr2: an amended version carries a NEW accession sequence,
# so a non-current version can point at the wrong exam).
_ENRICH_SITE_SQL = text("""
    UPDATE std_reports r
    SET    site_id = o.site_id
    FROM   etl_orders o
    WHERE  o.accession_number = r.reported_acc_number
      AND  r.reported_acc_number IS NOT NULL
      AND  r.is_max_version
      AND  r.site_id IS NULL
      AND  o.site_id IS NOT NULL
""")


def run_ris_reports_etl(pg_engine, oracle_source, go_live_date):
    job_name   = "RIS_REPORTS_ETL"
    start_time = datetime.now()
    total      = 0
    skipped    = 0
    status     = "RUNNING"
    error_msg  = None
    log_id     = None

    try:
        with pg_engine.connect() as conn:
            log_id = conn.execute(
                text("INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                     "VALUES (:n, :s, :t, 0) RETURNING id"),
                {"n": job_name, "s": status, "t": start_time}
            ).fetchone()[0]
            conn.commit()
    except Exception as e:
        logging.error(f"RIS Reports ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(
                text("SELECT MAX(last_modified_date) FROM std_reports")
            ).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Reports ETL: could not read watermark, full pull: {e}")
        watermark = None

    is_fresh_load = watermark is None
    if is_fresh_load:
        wm_str = gd_str + " 00:00:00"
    else:
        wm_str = (watermark - timedelta(days=_LOOKBACK_DAYS)).strftime('%Y-%m-%d %H:%M:%S')

    # COALESCE(LAST_MODIFIED_DATE, REPORT_CREATED_DATE): a report never edited since
    # creation can carry a NULL LAST_MODIFIED_DATE, and filtering on that column alone
    # would drop those permanently once the watermark passes their creation date. Same
    # guard etl_orders.py applies to w.LAST_UPDATE_DATE.
    query = f"""
        SELECT
            r.REPORT_KEY, r.VERSION, r.IS_MAX_VERSION,
            r.REPORTED_ACC_NUMBER, r.BODY_KEY, r.CR_MESSAGE_KEY, r.REPORT_TEMPLATE_KEY,
            r.VERSION_STATUS_KEY, r.FINALIZATION_STATE, r.INTERPRETATION_TYPE_KEY, r.ADDENDUM,
            r.DRAFT_DATE, r.WET_READ_DATE, r.TRANSCRIPTION_DATE,
            r.VERIFIED1_DATE, r.VERIFIED2_DATE, r.VERIFIED3_DATE, r.APPROVED_DATE,
            r.REVIEWED_DATE, r.RETURNED_DATE, r.REPORT_TIME, r.REPORT_CREATED_DATE,
            r.LAST_MODIFIED_DATE,
            r.REPORTED_BY, r.TRANSCRIBED_BY_RESOURCE_ID_KEY,
            r.VERIFIED1_BY_RESOURCE_ID_KEY, r.VERIFIED2_BY_RESOURCE_ID_KEY,
            r.VERIFIED3_BY_RESOURCE_ID_KEY, r.APPROVED_BY_RESOURCE_ID_KEY,
            r.CREATED_BY_RESOURCE_ID_KEY, r.LAST_MODIFIED_RESOURCE_ID_KEY,
            r.SIGNED_BEHALF_RESOURCE_ID_KEY, r.WET_READ_BY_RESOURCE_ID_KEY,
            r.REVIEWED_BY_RESOURCE_ID_KEY, r.RETURNED_BY_RESOURCE_ID_KEY,
            r.DRAFT_BY_RESOURCE_ID_KEY, r.REPORT_TO,
            r.CHARACTER_COUNT, r.WORD_COUNT, r.LINE_COUNT, r.TOTAL_LINES_IN_DOCUMENT,
            r.MINUTES_OF_EDITING_FOR_SESSION
        FROM {_REPORT_TABLE} r
        WHERE COALESCE(r.LAST_MODIFIED_DATE, r.REPORT_CREATED_DATE)
                  >= TO_TIMESTAMP(:wm, 'YYYY-MM-DD HH24:MI:SS')
          AND COALESCE(r.REPORT_CREATED_DATE, r.LAST_MODIFIED_DATE)
                  >= TO_DATE(:gd, 'YYYY-MM-DD')
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        mode = "FULL BACKFILL" if is_fresh_load else "INCREMENTAL"
        print(f"[RIS Reports ETL] 🚀 Starting ({_REPORT_TABLE}) — mode: {mode}, watermark: {wm_str}")
        logging.info(f"RIS Reports ETL starting — mode: {mode}, watermark: {wm_str}")

        cursor.execute(query, {"wm": wm_str, "gd": gd_str})

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (report_key, version, is_max, acc, body_key, cr_msg, tmpl,
                 ver_status, finalization, interp_type, addendum,
                 draft_d, wet_d, trans_d, v1_d, v2_d, v3_d, appr_d,
                 rev_d, ret_d, rep_time, created_d, last_mod,
                 reported_by, transcribed_by, v1_by, v2_by, v3_by, appr_by,
                 created_by, last_mod_by, signed_behalf, wet_by,
                 rev_by, ret_by, draft_by, report_to,
                 char_cnt, word_cnt, line_cnt, total_lines, edit_mins) = row

                if report_key is None:
                    skipped += 1
                    continue

                params.append({
                    "report_key": _safe_int(report_key),
                    # VERSION is part of the PK — a NULL would break the upsert, and 0
                    # is the natural "unversioned" value.
                    "version": _safe_int(version) or 0,
                    "is_max_version": _safe_bool(is_max),
                    "reported_acc_number": _safe_str(acc, 64),
                    "body_key": _safe_int(body_key),
                    "cr_message_key": _safe_int(cr_msg),
                    "report_template_key": _safe_int(tmpl),
                    "version_status_key": _safe_int(ver_status),
                    "finalization_state": _safe_str(finalization, 64),
                    "interpretation_type_key": _safe_int(interp_type),
                    "addendum": _safe_str(addendum, 64),
                    "draft_date": _safe_date(draft_d),
                    "wet_read_date": _safe_date(wet_d),
                    "transcription_date": _safe_date(trans_d),
                    "verified1_date": _safe_date(v1_d),
                    "verified2_date": _safe_date(v2_d),
                    "verified3_date": _safe_date(v3_d),
                    "approved_date": _safe_date(appr_d),
                    "reviewed_date": _safe_date(rev_d),
                    "returned_date": _safe_date(ret_d),
                    "report_time": _safe_date(rep_time),
                    "report_created_date": _safe_date(created_d),
                    "last_modified_date": _safe_date(last_mod),
                    "reported_by": _safe_int(reported_by),
                    "transcribed_by_resource_id_key": _safe_int(transcribed_by),
                    "verified1_by_resource_id_key": _safe_int(v1_by),
                    "verified2_by_resource_id_key": _safe_int(v2_by),
                    "verified3_by_resource_id_key": _safe_int(v3_by),
                    "approved_by_resource_id_key": _safe_int(appr_by),
                    "created_by_resource_id_key": _safe_int(created_by),
                    "last_modified_resource_id_key": _safe_int(last_mod_by),
                    "signed_behalf_resource_id_key": _safe_int(signed_behalf),
                    "wet_read_by_resource_id_key": _safe_int(wet_by),
                    "reviewed_by_resource_id_key": _safe_int(rev_by),
                    "returned_by_resource_id_key": _safe_int(ret_by),
                    "draft_by_resource_id_key": _safe_int(draft_by),
                    "report_to": _safe_str(report_to, 256),
                    "character_count": _safe_int(char_cnt),
                    "word_count": _safe_int(word_cnt),
                    "line_count": _safe_int(line_cnt),
                    "total_lines_in_document": _safe_int(total_lines),
                    "minutes_of_editing_for_session": _safe_num(edit_mins),
                    "source_last_updated": _safe_date(last_mod),
                    "last_update": datetime.now(),
                })

            if params:
                # Two versions of the same report can land in one batch; executemany
                # runs each row as its own statement so ON CONFLICT handles that, but
                # only because the PK includes version.
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        with pg_engine.begin() as conn:
            enriched = conn.execute(_ENRICH_SITE_SQL).rowcount

        status = "SUCCESS"
        print(f"[RIS Reports ETL] ✅ Done — {total:,} report versions upserted, "
              f"{skipped} skipped (no REPORT_KEY), {enriched:,} site-enriched")
        logging.info(f"RIS Reports ETL complete: {total:,} versions, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Reports ETL error: {error_msg}")
        raise

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        if log_id:
            try:
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, error_message=:e WHERE id=:i"),
                        {"s": status, "et": datetime.now(), "r": total,
                         "e": error_msg, "i": log_id}
                    )
                    conn.commit()
            except Exception as e:
                logging.error(f"RIS Reports ETL log close error: {e}")

    return total
