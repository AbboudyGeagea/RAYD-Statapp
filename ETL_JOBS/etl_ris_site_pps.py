"""
ETL_JOBS/etl_ris_site_pps.py — RIS SITE_PPS -> std_site_pps_ext (LAUMC), the per-step
QA/compliance extension table.

See migration 0066 for the target table and full design notes.

*** 6 columns are NEVER FETCHED *** (excluded from the Oracle SELECT entirely) — a name
field and free-text comments, same category as the patient-names / PPS-notes exclusions:
    HOLDER_NAME, RAD_NOTE, CD_BURNED_COMMENT, RADIOLOGIST_REVIEW_COMMENTS,
    DOC_PATIENT_COMPL_COMMENTS, DOC_STAFF_COMPL_COMMENTS

FK safety: rows referencing a pps_key not present in std_pps (yet) are skipped rather
than left to fail the batch — same defensive pattern used throughout (series,
raw-images, patient_ids, etc.). Depends on Phase 14 running std_pps first.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_SITE_PPS_TABLE = os.getenv("RAYD_RIS_SITE_PPS_TABLE", "SITE_PPS")
_FETCH_BATCH = 2000

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
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


_UPSERT_SQL = text("""
    INSERT INTO std_site_pps_ext (
        pps_key, change_room_location, film_reject_number, film_reject_reason,
        filmed_at_exam_time_flag, films_digital_number, films_used, film_retake_number,
        holder_dosimeter, holder_last_menstrual_date, holder_shielded, patient_shielded,
        education, obstetrics_ac_est_due_date_1, holder_pregnant, criticalresultmessage,
        radiologist_present, patient_waittime_indicator, extra_views_complete,
        number_of_projections, number_hard_copies, time_out, cd_burned, image_sent_to,
        image_sent_to_2, image_sent_to_3, cd_burned_coll_deliv, cd_burned_date,
        cd_burned_requested_by, consent_gained, performing_clinician, nrr_ind,
        assigned_to, into_private_folder, doc_patient_complication,
        doc_operator_complication, assigned_to_res, last_update
    ) VALUES (
        :pps_key, :change_room_location, :film_reject_number, :film_reject_reason,
        :filmed_at_exam_time_flag, :films_digital_number, :films_used, :film_retake_number,
        :holder_dosimeter, :holder_last_menstrual_date, :holder_shielded, :patient_shielded,
        :education, :obstetrics_ac_est_due_date_1, :holder_pregnant, :criticalresultmessage,
        :radiologist_present, :patient_waittime_indicator, :extra_views_complete,
        :number_of_projections, :number_hard_copies, :time_out, :cd_burned, :image_sent_to,
        :image_sent_to_2, :image_sent_to_3, :cd_burned_coll_deliv, :cd_burned_date,
        :cd_burned_requested_by, :consent_gained, :performing_clinician, :nrr_ind,
        :assigned_to, :into_private_folder, :doc_patient_complication,
        :doc_operator_complication, :assigned_to_res, :last_update
    )
    ON CONFLICT (pps_key) DO UPDATE SET
        change_room_location = EXCLUDED.change_room_location,
        film_reject_number = EXCLUDED.film_reject_number,
        film_reject_reason = EXCLUDED.film_reject_reason,
        filmed_at_exam_time_flag = EXCLUDED.filmed_at_exam_time_flag,
        films_digital_number = EXCLUDED.films_digital_number, films_used = EXCLUDED.films_used,
        film_retake_number = EXCLUDED.film_retake_number, holder_dosimeter = EXCLUDED.holder_dosimeter,
        holder_last_menstrual_date = EXCLUDED.holder_last_menstrual_date,
        holder_shielded = EXCLUDED.holder_shielded, patient_shielded = EXCLUDED.patient_shielded,
        education = EXCLUDED.education, obstetrics_ac_est_due_date_1 = EXCLUDED.obstetrics_ac_est_due_date_1,
        holder_pregnant = EXCLUDED.holder_pregnant, criticalresultmessage = EXCLUDED.criticalresultmessage,
        radiologist_present = EXCLUDED.radiologist_present,
        patient_waittime_indicator = EXCLUDED.patient_waittime_indicator,
        extra_views_complete = EXCLUDED.extra_views_complete,
        number_of_projections = EXCLUDED.number_of_projections,
        number_hard_copies = EXCLUDED.number_hard_copies, time_out = EXCLUDED.time_out,
        cd_burned = EXCLUDED.cd_burned, image_sent_to = EXCLUDED.image_sent_to,
        image_sent_to_2 = EXCLUDED.image_sent_to_2, image_sent_to_3 = EXCLUDED.image_sent_to_3,
        cd_burned_coll_deliv = EXCLUDED.cd_burned_coll_deliv, cd_burned_date = EXCLUDED.cd_burned_date,
        cd_burned_requested_by = EXCLUDED.cd_burned_requested_by, consent_gained = EXCLUDED.consent_gained,
        performing_clinician = EXCLUDED.performing_clinician, nrr_ind = EXCLUDED.nrr_ind,
        assigned_to = EXCLUDED.assigned_to, into_private_folder = EXCLUDED.into_private_folder,
        doc_patient_complication = EXCLUDED.doc_patient_complication,
        doc_operator_complication = EXCLUDED.doc_operator_complication,
        assigned_to_res = EXCLUDED.assigned_to_res, last_update = EXCLUDED.last_update
""")


def run_ris_site_pps_etl(pg_engine, oracle_source):
    job_name   = "RIS_SITE_PPS_ETL"
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
        logging.error(f"RIS Site PPS ETL log error: {e}")

    with pg_engine.connect() as _c:
        _rows = _c.execute(text("SELECT pps_key FROM std_pps")).fetchall()
    valid_pps_keys = {r[0] for r in _rows}
    logging.info(f"RIS Site PPS ETL: {len(valid_pps_keys):,} valid pps_key values loaded from PG")

    # NOTE: HOLDER_NAME, RAD_NOTE, CD_BURNED_COMMENT, RADIOLOGIST_REVIEW_COMMENTS,
    # DOC_PATIENT_COMPL_COMMENTS, DOC_STAFF_COMPL_COMMENTS are deliberately NOT selected.
    query = f"""
        SELECT
            PPS_KEY, CHANGE_ROOM_LOCATION, FILM_REJECT_NUMBER, FILM_REJECT_REASON,
            FILMED_AT_EXAM_TIME_FLAG, FILMS_DIGITAL_NUMBER, FILMS_USED, FILM_RETAKE_NUMBER,
            HOLDER_DOSIMETER, HOLDER_LAST_MENSTRUAL_DATE, HOLDER_SHIELDED, PATIENT_SHIELDED,
            EDUCATION, OBSTETRICS_AC_EST_DUE_DATE_1, HOLDER_PREGNANT, CRITICALRESULTMESSAGE,
            RADIOLOGIST_PRESENT, PATIENT_WAITTIME_INDICATOR, EXTRA_VIEWS_COMPLETE,
            NUMBER_OF_PROJECTIONS, NUMBER_HARD_COPIES, TIME_OUT, CD_BURNED, IMAGE_SENT_TO,
            IMAGE_SENT_TO_2, IMAGE_SENT_TO_3, CD_BURNED_COLL_DELIV, CD_BURNED_DATE,
            CD_BURNED_REQUESTED_BY, CONSENT_GAINED, PERFORMING_CLINICIAN, NRR_IND,
            ASSIGNED_TO, INTO_PRIVATE_FOLDER, DOC_PATIENT_COMPLICATION,
            DOC_OPERATOR_COMPLICATION, ASSIGNED_TO_RES
        FROM {_SITE_PPS_TABLE}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        print(f"[RIS Site PPS ETL] 🚀 Starting ({_SITE_PPS_TABLE}) — name/free-text columns excluded")
        cursor.execute(query)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (pps_key, change_room, film_reject_num, film_reject_reason,
                 filmed_at_exam, films_digital_num, films_used, film_retake_num,
                 holder_dosimeter, holder_lmp, holder_shielded, patient_shielded,
                 education, ob_due_date, holder_pregnant, critical_msg,
                 rad_present, wait_indicator, extra_views, num_projections,
                 num_hard_copies, time_out, cd_burned, image_sent_to,
                 image_sent_to_2, image_sent_to_3, cd_coll_deliv, cd_date,
                 cd_requested_by, consent, performing_clinician, nrr_ind,
                 assigned_to, private_folder, doc_pt_compl, doc_op_compl,
                 assigned_to_res) = row

                if pps_key is None or pps_key not in valid_pps_keys:
                    skipped += 1
                    continue

                params.append({
                    "pps_key": pps_key, "change_room_location": _safe_str(change_room),
                    "film_reject_number": _safe_str(film_reject_num),
                    "film_reject_reason": _safe_str(film_reject_reason),
                    "filmed_at_exam_time_flag": _safe_str(filmed_at_exam),
                    "films_digital_number": _safe_str(films_digital_num),
                    "films_used": _safe_str(films_used), "film_retake_number": _safe_str(film_retake_num),
                    "holder_dosimeter": _safe_str(holder_dosimeter),
                    "holder_last_menstrual_date": _safe_date(holder_lmp),
                    "holder_shielded": _safe_str(holder_shielded), "patient_shielded": _safe_str(patient_shielded),
                    "education": _safe_str(education), "obstetrics_ac_est_due_date_1": _safe_date(ob_due_date),
                    "holder_pregnant": _safe_str(holder_pregnant), "criticalresultmessage": _safe_str(critical_msg),
                    "radiologist_present": _safe_str(rad_present),
                    "patient_waittime_indicator": _safe_str(wait_indicator),
                    "extra_views_complete": _safe_str(extra_views),
                    "number_of_projections": _safe_str(num_projections),
                    "number_hard_copies": _safe_str(num_hard_copies), "time_out": _safe_str(time_out),
                    "cd_burned": _safe_str(cd_burned), "image_sent_to": _safe_str(image_sent_to),
                    "image_sent_to_2": _safe_str(image_sent_to_2), "image_sent_to_3": _safe_str(image_sent_to_3),
                    "cd_burned_coll_deliv": _safe_str(cd_coll_deliv), "cd_burned_date": _safe_date(cd_date),
                    "cd_burned_requested_by": _safe_str(cd_requested_by), "consent_gained": _safe_str(consent),
                    "performing_clinician": _safe_str(performing_clinician), "nrr_ind": _safe_str(nrr_ind),
                    "assigned_to": _safe_str(assigned_to), "into_private_folder": _safe_str(private_folder),
                    "doc_patient_complication": _safe_str(doc_pt_compl),
                    "doc_operator_complication": _safe_str(doc_op_compl),
                    "assigned_to_res": assigned_to_res, "last_update": datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        status = "SUCCESS"
        print(f"[RIS Site PPS ETL] ✅ Done — {total:,} rows upserted, {skipped} skipped (orphan/no key)")
        logging.info(f"RIS Site PPS ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Site PPS ETL error: {error_msg}")
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
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Site PPS log: {le}")

    return total
