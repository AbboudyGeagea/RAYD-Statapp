"""
ETL_JOBS/etl_ris_pps.py — RIS PPS (Performed Procedure Step) -> std_pps (LAUMC), plus
the STUDY_INSTANCE_UID <-> PACS enrichment test.

See migration 0065 for the target table and full design notes. Key points:

*** TECHNURSE_NOTES / DEMONSTRATION_NOTES ARE NEVER FETCHED *** — excluded from the
Oracle SELECT entirely, per operator instruction ("we're not going there yet"), same
category as the patient-names exclusion.

procedure_code/procedure_name resolved via a live SPS_CODE join (matches
procedure_duration_map.procedure_code — same pattern as etl_orders.py).
performing_ae_title resolved via a live MODALITY join — PPS.MODALITY_KEY is the DEVICE
key (MODALITY.MODALITY_KEY), not a modality-type key, so this is more precise than the
modality-type-level resolution used elsewhere; matches aetitle_modality_map.aetitle.

Watermark: created_date >= go_live cutoff, same convention as etl_orders.py — PPS could
be a large table and go-live bounding keeps the initial load reasonable.

The enrichment pass (run_pps_study_enrichment) is the actual empirical test of whether
PPS.STUDY_INSTANCE_UID is the clean RIS<->PACS join docs/LAUMC_NEXT_SESSION.md's blocked
item #4 was after — a real DICOM UID match, not an inferred accession/linked_id one. The
match-rate it reports (rows resolved vs total std_pps rows with a study_instance_uid) is
the actual answer, not a guess.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_PPS_TABLE      = os.getenv("RAYD_RIS_PPS_TABLE", "PPS")
_SPS_CODE_TABLE = os.getenv("RAYD_RIS_SPS_CODE_TABLE", "SPS_CODE")
_MODALITY_TABLE = os.getenv("RAYD_RIS_MODALITY_TABLE", "MODALITY")
_FETCH_BATCH = 1000

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


def _safe_num(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


_UPSERT_PPS_SQL = text("""
    INSERT INTO std_pps (
        pps_key, patient_person_key, performed_procedure_step_id, pps_instance_uid,
        study_instance_uid, pps_code_key, procedure_code, procedure_name, description,
        start_datetime, end_datetime, discontinued_reason, modality_key,
        performing_ae_title, patient_folder_key, report_key, dictation_key, status_key,
        priority_key, status_reason_key, created_by_person_key, created_date,
        worklist_flagset, linked_id, birads_key, created_by_mpps, considered_for_review,
        followup_override, radiation_dose, radiation_dose_units, inv_auto_populate_flag,
        external_reporting_action, cr_message_key, primary_tech_person_key,
        protocol_key, protocol_status_key, last_update
    ) VALUES (
        :pps_key, :patient_person_key, :performed_procedure_step_id, :pps_instance_uid,
        :study_instance_uid, :pps_code_key, :procedure_code, :procedure_name, :description,
        :start_datetime, :end_datetime, :discontinued_reason, :modality_key,
        :performing_ae_title, :patient_folder_key, :report_key, :dictation_key, :status_key,
        :priority_key, :status_reason_key, :created_by_person_key, :created_date,
        :worklist_flagset, :linked_id, :birads_key, :created_by_mpps, :considered_for_review,
        :followup_override, :radiation_dose, :radiation_dose_units, :inv_auto_populate_flag,
        :external_reporting_action, :cr_message_key, :primary_tech_person_key,
        :protocol_key, :protocol_status_key, :last_update
    )
    ON CONFLICT (pps_key) DO UPDATE SET
        patient_person_key = EXCLUDED.patient_person_key,
        performed_procedure_step_id = EXCLUDED.performed_procedure_step_id,
        pps_instance_uid = EXCLUDED.pps_instance_uid,
        study_instance_uid = EXCLUDED.study_instance_uid,
        pps_code_key = EXCLUDED.pps_code_key, procedure_code = EXCLUDED.procedure_code,
        procedure_name = EXCLUDED.procedure_name, description = EXCLUDED.description,
        start_datetime = EXCLUDED.start_datetime, end_datetime = EXCLUDED.end_datetime,
        discontinued_reason = EXCLUDED.discontinued_reason, modality_key = EXCLUDED.modality_key,
        performing_ae_title = EXCLUDED.performing_ae_title,
        patient_folder_key = EXCLUDED.patient_folder_key, report_key = EXCLUDED.report_key,
        dictation_key = EXCLUDED.dictation_key, status_key = EXCLUDED.status_key,
        priority_key = EXCLUDED.priority_key, status_reason_key = EXCLUDED.status_reason_key,
        created_by_person_key = EXCLUDED.created_by_person_key, created_date = EXCLUDED.created_date,
        worklist_flagset = EXCLUDED.worklist_flagset, linked_id = EXCLUDED.linked_id,
        birads_key = EXCLUDED.birads_key, created_by_mpps = EXCLUDED.created_by_mpps,
        considered_for_review = EXCLUDED.considered_for_review,
        followup_override = EXCLUDED.followup_override, radiation_dose = EXCLUDED.radiation_dose,
        radiation_dose_units = EXCLUDED.radiation_dose_units,
        inv_auto_populate_flag = EXCLUDED.inv_auto_populate_flag,
        external_reporting_action = EXCLUDED.external_reporting_action,
        cr_message_key = EXCLUDED.cr_message_key,
        primary_tech_person_key = EXCLUDED.primary_tech_person_key,
        protocol_key = EXCLUDED.protocol_key, protocol_status_key = EXCLUDED.protocol_status_key,
        last_update = EXCLUDED.last_update
""")

# The empirical join test — see module docstring.
_ENRICH_STUDY_UID_SQL = text("""
    UPDATE std_pps p
    SET study_db_uid = s.study_db_uid
    FROM etl_didb_studies s
    WHERE p.study_instance_uid = s.study_instance_uid
      AND p.study_db_uid IS NULL
      AND p.study_instance_uid IS NOT NULL
""")


def run_ris_pps_etl(pg_engine, oracle_source, go_live_date):
    job_name   = "RIS_PPS_ETL"
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
        logging.error(f"RIS PPS ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    # NOTE: TECHNURSE_NOTES / DEMONSTRATION_NOTES are deliberately NOT in this SELECT.
    query = f"""
        SELECT
            p.PPS_KEY, p.PATIENT_PERSON_KEY, p.PERFORMED_PROCEDURE_STEP_ID,
            p.PPS_INSTANCE_UID, p.STUDY_INSTANCE_UID, p.PPS_CODE_KEY,
            sc.CODE, sc.DESCRIPTION,
            p.DESCRIPTION, p.START_DATETIME, p.END_DATETIME, p.DISCONTINUED_REASON,
            p.MODALITY_KEY, m.AE_TITLE,
            p.PATIENT_FOLDER_KEY, p.REPORT_KEY, p.DICTATION_KEY, p.STATUS_KEY,
            p.PRIORITY_KEY, p.STATUS_REASON_KEY, p.CREATED_BY_PERSON_KEY, p.CREATED_DATE,
            p.WORKLIST_FLAGSET, p.LINKED_ID, p.BIRADS_KEY, p.CREATED_BY_MPPS,
            p.CONSIDERED_FOR_REVIEW, p.FOLLOWUP_OVERRIDE, p.RADIATION_DOSE,
            p.RADIATION_DOSE_UNITS, p.INV_AUTO_POPULATE_FLAG, p.EXTERNAL_REPORTING_ACTION,
            p.CR_MESSAGE_KEY, p.PRIMARY_TECH_PERSON_KEY, p.PROTOCOL_KEY, p.PROTOCOL_STATUS_KEY
        FROM {_PPS_TABLE} p
        LEFT JOIN {_SPS_CODE_TABLE} sc ON sc.SPS_CODE_KEY = p.PPS_CODE_KEY
        LEFT JOIN {_MODALITY_TABLE} m ON m.MODALITY_KEY = p.MODALITY_KEY
        WHERE p.CREATED_DATE >= TO_DATE(:cutoff, 'YYYY-MM-DD')
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        logging.info(f"RIS PPS ETL starting — cutoff: {gd_str}")
        print(f"[RIS PPS ETL] 🚀 Starting ({_PPS_TABLE} ⋈ {_SPS_CODE_TABLE} ⋈ {_MODALITY_TABLE}) — "
              f"cutoff: {gd_str} — TECHNURSE_NOTES/DEMONSTRATION_NOTES excluded")

        cursor.execute(query, {"cutoff": gd_str})

        batch_num = 0
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            batch_num += 1

            params = []
            for row in batch:
                (pps_key, patient_person_key, pps_step_id, pps_instance_uid,
                 study_instance_uid, pps_code_key, proc_code, proc_name,
                 description, start_dt, end_dt, discontinued_reason,
                 modality_key, ae_title,
                 patient_folder_key, report_key, dictation_key, status_key,
                 priority_key, status_reason_key, created_by_person_key, created_date,
                 worklist_flagset, linked_id, birads_key, created_by_mpps,
                 considered_for_review, followup_override, radiation_dose,
                 radiation_dose_units, inv_auto_populate_flag, external_reporting_action,
                 cr_message_key, primary_tech_person_key, protocol_key, protocol_status_key) = row

                if pps_key is None:
                    skipped += 1
                    continue

                params.append({
                    "pps_key": pps_key, "patient_person_key": patient_person_key,
                    "performed_procedure_step_id": _safe_str(pps_step_id),
                    "pps_instance_uid": _safe_str(pps_instance_uid),
                    "study_instance_uid": _safe_str(study_instance_uid),
                    "pps_code_key": pps_code_key, "procedure_code": _safe_str(proc_code),
                    "procedure_name": _safe_str(proc_name), "description": _safe_str(description, 4000),
                    "start_datetime": _safe_date(start_dt), "end_datetime": _safe_date(end_dt),
                    "discontinued_reason": _safe_str(discontinued_reason),
                    "modality_key": modality_key, "performing_ae_title": _safe_str(ae_title),
                    "patient_folder_key": patient_folder_key, "report_key": report_key,
                    "dictation_key": dictation_key, "status_key": status_key,
                    "priority_key": priority_key, "status_reason_key": status_reason_key,
                    "created_by_person_key": created_by_person_key,
                    "created_date": _safe_date(created_date),
                    "worklist_flagset": _safe_str(worklist_flagset), "linked_id": linked_id,
                    "birads_key": birads_key, "created_by_mpps": _safe_str(created_by_mpps),
                    "considered_for_review": _safe_str(considered_for_review),
                    "followup_override": _safe_str(followup_override),
                    "radiation_dose": _safe_num(radiation_dose),
                    "radiation_dose_units": _safe_str(radiation_dose_units),
                    "inv_auto_populate_flag": _safe_str(inv_auto_populate_flag),
                    "external_reporting_action": _safe_str(external_reporting_action),
                    "cr_message_key": cr_message_key,
                    "primary_tech_person_key": primary_tech_person_key,
                    "protocol_key": protocol_key, "protocol_status_key": protocol_status_key,
                    "last_update": datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_PPS_SQL, params)
                total += len(params)

            if batch_num % 50 == 0:
                print(f"[RIS PPS ETL] 📦 {total:,} rows loaded")

        status = "SUCCESS"
        print(f"[RIS PPS ETL] ✅ Done — {total:,} PPS rows, {skipped} skipped (no key)")
        logging.info(f"RIS PPS ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS PPS ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS PPS log: {le}")

    return total


def run_pps_study_enrichment(pg_engine):
    """The empirical RIS<->PACS join test — see module docstring. Returns the match count."""
    with pg_engine.begin() as conn:
        result = conn.execute(_ENRICH_STUDY_UID_SQL)
    matched = result.rowcount
    print(f"[RIS PPS ETL] 🔗 STUDY_INSTANCE_UID enrichment: {matched:,} std_pps rows matched to a PACS study")
    logging.info(f"RIS PPS study enrichment: {matched:,} rows matched")
    return matched
