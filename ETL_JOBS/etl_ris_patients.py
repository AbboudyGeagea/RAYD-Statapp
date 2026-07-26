"""
ETL_JOBS/etl_ris_patients.py — RIS PATIENT/PERSON/PATIENT_ID_LIST ->
std_patients_ris / std_patient_ids (LAUMC), plus an age_at_study enrichment pass.

See migration 0060 for the target tables and design notes.

*** NO PATIENT NAMES *** — explicit operator instruction. PERSON's name columns are
never selected, and PATIENT_ALIAS (entirely name data) is not loaded at all — with
names excluded there is nothing left in it worth keeping.

Two source tables, two grains, loaded in dependency order in one job (one Oracle
connection, sequential extracts) since std_patient_ids FK-references std_patients_ris:
    1. PATIENT ⋈ PERSON  (1 row/patient)   -> std_patients_ris
    2. PATIENT_ID_LIST    (N rows/patient)   -> std_patient_ids

No date/whitelist filter on either — master/reference data, not transactional, and
PERSON carries no obviously correct watermark column to bound it by.

GENDER_KEY is resolved to its short code at load time via a hardcoded lookup (small,
vendor-provided, 2026-07-27 — not expected to change often; revisit if LAUMC's RIS ever
reports an unmapped key, which resolves to NULL rather than a guess):
    1=F Female, 2=M Male, 4=U Unknown, 3206=I Intermediate, 3207=0 Not known,
    3208=NSP Not Specified, 3226=O Other, 3266=A Ambiguous.
LANGUAGE_KEY is still pulled RAW — no lookup provided for it yet.

FK safety: PATIENT_ID_LIST rows referencing a patient_person_key not present in
std_patients_ris are skipped rather than left to fail the batch on an FK violation —
same defensive pattern used for series/raw-images/image-locations orphan checks.

age_at_study enrichment (final step): neither PACS nor RIS stores a trustworthy
per-study age on its own — PACS's age_at_exam can be wrong (e.g. ER quick-registration
placeholder DOBs). Computed here instead from std_patients_ris.birth_date via the
study<->patient bridge etl_orders already provides (study_db_uid -> the study,
patient_dbid -> patient_person_key). Idempotent UPDATE, safe to re-run any time either
side changes — order-independent: if Phase 6 (Orders) hasn't run yet, this just updates
zero rows harmlessly rather than failing.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_PATIENT_TABLE    = os.getenv("RAYD_RIS_PATIENT_TABLE", "PATIENT")
_PERSON_TABLE     = os.getenv("RAYD_RIS_PERSON_TABLE", "PERSON")
_PATIENT_ID_TABLE = os.getenv("RAYD_RIS_PATIENT_ID_TABLE", "PATIENT_ID_LIST")

_FETCH_BATCH = 2000

_SAFE_DATE_MIN = datetime(1900, 1, 1)
_SAFE_DATE_MAX = datetime(9999, 12, 31)

# Vendor-provided GENDER_KEY lookup (2026-07-27). Unmapped keys resolve to None
# (surfaced as unknown, not guessed) rather than silently defaulting to something.
_GENDER_CODE_MAP = {
    1: 'F', 2: 'M', 4: 'U',
    3206: 'I', 3207: '0', 3208: 'NSP', 3226: 'O', 3266: 'A',
}


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


_UPSERT_PATIENT_SQL = text("""
    INSERT INTO std_patients_ris (
        patient_person_key, birth_date, birth_time, gender_key, gender_code,
        mobile_phone_number, pager_number, primary_email_address, secondary_email_address,
        language_key, multiindexref, edi_location_number, edi_send,
        preferred_delivery_method_key, death_date, death_time, death_indicator,
        worklist_flagset, permission_level, rcopia_patient_last_updated,
        rcopia_allergies_last_updated, rcopia_medication_last_updated, deleted,
        deleted_date, person_last_updated, last_update
    ) VALUES (
        :patient_person_key, :birth_date, :birth_time, :gender_key, :gender_code,
        :mobile_phone_number, :pager_number, :primary_email_address, :secondary_email_address,
        :language_key, :multiindexref, :edi_location_number, :edi_send,
        :preferred_delivery_method_key, :death_date, :death_time, :death_indicator,
        :worklist_flagset, :permission_level, :rcopia_patient_last_updated,
        :rcopia_allergies_last_updated, :rcopia_medication_last_updated, :deleted,
        :deleted_date, :person_last_updated, :last_update
    )
    ON CONFLICT (patient_person_key) DO UPDATE SET
        birth_date = EXCLUDED.birth_date, birth_time = EXCLUDED.birth_time,
        gender_key = EXCLUDED.gender_key, gender_code = EXCLUDED.gender_code,
        mobile_phone_number = EXCLUDED.mobile_phone_number, pager_number = EXCLUDED.pager_number,
        primary_email_address = EXCLUDED.primary_email_address,
        secondary_email_address = EXCLUDED.secondary_email_address,
        language_key = EXCLUDED.language_key, multiindexref = EXCLUDED.multiindexref,
        edi_location_number = EXCLUDED.edi_location_number, edi_send = EXCLUDED.edi_send,
        preferred_delivery_method_key = EXCLUDED.preferred_delivery_method_key,
        death_date = EXCLUDED.death_date, death_time = EXCLUDED.death_time,
        death_indicator = EXCLUDED.death_indicator, worklist_flagset = EXCLUDED.worklist_flagset,
        permission_level = EXCLUDED.permission_level,
        rcopia_patient_last_updated = EXCLUDED.rcopia_patient_last_updated,
        rcopia_allergies_last_updated = EXCLUDED.rcopia_allergies_last_updated,
        rcopia_medication_last_updated = EXCLUDED.rcopia_medication_last_updated,
        deleted = EXCLUDED.deleted, deleted_date = EXCLUDED.deleted_date,
        person_last_updated = EXCLUDED.person_last_updated, last_update = EXCLUDED.last_update
""")

_UPSERT_ID_SQL = text("""
    INSERT INTO std_patient_ids (
        patient_id_list_key, patient_person_key, patient_id, description, is_primary,
        sequence_id, issuer_of_pid_key, display_sort_order, last_update
    ) VALUES (
        :patient_id_list_key, :patient_person_key, :patient_id, :description, :is_primary,
        :sequence_id, :issuer_of_pid_key, :display_sort_order, :last_update
    )
    ON CONFLICT (patient_id_list_key) DO UPDATE SET
        patient_id = EXCLUDED.patient_id, description = EXCLUDED.description,
        is_primary = EXCLUDED.is_primary, sequence_id = EXCLUDED.sequence_id,
        issuer_of_pid_key = EXCLUDED.issuer_of_pid_key,
        display_sort_order = EXCLUDED.display_sort_order, last_update = EXCLUDED.last_update
""")

# age_at_study enrichment — see module docstring. FLOOR(days/365.25), matching the
# same formula the PACS-side age_at_exam already uses in etl_didb_studies.py.
# etl_orders.patient_dbid is TEXT (holds the RIS patient_person_key as a string); the
# CASE guards the ::BIGINT cast so one malformed value can't abort the whole UPDATE —
# a non-numeric patient_dbid just fails to join (excluded), not a hard error.
#
# age_at_study is NUMERIC(5,2) (migration 0061) — max magnitude 999.99. A placeholder/
# sentinel birth_date (the same "quick-registration DOB" data-quality issue this module
# was built to work around in the first place — see docstring above) produces a
# negative or absurd age that overflows the column and aborted the WHOLE ETL run
# (NumericValueOutOfRange, live LAUMC 2026-07-26). Bound the computed age to a plausible
# human range so one bad birth_date is skipped, not a hard failure.
_AGE_AT_STUDY_SQL = text("""
    UPDATE etl_didb_studies s
    SET age_at_study = FLOOR((s.study_date - pr.birth_date) / 365.25)
    FROM etl_orders eo
    JOIN std_patients_ris pr
        ON pr.patient_person_key = CASE WHEN eo.patient_dbid ~ '^[0-9]+$'
                                         THEN eo.patient_dbid::BIGINT END
    WHERE eo.study_db_uid = s.study_db_uid
      AND pr.birth_date IS NOT NULL
      AND s.study_date IS NOT NULL
      AND FLOOR((s.study_date - pr.birth_date) / 365.25) BETWEEN 0 AND 130
""")


def run_ris_patients_etl(pg_engine, oracle_source):
    job_name   = "RIS_PATIENTS_ETL"
    start_time = datetime.now()
    total_patients = 0
    total_ids       = 0
    skipped          = 0
    status           = "RUNNING"
    error_msg        = None
    log_id           = None

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
        logging.error(f"RIS Patients ETL log error: {e}")

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        # ── 1. PATIENT ⋈ PERSON (no name columns selected) ──────────────────
        logging.info("RIS Patients ETL starting")
        print(f"[RIS Patients ETL] 🚀 Starting ({_PATIENT_TABLE} ⋈ {_PERSON_TABLE}) — names excluded by design")

        patient_query = f"""
            SELECT
                p.PATIENT_PERSON_KEY,
                per.BIRTH_DATE, per.BIRTH_TIME, per.GENDER_KEY, per.MOBILE_PHONE_NUMBER,
                per.PAGER_NUMBER, per.PRIMARY_EMAIL_ADDRESS, per.SECONDARY_EMAIL_ADDRESS,
                per.LANGUAGE_KEY, per.MULTIINDEXREF, per.EDI_LOCATION_NUMBER,
                per.EDI_SEND, per.PREFERRED_DELIVERY_METHOD_KEY,
                p.DEATH_DATE, p.DEATH_TIME, p.DEATH_INDICATOR, p.WORKLIST_FLAGSET,
                p.PERMISSION_LEVEL, p.RCOPIA_PATIENT_LAST_UPDATED,
                p.RCOPIA_ALLERGIES_LAST_UPDATED, p.RCOPIA_MEDICATION_LAST_UPDATED,
                per.DELETED, per.DELETED_DATE, per.LAST_UPDATED
            FROM {_PATIENT_TABLE} p
            JOIN {_PERSON_TABLE} per ON per.PERSON_KEY = p.PATIENT_PERSON_KEY
        """
        cursor.execute(patient_query)

        valid_patient_keys = set()
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (ppk, birth_date, birth_time, gender_key, mobile, pager,
                 email1, email2, language_key, multiindexref, edi_loc, edi_send,
                 pref_delivery_key, death_date, death_time, death_indicator,
                 worklist_flagset, permission_level, rcopia_pt, rcopia_allerg,
                 rcopia_med, deleted, deleted_date, last_updated) = row

                if ppk is None:
                    skipped += 1
                    continue
                valid_patient_keys.add(ppk)

                gender_code = None
                if gender_key is not None:
                    gender_code = _GENDER_CODE_MAP.get(int(gender_key))
                    if gender_code is None:
                        logging.warning(f"RIS Patients ETL: unmapped GENDER_KEY={gender_key}")

                params.append({
                    "patient_person_key": ppk,
                    "birth_date": _safe_date(birth_date), "birth_time": _safe_str(birth_time),
                    "gender_key": gender_key, "gender_code": gender_code,
                    "mobile_phone_number": _safe_str(mobile), "pager_number": _safe_str(pager),
                    "primary_email_address": _safe_str(email1),
                    "secondary_email_address": _safe_str(email2), "language_key": language_key,
                    "multiindexref": _safe_str(multiindexref), "edi_location_number": _safe_str(edi_loc),
                    "edi_send": _safe_str(edi_send), "preferred_delivery_method_key": pref_delivery_key,
                    "death_date": _safe_date(death_date), "death_time": _safe_str(death_time),
                    "death_indicator": _safe_str(death_indicator),
                    "worklist_flagset": _safe_str(worklist_flagset),
                    "permission_level": _safe_str(permission_level),
                    "rcopia_patient_last_updated": _safe_date(rcopia_pt),
                    "rcopia_allergies_last_updated": _safe_date(rcopia_allerg),
                    "rcopia_medication_last_updated": _safe_date(rcopia_med),
                    "deleted": _safe_str(deleted), "deleted_date": _safe_date(deleted_date),
                    "person_last_updated": _safe_date(last_updated),
                    "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_PATIENT_SQL, params)
                total_patients += len(params)

        print(f"[RIS Patients ETL] ✅ Patients: {total_patients:,} upserted, {skipped} skipped (no key)")

        # ── 2. PATIENT_ID_LIST (MRNs etc. — no names here either) ───────────
        id_query = f"""
            SELECT PATIENT_ID_LIST_KEY, PATIENT_PERSON_KEY, DESCRIPTION, PATIENT_ID,
                   "PRIMARY", SEQUENCE_ID, ISSUER_OF_PID_KEY, DISPLAY_SORT_ORDER
            FROM {_PATIENT_ID_TABLE}
        """
        cursor.execute(id_query)

        id_skipped = 0
        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            params = []
            for row in batch:
                (id_key, ppk, description, patient_id, is_primary, seq_id,
                 issuer_pid, sort_order) = row
                if id_key is None or ppk not in valid_patient_keys:
                    id_skipped += 1
                    continue
                params.append({
                    "patient_id_list_key": id_key, "patient_person_key": ppk,
                    "patient_id": _safe_str(patient_id), "description": _safe_str(description),
                    "is_primary": _safe_str(is_primary), "sequence_id": seq_id,
                    "issuer_of_pid_key": _safe_str(issuer_pid), "display_sort_order": sort_order,
                    "last_update": datetime.now(),
                })
            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_ID_SQL, params)
                total_ids += len(params)

        print(f"[RIS Patients ETL] ✅ Patient IDs: {total_ids:,} upserted, {id_skipped} skipped (orphan/no key)")
        skipped += id_skipped

        # ── 3. age_at_study enrichment ───────────────────────────────────────
        with pg_engine.begin() as conn:
            age_result = conn.execute(_AGE_AT_STUDY_SQL)
        print(f"[RIS Patients ETL] 🔗 age_at_study computed for {age_result.rowcount:,} studies")

        status = "SUCCESS"
        logging.info(
            f"RIS Patients ETL complete: {total_patients:,} patients, {total_ids:,} ids, "
            f"{age_result.rowcount:,} ages computed, {skipped} total skipped"
        )

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Patients ETL error: {error_msg}")
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
                        {"s": status, "et": end_time, "r": total_patients,
                         "d": round(duration, 2), "na": skipped, "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update RIS Patients log: {le}")

    return total_patients
