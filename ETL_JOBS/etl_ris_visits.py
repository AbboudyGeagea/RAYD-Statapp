"""
ETL_JOBS/etl_ris_visits.py — RIS VISIT -> std_visits (LAUMC).

See migration 0059 for the target table and why it lives in the main etl_db rather than
a separate provisioned database. Column map is 1:1 (target names = lowercased source
column names), per docs/LAUMC_RIS_TABLES.md's VISIT section:

    VISIT_NUMBER = HL7 PV1.19 (links live ADT/ORM messages to this table)
    DELETED='Y' rows are imported, not dropped — vendor guidance excludes them from
        STATS (any future query over std_visits should filter deleted != 'Y'), not
        from the load itself.
    patient_class_key / financial_class_key / hospital_service_key / mobility_status_key
        are pulled RAW (unresolved key, not label) — their lookup tables haven't been
        provided yet.
    site_id is NOT resolved here — VISIT carries no org/issuer column of its own; that
        would need a join through etl_orders.visit_dbid, left for a later enrichment
        pass rather than guessed at now.

Watermark: MAX(visit_number) already loaded (fixed 2026-07-30, operator instruction),
plus the go-live cutoff for the initial fresh load only. NOTE: visit_number is stored
as TEXT (it's the raw HL7 PV1.19 value) -- if it's ever a non-zero-padded numeric
string of varying length, a textual MAX() could pick the wrong "highest" value. Not
verified against real data; flagging here rather than silently assuming it's safe.
"""
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db import OracleConnector

_VISIT_TABLE = os.getenv("RAYD_RIS_VISIT_TABLE", "VISIT")
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


_UPSERT_SQL = text("""
    INSERT INTO std_visits (
        visit_key, patient_person_key, patient_class_key, preadmit_number, visit_number,
        financial_class_key, admit_date_time, discharge_date_time,
        expected_admit_date_time, expected_discharge_date_time, visit_description,
        visit_priority_key, hospital_service_key, visit_indicator,
        issuer_of_visit_number, issuer_of_preadmit_number, alternate_visit_id,
        mobility_status_key, created_by_person_key, created_on_date,
        patient_account_number, is_master, deleted, deleted_date, last_update
    ) VALUES (
        :visit_key, :patient_person_key, :patient_class_key, :preadmit_number, :visit_number,
        :financial_class_key, :admit_date_time, :discharge_date_time,
        :expected_admit_date_time, :expected_discharge_date_time, :visit_description,
        :visit_priority_key, :hospital_service_key, :visit_indicator,
        :issuer_of_visit_number, :issuer_of_preadmit_number, :alternate_visit_id,
        :mobility_status_key, :created_by_person_key, :created_on_date,
        :patient_account_number, :is_master, :deleted, :deleted_date, :last_update
    )
    ON CONFLICT (visit_key) DO UPDATE SET
        patient_person_key           = EXCLUDED.patient_person_key,
        patient_class_key            = EXCLUDED.patient_class_key,
        preadmit_number               = EXCLUDED.preadmit_number,
        visit_number                  = EXCLUDED.visit_number,
        financial_class_key           = EXCLUDED.financial_class_key,
        admit_date_time                = EXCLUDED.admit_date_time,
        discharge_date_time            = EXCLUDED.discharge_date_time,
        expected_admit_date_time       = EXCLUDED.expected_admit_date_time,
        expected_discharge_date_time   = EXCLUDED.expected_discharge_date_time,
        visit_description              = EXCLUDED.visit_description,
        visit_priority_key             = EXCLUDED.visit_priority_key,
        hospital_service_key           = EXCLUDED.hospital_service_key,
        visit_indicator                = EXCLUDED.visit_indicator,
        issuer_of_visit_number         = EXCLUDED.issuer_of_visit_number,
        issuer_of_preadmit_number      = EXCLUDED.issuer_of_preadmit_number,
        alternate_visit_id             = EXCLUDED.alternate_visit_id,
        mobility_status_key            = EXCLUDED.mobility_status_key,
        created_by_person_key          = EXCLUDED.created_by_person_key,
        created_on_date                = EXCLUDED.created_on_date,
        patient_account_number         = EXCLUDED.patient_account_number,
        is_master                      = EXCLUDED.is_master,
        deleted                        = EXCLUDED.deleted,
        deleted_date                   = EXCLUDED.deleted_date,
        last_update                    = EXCLUDED.last_update
""")


def run_ris_visits_etl(pg_engine, oracle_source, go_live_date):
    job_name   = "RIS_VISITS_ETL"
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
        logging.error(f"RIS Visits ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    try:
        with pg_engine.connect() as conn:
            watermark = conn.execute(text("SELECT MAX(visit_number) FROM std_visits")).fetchone()[0]
    except Exception as e:
        logging.warning(f"RIS Visits ETL: could not read watermark, falling back to full pull: {e}")
        watermark = None

    is_fresh_load = not watermark

    query = f"""
        SELECT
            VISIT_KEY, PATIENT_PERSON_KEY, PATIENT_CLASS_KEY, PREADMIT_NUMBER, VISIT_NUMBER,
            FINANCIAL_CLASS_KEY, ADMIT_DATE_TIME, DISCHARGE_DATE_TIME,
            EXPECTED_ADMIT_DATE_TIME, EXPECTED_DISCHARGE_DATE_TIME, VISIT_DESCRIPTION,
            VISIT_PRIORITY_KEY, HOSPITAL_SERVICE_KEY, VISIT_INDICATOR,
            ISSUER_OF_VISIT_NUMBER, ISSUER_OF_PREADMIT_NUMBER, ALTERNATE_VISIT_ID,
            MOBILITY_STATUS_KEY, CREATED_BY_PERSON_KEY, CREATED_ON_DATE,
            PATIENT_ACCOUNT_NUMBER, IS_MASTER, DELETED, DELETED_DATE
        FROM {_VISIT_TABLE}
        WHERE CREATED_ON_DATE >= TO_DATE(:cutoff, 'YYYY-MM-DD')
        {"" if is_fresh_load else "AND VISIT_NUMBER > :watermark"}
    """

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        mode_str = f"fresh load, cutoff: {gd_str}" if is_fresh_load else f"incremental, watermark visit_number>{watermark}"
        logging.info(f"RIS Visits ETL starting — {mode_str}")
        print(f"[RIS Visits ETL] 🚀 Starting ({_VISIT_TABLE}) — {mode_str}")

        params = {"cutoff": gd_str}
        if not is_fresh_load:
            params["watermark"] = watermark
        cursor.execute(query, params)

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break

            params = []
            for row in batch:
                (visit_key, patient_person_key, patient_class_key, preadmit_number, visit_number,
                 financial_class_key, admit_dt, discharge_dt, exp_admit_dt, exp_discharge_dt,
                 visit_description, visit_priority_key, hospital_service_key, visit_indicator,
                 issuer_visit, issuer_preadmit, alt_visit_id, mobility_status_key,
                 created_by_person_key, created_on_date, patient_account_number, is_master,
                 deleted, deleted_date) = row

                if visit_key is None:
                    skipped += 1
                    continue

                params.append({
                    "visit_key":                    visit_key,
                    "patient_person_key":           patient_person_key,
                    "patient_class_key":             patient_class_key,
                    "preadmit_number":                _safe_str(preadmit_number),
                    "visit_number":                    _safe_str(visit_number),
                    "financial_class_key":             financial_class_key,
                    "admit_date_time":                  _safe_date(admit_dt),
                    "discharge_date_time":              _safe_date(discharge_dt),
                    "expected_admit_date_time":          _safe_date(exp_admit_dt),
                    "expected_discharge_date_time":      _safe_date(exp_discharge_dt),
                    "visit_description":                 _safe_str(visit_description),
                    "visit_priority_key":                 visit_priority_key,
                    "hospital_service_key":               hospital_service_key,
                    "visit_indicator":                     _safe_str(visit_indicator),
                    "issuer_of_visit_number":               _safe_str(issuer_visit),
                    "issuer_of_preadmit_number":             _safe_str(issuer_preadmit),
                    "alternate_visit_id":                     _safe_str(alt_visit_id),
                    "mobility_status_key":                     mobility_status_key,
                    "created_by_person_key":                    created_by_person_key,
                    "created_on_date":                           _safe_date(created_on_date),
                    "patient_account_number":                     _safe_str(patient_account_number),
                    "is_master":                                   _safe_str(is_master),
                    "deleted":                                      _safe_str(deleted),
                    "deleted_date":                                  _safe_date(deleted_date),
                    "last_update":                                    datetime.now(),
                })

            if params:
                with pg_engine.begin() as conn:
                    conn.execute(_UPSERT_SQL, params)
                total += len(params)

        status = "SUCCESS"
        print(f"[RIS Visits ETL] ✅ Done — {total:,} visits upserted, {skipped} skipped (no visit_key)")
        logging.info(f"RIS Visits ETL complete: {total:,} rows, {skipped} skipped")

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"RIS Visits ETL error: {error_msg}")
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
                logging.error(f"Failed to update RIS Visits log: {le}")

    return total
