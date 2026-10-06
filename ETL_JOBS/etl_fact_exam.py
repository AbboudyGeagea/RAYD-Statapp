"""
ETL_JOBS/etl_fact_exam.py — PACS medistore.didb_studies -> fact_exam (LAUMC, phase 20).

See migration 0125 for the table and the counting rules. Two steps per run:

  1. Extract: upsert the raw columns of every study since go-live (fresh load) or of
     new / recent studies (incremental: STUDY_DB_UID above the max loaded, or
     STUDY_DATE inside the 10-day lookback -- the same window etl_didb_studies.py
     uses). No AE / signer filtering here: exclusions are recorded in `reason`, not
     dropped, so a linked partner stored under an excluded AE still reaches its group.

  2. Derive: recompute own_modality .. reason for every work-item group with a row
     upserted since its last derivation, reading the WHOLE group from fact_exam -- so
     an SR-only partner loaded today still finds the imaging study it was linked to
     last week.

The rules mirror the Oracle validation query handed to the operator on 2026-10-06
("query 0"); its COUNTED total for a period is the acceptance test for this table.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

_TABLE       = "fact_exam"
_FETCH_BATCH = 2000
_LOOKBACK_DAYS = 10

# Same lists as ETL_JOBS/etl_didb_studies.py's _EXCLUDED_AE_SQL (the cardiology
# echo/cath/angio devices, and the LAUMC/SVSM duplicate copies). Kept separately here
# because this table records them as a `reason` instead of dropping the rows.
_CARDIO_DEVICES = (
    'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
    'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01',
)
_DUPLICATE_DEVICES = ('LAUMC', 'SVSM')

# Bone densitometry (GE Lunar) stores its studies as OT only, so the token cleaning
# below would leave it with no modality and drop it as "SR / OT only". Its modality
# has to come from the device -- same AEs migration 0120 maps to BMD. Counted here
# (operator, 2026-10-06), unlike RAYD's existing reports, which exclude BMD.
_BMD_DEVICES = ('GELUNAR', 'GELUNAR11')

# PACS STUDY_MODALITY tokens that are objects, not exams.
_NON_EXAM_TOKENS = ('SR', 'OT', 'PR', 'KO', 'DOC', 'SC', 'REG', 'SEG', 'FID', 'RWV', 'PLAN')

# When one study holds two real modalities, it counts once, under the first of these
# it contains (PET/CT -> PT). Anything not listed falls back to its first token.
_MODALITY_PRIORITY = ('PT', 'NM', 'MR', 'CT', 'XA', 'RF', 'MG', 'US', 'DX', 'CR')

_COLS = [
    'study_db_uid', 'workitem_db_uid', 'is_linked', 'accession_number', 'study_ts',
    'insert_time', 'storing_ae', 'pacs_site_id', 'patient_db_uid', 'patient_class',
    'patient_location', 'study_description', 'procedure_code', 'number_of_images',
    'study_modality_raw', 'report_status', 'composed_by', 'composed_ts',
    'referring_physician', 'dn_signer', 'synced_at',
]

_SELECT = """
    SELECT s.STUDY_DB_UID,
           s.WORKITEM_DB_UID,
           CASE WHEN UPPER(TRIM(s.IS_LINKED_STUDY)) = 'Y' THEN 1 ELSE 0 END,
           TRIM(s.ACCESSION_NUMBER),
           s.STUDY_DATE,
           s.INSERT_TIME,
           UPPER(TRIM(s.STORING_AE)),
           TO_CHAR(s.SITE_ID),
           TO_CHAR(s.PATIENT_DB_UID),
           UPPER(TRIM(s.PATIENT_CLASS)),
           UPPER(TRIM(s.PATIENT_LOCATION)),
           CAST(SUBSTR(s.STUDY_DESCRIPTION, 1, 400) AS VARCHAR2(4000)),
           TRIM(s.PROCEDURE_CODE),
           s.NUMBER_OF_STUDY_IMAGES,
           UPPER(TRIM(s.STUDY_MODALITY)),
           TRIM(s.REPORT_STATUS),
           s.REP_STUDY_LAST_COMPOSED_BY,
           s.REP_STUDY_LAST_COMPOSED_TS,
           TRIM(s.REFERRING_PHYSICIAN_FIRST_NAME || ' ' || s.REFERRING_PHYSICIAN_LAST_NAME),
           CASE WHEN UPPER(s.REP_FINAL_SIGNED_BY)        LIKE '%@DN'
                  OR UPPER(s.REP_PRELIM_SIGNED_BY)       LIKE '%@DN'
                  OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) LIKE '%@DN'
                THEN 1 ELSE 0 END
    FROM medistore.didb_studies s
    WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
"""


def _sql_list(values):
    return ", ".join(f"'{v}'" for v in values)


def _priority_case():
    whens = "\n".join(f"WHEN '{m}' = ANY(t.mods) THEN '{m}'" for m in _MODALITY_PRIORITY)
    return f"CASE WHEN cardinality(t.mods) = 0 THEN NULL\n{whens}\nELSE t.mods[1] END"


# Step 2. Any group with a row upserted after its last derivation is recomputed in
# full -- which also picks up groups left behind by a run that failed between the two
# steps. DISTINCT ON picks each group's imaging study: has its own modality first,
# then most images, then lowest uid so a tie resolves the same way every run.
_DERIVE_SQL = f"""
    WITH g AS (
        SELECT DISTINCT grp_key FROM {_TABLE}
        WHERE derived_at IS NULL OR derived_at < synced_at
    ),
    t AS (
        SELECT f.study_db_uid,
               ARRAY(
                   SELECT TRIM(u.tok)
                   FROM unnest(string_to_array(COALESCE(f.study_modality_raw, ''), '\\'))
                        WITH ORDINALITY AS u(tok, ord)
                   WHERE TRIM(u.tok) <> ''
                     AND TRIM(u.tok) NOT IN ({_sql_list(_NON_EXAM_TOKENS)})
                   ORDER BY u.ord
               ) AS mods
        FROM {_TABLE} f
        WHERE f.grp_key IN (SELECT grp_key FROM g)
    ),
    base AS (
        SELECT f.study_db_uid, f.grp_key, f.is_linked, f.dn_signer,
               f.accession_number, f.storing_ae, f.study_ts,
               f.patient_class, f.patient_location, f.pacs_site_id, f.study_modality_raw,
               COALESCE(f.number_of_images, 0) AS images,
               CASE WHEN f.storing_ae IN ({_sql_list(_BMD_DEVICES)}) THEN 'BMD'
                    ELSE {_priority_case()} END AS own_mod,
               CASE WHEN f.study_ts <> date_trunc('day', f.study_ts) THEN f.study_ts
                    ELSE f.insert_time END AS scan_ts
        FROM {_TABLE} f
        JOIN t ON t.study_db_uid = f.study_db_uid
    ),
    master AS (
        SELECT DISTINCT ON (grp_key)
               grp_key, own_mod AS m_mod, storing_ae AS m_ae, scan_ts AS m_ts,
               accession_number AS m_acc, patient_class AS m_pclass,
               patient_location AS m_ploc
        FROM base
        ORDER BY grp_key, (own_mod IS NULL), images DESC, study_db_uid
    ),
    d AS (
        SELECT b.study_db_uid, b.is_linked, b.dn_signer, b.accession_number,
               b.study_modality_raw, b.own_mod,
               COALESCE(b.own_mod, m.m_mod, 'UNKNOWN')                         AS modality,
               CASE WHEN b.own_mod IS NOT NULL THEN b.storing_ae ELSE m.m_ae END     AS device,
               CASE WHEN b.own_mod IS NOT NULL THEN b.scan_ts    ELSE m.m_ts END     AS exam_ts,
               CAST(b.study_ts AS DATE)                                          AS exam_date,
               CASE WHEN b.own_mod IS NOT NULL THEN b.accession_number ELSE m.m_acc END AS main_accession,
               COALESCE(b.patient_class, m.m_pclass)                             AS pclass,
               COALESCE(b.patient_location, m.m_ploc)                            AS ploc,
               st.id                                                             AS site_id
        FROM base b
        JOIN master m ON m.grp_key = b.grp_key
        LEFT JOIN sites st ON st.pacs_site_id = b.pacs_site_id
    ),
    r AS (
        SELECT d.*,
               CASE WHEN d.ploc LIKE 'ER%' OR d.ploc LIKE 'EM%' THEN 'ER'
                    WHEN d.pclass = 'I' THEN 'Inpatient'
                    WHEN d.pclass = 'O' THEN 'Outpatient'
                    ELSE COALESCE(d.pclass, '(blank)') END                        AS patient_type,
               CASE
                   -- RH2026... / SJ2026...: imported outside studies (operator,
                   -- 2026-10-06). 'SJ' also covers anything starting 'SJH'.
                   WHEN UPPER(COALESCE(d.accession_number, '')) LIKE 'RH%'
                     OR UPPER(COALESCE(d.accession_number, '')) LIKE 'SJ%'
                        THEN 'accession starts with RH / SJ'
                   WHEN COALESCE(d.study_modality_raw, '') LIKE '%CARD%'
                     OR d.modality LIKE '%CARD%'
                        THEN 'CARD modality'
                   WHEN d.device IN ({_sql_list(_CARDIO_DEVICES)})
                        THEN 'cardiology device (echo/cath/angio)'
                   WHEN d.device IN ({_sql_list(_DUPLICATE_DEVICES)})
                        THEN 'duplicate copy (AE LAUMC/SVSM)'
                   WHEN d.dn_signer
                        THEN '@dn records (not LAUMC)'
                   WHEN d.own_mod IS NULL AND NOT d.is_linked
                        THEN 'SR / OT only, not linked'
               END                                                               AS reason
        FROM d
    )
    UPDATE {_TABLE} f
    SET own_modality   = r.own_mod,
        modality       = r.modality,
        device         = r.device,
        exam_ts        = r.exam_ts,
        exam_date      = r.exam_date,
        main_accession = r.main_accession,
        patient_type   = r.patient_type,
        site_id        = r.site_id,
        reason         = r.reason,
        derived_at     = LOCALTIMESTAMP
    FROM r
    WHERE f.study_db_uid = r.study_db_uid
"""


def _to_row(raw, synced_at):
    (uid, workitem, linked, acc, study_ts, insert_time, ae, site, patient, pclass, ploc,
     descr, proc_code, images, raw_mod, rep_status, composed_by, composed_ts,
     referring, dn) = raw
    return (
        uid, workitem, bool(linked), acc or None, study_ts, insert_time, ae or None,
        site, patient, pclass or None, ploc or None, descr or None, proc_code or None,
        images, raw_mod or None, rep_status or None, composed_by or None, composed_ts,
        (referring or '').strip() or None, bool(dn), synced_at,
    )


def run_fact_exam_etl(pg_engine, oracle_source, go_live_date):
    job_name   = "FACT_EXAM_ETL"
    start_time = datetime.now()
    total      = 0
    derived    = 0
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
        logging.error(f"Fact Exam ETL log error: {e}")

    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)
    lookback_date = (datetime.now() - timedelta(days=_LOOKBACK_DAYS)).strftime('%Y-%m-%d')

    with pg_engine.connect() as conn:
        max_uid = conn.execute(text(f"SELECT MAX(study_db_uid) FROM {_TABLE}")).scalar() or 0

    # Rows are stamped with this run's start; step 2 compares it with derived_at, so it
    # is taken from Postgres to keep both columns on the same clock.
    with pg_engine.connect() as conn:
        run_start = conn.execute(text("SELECT LOCALTIMESTAMP")).scalar()

    ora_conn = OracleConnector.get_connection(oracle_source)
    cursor   = ora_conn.cursor()

    try:
        if max_uid == 0:
            print(f"[Fact Exam ETL] 🆕 Fresh load — all studies since {gd_str}")
            cursor.execute(_SELECT, {"gd": gd_str})
        else:
            print(f"[Fact Exam ETL] 🔄 Incremental — max_uid={max_uid:,}, lookback={lookback_date}")
            cursor.execute(
                _SELECT + " AND (s.STUDY_DB_UID > :max_id OR s.STUDY_DATE >= TO_DATE(:lb, 'YYYY-MM-DD'))",
                {"gd": gd_str, "max_id": max_uid, "lb": lookback_date}
            )

        upsert = text(
            f"INSERT INTO {_TABLE} ({', '.join(_COLS)}) "
            f"VALUES ({', '.join(':' + c for c in _COLS)}) "
            f"ON CONFLICT (study_db_uid) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in _COLS if c != 'study_db_uid')
        )

        while True:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            rows = [dict(zip(_COLS, _to_row(r, run_start))) for r in batch if r[0] is not None]
            if rows:
                with pg_engine.begin() as conn:
                    conn.execute(upsert, rows)
                total += len(rows)
                if total % 50000 < _FETCH_BATCH:
                    print(f"[Fact Exam ETL] 📦 {total:,} studies loaded")

        with pg_engine.begin() as conn:
            derived = conn.execute(text(_DERIVE_SQL)).rowcount

        print(f"[Fact Exam ETL] ✅ {total:,} studies upserted, {derived:,} rows re-derived")
        logging.info(f"Fact Exam ETL complete: {total:,} upserted, {derived:,} derived")
        status = "SUCCESS"

    except Exception as e:
        status    = "FAILED"
        error_msg = str(e)
        logging.error(f"Fact Exam ETL error: {error_msg}")
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
                             "error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total,
                         "d": round(duration, 2), "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update Fact Exam ETL log: {le}")

    return total, derived
