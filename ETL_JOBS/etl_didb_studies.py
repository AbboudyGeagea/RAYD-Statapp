import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from db import OracleConnector

# Cardiology devices: out of RAYD's scope (operator, 2026-11-17: "leave cardiology out, filter
# them from ETL level"). Both reach PACS through SJH's middleware, so they show as its original
# storing AE, not as the storing AE: EPIQCVX, Philips EPIQ CVx cardiac echo (1,472 of the last
# ~60,000 studies, not on the RIS); AZURION, SJH's cath lab (339, 12 with a RIS exam; it now
# sends to the CVIS). Not AZURION7FLEXARM, Rizk's hybrid OR. The dedicated cardiology AEs that
# store directly are in _EXCLUDED_AE_SQL; CARD-labelled studies of the gateways go in Phase 2c.
CARDIOLOGY_AES = ('EPIQCVX', 'AZURION')
_CARDIOLOGY_LIST = ", ".join(f"'{ae}'" for ae in CARDIOLOGY_AES)
# Each check keeps its NULL guard: a NOT IN on a NULL column drops the row (the 2026-07-30 bug
# below), and most studies have no original storing AE.
_CARDIOLOGY_SQL = (
    f" AND UPPER(TRIM(s.STORING_AE)) NOT IN ({_CARDIOLOGY_LIST})"
    f" AND (s.ORIGINAL_STORING_AE IS NULL OR UPPER(TRIM(s.ORIGINAL_STORING_AE)) NOT IN ({_CARDIOLOGY_LIST}))"
)

# Gateways (item 13, 2026-11-18): SJH's middleware and the second workflow manager forward
# other devices' studies, so PACS names the gateway as the storing AE and keeps the scanner in
# ORIGINAL_STORING_AE. RAYD stores the scanner as the study's storing_ae and the gateway in
# gateway_ae (migration 0164): every report joins the device mapping on storing_ae (about 70
# queries), so all of them read the scanner, its modality, room and site, unchanged.
# Only when the original AE is a DEVICE the mapping knows (a modality other than 'PACS'): an
# import agent (XVIMPORT), a workstation, an unmapped or empty AE keeps the gateway, as before,
# because reports keep only the site's mapped devices and would lose those studies (production
# list of 2026-11-18). Mapping a scanner later moves its studies the next night
# (apply_gateway_devices runs after every load). LAUMCWFM1AR is not one of these: it names
# itself as the original AE; its devices come from each exam's RIS room. To undo:
# UPDATE etl_didb_studies SET storing_ae = gateway_ae, gateway_ae = NULL WHERE gateway_ae IS NOT NULL
GATEWAY_AES = ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR')
_GATEWAY_LIST = ", ".join(f"'{ae}'" for ae in GATEWAY_AES)
# A study that came through a gateway, whether its device was swapped or not.
GATEWAY_STUDY_SQL = f"UPPER(TRIM(COALESCE(gateway_ae, storing_ae))) IN ({_GATEWAY_LIST})"
# The AE titles mapped as a device (m.<column>); the same test in Python and SQL.
_IS_DEVICE_SQL = "NULLIF(TRIM(m.modality), '') IS NOT NULL AND UPPER(TRIM(m.modality)) <> 'PACS'"
_MAPPED_DEVICES_SQL = text(f"SELECT DISTINCT UPPER(TRIM(m.aetitle)) FROM aetitle_modality_map m WHERE {_IS_DEVICE_SQL}")
_APPLY_GATEWAY_SQL = text(f"""
    UPDATE etl_didb_studies s
       SET gateway_ae = UPPER(TRIM(s.storing_ae)), storing_ae = UPPER(TRIM(s.original_storing_ae))
     WHERE UPPER(TRIM(s.storing_ae)) IN ({_GATEWAY_LIST})
       AND UPPER(TRIM(s.original_storing_ae)) <> UPPER(TRIM(s.storing_ae))
       AND EXISTS (SELECT 1 FROM aetitle_modality_map m
                   WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.original_storing_ae)) AND {_IS_DEVICE_SQL})
""")


def mapped_devices(conn):
    """The AE titles the mapping knows as a device, upper-cased and trimmed."""
    return set(conn.execute(_MAPPED_DEVICES_SQL).scalars())


def device_of(storing_ae, original_ae, devices):
    """(storing_ae, gateway_ae) as RAYD stores a study PACS gives with these two AEs, given
    the mapped devices (mapped_devices())."""
    if storing_ae in GATEWAY_AES and original_ae and original_ae != storing_ae and original_ae in devices:
        return original_ae, storing_ae
    return storing_ae, None


def apply_gateway_devices(conn):
    """Gateway studies already loaded whose original AE is a mapped device: the scanner becomes
    their storing_ae. Idempotent; run after every load. Returns the studies changed."""
    return conn.execute(_APPLY_GATEWAY_SQL).rowcount


# Studies loaded before migration 0159 have no original storing AE. It matters only behind a
# gateway (a device mapped to modality 'PACS'), so those are filled once, a range of study IDs
# per query through the primary key, a few nights at most; the progress is kept in settings.
_FILL_DONE_KEY = 'original_ae_filled'
_FILL_UPTO_KEY = 'original_ae_filled_upto'
_FILL_CHUNK = 50_000
_FILL_MAX_CHUNKS = 40


def _setting(conn, key):
    return conn.execute(text("SELECT value FROM settings WHERE key = :k"), {'k': key}).scalar()


def _save(conn, key, value):
    conn.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                      "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {'k': key, 'v': str(value)})


def fill_original_ae(pg_engine, oracle_source, chunk=_FILL_CHUNK, max_chunks=_FILL_MAX_CHUNKS):
    """Fill original_storing_ae on gateway studies already loaded (once). Returns rows filled."""
    with pg_engine.begin() as conn:
        if _setting(conn, _FILL_DONE_KEY) == 'true':
            return 0
        gateways = conn.execute(text(
            "SELECT DISTINCT UPPER(TRIM(aetitle)) FROM aetitle_modality_map WHERE modality = 'PACS'")).scalars().all()
        lo, hi = conn.execute(text(
            "SELECT MIN(study_db_uid), MAX(study_db_uid) FROM etl_didb_studies "
            "WHERE UPPER(TRIM(storing_ae)) = ANY(:g) AND original_storing_ae IS NULL"),
            {'g': list(gateways)}).one()
        upto = int(_setting(conn, _FILL_UPTO_KEY) or 0)
        if lo is None or not gateways:
            _save(conn, _FILL_DONE_KEY, 'true')
            return 0
    start = max(int(lo), upto + 1)
    binds = {f'g{i}': g for i, g in enumerate(gateways)}
    query = ("SELECT STUDY_DB_UID, UPPER(TRIM(ORIGINAL_STORING_AE)) FROM medistore.didb_studies "
             "WHERE STUDY_DB_UID BETWEEN :lo AND :hi AND UPPER(TRIM(STORING_AE)) IN ("
             + ", ".join(f':{k}' for k in binds) + ") AND ORIGINAL_STORING_AE IS NOT NULL")
    filled = 0
    ora = OracleConnector.get_connection(oracle_source)
    cursor = ora.cursor()
    try:
        for _ in range(max_chunks):
            end = start + chunk - 1
            cursor.execute(query, {'lo': start, 'hi': end, **binds})
            rows = cursor.fetchall()
            with pg_engine.begin() as conn:
                if rows:
                    filled += conn.execute(text("""
                        UPDATE etl_didb_studies s SET original_storing_ae = u.ae
                        FROM unnest(CAST(:uids AS bigint[]), CAST(:aes AS text[])) AS u(uid, ae)
                        WHERE s.study_db_uid = u.uid AND s.original_storing_ae IS NULL
                    """), {'uids': [int(r[0]) for r in rows], 'aes': [r[1] for r in rows]}).rowcount
                    apply_gateway_devices(conn)
                _save(conn, _FILL_UPTO_KEY, end)
                if end >= int(hi):
                    _save(conn, _FILL_DONE_KEY, 'true')
                    break
            start = end + 1
    finally:
        cursor.close()
        ora.close()
    logging.info(f"Original storing AE filled on {filled:,} gateway studies (up to study {start - 1:,})")
    return filled


def purge_cardiology(pg_engine):
    """Delete the cardiology studies already loaded (CARDIOLOGY_AES as storing or original AE).
    Their series and images go with them (ON DELETE CASCADE); a RIS step pointing at one is
    unlinked first, as Phase 2c does. Returns the studies deleted."""
    cardio = (f"UPPER(TRIM(COALESCE(original_storing_ae, ''))) IN ({_CARDIOLOGY_LIST}) "
              f"OR UPPER(TRIM(storing_ae)) IN ({_CARDIOLOGY_LIST})")
    with pg_engine.begin() as conn:
        conn.execute(text(f"UPDATE std_pps SET study_db_uid = NULL WHERE study_db_uid IN "
                          f"(SELECT study_db_uid FROM etl_didb_studies WHERE {cardio})"))
        deleted = conn.execute(text(f"DELETE FROM etl_didb_studies WHERE {cardio}")).rowcount
    logging.info(f"Cardiology studies purged: {deleted:,}")
    return deleted


def run_studies_etl(pg_engine, oracle_source, pg_table, chunked_upsert_func, go_live_date, force_ae=None):
    """force_ae: one-off backfill mode (see backfill_ae_studies.py). When set, pulls the
    FULL history for that single AE since go_live_date, ignoring the incremental
    max_uid/lookback watermark entirely — for recovering an AE that was wrongly on
    _EXCLUDED_AE_SQL and has been silently skipped by every regular incremental run
    since. Not used by the normal nightly path."""
    job_name = "STUDIES_ETL" if not force_ae else f"STUDIES_ETL_BACKFILL_{force_ae}"
    start_time = datetime.now()
    total_rows = 0
    status = "RUNNING"
    error_msg = None
    log_id = None
    processed_uids = []

    # 1. Start Log Entry
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
        logging.error(f"Log error: {e}")

    # Prepare Dates
    lookback_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    gd_str = go_live_date.strftime('%Y-%m-%d') if hasattr(go_live_date, 'strftime') else str(go_live_date)

    try:
        # Get Max UID — determines FULL load vs INCREMENTAL load
        try:
            with pg_engine.connect() as conn:
                val = conn.execute(text(f"SELECT MAX(study_db_uid) FROM {pg_table}")).fetchone()[0]
                max_uid = val if val else 0
        except:
            max_uid = 0

        is_fresh_load = (max_uid == 0)

        if force_ae:
            print(f"[Studies ETL] 🩹 Backfill mode — pulling ALL '{force_ae}' studies since {gd_str}, ignoring watermark")
            logging.info(f"Studies ETL: force_ae backfill for {force_ae}, since {gd_str}")
        elif is_fresh_load:
            print(f"[Studies ETL] 🆕 Fresh load detected — pulling ALL studies since {gd_str}")
            logging.info(f"Studies ETL: fresh load, pulling all since {gd_str}")
        else:
            print(f"[Studies ETL] 🔄 Incremental load — max_uid={max_uid:,}, lookback={lookback_date}")
            logging.info(f"Studies ETL: incremental, max_uid={max_uid}, lookback={lookback_date}")

        col_names = [
            'study_db_uid', 'patient_db_uid', 'study_instance_uid', 'accession_number',
            'study_id', 'storing_ae', 'study_date', 'study_description',
            'study_body_part', 'study_age', 'age_at_exam', 'number_of_study_series',
            'number_of_study_images', 'study_status', 'patient_class', 'procedure_code',
            'referring_physician_first_name', 'referring_physician_mid_name',
            'referring_physician_last_name', 'report_status', 'order_status',
            'last_accessed_time', 'insert_time', 'last_update',
            'reading_physician_first_name', 'reading_physician_last_name', 'reading_physician_id',
            'signing_physician_first_name', 'signing_physician_last_name', 'signing_physician_id',
            'study_has_report', 'rep_prelim_timestamp', 'rep_prelim_signed_by',
            'rep_transcribed_by', 'rep_transcribed_timestamp',
            'rep_final_signed_by', 'rep_final_timestamp',
            'rep_addendum_by', 'rep_addendum_timestamp', 'rep_has_addendum',
            'is_linked_study', 'patient_location',
            'pacs_site_id_raw',  # raw PACS SITE_ID ('0'=RH, '1'=SJH) — resolved to sites.id by enrichment
            # This PACS version is older / sparser than sites worked with earlier — rep_final_timestamp/
            # rep_final_signed_by are unreliably populated here. REP_STUDY_LAST_COMPOSED_BY/_TS are the
            # fields that actually carry data on this install (operator, 2026-07-27). Raw value for now
            # (e.g. "abdallah.noufaily@ad") — resolving to a real name/role (radiologist vs resident) is
            # deferred, operator: "i will find a way, for now let's proceed with this change."
            'rep_study_last_composed_by', 'rep_study_last_composed_ts',
            # The latest lock on the study, for the JCI urgent read measure (migration 0143).
            'study_locked', 'study_lock_time', 'rep_last_accessed_by',
            # patient_location above is cut to 3 characters; this is the whole value (0144).
            'patient_location_full',
            # The device behind a gateway: SJH's studies all reach PACS through its middleware
            # (SJHCSAPWFMFIR), which keeps the real device here (migration 0159).
            'original_storing_ae',
            # Not read from PACS: set below, with storing_ae, by device_of() (migration 0164).
            'gateway_ae'
        ]
        _AE, _ORIGINAL = col_names.index('storing_ae'), col_names.index('original_storing_ae')
        try:
            with pg_engine.connect() as conn:
                devices = mapped_devices(conn)
        except Exception as dev_err:          # no swap tonight; apply_gateway_devices catches up
            logging.warning(f"Studies ETL: mapped devices not read, gateway studies keep the gateway: {dev_err}")
            devices = set()

        def _with_device(row):
            """The PACS row as RAYD stores it: a gateway study gets its scanner as storing_ae."""
            row = list(row)
            row[_AE], gateway = device_of(row[_AE], row[_ORIGINAL], devices)
            return tuple(row) + (gateway,)

        ora_conn = OracleConnector.get_connection(oracle_source)
        cursor = ora_conn.cursor()

        # ── Build both query variants ─────────────────────────────────────────
        # "with_join"    — includes LEFT JOIN to didb_patients_view for age_at_exam
        # "without_join" — falls back to NULL for age_at_exam if the view is
        #                  inaccessible (ORA-00942 / privilege issue)
        _SELECT_WITH_JOIN = """
                SELECT
                    s.STUDY_DB_UID, s.PATIENT_DB_UID, s.STUDY_INSTANCE_UID, s.ACCESSION_NUMBER,
                    s.STUDY_ID,
                    UPPER(TRIM(s.STORING_AE)),
                    s.STUDY_DATE,
                    CAST(SUBSTR(s.STUDY_DESCRIPTION, 1, 4000) AS VARCHAR2(4000)),
                    s.STUDY_BODY_PART, s.STUDY_AGE,
                    CASE
                        WHEN p.BIRTH_DATE IS NOT NULL AND s.STUDY_DATE IS NOT NULL
                        THEN FLOOR((s.STUDY_DATE - p.BIRTH_DATE) / 365.25)
                        ELSE NULL
                    END as age_at_exam,
                    s.NUMBER_OF_STUDY_SERIES, s.NUMBER_OF_STUDY_IMAGES, s.STUDY_STATUS,
                    s.PATIENT_CLASS, s.PROCEDURE_CODE, s.REFERRING_PHYSICIAN_FIRST_NAME,
                    s.REFERRING_PHYSICIAN_MID_NAME, s.REFERRING_PHYSICIAN_LAST_NAME,
                    s.REPORT_STATUS, s.ORDER_STATUS, s.LAST_ACCESS_TIME, s.INSERT_TIME,
                    CURRENT_TIMESTAMP,
                    s.READING_PHYSICIAN_FIRST_NAME, s.READING_PHYSICIAN_LAST_NAME, s.READING_PHYSICIAN_ID,
                    s.SIGNING_PHYSICIAN_FIRST_NAME, s.SIGNING_PHYSICIAN_LAST_NAME, s.SIGNING_PHYSICIAN_ID,
                    CASE WHEN s.STUDY_HAS_REPORT = 'Y' THEN 'true' ELSE 'false' END,
                    s.REP_PRELIM_TIMESTAMP, s.REP_PRELIM_SIGNED_BY,
                    s.REP_TRANSCRIBED_BY, s.REP_TRANSCRIBED_TIMESTAMP,
                    s.REP_FINAL_SIGNED_BY, s.REP_FINAL_TIMESTAMP,
                    s.REP_ADDENDUM_BY, s.REP_ADDENDUM_TIMESTAMP,
                    CASE WHEN s.REP_HAS_ADDENDUM = 'Y' THEN 'true' ELSE 'false' END,
                    CASE WHEN s.IS_LINKED_STUDY = 'Y' THEN 'true' ELSE 'false' END,
                    SUBSTR(s.PATIENT_LOCATION, 1, 3),
                    TO_CHAR(s.SITE_ID),
                    s.REP_STUDY_LAST_COMPOSED_BY, s.REP_STUDY_LAST_COMPOSED_TS,
                    s.STUDY_LOCKED, s.STUDY_LOCK_TIME, s.REP_LAST_ACCESSED_BY,
                    s.PATIENT_LOCATION,
                    UPPER(TRIM(s.ORIGINAL_STORING_AE))
                FROM medistore.didb_studies s
                LEFT JOIN medistore.didb_patients_view p ON p.PATIENT_DB_UID = s.PATIENT_DB_UID
        """
        _SELECT_NO_JOIN = """
                SELECT
                    s.STUDY_DB_UID, s.PATIENT_DB_UID, s.STUDY_INSTANCE_UID, s.ACCESSION_NUMBER,
                    s.STUDY_ID,
                    UPPER(TRIM(s.STORING_AE)),
                    s.STUDY_DATE,
                    CAST(SUBSTR(s.STUDY_DESCRIPTION, 1, 4000) AS VARCHAR2(4000)),
                    s.STUDY_BODY_PART, s.STUDY_AGE,
                    NULL AS age_at_exam,
                    s.NUMBER_OF_STUDY_SERIES, s.NUMBER_OF_STUDY_IMAGES, s.STUDY_STATUS,
                    s.PATIENT_CLASS, s.PROCEDURE_CODE, s.REFERRING_PHYSICIAN_FIRST_NAME,
                    s.REFERRING_PHYSICIAN_MID_NAME, s.REFERRING_PHYSICIAN_LAST_NAME,
                    s.REPORT_STATUS, s.ORDER_STATUS, s.LAST_ACCESS_TIME, s.INSERT_TIME,
                    CURRENT_TIMESTAMP,
                    s.READING_PHYSICIAN_FIRST_NAME, s.READING_PHYSICIAN_LAST_NAME, s.READING_PHYSICIAN_ID,
                    s.SIGNING_PHYSICIAN_FIRST_NAME, s.SIGNING_PHYSICIAN_LAST_NAME, s.SIGNING_PHYSICIAN_ID,
                    CASE WHEN s.STUDY_HAS_REPORT = 'Y' THEN 'true' ELSE 'false' END,
                    s.REP_PRELIM_TIMESTAMP, s.REP_PRELIM_SIGNED_BY,
                    s.REP_TRANSCRIBED_BY, s.REP_TRANSCRIBED_TIMESTAMP,
                    s.REP_FINAL_SIGNED_BY, s.REP_FINAL_TIMESTAMP,
                    s.REP_ADDENDUM_BY, s.REP_ADDENDUM_TIMESTAMP,
                    CASE WHEN s.REP_HAS_ADDENDUM = 'Y' THEN 'true' ELSE 'false' END,
                    CASE WHEN s.IS_LINKED_STUDY = 'Y' THEN 'true' ELSE 'false' END,
                    SUBSTR(s.PATIENT_LOCATION, 1, 3),
                    TO_CHAR(s.SITE_ID),
                    s.REP_STUDY_LAST_COMPOSED_BY, s.REP_STUDY_LAST_COMPOSED_TS,
                    s.STUDY_LOCKED, s.STUDY_LOCK_TIME, s.REP_LAST_ACCESSED_BY,
                    s.PATIENT_LOCATION,
                    UPPER(TRIM(s.ORIGINAL_STORING_AE))
                FROM medistore.didb_studies s
        """

        # Operator instruction (2026-07-27): 'LAUMC' and 'SVSM' are not real imaging
        # devices — studies land there as duplicates of a real device's AE and should
        # never be loaded. Existing duplicate rows cleaned up separately, migration 0073.
        #
        # The 12 AEs below are dedicated Cardiology/Vascular-Lab PACS-only-archival
        # equipment (echo/cath/angio workstations) — they archive straight to PACS and
        # never go through the RIS ordering workflow, so their studies inflate
        # studies-vs-orders counts in every report with no way to ever reconcile.
        # Existing rows cleaned up separately, migration 0111. NOT included here:
        # SJHCSAPWFMFIR / LAUMCWFM2FIR — those are shared SJH gateway AEs that also
        # carry ~182,700 legitimate non-Cardiology studies, so they can't be excluded
        # by AE alone; see etl_runner.py Phase 2c, which excludes only their
        # CARD-family-tagged rows post-modality-backfill. Since 2026-11-17 the cardiology
        # devices behind those gateways are also left out by their original storing AE
        # (_CARDIOLOGY_SQL, CARDIOLOGY_AES at the top of this file).
        #
        # 'DEFINIUM1' REMOVED 2026-09-04: it was bundled into this list alongside the
        # echo/cath/angio devices, but it's a GE Definium general digital-radiography
        # (X-ray) room, not cardiology equipment. Confirmed against live Oracle DIDB
        # data (real XR/CT orders, real non-'@dn' signing physicians) that every study
        # on this AE was being silently dropped, which orphaned ~12k hl7_oru_reports
        # rows (real reports with no matching PACS study -> blank modality in the ORU
        # tab). See the one-time backfill note near run_studies_etl(): removing it here
        # only affects studies from now on -- historical DEFINIUM1 studies need a
        # separate one-off pull since they predate the incremental watermark/lookback.
        _EXCLUDED_AE_SQL = """AND UPPER(TRIM(s.STORING_AE)) NOT IN (
            'LAUMC', 'SVSM',
            'ECHOPAC-PC', 'ADW_8', 'AETITLE', 'VIVIDE9-003168', 'VIVID_S5-050514', 'TERRA',
            'VIVIDS70-003049', 'TERRA2', 'AWVASC', 'AWCTHD1', 'PHCARDIO', 'LOGIQV2-01'
        )""" + _CARDIOLOGY_SQL

        # Operator instruction (2026-07-29, extended 2026-07-31): '@dn'-suffixed signer
        # values (akehdi@dn, eva.d@dn, mi70@dn, ...) are not real LAUMC data — confirmed
        # not a different PACS_SITE_ID (they share site 0 with legitimate RH studies),
        # just not legitimate regardless. Originally only checked REP_FINAL_SIGNED_BY
        # (migration 0084); '@dn' values were then found leaking through
        # REP_PRELIM_SIGNED_BY and REP_STUDY_LAST_COMPOSED_BY too (e.g. EVA.D@DN
        # surfacing via rep_prelim_signed_by, not rep_final_signed_by) — all three
        # signer fields are checked now. Existing rows cleaned up separately,
        # migrations 0084 and 0086.
        #
        # BUG FIXED 2026-07-30: the original "NOT LIKE" with no NULL guard silently
        # dropped every row where the signer field IS NULL, since NULL NOT LIKE x
        # evaluates to NULL (excluded), not TRUE. That's virtually every brand-new
        # study (signatures are set well after the study lands), so incremental
        # runs — which target exactly those new studies — came back with an empty or
        # near-empty active-study-id list most nights. That empty list tripped
        # etl_runner.py's _ensure_active_ids() fallback ("reload every study ID ever
        # loaded from Postgres") for every downstream phase — series, raw_images,
        # image_locations all trust that list blindly — turning every incremental
        # sync into 4 full reloads. Confirmed live on LAUMC, 2026-07-30. Every clause
        # below keeps the same NULL guard.
        _EXCLUDED_SIGNER_SQL = (
            "AND (s.REP_FINAL_SIGNED_BY IS NULL OR UPPER(s.REP_FINAL_SIGNED_BY) NOT LIKE '%@DN') "
            "AND (s.REP_PRELIM_SIGNED_BY IS NULL OR UPPER(s.REP_PRELIM_SIGNED_BY) NOT LIKE '%@DN') "
            "AND (s.REP_STUDY_LAST_COMPOSED_BY IS NULL OR UPPER(s.REP_STUDY_LAST_COMPOSED_BY) NOT LIKE '%@DN')"
        )

        def _execute_query(use_join):
            select = _SELECT_WITH_JOIN if use_join else _SELECT_NO_JOIN
            if force_ae:
                # No AE exclusion (that's the whole point — this AE was wrongly on that
                # list) and no max_uid/lookback restriction — full history for this one
                # AE only, so it can't touch any other device's rows.
                q = select + f"""
                    WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
                    AND UPPER(TRIM(s.STORING_AE)) = :force_ae
                    {_EXCLUDED_SIGNER_SQL}
                    ORDER BY s.STUDY_DB_UID
                """
                cursor.execute(q, {'gd': gd_str, 'force_ae': force_ae.upper()})
            elif is_fresh_load:
                q = select + f"""
                    WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
                    {_EXCLUDED_AE_SQL}
                    {_EXCLUDED_SIGNER_SQL}
                    ORDER BY s.STUDY_DB_UID
                """
                cursor.execute(q, {'gd': gd_str})
            else:
                q = select + f"""
                    WHERE s.STUDY_DATE >= TO_DATE(:gd, 'YYYY-MM-DD')
                    AND (
                        s.STUDY_DB_UID > :max_id
                        OR s.STUDY_DATE >= TO_DATE(:lb, 'YYYY-MM-DD')
                    )
                    {_EXCLUDED_AE_SQL}
                    {_EXCLUDED_SIGNER_SQL}
                    ORDER BY s.STUDY_DB_UID
                """
                cursor.execute(q, {'gd': gd_str, 'max_id': max_uid, 'lb': lookback_date})

        # ── Try with patients view first; fall back silently on ORA-00942 ─────
        try:
            _execute_query(use_join=True)
            logging.info("Studies ETL: using didb_patients_view for age_at_exam")
        except Exception as _join_err:
            err_str = str(_join_err)
            if 'ORA-00942' in err_str or 'table or view does not exist' in err_str.lower():
                logging.warning(
                    f"Studies ETL: didb_patients_view not accessible ({_join_err}) — "
                    "falling back to NULL age_at_exam"
                )
                print("[Studies ETL] ⚠️  didb_patients_view unavailable — age_at_exam will be NULL")
                _execute_query(use_join=False)
            else:
                raise

        # ── Batch fetch & upsert ─────────────────────────────────────────────
        # Strategy: attempt bulk upsert first (fast path).
        # If the batch contains any corrupt Oracle value (e.g. "R3" in a bigint
        # column), PostgreSQL will reject the whole batch. Fall back to row-by-row
        # insertion so only the single bad row is dropped, not the entire batch.
        batch_num   = 0
        skipped_bad = 0

        # Nullable bigint columns by index — corrupt values are NULL-ed out,
        # not skipped. study_db_uid (0) is the PK and must be valid; if it's
        # corrupt the row is dropped entirely.
        NULLABLE_BIGINT_COLS = {1}  # patient_db_uid

        def _sanitize(row):
            """Return a sanitized copy: NULL out any corrupt nullable bigint."""
            row = list(row)
            for idx in NULLABLE_BIGINT_COLS:
                if row[idx] is None:
                    continue
                try:
                    int(row[idx])
                except (TypeError, ValueError):
                    row[idx] = None
            return tuple(row)

        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break
            batch = [_with_device(row) for row in batch]
            batch_num += 1

            try:
                # Fast path — bulk insert the whole batch
                chunked_upsert_func(pg_engine, pg_table, col_names, batch, 'study_db_uid')
                processed_uids.extend([row[0] for row in batch])
                total_rows += len(batch)
            except Exception:
                # Slow path — sanitize nullable bigints, skip only truly unrecoverable rows
                for row in batch:
                    row = _sanitize(row)
                    try:
                        chunked_upsert_func(pg_engine, pg_table, col_names, [row], 'study_db_uid')
                        processed_uids.append(row[0])
                        total_rows += 1
                    except Exception as row_err:
                        skipped_bad += 1
                        logging.warning(f"[Studies ETL] Skipping bad row (uid={row[0]!r}): {row_err}")

            if batch_num % 5 == 0 or skipped_bad:
                print(f"[Studies ETL] 📦 {total_rows:,} rows loaded{f', {skipped_bad} dropped' if skipped_bad else ''}")

        status = "SUCCESS"
        cursor.close()
        ora_conn.close()

    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logging.error(f"Studies ETL Failed: {error_msg}")

    finally:
        if log_id:
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                rows_per_sec = round(total_rows / duration, 2) if duration > 0 else 0
                with pg_engine.connect() as conn:
                    conn.execute(
                        text("UPDATE etl_job_log SET status=:s, end_time=:et, "
                             "records_processed=:r, duration_seconds=:d, "
                             "rows_per_second=:rps, error_message=:e WHERE id=:id"),
                        {"s": status, "et": end_time, "r": total_rows,
                         "d": round(duration, 2), "rps": rows_per_sec,
                         "e": error_msg, "id": log_id}
                    )
                    conn.commit()
            except Exception as le:
                logging.error(f"Failed to update studies log: {le}")

    return total_rows, processed_uids
