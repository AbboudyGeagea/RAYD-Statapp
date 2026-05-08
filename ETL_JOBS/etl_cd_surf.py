"""
CD surf Oracle → PostgreSQL ETL.
Pulls completed burn tasks from CDSURF.TASKS and stores them in cd_print_log.
Linked to etl_didb_studies via TASKS.STUDY_FOR_REPORT = study_instance_uid.
"""
import logging
from datetime import datetime
from sqlalchemy import text

logger = logging.getLogger("CD_SURF_ETL")

_ORACLE_QUERY = """
    SELECT
        t.TASK_ID,
        t.PATIENT_NAME,
        t.START_TIME,
        t.TASK_STATUS,
        t.BURN_IDENTIFIER,
        t.STUDY_FOR_REPORT,
        MAX(c.CD_ID)            AS cd_id,
        MAX(c.NUMBER_OF_COPIES) AS number_of_copies,
        MAX(c.STATUS)           AS cd_status,
        MAX(c.TYPE)             AS media_type,
        MAX(c.CD_FOLDER)        AS cd_folder,
        MAX(c.ORDER_ID)         AS order_id
    FROM CDSURF.TASKS t
    JOIN CDSURF.TASK_CD tc ON tc.TASK_ID = t.TASK_ID
    JOIN CDSURF.CDS    c  ON c.CD_ID     = tc.CD_ID
    WHERE t.TASK_STATUS = 6
      AND t.START_TIME IS NOT NULL
      AND t.START_TIME > :last_sync
    GROUP BY t.TASK_ID, t.PATIENT_NAME, t.START_TIME,
             t.TASK_STATUS, t.BURN_IDENTIFIER, t.STUDY_FOR_REPORT
    ORDER BY t.END_TIME
"""


def _get_cd_surf_conn(pg_engine=None):
    """Return an oracledb connection to the CD surf instance from db_params."""
    import oracledb
    from utils.crypto import decrypt

    if pg_engine is None:
        from sqlalchemy import create_engine
        import os
        uri = os.getenv('SQLALCHEMY_DATABASE_URI')
        if not uri:
            raise RuntimeError("SQLALCHEMY_DATABASE_URI not set.")
        pg_engine = create_engine(uri)
        _dispose = True
    else:
        _dispose = False

    try:
        with pg_engine.connect() as conn:
            row = conn.execute(text("""
                SELECT host, port, sid, username, password, mode
                FROM db_params
                WHERE UPPER(owner) = 'CDSURF'
                LIMIT 1
            """)).fetchone()
    finally:
        if _dispose:
            pg_engine.dispose()

    if not row:
        raise RuntimeError("No CD surf connection found in db_params (owner='CDSURF').")

    host, port, sid, username, password_enc, mode = row

    missing = [f for f, v in [('host', host), ('sid', sid), ('username', username)] if not v]
    if missing:
        raise RuntimeError(
            f"CD surf connection in db_params is incomplete — missing: {', '.join(missing)}. "
            "Update it in Admin → DB Manager."
        )

    host, port, sid, username, password_enc, mode = row
    dsn = oracledb.makedsn(host, int(port or 1521), sid=sid)
    kwargs = {"user": username, "password": decrypt(password_enc), "dsn": dsn}
    if mode and mode.upper() == 'SYSDBA':
        kwargs["mode"] = oracledb.SYSDBA
    return oracledb.connect(**kwargs)


def _resolve_studies(pg_engine, study_uids):
    """Batch-lookup study metadata from PostgreSQL for a list of study_instance_uids."""
    unique = list({u for u in study_uids if u})
    if not unique:
        return {}
    result = {}
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT study_instance_uid, study_db_uid, accession_number,
                       study_date, study_modality,
                       COALESCE(reading_physician_last_name, '') AS reading_physician
                FROM etl_didb_studies
                WHERE study_instance_uid = ANY(:uids)
            """), {"uids": unique}).fetchall()
        for r in rows:
            result[r[0]] = {
                "study_db_uid":     r[1],
                "accession_number": r[2],
                "study_date":       r[3],
                "study_modality":   r[4],
                "reading_physician": r[5],
            }
    except Exception as e:
        logger.error(f"Study resolution error: {e}")
    return result


def _update_log(pg_engine, log_id, status, records, error=None):
    if not log_id:
        return
    try:
        with pg_engine.begin() as conn:
            conn.execute(text("""
                UPDATE etl_job_log
                SET status=:s, end_time=NOW(), records_processed=:r, error_message=:e
                WHERE id=:id
            """), {"s": status, "r": records, "e": error, "id": log_id})
    except Exception:
        pass


def run_cd_surf_etl(pg_engine):
    job_name = "CD_SURF_ETL"
    start_time = datetime.now()
    total = 0
    log_id = None

    try:
        with pg_engine.connect() as conn:
            res = conn.execute(text(
                "INSERT INTO etl_job_log (job_name, status, start_time, records_processed) "
                "VALUES (:n, 'RUNNING', :t, 0) RETURNING id"
            ), {"n": job_name, "t": start_time})
            log_id = res.fetchone()[0]
            conn.commit()
    except Exception as e:
        logger.error(f"Log insert error: {e}")

    try:
        # Watermark: most recent burned_at already stored
        with pg_engine.connect() as conn:
            row = conn.execute(text(
                "SELECT MAX(burned_at) FROM cd_print_log WHERE burned_at IS NOT NULL"
            )).fetchone()
        last_sync = row[0] if row and row[0] else datetime(2000, 1, 1)
        logger.info(f"CD surf ETL: watermark={last_sync}")

        # Pull from Oracle
        conn_ora = _get_cd_surf_conn(pg_engine)
        try:
            cur = conn_ora.cursor()
            cur.execute(_ORACLE_QUERY, {"last_sync": last_sync})
            ora_rows = cur.fetchall()
            cur.close()
        finally:
            conn_ora.close()

        if not ora_rows:
            logger.info("CD surf ETL: no new records.")
            _update_log(pg_engine, log_id, "SUCCESS", 0)
            return 0

        logger.info(f"CD surf ETL: {len(ora_rows)} rows to upsert.")

        # Enrich with PACS study metadata
        study_uid_map = _resolve_studies(pg_engine, [r[5] for r in ora_rows])

        rows_to_upsert = []
        skipped_sr = 0
        for r in ora_rows:
            study_uid = str(r[5]).strip() if r[5] else None
            info = study_uid_map.get(study_uid, {})
            if info.get("study_modality") == 'SR':
                skipped_sr += 1
                continue
            rows_to_upsert.append({
                "task_id":           int(r[0]),
                "patient_name":      str(r[1]).strip() if r[1] else None,
                "burned_at":         r[2],
                "task_status":       int(r[3]) if r[3] is not None else None,
                "burn_identifier":   str(r[4]).strip() if r[4] else None,
                "study_instance_uid": study_uid,
                "cd_id":             int(r[6]) if r[6] is not None else None,
                "number_of_copies":  int(r[7]) if r[7] is not None else None,
                "cd_status":         int(r[8]) if r[8] is not None else None,
                "media_type":        str(r[9]).strip() if r[9] else None,
                "cd_folder":         str(r[10]).strip() if r[10] else None,
                "order_id":          str(r[11]).strip() if r[11] else None,
                "study_db_uid":      info.get("study_db_uid"),
                "accession_number":  info.get("accession_number"),
                "study_date":        info.get("study_date"),
                "study_modality":    info.get("study_modality"),
                "reading_physician": info.get("reading_physician"),
            })

        with pg_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO cd_print_log (
                    task_id, patient_name, burned_at, task_status, burn_identifier,
                    study_instance_uid, cd_id, number_of_copies, cd_status, media_type,
                    cd_folder, order_id, study_db_uid, accession_number, study_date,
                    study_modality, reading_physician, synced_at
                )
                VALUES (
                    :task_id, :patient_name, :burned_at, :task_status, :burn_identifier,
                    :study_instance_uid, :cd_id, :number_of_copies, :cd_status, :media_type,
                    :cd_folder, :order_id, :study_db_uid, :accession_number, :study_date,
                    :study_modality, :reading_physician, NOW()
                )
                ON CONFLICT (task_id) DO UPDATE SET
                    patient_name      = EXCLUDED.patient_name,
                    burned_at         = EXCLUDED.burned_at,
                    task_status       = EXCLUDED.task_status,
                    cd_status         = EXCLUDED.cd_status,
                    media_type        = EXCLUDED.media_type,
                    study_db_uid      = EXCLUDED.study_db_uid,
                    accession_number  = EXCLUDED.accession_number,
                    study_date        = EXCLUDED.study_date,
                    study_modality    = EXCLUDED.study_modality,
                    reading_physician = EXCLUDED.reading_physician,
                    synced_at         = NOW()
            """), rows_to_upsert)
            total = len(rows_to_upsert)

        logger.info(f"CD surf ETL: done, {total} records upserted, {skipped_sr} SR skipped.")
        _update_log(pg_engine, log_id, "SUCCESS", total)
        return total

    except Exception as e:
        logger.error(f"CD surf ETL failed: {e}", exc_info=True)
        _update_log(pg_engine, log_id, "FAILED", total, str(e)[:500])
        raise
