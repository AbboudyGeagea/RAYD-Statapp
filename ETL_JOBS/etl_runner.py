import os
import sys
import logging
import argparse
import datetime
import traceback
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import random
import time

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app
from db import db, DBParams, OracleConnector

# ----------------------------
# LOGGING
# ----------------------------
logger = logging.getLogger("ETL")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d → %(message)s")
)
logger.addHandler(console)

# ----------------------------
# ORACLE THICK CLIENT INIT
# ----------------------------
INSTANT_CLIENT_DIR = "/opt/oracle/instantclient_21_13"
import oracledb

try:
    if os.path.isdir(INSTANT_CLIENT_DIR):
        oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
        logger.info("Oracle Thick Mode ENABLED")
except Exception:
    logger.error("❌ Oracle thick mode init failed")
    logger.error(traceback.format_exc())

# ----------------------------
# POSTGRES CONNECTION
# ----------------------------
def get_postgres_engine(target_name: str):
    try:
        record = DBParams.query.filter_by(name=target_name, db_type='postgres').first()
        if not record:
            logger.error(f"No Postgres DBParams found for {target_name}")
            return None
        engine = create_engine(record.conn_string, echo=True)
        return engine
    except Exception:
        logger.error(f"❌ Postgres connection failed for '{target_name}'")
        return None

def get_go_live_date(pg_engine):
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(text("SELECT go_live_date FROM go_live_config LIMIT 1")).scalar()
            logger.info(f"📅 Go-live date fetched from DB: {result}")
            return result
    except Exception:
        logger.error("❌ Failed to fetch go-live date from DB")
        return None

# ----------------------------
# CHUNKED UPSERT HELPER
# ----------------------------
def chunked_upsert(pg_engine, table_name, col_names, rows, conflict_key, logger, batch_size=5000):
    metadata = MetaData()
    metadata.reflect(bind=pg_engine)
    table = metadata.tables[table_name]

    stmt = insert(table)
    update_dict = {col: getattr(stmt.excluded, col) for col in col_names}

    stmt = stmt.on_conflict_do_update(
        index_elements=[conflict_key],
        set_=update_dict
    )

    with pg_engine.begin() as connection:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            mapped_rows = [dict(zip(col_names[:-1], row)) for row in batch]

            for r in mapped_rows:
                r['last_update'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            connection.execute(stmt, mapped_rows)

# ----------------------------
# INCREMENTAL ETL
# ----------------------------
def incremental_etl(name, oracle_source, oracle_table, postgres_target, pg_table, go_live_date=None, simulate_rows=None):
    logger.info(f"▶ ETL START: {name}")
    start_time = time.time()
    pg_engine = get_postgres_engine(postgres_target)
    if not pg_engine: return

    try:
        rows = []
        col_names = []

        # STUDIES
        if oracle_table.endswith("didb_studies"):
            col_names = [
                'study_db_uid', 'patient_db_uid', 'study_instance_uid', 'last_accessed_time',
                'number_of_study_series', 'number_of_study_images', 'accession_number',
                'study_body_part', 'study_date', 'insert_time', 'mdl_order_dbid',
                'referring_physician_first_name', 'referring_physician_last_name',
                'referring_physician_mid_name', 'study_status', 'study_description',
                'storing_ae', 'patient_class', 'procedure_code', 'report_status',
                'order_status', 'signing_physician_first_name', 'signing_physician_last_name',
                'signing_physician_id', 'study_age', 'rep_transcribed_by',
                'rep_transcribed_timestamp', 'rep_prelim_timestamp', 'rep_final_signed_by',
                'rep_final_timestamp', 'rep_addendum_by', 'rep_addendum_timestamp',
                'rep_has_addendum', 'study_locked', 'last_update'
            ]
            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor = ora_conn.cursor()
            query = """
                SELECT study_db_uid, patient_db_uid, study_instance_uid, last_access_time,
                       number_of_study_series, number_of_study_images, accession_number,
                       study_body_part, study_date, insert_time, mdl_order_dbid,
                       referring_physician_first_name, referring_physician_last_name,
                       referring_physician_mid_name, study_status, study_description,
                       storing_ae, patient_class, procedure_code, report_status,
                       order_status, signing_physician_first_name, signing_physician_last_name,
                       signing_physician_id, study_age, rep_transcribed_by,
                       rep_transcribed_timestamp, rep_prelim_timestamp,
                       rep_final_signed_by, rep_final_timestamp, rep_addendum_by,
                       rep_addendum_timestamp, rep_has_addendum, study_locked
                FROM medistore.didb_studies
                WHERE study_date >= TO_DATE(:go_live_date, 'YYYY-MM-DD')
            """
            cursor.execute(query, {'go_live_date': str(go_live_date)})
            rows = cursor.fetchall()
            cursor.close()
            chunked_upsert(pg_engine, pg_table, col_names, rows, 'study_db_uid', logger)

        # PATIENTS
        elif oracle_table.endswith("didb_patients_view"):
            col_names = [
                'patient_db_uid', 'id', 'birth_date', 'sex',
                'number_of_patient_studies', 'number_of_patient_series',
                'number_of_patient_images', 'mdl_patient_dbid',
                'fallback_id', 'age_group', 'last_update'
            ]
            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor = ora_conn.cursor()
            query = """
                SELECT patient_db_uid, id, birth_date, sex,
                       number_of_patient_studies, number_of_patient_series,
                       number_of_patient_images, mdl_patient_dbid, fallback_pid
                FROM medistore.didb_patients_view
            """
            cursor.execute(query)
            raw_rows = cursor.fetchall()
            cursor.close()

            today = datetime.date.today()
            processed_rows = []
            for r in raw_rows:
                row_list = list(r)
                bday = row_list[2]
                
                # Calculate age
                age = None
                if bday and isinstance(bday, (datetime.date, datetime.datetime)):
                    age = today.year - bday.year - ((today.month, today.day) < (bday.month, bday.day))
                
                row_list.append(age) # Append to 'age_group' column
                processed_rows.append(tuple(row_list))

            chunked_upsert(pg_engine, pg_table, col_names, processed_rows, 'patient_db_uid', logger)

        # ORDERS
        elif oracle_table.endswith("mdb_orders"):
            col_names = [
                'order_dbid', 'patient_dbid', 'proc_id', 'proc_text',
                'study_instance_uid', 'study_db_uid', 'has_study',
                'scheduled_datetime', 'order_control', 'order_status',
                'placer_field1', 'placer_field2', 'last_update'
            ]
            ora_conn = OracleConnector.get_connection(oracle_source)
            cursor = ora_conn.cursor()
            query = """
                SELECT order_dbid, patient_dbid, proc_id, proc_text,
                       study_instance_uid, study_db_uid, has_study,
                       scheduled_datetime, order_control, order_status,
                       placer_field1, placer_field2
                FROM medilink.mdb_orders
                WHERE scheduled_datetime >= TO_DATE(:go_live_date, 'YYYY-MM-DD')
            """
            cursor.execute(query, {'go_live_date': str(go_live_date)})
            rows = cursor.fetchall()
            cursor.close()
            chunked_upsert(pg_engine, pg_table, col_names, rows, 'order_dbid', logger)

    except Exception:
        logger.error(f"❌ ETL error in {name}")
        logger.error(traceback.format_exc())

def run_all_etl():
    pg_engine = get_postgres_engine("etl_db")
    go_live_date = get_go_live_date(pg_engine)
    incremental_etl("Studies", "oracle_PACS", "medistore.didb_studies", "etl_db", "etl_didb_studies", go_live_date)
    incremental_etl("Patients", "oracle_PACS", "medistore.didb_patients_view", "etl_db", "etl_patient_view", None)
    incremental_etl("Orders", "oracle_PACS", "medilink.mdb_orders", "etl_db", "etl_orders", go_live_date)

def main():
    app = create_app()
    with app.app_context():
        run_all_etl()

if __name__ == "__main__":
    main()
