"""
tests/test_sample_data.py — scripts/sample_data/: a sample of production's Postgres for the
local stack. The export is read-only and never carries names, phones or raw HL7; with
--pseudo (the default) identifiers are hashed consistently, so the sample still joins, birth
dates keep only the year and report text is dropped. The import loads only a local stack's
database and refuses anything else.
"""
import os
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text

from conftest import ROOT, TEST_DB

SCRIPTS = os.path.join(ROOT, 'scripts', 'sample_data')
SCRATCH = TEST_DB + '_import'


@pytest.fixture
def rows(app):
    """A patient, a study, its RIS order and RIS patient, and an ORU report, in October."""
    from db import db
    stmts = [
        "INSERT INTO etl_patient_view (patient_db_uid, id, birth_date, sex) VALUES (96001, 'MRN-96001', '1980-06-15', 'F')",
        """INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, study_instance_uid,
               storing_ae, study_date, study_modality) VALUES (96001, 96001, 'ACC-96001', '1.2.840.96001', 'CT1', '2026-10-05', 'CT')""",
        """INSERT INTO etl_orders (order_dbid, patient_dbid, study_db_uid, accession_number, scheduled_datetime, proc_text)
           VALUES (96010, '96007', 96001, 'ACC-96001', '2026-10-05 09:00', 'CT HEAD')""",
        """INSERT INTO std_patients_ris (patient_person_key, birth_date, mobile_phone_number)
           VALUES (96007, '1980-06-15', '+96170000000')""",
        """INSERT INTO hl7_oru_reports (id, accession_number, patient_id, report_text, impression_text, result_datetime)
           VALUES (96001, 'ACC-96001', 'MRN-96001', 'Findings: normal.', 'Normal.', '2026-10-05 12:00')""",
        """INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae, study_date)
           VALUES (96002, 96001, 'ACC-OUTSIDE', 'CT1', '2026-08-01')""",
    ]
    with app.app_context():
        for s in stmts:
            db.session.execute(text(s))
        db.session.commit()
    yield
    with app.app_context():
        db.session.execute(text("DELETE FROM std_patients_ris WHERE patient_person_key = 96007"))
        db.session.commit()


def export(database_url, out, *extra):
    r = subprocess.run([sys.executable, os.path.join(SCRIPTS, 'export_sample.py'), '--dsn', database_url,
                        '--from', '2026-10-01', '--to', '2026-10-14', '--out', out, *extra],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr + r.stdout
    return out + '.tar.gz'


def load(dsn, archive):
    return subprocess.run([sys.executable, os.path.join(SCRIPTS, 'import_sample.py'), archive, '--dsn', dsn],
                          capture_output=True, text=True)


@pytest.fixture
def scratch(database_url):
    """A database set up as a local stack is: schema, migrations, the PACS pointed nowhere."""
    import psycopg2
    import db_bootstrap
    import db_migrations
    admin = database_url.rsplit('/', 1)[0] + '/postgres'
    c = psycopg2.connect(admin)
    c.autocommit = True
    c.cursor().execute(f'DROP DATABASE IF EXISTS "{SCRATCH}" WITH (FORCE)')
    c.cursor().execute(f'CREATE DATABASE "{SCRATCH}"')
    url = database_url.rsplit('/', 1)[0] + '/' + SCRATCH
    db_bootstrap.load_schema(url)
    engine = create_engine(url.replace('postgresql://', 'postgresql+psycopg2://', 1))
    db_migrations.run_migrations_on(engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE db_params SET host = 'oracle-disabled.invalid' WHERE name = 'oracle_PACS'"))
    yield url, engine
    engine.dispose()
    c.cursor().execute(f'DROP DATABASE IF EXISTS "{SCRATCH}" WITH (FORCE)')
    c.close()


def one(engine, sql):
    with engine.connect() as c:
        return c.execute(text(sql)).fetchall()


def test_a_pseudonymised_sample_still_joins_and_carries_no_identifier(rows, database_url, scratch, tmp_path):
    url, engine = scratch
    r = load(url, export(database_url, str(tmp_path / 'sample')))
    assert r.returncode == 0, r.stderr + r.stdout
    study = one(engine, "SELECT accession_number, study_instance_uid FROM etl_didb_studies WHERE study_db_uid = 96001")[0]
    order = one(engine, "SELECT accession_number, proc_text FROM etl_orders WHERE order_dbid = 96010")[0]
    oru = one(engine, "SELECT accession_number, patient_id, report_text, impression_text FROM hl7_oru_reports WHERE id = 96001")[0]
    patient = one(engine, "SELECT id, birth_date FROM etl_patient_view WHERE patient_db_uid = 96001")[0]
    ris = one(engine, "SELECT birth_date, mobile_phone_number FROM std_patients_ris WHERE patient_person_key = 96007")[0]
    assert study[0].startswith('X') and study[0] != 'ACC-96001'
    assert study[0] == order[0] == oru[0]                      # the same accession everywhere
    assert study[1].startswith('X')                            # DICOM UID hashed
    assert patient[0].startswith('X') and patient[0] == oru[1] # the same patient ID everywhere
    assert str(patient[1]) == '1980-01-01' and str(ris[0]) == '1980-01-01'
    assert oru[2] is None and oru[3] is None                   # no report text
    assert ris[1] is None                                      # no phone number
    assert order[1] == 'CT HEAD'                               # what is not identifying stays
    assert one(engine, "SELECT COUNT(*) FROM etl_didb_studies WHERE study_db_uid = 96002")[0][0] == 0   # outside the dates


def test_without_pseudo_identifiers_stay_but_contacts_never_leave(rows, database_url, scratch, tmp_path):
    url, engine = scratch
    r = load(url, export(database_url, str(tmp_path / 'sample'), '--no-pseudo'))
    assert r.returncode == 0, r.stderr + r.stdout
    assert one(engine, "SELECT accession_number FROM etl_didb_studies WHERE study_db_uid = 96001")[0][0] == 'ACC-96001'
    assert one(engine, "SELECT report_text FROM hl7_oru_reports WHERE id = 96001")[0][0] == 'Findings: normal.'
    assert one(engine, "SELECT mobile_phone_number FROM std_patients_ris WHERE patient_person_key = 96007")[0][0] is None


def test_the_export_never_writes(rows, database_url, tmp_path):
    """It runs in a READ ONLY transaction: the scripts' own statements could not write."""
    src = open(os.path.join(SCRIPTS, 'export_sample.py'), encoding='utf-8').read()
    assert 'set_session(readonly=True)' in src
    for word in ('INSERT ', 'UPDATE ', 'DELETE ', 'TRUNCATE', 'DROP ', 'CREATE '):
        assert word not in src.upper().replace('INSERT_TIME', ''), word


def test_the_import_refuses_a_database_that_is_not_a_local_stack(rows, database_url, tmp_path):
    """The test database keeps the PACS host the migrations seed, as production does."""
    r = load(database_url, export(database_url, str(tmp_path / 'sample')))
    assert r.returncode != 0 and 'not a local stack' in (r.stderr + r.stdout)


def test_the_import_refuses_a_site_database(app, rows, database_url, tmp_path):
    from utils import site_db
    with app.app_context():
        ids = site_db.each_site_id()
        sjh_url = site_db.engine_for(ids[1]).url
    sjh_dsn = sjh_url.set(drivername='postgresql').render_as_string(hide_password=False)
    with create_engine(sjh_url).begin() as c:
        c.execute(text("UPDATE db_params SET host = 'oracle-disabled.invalid' WHERE name = 'oracle_PACS'"))
    try:
        r = load(sjh_dsn, export(database_url, str(tmp_path / 'sample')))
        assert r.returncode != 0 and 'site database' in (r.stderr + r.stdout)
    finally:
        with create_engine(sjh_url).begin() as c:
            c.execute(text("UPDATE db_params SET host = '192.168.2.70' WHERE name = 'oracle_PACS'"))
