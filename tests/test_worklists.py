"""
tests/test_worklists.py — Report Composer on LAUMC's RIS and PACS data.

Bekaa's suite (5d06f2cc), with its PACS orders replaced by the RIS exam and LAUMC's
own rules added (the RIS MRN, sites, pickers bounded to a year).

    catalog ↔ views     every column, field, grouping, measure and example runs
    views               what one row says, from seeded RIS / PACS / ORU / NLP / CRN rows
    compiler            values bound, never pasted; anything off-catalog refused
    sandbox             the rpt_ role reads rpt.* only, read-only, time-limited
    API                 page permission, personal / shared, admin-only SQL
"""
import os
import re
import shutil
import subprocess
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

from conftest import ROOT
from utils import worklist_catalog as cat
from utils import worklist_engine as engine
from utils.worklist_engine import WorklistError

ADMIN, VIEWER, TEC = 9001, 9002, 9003          # users 1-5 are seeded by init-db/schema.sql
TODAY = date.today()
MIDNIGHT = datetime.combine(TODAY, datetime.min.time())


@pytest.fixture(autouse=True)
def ctx(app):
    """Each test inside an app context (the engine and sql() use the app's session);
    the users and worklists it made go afterwards."""
    from db import db
    with app.app_context():
        yield
        db.session.rollback()
        for stmt in ("DELETE FROM worklists WHERE owner_id IS NOT NULL OR folder <> 'Examples'",
                     f"DELETE FROM user_page_permissions WHERE user_id IN ({ADMIN}, {VIEWER}, {TEC})",
                     f"DELETE FROM users WHERE id IN ({ADMIN}, {VIEWER}, {TEC})"):
            db.session.execute(text(stmt))
        db.session.commit()


# ── helpers ───────────────────────────────────────────────────────────────────

def sql(statement, **params):
    """Run one statement on the app's own (owner) connection and commit."""
    from db import db
    res = db.session.execute(text(statement), params)
    rows = [dict(r) for r in res.mappings().all()] if res.returns_rows else None
    db.session.commit()
    return rows


def users():
    sql(f"""INSERT INTO users (id, username, password_hash, role) VALUES
            ({ADMIN}, 'wl-admin', 'x', 'admin'), ({VIEWER}, 'wl-viewer', 'x', 'viewer'),
            ({TEC}, 'wl-tec', 'x', 'tec')""")


def h(client, method, url, uid=ADMIN, **kw):
    return getattr(client, method)(url, headers={'X-Test-Uid': str(uid)}, **kw)


def study(uid, **kw):
    row = dict(study_db_uid=uid, patient_db_uid=kw.pop('patient', 500), accession_number=f'ACC{uid}',
               storing_ae='CT_1', study_date=TODAY, study_time='103000', study_modality='CT',
               study_description='CT HEAD', procedure_code='CT01', patient_class='O',
               number_of_study_images=100, number_of_study_series=3,
               insert_time=MIDNIGHT + timedelta(hours=11))
    row.update(kw)
    cols = ', '.join(row)
    sql(f"INSERT INTO etl_didb_studies ({cols}) VALUES ({', '.join(':' + c for c in row)})", **row)


def patient(uid=500, pid='MRN500', birth=date(1980, 6, 1), sex='F', mdl=None, fallback=None):
    sql("""INSERT INTO etl_patient_view (patient_db_uid, id, birth_date, sex, mdl_patient_dbid,
                                         fallback_id)
           VALUES (:u, :p, :b, :s, :m, :f)""", u=uid, p=pid, b=birth, s=sex, m=mdl, f=fallback)


def ae(aetitle, modality, room=None, name=None):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, room_name, display_aetitle)
           VALUES (:a, :m, :r, :n)""", a=aetitle, m=modality, r=room, n=name)


def oru(rid, accession, labels=None, critical=False, text='Normal study.', at=None, **kw):
    sql("""INSERT INTO hl7_oru_reports (id, accession_number, physician_id, report_text,
                                        impression_text, result_datetime, modality, procedure_name)
           VALUES (:id, :acc, :phys, :txt, :imp, :at, :mod, :proc)""",
        id=rid, acc=accession, phys=kw.get('phys', 'DR1'), txt=text, imp=kw.get('imp'),
        at=at or MIDNIGHT + timedelta(hours=14), mod=kw.get('mod'), proc=kw.get('proc'))
    if labels is not None:
        sql("""INSERT INTO hl7_oru_analysis (report_id, affirmed_labels, is_critical, nlp_version)
               VALUES (:r, :l, :c, 'medspacy-v5')""", r=rid, l=labels, c=critical)


def ris_patient(person=7000, mrn='MRN7000', birth=date(1990, 1, 15), gender='M', primary='Y',
                other=None):
    """A RIS patient and its IDs: the MRN, and another ID that must not win."""
    sql("""INSERT INTO std_patients_ris (patient_person_key, birth_date, gender_code)
           VALUES (:p, :b, :g)""", p=person, b=birth, g=gender)
    sql("""INSERT INTO std_patient_ids (patient_id_list_key, patient_person_key, patient_id,
                                        is_primary, display_sort_order)
           VALUES (:k, :p, :m, :y, 2)""", k=person * 10, p=person, m=mrn, y=primary)
    if other:
        sql("""INSERT INTO std_patient_ids (patient_id_list_key, patient_person_key, patient_id,
                                            is_primary, display_sort_order)
               VALUES (:k, :p, :m, 'N', 1)""", k=person * 10 + 1, p=person, m=other)


def ris_exam(key, status=40, accession=None, at=None, **kw):
    """One RIS worklist row as etl_orders holds it on LAUMC (order_dbid = SITE_WORKLIST_KEY)."""
    row = dict(order_dbid=key, patient_dbid='7000', proc_id='CT01', proc_text='CT BRAIN',
               scheduled_datetime=at or MIDNIGHT + timedelta(hours=10), status_key=status,
               modality='CT', order_control='SAP_PROD', accession_number=accession or f'R{key}')
    row.update(kw)
    cols = ', '.join(row)
    sql(f"INSERT INTO etl_orders ({cols}) VALUES ({', '.join(':' + c for c in row)})", **row)


def milestone(table, key, when, pps=None, status=None):
    col = {'scheduled': 'scheduled_at', 'arrivals': 'arrived_at', 'exam_done': 'exam_done_at',
           'cancellations': 'cancelled_at'}[table]
    if table == 'cancellations':
        sql(f"""INSERT INTO std_worklist_cancellations (site_worklist_key, status_key, {col})
                VALUES (:k, :s, :t)""", k=key, s=status or 50, t=when)
    else:
        sql(f"""INSERT INTO std_worklist_{table} (site_worklist_key, pps_key, {col})
                VALUES (:k, :p, :t)""", k=key, p=pps, t=when)


def resource(key, first, last, role):
    sql("""INSERT INTO std_resources_ris (resource_id_key, first_name, last_name, role_code)
           VALUES (:k, :f, :l, :r)""", k=key, f=first, l=last, r=role)


def one(view, where='TRUE', **params):
    rows = sql(f'SELECT * FROM {view} WHERE {where}', **params)
    assert len(rows) == 1, rows
    return rows[0]


def run_defn(defn, now=None):
    return engine.run(engine.compile_definition(defn, now=now))


# ── migration ─────────────────────────────────────────────────────────────────

def test_migrations_applied_through_the_app_runner(app):
    names = {r['name'] for r in sql("SELECT name FROM schema_migrations")}
    assert {'0136_report_composer.sql', '0137_worklist_indexes.sql'} <= names
    idx = {r['indexname'] for r in sql("SELECT indexname FROM pg_indexes")}
    assert {'idx_etl_didb_serieses_study_db_uid', 'idx_etl_didb_studies_study_date'} <= idx
    role = sql("SELECT value FROM settings WHERE key = 'worklist_db_role'")[0]['value']
    assert sql("SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname = :r", r=role)[0] == \
        {'rolcanlogin': True, 'rolsuper': False}


def test_custom_reports_is_gone(app):
    for path in ('routes/custom_reports.py', 'routes/report_widgets.py',
                 'templates/custom_reports_list.html', 'templates/custom_reports_view.html',
                 'templates/custom_reports_composer.html'):
        assert not os.path.exists(os.path.join(ROOT, path)), path
    # the AI assistant points to the editor that replaced it
    sections = {r['section'] for r in sql('SELECT section FROM ai_doc_chunks')}
    assert 'Report Composer' in sections and 'Custom Reports' not in sections


# ── catalog ↔ views ───────────────────────────────────────────────────────────

def _view_columns(dataset):
    name = cat.DATASETS[dataset]['view'].split('.', 1)[1]
    return [r['column_name'] for r in sql("""
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'rpt' AND table_name = :t ORDER BY ordinal_position""", t=name)]


@pytest.mark.parametrize('dataset', list(cat.DATASETS))
def test_catalog_columns_are_exactly_the_view_columns(app, dataset):
    view = _view_columns(dataset)
    assert view, f'{dataset}: view missing'
    assert sorted(c for c, _ in cat.COLUMNS[dataset]) == sorted(view)
    assert sorted(cat.COLUMN_DOCS[dataset]) == sorted(view)
    assert all(cat.COLUMN_DOCS[dataset][c].strip() for c in view)


@pytest.mark.parametrize('dataset', list(cat.DATASETS))
def test_every_field_column_exists_in_its_datasets(app, dataset):
    view = set(_view_columns(dataset))
    for f in cat.fields_for(dataset):
        assert f['col'] in view, (dataset, f['id'])
        day = (f.get('date_col') or {}).get(dataset)
        assert day is None or day in view, (dataset, f['id'], day)


@pytest.mark.parametrize('dataset,measure', [(d, m) for d in cat.DATASETS
                                             for m, _ in cat.measures_for(d)])
def test_every_measure_runs(app, dataset, measure):
    res = run_defn({'dataset': dataset, 'output': {'kind': 'table', 'measures': [measure]}})
    assert len(res['rows']) == 1


@pytest.mark.parametrize('dataset,dim', [(d, x) for d in cat.DATASETS
                                         for x, _ in cat.dimensions_for(d)])
def test_every_grouping_runs(app, dataset, dim):
    run_defn({'dataset': dataset, 'output': {'kind': 'table', 'rows': dim}})


_SAMPLE = {'date': {'mode': 'quick', 'quick': 'last_30_days'}, 'text': {'value': 'x'},
           'yesno': {'value': 'yes'}, 'range': {'min': 0, 'max': 5}}


@pytest.mark.parametrize('dataset,fid', [(d, f['id']) for d in cat.DATASETS
                                         for f in cat.fields_for(d)])
def test_every_criterion_compiles_and_runs(app, dataset, fid):
    f = cat.FIELD[fid]
    c = _SAMPLE.get(f['type']) or {'values': [f['options'][0][0] if isinstance(f['options'], list)
                                              else 'x']}
    run_defn({'dataset': dataset, 'criteria': {fid: c}})


def test_every_seeded_example_runs(app):
    rows = sql("SELECT name, definition FROM worklists WHERE folder = 'Examples' ORDER BY id")
    assert len(rows) == 14
    datasets = set()
    for r in rows:
        run_defn(r['definition'])
        datasets.add(r['definition']['dataset'])
    assert datasets == set(cat.DATASETS)          # every dataset has an example


@pytest.mark.parametrize('i', range(len(cat.SQL_EXAMPLES)))
def test_every_sql_example_runs(app, i):
    engine.run(hand_sql=cat.SQL_EXAMPLES[i]['sql'])


# ── rpt.exams (RIS) ───────────────────────────────────────────────────────────

@pytest.fixture
def full_exam(app):
    """One CT exam through its whole RIS life: booked, arrived, scanned on a mapped
    device by a technologist, exam done, signed and approved, in PACS."""
    ae('CT_RIS', 'CT', room='CT Room 1', name='CT 64')
    ris_patient(other='HIS-123')
    patient(500, 'PIX_500')
    study(1, accession_number='R10', storing_ae='CT_STORE', patient_class='E',
          referring_physician_first_name='Rima', referring_physician_last_name='Haddad')
    ris_exam(10, status=160, study_db_uid=1)
    milestone('scheduled', 10, MIDNIGHT - timedelta(days=2), pps=55)
    milestone('scheduled', 10, MIDNIGHT - timedelta(days=1), pps=55)          # re-booked
    milestone('arrivals', 10, MIDNIGHT + timedelta(hours=9, minutes=50), pps=55)
    milestone('exam_done', 10, MIDNIGHT + timedelta(hours=10, minutes=40), pps=55)
    sql("INSERT INTO std_procedure_priorities (priority_key, code, description) VALUES (3, 'U', 'Urgent')")
    sql("""INSERT INTO std_pps (pps_key, start_datetime, end_datetime, performing_ae_title, priority_key)
           VALUES (55, :s, :e, 'ct_ris', 3)""",
        s=MIDNIGHT + timedelta(hours=10, minutes=5), e=MIDNIGHT + timedelta(hours=10, minutes=25))
    resource(1, 'Nadia', 'Saab', 'REC')
    resource(2, 'Karim', 'Aoun', 'TEC')
    resource(3, 'Lina', 'Rizk', 'RAD')
    resource(4, 'Tamina', 'Karam', 'RAD')
    sql("""INSERT INTO std_pps_person_reference (pps_person_reference_key, pps_key, resource_id_key,
                                                 display_sort_order)
           VALUES (1, 55, 1, 0), (2, 55, 2, 1)""")
    sql("""INSERT INTO std_reports (report_key, version, is_max_version, reported_acc_number,
                                    verified1_date, approved_date,
                                    verified1_by_resource_id_key, approved_by_resource_id_key)
           VALUES (900, 1, false, 'R10', :old, :old, 4, 4),
                  (900, 2, true,  'R10', :s, :a, 3, 4)""",
        old=MIDNIGHT + timedelta(hours=11), s=MIDNIGHT + timedelta(hours=12, minutes=40),
        a=MIDNIGHT + timedelta(hours=16, minutes=40))


@pytest.mark.parametrize('column,want', [
    ('accession', 'R10'), ('patient_id', 'MRN7000'), ('sex', 'M'), ('age_band', '18-39'),
    ('patient_class', 'E'), ('patient_type', 'Emergency'), ('modality', 'CT'),
    # device_name: the RIS room name, no longer the typed alias 'CT 64' (migration 0148)
    ('ris_modality', 'CT'), ('device', 'ct_ris'), ('device_name', 'CT Room 1'), ('room', 'CT Room 1'),
    ('site', 'LAUMC - Rizk Hospital (Main)'), ('procedure', 'CT BRAIN'), ('procedure_code', 'CT01'),
    ('priority', 'Urgent'), ('exam_status', 'Approved'), ('ris_status', 'Approved'),
    ('cancelled', False), ('performed', True), ('technologist', 'Karim Aoun'),
    ('ris_report', 'Approved'), ('signed_by', 'Lina Rizk'), ('approved_by', 'Tamina Karam'),
    ('referring', 'Rima Haddad'), ('study_uid', 1), ('in_pacs', True), ('images', 100),
    ('hour', 10), ('shift', 'Morning'), ('cancelled_at', None),
])
def test_exams_columns(full_exam, column, want):
    assert one('rpt.exams')[column] == want


def test_exams_times_and_turnarounds(full_exam):
    r = one('rpt.exams')
    assert r['scheduled_at'] == MIDNIGHT + timedelta(hours=10)
    assert r['booked_at'] == MIDNIGHT - timedelta(days=2)               # the first booking
    assert r['arrived_at'] == MIDNIGHT + timedelta(hours=9, minutes=50)
    assert r['started_at'] == MIDNIGHT + timedelta(hours=10, minutes=5)
    assert r['exam_at'] == r['started_at']
    assert (float(r['wait_minutes']), float(r['start_delay_minutes']), float(r['exam_minutes']),
            float(r['arrival_to_done_minutes'])) == (15.0, 5.0, 20.0, 50.0)
    assert (float(r['signed_tat_hours']), float(r['report_tat_hours'])) == (2.0, 6.0)  # current version
    assert r['age'] == date_part_years(date(1990, 1, 15), TODAY)


def date_part_years(born, on):
    return on.year - born.year - ((on.month, on.day) < (born.month, born.day))


def test_exams_before_they_happen_and_cancelled(app):
    ris_patient()
    ris_exam(1, status=40)                                              # booked only
    ris_exam(2, status=50)                                              # cancelled
    milestone('cancellations', 2, MIDNIGHT + timedelta(hours=8))
    ris_exam(3, status=60)                                              # cancelled, then reinstated
    milestone('cancellations', 3, MIDNIGHT + timedelta(hours=7))
    milestone('arrivals', 3, MIDNIGHT + timedelta(hours=9))
    got = {r['exam_id']: r for r in sql('SELECT * FROM rpt.exams')}
    assert (got[1]['exam_status'], got[1]['performed'], got[1]['cancelled'], got[1]['in_pacs'],
            got[1]['ris_report'], got[1]['device'], got[1]['patient_type']) == \
        ('Scheduled', False, False, False, 'Not reported', None, None)
    assert got[1]['exam_at'] == MIDNIGHT + timedelta(hours=10)          # due, not done
    assert (got[2]['cancelled'], got[2]['cancelled_at']) == (True, MIDNIGHT + timedelta(hours=8))
    assert (got[3]['exam_status'], got[3]['cancelled'], got[3]['cancelled_at']) == ('Arrived', False, None)
    run = lambda m: run_defn({'dataset': 'exams', 'output': {'kind': 'table', 'measures': [m]}})['rows']
    assert (run('performed'), run('not_performed'), run('cancelled')) == ([[0]], [[2]], [[1]])


def test_exam_status_and_report_states(app):
    ris_patient()
    ris_exam(1, status=None, order_status='CM')                         # no status key: CM
    ris_exam(2, status=999999, order_status='on_hold')                  # unknown key
    ris_exam(3, status=100)
    ris_exam(4, status=130)
    sql("""INSERT INTO std_reports (report_key, version, is_max_version, reported_acc_number,
                                    verified1_date) VALUES
           (1, 0, true, 'R3', NULL), (2, 0, true, 'R4', :t)""", t=MIDNIGHT)
    got = {r['exam_id']: (r['exam_status'], r['ris_report']) for r in sql('SELECT * FROM rpt.exams')}
    assert got == {1: ('Exam done', 'Not reported'), 2: ('On Hold', 'Not reported'),
                   3: ('Exam done', 'Draft'), 4: ('Signed', 'Signed')}


def test_the_mrn_flagged_primary_wins_else_the_first_listed(app):
    ris_patient(7000, 'MRN-A', other='HIS-A')
    ris_patient(7001, 'MRN-B', primary='N', other='HIS-B')
    ris_exam(1)
    ris_exam(2, patient_dbid='7001')
    ris_exam(3, patient_dbid='not-a-number')
    got = {r['exam_id']: r['patient_id'] for r in sql('SELECT * FROM rpt.exams')}
    assert got == {1: 'MRN-A', 2: 'HIS-B', 3: None}


def test_site_by_the_ris_issuer(app):
    ris_exam(1, order_control='SAP_PROD')
    ris_exam(2, order_control='ELSEWHERE')
    ris_exam(3, order_control='SAP_SJH')   # RH only (operator, 2026-11-05): not shown at all
    got = {r['exam_id']: r['site'] for r in sql('SELECT * FROM rpt.exams')}
    assert got == {1: 'LAUMC - Rizk Hospital (Main)', 2: None}


def test_sr_exams_are_left_out(app):
    ae('SR_BOX', 'SR')
    ris_exam(1, modality='SR')
    ris_exam(2, modality='OT')
    sql("INSERT INTO std_pps (pps_key, performing_ae_title) VALUES (5, 'SR_BOX')")
    milestone('arrivals', 2, MIDNIGHT, pps=5)
    ris_exam(3)
    assert [r['exam_id'] for r in sql('SELECT exam_id FROM rpt.exams')] == [3]


def test_scheduled_filter_reads_the_appointment_index(app):
    """The RIS appointment is a column of etl_orders: a date range on it reaches the
    index before any of the per-exam lookups run."""
    sql("""INSERT INTO etl_orders (order_dbid, scheduled_datetime, modality)
           SELECT g, CAST(:t AS timestamp) - (g % 400) * INTERVAL '1 day', 'CT'
             FROM generate_series(1, 2000) g""", t=MIDNIGHT + timedelta(minutes=1))
    sql('ANALYZE etl_orders')
    c = engine.compile_definition({'dataset': 'exams',
                                   'criteria': {'scheduled_at': {'mode': 'quick', 'quick': 'today'}},
                                   'output': {'kind': 'table', 'measures': ['count']}})
    plan = '\n'.join(r['QUERY PLAN'] for r in sql('EXPLAIN ' + c['sql'], **c['params']))
    assert re.search(r'Index Scan on \w*sched\w* ', plan) or \
        re.search(r'Index Scan using \w*sched\w* on etl_orders', plan), plan
    assert 'Seq Scan on etl_orders' not in plan, plan
    # every RIS transition is read through its (site_worklist_key, time) index
    for table in ('std_worklist_scheduled', 'std_worklist_arrivals', 'std_worklist_exam_done',
                  'std_worklist_cancellations'):
        line = next(l for l in plan.splitlines() if f' on {table} ' in l)
        assert '_pps_key' not in line and 'Seq Scan' not in line, line
    assert engine.run(c)['rows'] == [[5]]


# ── rpt.studies ───────────────────────────────────────────────────────────────

@pytest.fixture
def full_study(app):
    """One CT study with everything around it: patient, AE map, two orders, an ORU
    report analysed by the NLP, a CRN, physician aliases."""
    patient()
    ae('ct_1', 'CT', room='Room 2', name='Big CT')          # case differs from storing_ae
    study(1, operators_name='DOE^JANE', performing_physicians_name='SMITH^JOHN\\ROE^ANN',
          reading_physician_first_name='Ali', reading_physician_last_name='Haddad',
          signing_physician_first_name='A.', signing_physician_last_name='Haddad',
          referring_physician_first_name='EXTERNAL DOC', age_at_exam=45.6,
          study_body_part='head', rep_prelim_timestamp=MIDNIGHT + timedelta(hours=12),
          rep_final_timestamp=MIDNIGHT + timedelta(hours=14), study_has_report=True,
          patient_location='ER', report_status='F', site_id=1)
    sql("""INSERT INTO etl_orders (order_dbid, study_db_uid, proc_id, proc_text, scheduled_datetime)
           VALUES (10, 1, 'CT01', 'old order', :a), (11, 1, 'CT01', 'CT BRAIN', :b)""",
        a=MIDNIGHT + timedelta(hours=8), b=MIDNIGHT + timedelta(hours=10))
    sql("""INSERT INTO physician_alias_map (alias, canonical_name, dismissed) VALUES
           ('A. Haddad', 'Ali Haddad', false), ('Ali Haddad', 'Dr Ali Haddad', true)""")
    oru(100, 'ACC1', labels=['Pneumothorax', 'Pleural Effusion'], critical=True)
    oru(101, 'ACC9', labels=['Pneumothorax'], critical=False)  # one report per accession at LAUMC (0054)
    sql("""INSERT INTO crn_notifications (ref_code, report_id, accession_number, marker,
                                          report_fingerprint)
           VALUES ('CRN-AAAA-BBBB', 100, 'ACC1', 'CRN', repeat('0', 64))""")


@pytest.mark.parametrize('column,want', [
    ('accession', 'ACC1'), ('patient_id', 'MRN500'), ('sex', 'F'), ('age', 45),
    ('age_band', '40-64'), ('patient_type', 'Outpatient'), ('patient_location', 'ER'),
    ('modality', 'CT'), ('pacs_modality', 'CT'), ('device', 'CT_1'), ('device_name', 'Room 2'),
    ('room', 'Room 2'), ('site', 'LAUMC - Rizk Hospital (Main)'), ('procedure', 'CT HEAD'),
    ('body_part', 'HEAD'), ('hour', 10), ('shift', 'Morning'), ('report_stage', 'Final'),
    ('has_report', True), ('technologist', 'DOE JANE'),
    ('performing_physician', 'SMITH JOHN, ROE ANN'),
    ('reading_radiologist', 'Ali Haddad'), ('signing_radiologist', 'Ali Haddad'),
    ('referring', 'External referrals'), ('images', 100), ('series', 3),
    ('oru_report', True), ('critical', True), ('findings', 'Pleural Effusion, Pneumothorax'),
    ('crn', True), ('report_status', 'F'),
])
def test_studies_columns(full_study, column, want):
    assert one('rpt.studies')[column] == want


def test_studies_times_and_delays(full_study):
    r = one('rpt.studies')
    assert r['study_at'] == MIDNIGHT + timedelta(hours=10, minutes=30)
    assert r['scheduled_at'] == MIDNIGHT + timedelta(hours=10)      # the latest order
    assert float(r['start_delay_minutes']) == 30.0
    assert float(r['store_delay_minutes']) == 30.0                  # stored 11:00
    assert float(r['report_tat_hours']) == 3.0                      # 11:00 → 14:00 final
    assert float(r['prelim_tat_hours']) == 1.0
    assert r['weekday'] == TODAY.isoweekday()
    assert r['week_of_month'] == (TODAY.day - 1) // 7 + 1


def test_the_patient_id_is_the_ris_mrn_then_the_pacs_fallback(app):
    """etl_patient_view.id is PACS-internal at LAUMC (PIX_...): the RIS MRN of the study's
    order comes first, then the fallback ID Report 22 shows."""
    ris_patient(7000, 'MRN7000')
    patient(500, 'PIX_500', fallback='FB500')
    patient(501, 'PIX_501', fallback=' ')
    study(1, patient=500)
    study(2, patient=500, accession_number='A2')
    study(3, patient=501, accession_number='A3')
    sql("INSERT INTO etl_orders (order_dbid, study_db_uid, patient_dbid) VALUES (1, 1, '7000')")
    got = {r['study_uid']: r['patient_id'] for r in sql('SELECT * FROM rpt.studies')}
    assert got == {1: 'MRN7000', 2: 'FB500', 3: 'PIX_501'}
    sql("INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid, modality) VALUES (1, 1, 'CT')")
    assert one('rpt.series')['patient_id'] == 'MRN7000'


@pytest.mark.parametrize('stored,want_hour,want_shift', [
    ('093015', 9, 'Morning'), ('09:30', 9, 'Morning'), ('7:05:00', 7, 'Morning'),
    ('153000.123', 15, 'Afternoon'), ('233000', 23, 'Night'), ('', None, None),
    ('garbage', None, None), ('250000', None, None),
])
def test_study_time_formats(app, stored, want_hour, want_shift):
    study(1, study_time=stored)
    r = one('rpt.studies')
    assert (r['hour'], r['shift']) == (want_hour, want_shift)
    if want_hour is None:
        assert r['study_at'] == MIDNIGHT
        assert r['store_delay_minutes'] is None


def test_shift_and_patient_type_follow_the_settings(app):
    study(1, study_time='080000', patient_class='AMB')
    sql("INSERT INTO settings (key, value) VALUES ('shift_morning_start', '9'), "
        "('shift_afternoon_start', '6'), ('shift_afternoon_end', '9'), "
        "('pc_emergency', 'AMB, XYZ') "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    r = one('rpt.studies')
    assert (r['shift'], r['patient_type']) == ('Afternoon', 'Emergency')


def test_procedure_falls_back_to_the_order_then_the_code(app):
    study(1, study_description=' ')
    sql("INSERT INTO etl_orders (order_dbid, study_db_uid, proc_text) VALUES (1, 1, 'CT BRAIN')")
    study(2, study_description=None, procedure_code='XR9', accession_number='A2')
    got = {r['study_uid']: r['procedure'] for r in sql('SELECT * FROM rpt.studies')}
    assert got == {1: 'CT BRAIN', 2: 'XR9'}


def test_sr_is_excluded_everywhere(app):
    ae('SR_BOX', 'SR')
    study(1, storing_ae='SR_BOX', study_modality='OT')
    study(2, study_modality='SR', storing_ae='UNMAPPED', accession_number='A2')
    study(3, accession_number='A3')
    sql("""INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid, modality) VALUES
           (1, 3, 'SR'), (2, 3, 'CT'), (3, 1, 'CT')""")
    assert [r['study_uid'] for r in sql('SELECT study_uid FROM rpt.studies')] == [3]
    assert [r['series_uid'] for r in sql('SELECT series_uid FROM rpt.series')] == [2]


def test_empty_accession_never_matches(app):
    study(1, accession_number='')
    oru(1, '', labels=['Mass'], critical=True)
    r = one('rpt.studies')
    assert (r['accession'], r['oru_report'], r['critical']) == (None, False, False)


def test_report_stages(app):
    study(1, rep_final_timestamp=MIDNIGHT, accession_number='A1')
    study(2, rep_prelim_timestamp=MIDNIGHT, accession_number='A2')
    study(3, rep_transcribed_timestamp=MIDNIGHT, accession_number='A3')
    study(4, study_has_report=True, accession_number='A4')
    study(5, accession_number='A5')
    got = {r['study_uid']: (r['report_stage'], r['has_report']) for r in sql('SELECT * FROM rpt.studies')}
    assert got == {1: ('Final', True), 2: ('Preliminary', True), 3: ('Transcribed', False),
                   4: ('Reported', True), 5: ('Not reported', False)}


# ── the other views ───────────────────────────────────────────────────────────

def test_series(full_study):
    sql("""INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid, modality, protocol_name,
                                          body_part_examined, number_of_series_images, station_name)
           VALUES (7, 1, 'CT', 'HEAD ROUTINE', 'brain', 40, 'CTSTATION')""")
    r = one('rpt.series')
    assert (r['study_modality'], r['protocol'], r['body_part'], r['images'], r['station'],
            r['patient_id'], r['hour']) == ('CT', 'HEAD ROUTINE', 'BRAIN', 40, 'CTSTATION', 'MRN500', 10)


def test_reports_and_findings(full_study):
    oru(102, 'NOPE', labels=[], text='  ', imp='Impression: fine', mod='MR', proc='MR KNEE')
    oru(103, 'NOPE2')                                          # not analysed yet
    got = {r['report_id']: r for r in sql('SELECT * FROM rpt.reports')}
    a = got[100]
    assert (a['patient_id'], a['modality'], a['procedure'], a['device'], a['patient_type']) == \
        ('MRN500', 'CT', 'CT HEAD', 'CT_1', 'Outpatient')
    assert (a['critical'], a['finding_count'], a['findings'], a['crn'], a['analysed']) == \
        (True, 2, 'Pneumothorax, Pleural Effusion', True, True)
    assert float(a['report_tat_hours']) == 3.0 and a['report_words'] == 2
    b = got[102]
    assert (b['modality'], b['procedure'], b['findings'], b['report_words'], b['has_impression']) == \
        ('MR', 'MR KNEE', None, 0, True)
    assert got[103]['analysed'] is False
    findings = sorted((r['report_id'], r['finding']) for r in sql('SELECT * FROM rpt.findings'))
    assert findings == [(100, 'Pleural Effusion'), (100, 'Pneumothorax'), (101, 'Pneumothorax')]


def test_storage(app):
    ae('CT_1', 'CT')
    sql("""INSERT INTO summary_storage_daily (study_date, modality, procedure_code, total_gb,
                                              study_count, storing_ae) VALUES
           (:d, 'OT', 'CT01', 1.5, 3, 'ct_1'), (:d, 'MR', 'MR01', 2.25, 1, 'MR_9')""", d=TODAY)
    res = run_defn({'dataset': 'storage', 'output': {'kind': 'table', 'rows': 'modality',
                                                     'measures': ['gb', 'stored_studies']}})
    assert res['rows'] == [['CT', 1.5, 3], ['MR', 2.25, 1]]


def test_crn(full_study):
    t0 = MIDNIGHT + timedelta(hours=14)
    sql("UPDATE crn_notifications SET report_received_at = :t, acknowledged_at = :a, "
        "doctor_name = 'Dr Who', status = 'acknowledged'", t=t0, a=t0 + timedelta(minutes=12))
    sql("""INSERT INTO crn_recipients (notification_id, role, phone_e164, token_hash, token_enc,
                                       expires_at)
           SELECT id, 'fallback', '+9611', repeat('a', 64), 'x', :e FROM crn_notifications""",
        e=t0 + timedelta(days=3))
    sql("UPDATE crn_notifications SET acknowledged_by = (SELECT MAX(id) FROM crn_recipients)")
    sql("INSERT INTO crn_events (notification_id, event_type) "
        "SELECT id, e FROM crn_notifications, unnest(ARRAY['overdue', 'resent']) e")
    r = one('rpt.crn')
    assert (r['doctor'], r['acknowledged'], float(r['ack_minutes']), r['overdue'], r['resent'],
            r['acknowledged_how'], r['modality'], r['device']) == \
        ('Dr Who', True, 12.0, True, True, 'fallback contact', 'CT', 'CT_1')


# ── compiler ──────────────────────────────────────────────────────────────────

def test_study_date_filter_reads_the_study_date_index(app):
    """study_at is computed, so on its own it could only be filtered after reading
    every study; the compiler adds the same day range on study_date."""
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_date, study_modality)
           SELECT g, g, CURRENT_DATE - (g % 400), 'CT' FROM generate_series(1, 2000) g""")
    sql('ANALYZE etl_didb_studies')
    c = engine.compile_definition({'dataset': 'studies',
                                   'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'}},
                                   'output': {'kind': 'table', 'measures': ['count']}})
    assert 'd."study_date" >= CAST(:study_at_from AS date)' in c['sql']
    plan = '\n'.join(r['QUERY PLAN'] for r in sql('EXPLAIN ' + c['sql'], **c['params']))
    assert 'study_date' in plan and 'Seq Scan on etl_didb_studies' not in plan, plan
    assert engine.run(c)['rows'] == [[5]]


def test_date_edges(app):
    yesterday = TODAY - timedelta(1)
    study(1, study_date=yesterday, study_time='233000', accession_number='A1')
    study(2, study_date=TODAY, study_time='000500', accession_number='A2')
    now = MIDNIGHT + timedelta(hours=1)
    get = lambda crit: sorted(r[0] for r in run_defn(
        {'dataset': 'studies', 'criteria': {'study_at': crit},
         'output': {'kind': 'list', 'columns': ['accession']}}, now=now)['rows'])
    assert get({'mode': 'quick', 'quick': 'yesterday'}) == ['A1']
    assert get({'mode': 'quick', 'quick': 'today'}) == ['A2']
    assert get({'mode': 'relative', 'dir': 'prev', 'n': 2, 'unit': 'hours'}) == ['A1', 'A2']
    assert get({'mode': 'fixed', 'from': str(TODAY), 'to': str(yesterday)}) == ['A1', 'A2']


def test_multi_text_yesno_range_and_blank(app):
    ae('CT_1', 'CT')
    study(1, operators_name='DOE^JANE')
    study(2, storing_ae='MR_1', study_modality='MR', accession_number='A2', number_of_study_images=5)
    study(3, storing_ae=None, study_modality=None, accession_number='A3', number_of_study_images=None)
    q = lambda crit: sorted(r[0] for r in run_defn(
        {'dataset': 'studies', 'criteria': crit,
         'output': {'kind': 'list', 'columns': ['study_uid']}})['rows'])
    assert q({'modality': {'values': ['CT', 'MR']}}) == [1, 2]
    assert q({'modality': {'values': ['(blank)']}}) == [3]
    assert q({'accession': {'op': 'starts with', 'value': 'acc'}}) == [1]
    assert q({'accession': {'op': 'does not contain', 'value': '2'}}) == [1, 3]
    assert q({'accession': {'op': 'equals', 'value': 'a3'}}) == [3]
    assert q({'technologist': {'values': ['DOE JANE']}}) == [1]
    assert q({'images': {'min': 6}}) == [1]
    assert q({'has_report': {'value': 'no'}}) == [1, 2, 3]


def test_groups_measures_and_totals(app):
    ae('CT_1', 'CT')
    study(1, rep_final_timestamp=MIDNIGHT + timedelta(hours=13))
    study(2, rep_final_timestamp=MIDNIGHT + timedelta(hours=15), accession_number='A2')
    study(3, storing_ae='MR_1', study_modality='MR', accession_number='A3')
    res = run_defn({'dataset': 'studies', 'output': {'kind': 'table', 'rows': 'modality',
                    'measures': ['count', 'final', 'median_report_tat']}})
    assert res['rows'] == [['CT', 2, 2, 3.0], ['MR', 1, 0, None]]
    split = run_defn({'dataset': 'studies', 'output': {'kind': 'table', 'rows': 'modality',
                      'cols': 'report_stage', 'measures': ['count', 'final']}})
    assert split['columns'] == ['row', 'col', 'count']
    by_hour = run_defn({'dataset': 'studies', 'output': {'kind': 'table', 'rows': 'hour'}})
    assert by_hour['rows'] == [['10', 3]]


@pytest.mark.parametrize('defn,msg', [
    ({'dataset': 'users'}, 'unknown dataset'),
    ({'dataset': 'orders'}, 'unknown dataset'),
    ({'dataset': 'studies', 'criteria': {'finding': {'values': ['x']}}}, 'not a field'),
    ({'dataset': 'studies', 'output': {'kind': 'list', 'columns': ['password_hash']}}, 'not a column'),
    ({'dataset': 'studies', 'output': {'kind': 'list', 'sort': 'study_at; DROP TABLE users'}},
     'cannot sort'),
    ({'dataset': 'studies', 'output': {'kind': 'table', 'measures': ['gb']}}, 'not a measure'),
    ({'dataset': 'studies', 'output': {'kind': 'table', 'rows': 'protocol'}}, 'cannot group'),
    ({'dataset': 'studies', 'criteria': {'shift': {'values': ['Lunch']}}}, 'not one of its values'),
    ({'dataset': 'studies', 'criteria': {'images': {'min': '1 OR 1=1'}}}, 'not a number'),
    ({'dataset': 'studies', 'output': {'kind': 'pie'}}, 'unknown output'),
])
def test_compiler_refuses(defn, msg):
    with pytest.raises(WorklistError, match=msg):
        engine.compile_definition(defn)


def test_values_never_reach_the_sql_text(app):
    evil = "x'); DROP TABLE users; --"
    before = sql('SELECT count(*) AS n FROM users')[0]['n']
    c = engine.compile_definition({'dataset': 'studies',
                                   'criteria': {'accession': {'value': evil},
                                                'modality': {'values': [evil]}}})
    assert evil not in c['sql'] and 'DROP' not in c['sql']
    engine.run(c)
    assert sql('SELECT count(*) AS n FROM users')[0]['n'] == before


def test_editable_sql_runs_like_the_generated_one(app):
    ae('CT_1', 'CT')
    study(1, accession_number="O'BRIEN")
    defn = {'dataset': 'studies', 'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'},
                                               'accession': {'value': "o'b"}},
            'output': {'kind': 'table', 'rows': 'modality', 'measures': ['count']}}
    c = engine.compile_definition(defn)
    assert engine.run(hand_sql=engine.inline_sql(c))['rows'] == engine.run(c)['rows'] == [['CT', 1]]


# ── sandbox ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('bad', [
    'SELECT * FROM users',
    'SELECT * FROM public.settings',
    'SELECT * FROM etl_didb_studies',
    'SELECT * FROM std_patient_ids',
    'WITH x AS (DELETE FROM rpt.studies RETURNING 1) SELECT * FROM x',
    "SELECT set_config('role', 'postgres', false)",
    "SELECT pg_read_file('/etc/passwd')",
])
def test_sandbox_refuses(app, bad):
    with pytest.raises(WorklistError):
        engine.run(hand_sql=bad)


@pytest.mark.parametrize('bad', ['DELETE FROM rpt.studies', 'SET ROLE postgres', 'DROP VIEW rpt.studies',
                                 'UPDATE settings SET value = 1'])
def test_not_a_select_is_refused_up_front(app, bad):
    with pytest.raises(WorklistError, match='single SELECT'):
        engine.run(hand_sql=bad)


def test_sandbox_runs_as_the_report_role_read_only(app):
    role = sql("SELECT value FROM settings WHERE key = 'worklist_db_role'")[0]['value']
    res = engine.run(hand_sql="SELECT current_user, current_setting('transaction_read_only'), "
                              "current_setting('TimeZone')")
    assert res['rows'] == [[role, 'on', engine.hospital_timezone()]]


def test_hand_sql_reads_views_unqualified_with_literals(app):
    study(1)
    res = engine.run(hand_sql="-- note\nSELECT accession, 'a%b:10' AS lit FROM studies "
                              "WHERE accession LIKE 'ACC%';")
    assert res['rows'] == [['ACC1', 'a%b:10']]


def test_row_cap(app):
    res = engine.run(hand_sql='SELECT g FROM generate_series(1, 50) g', cap=10)
    assert len(res['rows']) == 10 and res['truncated']


def test_runaway_query_is_stopped(app, monkeypatch):
    monkeypatch.setattr(engine, 'STATEMENT_TIMEOUT_MS', 300)
    with pytest.raises(WorklistError, match='longer than'):
        engine.run(hand_sql='SELECT pg_sleep(2)')


def test_options_come_from_the_data(app):
    ae('CT_1', 'CT')
    study(1)
    study(2, storing_ae='MR_1', study_modality='MR', accession_number='A2')
    assert [o['value'] for o in engine.options('studies', 'modality')] == ['CT', 'MR']
    assert [o['value'] for o in engine.options('studies', 'shift')] == ['Morning', 'Afternoon', 'Night']
    with pytest.raises(WorklistError):
        engine.options('studies', 'finding')


def test_options_offer_the_last_year_and_the_month_ahead(app):
    study(1, storing_ae='OLD_CT', study_date=TODAY - timedelta(days=engine.OPTIONS_DAYS + 5))
    study(2, storing_ae='NOW_CT', accession_number='A2')
    ris_exam(1, at=MIDNIGHT + timedelta(days=20), proc_text='NEXT WEEK MR')
    ris_exam(2, at=MIDNIGHT - timedelta(days=engine.OPTIONS_DAYS + 5), proc_text='LONG AGO US')
    assert [o['value'] for o in engine.options('studies', 'device')] == ['NOW_CT']
    assert [o['value'] for o in engine.options('exams', 'procedure')] == ['NEXT WEEK MR']


def test_quick_dates_are_half_open_days():
    now = datetime(2026, 1, 15, 9, 30)
    a, b = engine.date_bounds({'mode': 'quick', 'quick': 'last_month'}, now)
    assert (a, b) == (datetime(2025, 12, 1), datetime(2026, 1, 1))
    a, b = engine.date_bounds({'mode': 'quick', 'quick': 'last_3_months'}, now)
    assert (a, b) == (datetime(2025, 10, 1), datetime(2026, 1, 1))
    for q, _ in cat.QUICK_DATES:
        a, b = engine.date_bounds({'mode': 'quick', 'quick': q}, now)
        assert a < b


# ── API ───────────────────────────────────────────────────────────────────────

def test_anonymous_is_refused(client):
    assert client.get('/worklists/api/catalog').status_code == 401


@pytest.mark.parametrize('uid,code', [(ADMIN, 200), (VIEWER, 200), (TEC, 403)])
def test_page_permission(client, uid, code):
    users()
    assert h(client, 'get', '/worklists/api/catalog', uid=uid).status_code == code


def test_a_granted_or_revoked_page_wins(client):
    users()
    sql(f"INSERT INTO user_page_permissions (user_id, page_key, is_enabled) VALUES "
        f"({TEC}, 'custom_reports', true), ({VIEWER}, 'custom_reports', false)")
    assert h(client, 'get', '/worklists/api/catalog', uid=TEC).status_code == 200
    assert h(client, 'get', '/worklists/api/catalog', uid=VIEWER).status_code == 403


def test_catalog(client):
    users()
    sql("INSERT INTO settings (key, value) VALUES ('shift_morning_start', '6') "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    c = h(client, 'get', '/worklists/api/catalog').get_json()
    assert set(c['datasets']) == set(cat.DATASETS)
    assert c['me'] == {'id': ADMIN, 'role': 'admin', 'can_write_sql': True, 'can_edit_shared': True,
                       'can_schedule': True}
    shift = next(f for f in c['fields'] if f['id'] == 'shift')
    assert 'Morning 6–15 h' in shift['hint']
    assert c['datasets']['storage']['default_measure'] == 'gb'
    assert c['datasets']['exams']['tabs'][:3] == ['primary', 'patient', 'exam']
    v = h(client, 'get', '/worklists/api/catalog', uid=VIEWER).get_json()['me']
    assert (v['can_write_sql'], v['can_edit_shared'], v['can_schedule']) == (False, False, False)


NEW = {'name': 'Mine', 'folder': 'Me', 'visibility': 'personal',
       'definition': {'dataset': 'studies', 'criteria': {}, 'output': {'kind': 'list'}}}


def save(client, body, uid=VIEWER):
    return h(client, 'post', '/worklists/api/save', uid=uid, json=body)


def test_save_version_and_history(client):
    users()
    r = save(client, NEW).get_json()
    again = save(client, {**NEW, 'id': r['id'], 'name': 'Mine v2'}).get_json()
    assert again['version'] == 2
    got = h(client, 'get', f"/worklists/api/{r['id']}", uid=VIEWER).get_json()
    assert [v['version'] for v in got['versions']] == [2, 1]
    v1 = h(client, 'get', f"/worklists/api/{r['id']}/versions/1", uid=VIEWER).get_json()
    assert v1['name'] == 'Mine'


def test_save_refuses_broken_or_nameless(client):
    users()
    assert save(client, {**NEW, 'name': '  '}).status_code == 400
    bad = {**NEW, 'definition': {'dataset': 'studies', 'criteria': {'nope': {}}}}
    assert save(client, bad).status_code == 400


def test_personal_is_private(client):
    users()
    wid = save(client, NEW).get_json()['id']
    listed = lambda uid: [w['id'] for w in h(client, 'get', '/worklists/api/list', uid=uid).get_json()]
    assert wid in listed(VIEWER) and wid not in listed(ADMIN)
    assert h(client, 'get', f'/worklists/api/{wid}', uid=ADMIN).status_code == 404


def test_only_admin_shares_and_edits_shared(client):
    users()
    assert save(client, {**NEW, 'visibility': 'shared'}).status_code == 403
    wid = save(client, {**NEW, 'visibility': 'shared'}, uid=ADMIN).get_json()['id']
    lib = h(client, 'get', '/worklists/api/list', uid=VIEWER).get_json()
    assert any(w['id'] == wid and not w['can_edit'] for w in lib)
    assert save(client, {**NEW, 'id': wid, 'visibility': 'personal'}).status_code == 403
    assert h(client, 'post', f'/worklists/api/{wid}/delete', uid=VIEWER).status_code == 403
    example = sql("SELECT id FROM worklists WHERE folder = 'Examples' LIMIT 1")[0]['id']
    assert h(client, 'post', f'/worklists/api/{example}/delete', uid=VIEWER).status_code == 403


def test_only_admin_writes_sql_everyone_runs_it(client):
    users()
    hand = {**NEW, 'definition': {'sql': 'SELECT 1 AS one'}}
    assert save(client, hand).status_code == 403
    wid = save(client, {**hand, 'visibility': 'shared'}, uid=ADMIN).get_json()['id']
    defn = h(client, 'get', f'/worklists/api/{wid}', uid=VIEWER).get_json()['definition']
    res = h(client, 'post', '/worklists/api/run', uid=VIEWER, json={'definition': defn}).get_json()
    assert res['rows'] == [[1]] and res['hand_sql']


def test_run_compile_csv_and_options(client):
    users()
    ae('CT_1', 'CT')
    study(1)
    defn = {'dataset': 'studies', 'criteria': {'study_at': {'mode': 'quick', 'quick': 'today'}},
            'output': {'kind': 'table', 'rows': 'modality', 'measures': ['count']}}
    res = h(client, 'post', '/worklists/api/run', uid=VIEWER, json={'definition': defn}).get_json()
    assert res['rows'] == [['CT', 1]] and res['measures'][0]['label'] == 'Studies'
    comp = h(client, 'post', '/worklists/api/compile', uid=VIEWER, json={'definition': defn}).get_json()
    assert ':study_at_from' in comp['sql'] and "TIMESTAMP '" in comp['editable_sql']
    csv = h(client, 'post', '/worklists/api/csv', uid=VIEWER, json={'definition': defn, 'name': 'x/y'})
    assert csv.status_code == 200 and 'CT,1' in csv.get_data(as_text=True)
    assert 'filename="x_y.csv"' in csv.headers['Content-Disposition']
    opts = h(client, 'get', '/worklists/api/options?dataset=studies&field=device', uid=VIEWER).get_json()
    assert opts == [{'value': 'CT_1', 'label': 'CT_1'}]
    err = h(client, 'post', '/worklists/api/run', uid=VIEWER, json={'definition': {'dataset': 'x'}})
    assert err.status_code == 400


def test_sql_reference_documents_every_column(client):
    users()
    ref = h(client, 'get', '/worklists/api/sql-reference', uid=VIEWER).get_json()
    assert [v['view'] for v in ref['views']] == [d['view'] for d in cat.DATASETS.values()]
    for v in ref['views']:
        assert v['columns'] and all(c['about'] for c in v['columns']), v['view']
    assert len(ref['examples']) == len(cat.SQL_EXAMPLES)


@pytest.mark.parametrize('old, new', [
    ('/reports/custom', '/report-composer'),
    ('/reports/custom/12/view', '/report-composer'),
    ('/worklists', '/report-composer'),
    ('/worklists?id=7', '/report-composer?id=7'),       # a bookmark of one report
    ('/worklists?id=x&_external=1', '/report-composer'),
])
def test_earlier_addresses_redirect(client, old, new):
    users()
    r = h(client, 'get', old, uid=VIEWER)
    assert r.status_code == 302 and r.headers['Location'].endswith(new)


# ── the page ──────────────────────────────────────────────────────────────────

def test_page_renders_for_a_viewer(client):
    users()
    r = h(client, 'get', '/report-composer', uid=VIEWER)
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and 'Report Composer' in html and 'csrf-token' in html
    assert 'href="/report-composer"' in html        # the sidebar


def test_page_template_parses_and_its_script_is_valid_js():
    import jinja2
    path = os.path.join(ROOT, 'templates', 'worklists.html')
    src = open(path, encoding='utf-8').read()
    jinja2.Environment().parse(src)
    assert not re.search(r'\b(alert|confirm|prompt)\(', src.replace('window.confirm(', '')), \
        'use SweetAlert2 (askYesNo / toast), not the browser dialogs'
    if not shutil.which('node'):
        pytest.skip('node not installed')
    script = re.search(r'<script>\n(.*?)</script>', src, re.S).group(1)
    script = re.sub(r'\{\{.*?\}\}', 'x', script)
    tmp = os.path.join(os.path.dirname(__file__), '_worklists_page.js')
    try:
        open(tmp, 'w', encoding='utf-8').write(script)
        done = subprocess.run(['node', '--check', tmp], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    finally:
        os.remove(tmp)
