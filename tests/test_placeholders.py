"""
tests/test_placeholders.py — LAUMC to-do item 19: placeholder records, kept out through BRAYN's
list (migration 0151; operator, 2026-11-13: "let brayn filter them").

Test patients are left out of every page; "Referring, Generic" keeps its exams in every count
with the referrer left blank; the CSH service account is left out of staff lists. Every page
reads the rh views, so each test asks what a request sees and checks the real table still holds
the row: nothing is deleted. BRAYN names a listed record on HL7 messages, and CRN texts the
fallback contact instead of a placeholder referrer.
"""
import json

import pytest
from sqlalchemy import text

import hl7kit as k

TEST_ID = 'MRN-TEST-1'
GENERIC = '4101031379966'
CSH = '9271030205716'


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def outside(statement, **params):
    """What the ETL, the listener and CRN read."""
    from db import db
    rows = db.session.execute(text(statement), params).scalars().all()
    db.session.commit()
    return rows


def in_request(app, statement, **params):
    """What a web request reads."""
    from db import db
    db.session.commit()
    with app.test_request_context('/'):
        rows = db.session.execute(text(statement), params).scalars().all()
        db.session.commit()
    return rows


def listed(kind, code=None, name=None, label=''):
    sql("INSERT INTO brayn_placeholders (kind, code, name, label) VALUES (:k, :c, :n, :l)",
        k=kind, c=code, n=name, l=label)


def study(uid, patient=None, accession=None, last=None, first=None):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae,
                                         study_date, study_modality, referring_physician_last_name,
                                         referring_physician_first_name)
           VALUES (:u, :p, :a, 'NOT_MAPPED', '2026-10-15', 'CT', :l, :f)""",
        u=uid, p=uid if patient is None else patient, a=accession, l=last, f=first)


def pacs_patient(uid, pacs_id):
    sql("INSERT INTO etl_patient_view (patient_db_uid, id) VALUES (:u, :i)", u=uid, i=pacs_id)


def ris_patient(key, patient_id):
    sql("INSERT INTO std_patients_ris (patient_person_key) VALUES (:k)", k=key)
    sql("""INSERT INTO std_patient_ids (patient_id_list_key, patient_person_key, patient_id, is_primary)
           VALUES (:k, :k, :i, 'Y')""", k=key, i=patient_id)


def order(key, person, accession):
    sql("""INSERT INTO etl_orders (order_dbid, patient_dbid, accession_number, order_status, modality)
           VALUES (:k, :p, :a, 'CM', 'CT')""", k=key, p=person, a=accession)


@pytest.fixture
def a_test_patient():
    """Studies 1 and 2 and everything hanging off them belong to the test patient; 3 is real.
    Study 1 is found by its PACS patient ID, study 2 only through its RIS order."""
    listed('patient', TEST_ID, label='QA phantom')
    pacs_patient(1, TEST_ID)
    pacs_patient(2, 'PIX_2')
    pacs_patient(3, 'MRN-REAL')
    ris_patient(501, TEST_ID)
    ris_patient(503, 'MRN-REAL')
    study(1, patient=1, accession='A1')
    study(2, patient=2, accession='A2')
    study(3, patient=3, accession='A3')
    order(12, '501', 'A2')
    order(13, '503', 'A3')
    for pps, person, wl in ((100, 501, 12), (300, 503, 13)):
        sql("""INSERT INTO std_pps (pps_key, study_instance_uid, patient_person_key)
               VALUES (:p, :u, :k)""", p=pps, u=f'9.{pps}', k=person)
        sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
               VALUES (:w, :p, 'S', NOW())""", w=wl, p=pps)
    sql("""INSERT INTO std_reports (report_key, version, reported_acc_number)
           VALUES (1, 0, 'A2'), (3, 0, 'A3')""")
    sql("""INSERT INTO hl7_oru_reports (id, accession_number, patient_id, report_text)
           VALUES (1, 'A9', 'MRN-TEST-1', 'by patient ID'), (2, 'A2', NULL, 'by its order'),
                  (3, 'A3', 'MRN-REAL', 'real')""")


# ── test patients ─────────────────────────────────────────────────────────────

def test_a_test_patient_is_left_out_of_every_page(app, a_test_patient):
    assert in_request(app, "SELECT study_db_uid FROM etl_didb_studies") == [3]
    assert in_request(app, "SELECT order_dbid FROM etl_orders") == [13]
    assert in_request(app, "SELECT pps_key FROM std_pps") == [300]
    assert in_request(app, "SELECT site_worklist_key FROM std_worklist_arrivals") == [13]
    assert in_request(app, "SELECT report_key FROM std_reports") == [3]
    assert in_request(app, "SELECT id FROM hl7_oru_reports") == [3]


def test_nothing_of_a_test_patient_is_deleted(app, a_test_patient):
    assert set(outside("SELECT study_db_uid FROM etl_didb_studies")) == {1, 2, 3}
    assert set(outside("SELECT order_dbid FROM etl_orders")) == {12, 13}
    assert set(outside("SELECT id FROM hl7_oru_reports")) == {1, 2, 3}
    assert set(outside("SELECT pps_key FROM std_pps")) == {100, 300}


def test_switching_a_patient_off_shows_it_again(app, a_test_patient):
    sql("UPDATE brayn_placeholders SET active = FALSE WHERE kind = 'patient'")
    assert set(in_request(app, "SELECT study_db_uid FROM etl_didb_studies")) == {1, 2, 3}
    assert set(in_request(app, "SELECT order_dbid FROM etl_orders")) == {12, 13}


def test_rows_with_no_patient_or_accession_stay_visible(app, a_test_patient):
    """A NULL in a NOT IN would hide them all: the trap every condition is guarded against.
    (A study always has a PACS patient; study 4's has no patient record and no accession.)"""
    study(4)
    order(14, None, None)
    sql("INSERT INTO hl7_oru_reports (id, accession_number, patient_id, report_text) VALUES (4, NULL, NULL, 'x')")
    sql("INSERT INTO std_pps (pps_key, study_instance_uid) VALUES (400, '9.400')")
    assert set(in_request(app, "SELECT study_db_uid FROM etl_didb_studies")) == {3, 4}
    assert set(in_request(app, "SELECT order_dbid FROM etl_orders")) == {13, 14}
    assert set(in_request(app, "SELECT id FROM hl7_oru_reports")) == {3, 4}
    assert set(in_request(app, "SELECT pps_key FROM std_pps")) == {300, 400}


def test_a_patient_is_never_listed_by_name():
    from sqlalchemy.exc import IntegrityError
    with pytest.raises(IntegrityError):
        listed('patient', TEST_ID, name='TEST, PATIENT')


# ── "Referring, Generic" ──────────────────────────────────────────────────────

def test_a_placeholder_referrer_keeps_the_exam_with_the_referrer_blank(app):
    study(1, last='Referring', first='Generic')
    study(2, last=' REFERRING', first='generic ')
    study(3, last='Wakim', first='Gerard')
    rows = {uid: (last, first) for uid, last, first in [
        tuple(r) for r in _rows(app, """SELECT study_db_uid, referring_physician_last_name,
                                               referring_physician_first_name FROM etl_didb_studies""")]}
    assert rows == {1: (None, None), 2: (None, None), 3: ('Wakim', 'Gerard')}
    assert outside("SELECT referring_physician_last_name FROM etl_didb_studies WHERE study_db_uid = 1") \
        == ['Referring']


def test_the_daily_briefing_ranks_no_placeholder_referrer(app):
    from datetime import date, timedelta
    from db import db
    from ETL_JOBS.daily_analytics import _collect
    from utils.rh_scope import rh_scope
    day = date.today() - timedelta(days=1)
    for uid, (last, first) in enumerate([('Referring', 'Generic')] * 3 + [('Wakim', 'Gerard')], 1):
        sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date,
                   study_modality, referring_physician_last_name, referring_physician_first_name)
               VALUES (:u, :u, 'NOT_MAPPED', :d, 'CT', :l, :f)""", u=uid, d=day, l=last, f=first)
    with rh_scope():
        with db.engine.begin() as conn:
            numbers = _collect(conn, day, day)
    assert numbers['total_studies'] == 4
    names = json.dumps(numbers)
    assert 'Generic' not in names and 'Gerard Wakim' in names


# ── the CSH service account ───────────────────────────────────────────────────

def test_a_service_account_is_left_out_of_staff_lists(app):
    listed('staff', '7000', label='matched on person key')
    listed('staff', name='Bot, Interface', label='matched on name')
    for key, person, last, first in ((int(CSH), 1, 'SERVICE-PROD', 'CSH'), (2, 7000, 'X', 'Y'),
                                     (3, 3, 'Bot', 'Interface'), (4, 4, 'Haddad', 'Rima')):
        sql("""INSERT INTO std_resources_ris (resource_id_key, person_key, last_name, first_name)
               VALUES (:k, :p, :l, :f)""", k=key, p=person, l=last, f=first)
    assert in_request(app, "SELECT resource_id_key FROM std_resources_ris") == [4]
    assert len(outside("SELECT resource_id_key FROM std_resources_ris")) == 4


def test_the_composer_reads_the_staff_list_through_the_filter():
    assert outside("""
        SELECT COUNT(*) FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class c ON c.oid = r.ev_class
        WHERE c.relnamespace = 'rpt'::regnamespace
          AND d.refobjid = 'public.std_resources_ris'::regclass""") == [0]


# ── BRAYN names them on HL7 ───────────────────────────────────────────────────

def _placeholder_findings(pipe):
    rows = pipe.q("SELECT detail FROM brayn_findings WHERE rule_code = 'PLACEHOLDER_DATA' ORDER BY id")
    return [' | '.join((r['detail'] if isinstance(r['detail'], dict) else json.loads(r['detail']))
                       ['placeholders']) for r in rows]


def test_brayn_names_a_listed_patient_referrer_and_account(pipe):
    pipe.brayn()
    pipe.exec("INSERT INTO brayn_placeholders (kind, code) VALUES ('patient', 'P-TEST')")
    pipe.send(k.report('ACC1', k.ts(5), pat='P-TEST'))
    pipe.send(k.report('ACC2', k.ts(4), pv1_seg=k.pv1(ref=f'{GENERIC}^REFERRING^GENERIC')))
    pipe.send(k.order('PL-3', acc='ACC3', when=k.ts(3), ref=f'{GENERIC}^REFERRING^GENERIC'))
    pipe.send(k.status('SC', 'ACC3', k.ts(2), tech=f'{CSH}^SERVICE-PROD^CSH'))
    pipe.send(k.report('ACC4', k.ts(1)))                           # nothing placeholder about it
    pipe.screen()
    found = _placeholder_findings(pipe)
    assert len(found) == 4, found
    assert 'P-TEST (a test patient' in found[0]
    assert f'referring doctor {GENERIC}' in found[1] and f'referring doctor {GENERIC}' in found[2]
    assert f'done by {CSH}' in found[3]


def test_brayn_only_observes_the_report_is_stored_as_before(pipe):
    pipe.brayn()
    pipe.exec("INSERT INTO brayn_placeholders (kind, code) VALUES ('patient', 'P-TEST')")
    pipe.send(k.report('ACC1', k.ts(5), pat='P-TEST'))
    pipe.screen()
    assert pipe.count('hl7_oru_reports', "accession_number = 'ACC1'") == 1


# ── CRN ───────────────────────────────────────────────────────────────────────

def _notification(code):
    sql("""INSERT INTO hl7_oru_reports (id, accession_number, report_text, referring_physician_code)
           VALUES (77, 'ACC77', 'x', :c)""", c=code)
    sql("""INSERT INTO crn_notifications (id, ref_code, report_id, accession_number, marker, report_fingerprint)
           VALUES (77, 'CRN-TEST-0001', 77, 'ACC77', 'CRITICAL', :f)""", f='a' * 64)
    sql("""INSERT INTO crn_contacts (doctor_code, full_name, phone_e164, active)
           VALUES ('FB1', 'Fallback desk', '+96170000001', TRUE),
                  ('20000191', 'Gerard Wakim', '+96170000002', TRUE)
           ON CONFLICT (doctor_code) DO UPDATE SET phone_e164 = EXCLUDED.phone_e164, active = TRUE""")
    sql("INSERT INTO settings (key, value) VALUES ('crn_fallback_code', 'FB1') "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    from db import db
    return db.session.execute(text("SELECT * FROM crn_notifications WHERE id = 77")).mappings().one()


@pytest.fixture
def crn_calls(monkeypatch):
    from utils import crn_dispatch
    calls = {'sent': [], 'ris': []}
    monkeypatch.setattr(crn_dispatch, '_send',
                        lambda n, role, contact, cfg, event: calls['sent'].append(
                            (role, contact['doctor_code'] if contact else None)) or True)
    monkeypatch.setattr(crn_dispatch, '_ris_refresh', lambda code: calls['ris'].append(code))
    yield calls
    sql("DELETE FROM crn_contacts WHERE doctor_code IN ('FB1', '20000191')")   # not reset by conftest


def test_crn_texts_the_fallback_for_a_placeholder_referrer(crn_calls):
    from utils import crn_dispatch
    crn_dispatch._route(_notification(GENERIC), crn_dispatch._settings())
    assert crn_calls == {'sent': [('fallback', 'FB1')], 'ris': []}
    reason = outside("""SELECT detail->>'reason' FROM crn_events
                        WHERE notification_id = 77 AND event_type = 'doctor_unreachable'""")
    assert len(reason) == 1 and 'placeholder' in reason[0]


def test_crn_still_texts_a_real_referrer(crn_calls):
    from utils import crn_dispatch
    crn_dispatch._route(_notification('20000191'), crn_dispatch._settings())
    assert crn_calls == {'sent': [('doctor', '20000191')], 'ris': ['20000191']}


# ── the console ───────────────────────────────────────────────────────────────

def post(client, url, body, role='cht'):
    return client.post(url, json=body, headers={'X-Test-Role': role, 'X-Test-Uid': '1'})


def test_the_console_lists_what_each_record_reaches(client):
    study(1, last='Referring', first='Generic')
    page = client.get('/brayn?view=settings', headers={'X-Test-Role': 'cht'}).get_data(as_text=True)
    assert 'Placeholder records' in page
    assert 'Referring, Generic' in page and GENERIC in page and CSH in page
    assert '1 study' in page


def test_cht_adds_and_switches_a_record(client):
    r = post(client, '/brayn/placeholders/save', {'kind': 'patient', 'code': ' P-77 ', 'label': 'QA'})
    assert r.status_code == 200
    pid = r.json['id']
    assert outside("SELECT code FROM brayn_placeholders WHERE id = :i", i=pid) == ['P-77']
    assert post(client, '/brayn/placeholders/save', {'kind': 'patient', 'code': 'p-77'}).status_code == 409
    r = post(client, '/brayn/placeholders/active', {'id': pid, 'active': False})
    assert r.status_code == 200
    assert outside("SELECT active FROM brayn_placeholders WHERE id = :i", i=pid) == [False]


def test_the_console_refuses_a_patient_name_and_an_empty_record(client):
    r = post(client, '/brayn/placeholders/save', {'kind': 'patient', 'code': 'P-1', 'name': 'DOE, JANE'})
    assert r.status_code == 400 and 'patient name' in r.json['error']
    assert post(client, '/brayn/placeholders/save', {'kind': 'staff'}).status_code == 400
    assert post(client, '/brayn/placeholders/save', {'kind': 'other', 'code': 'X'}).status_code == 400


def test_only_super_user_and_cht_change_the_list(client):
    for role in ('administrator', 'user', 'viewer'):
        assert post(client, '/brayn/placeholders/save', {'kind': 'patient', 'code': 'P-2'}, role).status_code == 403
        assert post(client, '/brayn/placeholders/active', {'id': 1, 'active': False}, role).status_code == 403


def _rows(app, statement):
    from db import db
    db.session.commit()
    with app.test_request_context('/'):
        rows = db.session.execute(text(statement)).fetchall()
        db.session.commit()
    return rows
