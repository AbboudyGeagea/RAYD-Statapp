"""
tests/test_sticky_notes_monthly.py
────────────────────────────────────────────────────────────────
The automated monthly JCI report (utils/sticky_notes_monthly.py): generated once for the
preceding month, kept as it was, and emailed through the Report Composer's relay. The relay is
the real SMTP stand-in from tests/test_worklist_email.py, and the workbooks are read back.
"""
import io
from datetime import datetime, timedelta

import openpyxl
import pytest
from sqlalchemy import text

from test_worklist_email import Relay
from utils import sticky_notes_monthly as monthly

ADMIN = {'X-Test-Role': 'admin'}
VIEWER = {'X-Test-Role': 'viewer'}

OCT_1 = datetime(2026, 10, 1).date()
NOV_2_MORNING = datetime(2026, 11, 2, 8, 0)
OCT_CASE = datetime(2026, 10, 15, 9, 0)


@pytest.fixture
def sql(app):
    from db import db

    def run(statement, **params):
        with app.app_context():
            db.session.execute(text(statement), params)
            db.session.commit()

    def clear():
        run("DELETE FROM sticky_notes_monthly_reports")
        run("DELETE FROM etl_sticky_notes")
        run("DELETE FROM study_access_history")
        run("DELETE FROM etl_job_log WHERE job_name = 'STICKY_NOTES_ETL'")
    clear()
    run("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CT_T', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    yield run
    clear()


@pytest.fixture
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


def rows(app, statement, **params):
    from db import db
    with app.app_context():
        return [dict(r) for r in db.session.execute(text(statement), params).mappings().all()]


def er_case(sql, uid=1, arrived=OCT_CASE, note_after=30):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae,
               study_date, patient_location, pacs_site_id_raw, insert_time)
           VALUES (:u, :u, :a, 'CT_T', :d, 'ER', '0', :t)""",
        u=uid, a=f'10050{uid}', d=arrived.date(), t=arrived)
    sql("INSERT INTO etl_patient_view (patient_db_uid, fallback_id) VALUES (:u, :p)", u=uid, p=f'MRN{uid:05d}')
    if note_after is not None:
        sql("""INSERT INTO etl_sticky_notes (data_db_uid, study_db_uid, user_name, date_created,
                   delete_indication, data_len)
               VALUES (:u, :u, 'res.one@ad', :t, 'N', 40)""",
            u=uid, t=arrived + timedelta(minutes=note_after))


def notes_copied(sql, day=NOV_2_MORNING):
    sql("""INSERT INTO etl_job_log (job_name, status, start_time, records_processed)
           VALUES ('STICKY_NOTES_ETL', 'SUCCESS', :t, 1)""", t=day.replace(hour=5))


def setting(sql, key, value):
    sql("INSERT INTO settings (key, value) VALUES (:k, :v) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", k=key, v=value)


@pytest.fixture
def relay(sql):
    r = Relay()
    for k, v in (('smtp_host', '127.0.0.1'), ('smtp_port', str(r.port)), ('smtp_security', 'none'),
                 ('smtp_from', 'rayd@laumc.test'), ('smtp_allowed_domains', 'laumc.test')):
        setting(sql, k, v)
    yield r
    r.stop()


def kept(app):
    return rows(app, "SELECT * FROM sticky_notes_monthly_reports ORDER BY report_month")


def run_due(app, now):
    with app.app_context():
        return monthly.run_due(now)


def sheet(data):
    return openpyxl.load_workbook(io.BytesIO(bytes(data))).active


def table(data):
    """(everything above the header row as a dict, the header, the case rows)."""
    ws = list(sheet(data).iter_rows(values_only=True))
    top = {r[0]: r[1] for r in ws if r[0] and r[1] is not None and r[0] != 'Patient ID'}
    i = next(n for n, r in enumerate(ws) if r[0] == 'Patient ID')
    return top, ws[i], ws[i + 1:]


# ── when ──────────────────────────────────────────────────────────────────────

def test_nothing_happens_on_the_first_of_the_month(app, sql):
    er_case(sql)
    notes_copied(sql, datetime(2026, 11, 1))

    assert run_due(app, datetime(2026, 11, 1, 8, 0)) is None
    assert kept(app) == []


def test_it_waits_for_that_mornings_note_copy(app, sql):
    er_case(sql)

    assert run_due(app, NOV_2_MORNING) is None
    assert kept(app) == []

    notes_copied(sql)

    assert run_due(app, NOV_2_MORNING + timedelta(hours=1)) == 'October 2026: generated'
    assert kept(app)[0]['report_month'] == OCT_1


def test_a_copy_from_another_day_does_not_count(app, sql):
    er_case(sql)
    notes_copied(sql, datetime(2026, 11, 1))

    assert run_due(app, NOV_2_MORNING) is None


def test_it_stops_waiting_on_the_fifth(app, sql):
    er_case(sql)

    assert run_due(app, datetime(2026, 11, 4, 8, 0)) is None
    assert run_due(app, datetime(2026, 11, 5, 8, 0)) == 'October 2026: generated'


def test_demo_mode_generates_nothing(app, sql):
    er_case(sql)
    notes_copied(sql)
    setting(sql, 'demo_mode', 'true')

    assert run_due(app, NOV_2_MORNING) is None
    assert kept(app) == []


def test_january_reports_december(app, sql):
    er_case(sql, arrived=datetime(2026, 12, 15, 9, 0))
    notes_copied(sql, datetime(2027, 1, 2))

    assert run_due(app, datetime(2027, 1, 2, 8, 0)) == 'December 2026: generated'
    assert kept(app)[0]['case_count'] == 1


# ── what is kept ──────────────────────────────────────────────────────────────

def test_a_month_is_generated_once_and_does_not_change_later(app, sql):
    er_case(sql)
    notes_copied(sql)
    run_due(app, NOV_2_MORNING)
    before = kept(app)[0]
    er_case(sql, uid=2)

    assert run_due(app, NOV_2_MORNING + timedelta(days=1)) is None

    after = kept(app)[0]
    assert after['case_count'] == before['case_count'] == 1
    assert bytes(after['workbook']) == bytes(before['workbook'])


def test_only_the_preceding_month_counts(app, sql):
    er_case(sql, uid=1)
    er_case(sql, uid=2, arrived=datetime(2026, 9, 30, 23, 0))
    er_case(sql, uid=3, arrived=datetime(2026, 11, 1, 0, 5))
    er_case(sql, uid=4, arrived=datetime(2026, 10, 31, 23, 59))
    notes_copied(sql)

    run_due(app, NOV_2_MORNING)

    _top, _header, cases = table(kept(app)[0]['workbook'])
    assert sorted(c[1] for c in cases) == ['100501', '100504']


def test_the_workbook_has_what_the_customer_asked_for(app, sql):
    er_case(sql)
    sql("""INSERT INTO study_access_history (study_db_uid, captured_at, last_accessed_time, rep_last_accessed_by)
           VALUES (1, :c, :a, 'rad.two@ad')""", c=OCT_CASE + timedelta(hours=20), a=OCT_CASE + timedelta(minutes=10))
    notes_copied(sql)

    run_due(app, NOV_2_MORNING)

    top, header, cases = table(kept(app)[0]['workbook'])
    case = dict(zip(header, cases[0]))
    assert case['Patient ID'] == 'MRN00001'
    assert case['Accession'] == '100501'
    assert case['First open seen'] == '2026-10-15 09:10'
    assert case['Opened by'] == 'Rad Two'
    assert case['Sticky note inserted'] == '2026-10-15 09:30'
    assert case['Note by'] == 'Res One'
    assert top['Period'] == '2026-10-01 to 2026-10-31'
    assert top['Site'] == 'RH only'
    assert 'PACS keeps only the latest open' in top['First open']
    assert kept(app)[0]['open_seen_count'] == 1


def test_a_case_with_no_note_is_still_in_the_report(app, sql):
    er_case(sql, note_after=None)
    notes_copied(sql)

    run_due(app, NOV_2_MORNING)

    row = kept(app)[0]
    assert (row['case_count'], row['noted_count']) == (1, 0)


def test_an_id_keeps_its_zeros_and_text_never_runs_as_a_formula(app, sql):
    er_case(sql)
    sql("UPDATE etl_patient_view SET fallback_id = '00123' WHERE patient_db_uid = 1")
    sql("UPDATE etl_didb_studies SET patient_location_full = '=SUM(A1)' WHERE study_db_uid = 1")
    setting(sql, 'sticky_notes_urgent_departments', '["=SUM(A1)"]')
    notes_copied(sql)

    run_due(app, NOV_2_MORNING)

    ws = sheet(kept(app)[0]['workbook'])
    cells = [c for row in ws.iter_rows() for c in row if c.value == '00123']
    assert cells and cells[0].data_type == 's'
    assert not [c for row in ws.iter_rows() for c in row if c.data_type == 'f']


def test_an_empty_month_is_still_a_report(app, sql):
    notes_copied(sql)

    assert run_due(app, NOV_2_MORNING) == 'October 2026: generated'
    assert kept(app)[0]['case_count'] == 0


# ── the email ─────────────────────────────────────────────────────────────────

def recipients(sql, value='jci@laumc.test'):
    setting(sql, monthly.RECIPIENTS_KEY, value)


def test_with_no_relay_it_is_kept_but_not_emailed(app, sql):
    er_case(sql)
    notes_copied(sql)
    recipients(sql)

    assert run_due(app, NOV_2_MORNING) == 'October 2026: generated'
    assert kept(app)[0]['emailed_at'] is None


def test_with_no_recipients_nothing_is_sent(app, sql, relay):
    er_case(sql)
    notes_copied(sql)

    assert run_due(app, NOV_2_MORNING) == 'October 2026: generated'
    assert relay.mails == []


def test_it_is_emailed_once_with_rows_only_in_the_attachment(app, sql, relay):
    er_case(sql)
    notes_copied(sql)
    recipients(sql)

    assert run_due(app, NOV_2_MORNING) == 'October 2026: generated and emailed'
    assert run_due(app, NOV_2_MORNING + timedelta(days=2)) is None

    assert len(relay.mails) == 1
    mail = relay.mails[0]
    assert mail.rcpts == ['jci@laumc.test']
    assert 'October 2026' in mail.msg['Subject']
    body = mail.msg.get_body().get_content()
    assert 'Urgent emergent cases: 1' in body
    assert 'MRN00001' not in body and '100501' not in body and 'res.one' not in body
    attachment = next(mail.msg.iter_attachments())
    assert attachment.get_filename() == 'urgent_emergent_2026_10.xlsx'
    _top, _header, cases = table(attachment.get_content())
    assert cases[0][0] == 'MRN00001'
    row = kept(app)[0]
    assert row['emailed_to'] == ['jci@laumc.test'] and row['email_error'] is None


def test_recipients_added_later_still_get_the_month(app, sql, relay):
    er_case(sql)
    notes_copied(sql)
    run_due(app, NOV_2_MORNING)
    recipients(sql)

    assert run_due(app, NOV_2_MORNING + timedelta(hours=1)) == 'October 2026: emailed'
    assert len(relay.mails) == 1


def test_a_failed_send_is_recorded_and_tried_again_a_day_later(app, sql, relay):
    er_case(sql)
    notes_copied(sql)
    recipients(sql)
    setting(sql, 'smtp_port', '1')      # nothing listens there

    assert run_due(app, NOV_2_MORNING) == 'October 2026: generated'
    row = kept(app)[0]
    assert row['emailed_at'] is None and row['email_error']

    setting(sql, 'smtp_port', str(relay.port))
    assert run_due(app, NOV_2_MORNING + timedelta(hours=1)) is None
    assert relay.mails == []

    assert run_due(app, NOV_2_MORNING + timedelta(days=1)) == 'October 2026: emailed'
    assert len(relay.mails) == 1
    assert kept(app)[0]['email_error'] is None


def test_a_recipient_the_relay_refuses_is_recorded(app, sql, relay):
    er_case(sql)
    notes_copied(sql)
    recipients(sql, 'jci@laumc.test, gone@laumc.test')
    relay.refuse.add('gone@laumc.test')

    run_due(app, NOV_2_MORNING)

    row = kept(app)[0]
    assert row['emailed_to'] == ['jci@laumc.test']
    assert 'gone@laumc.test' in row['email_error']


def test_a_domain_removed_later_is_not_emailed(app, sql, relay):
    er_case(sql)
    notes_copied(sql)
    recipients(sql, 'jci@elsewhere.test')

    run_due(app, NOV_2_MORNING)

    assert relay.mails == []
    assert 'outside the allowed domains' in kept(app)[0]['email_error']


# ── the page ──────────────────────────────────────────────────────────────────

def load(client):
    r = client.get('/sticky-notes/data', headers=ADMIN, query_string={'start': '2026-10-01', 'end': '2026-10-31'})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def test_the_page_lists_kept_months_and_they_download(app, client, sql):
    er_case(sql)
    notes_copied(sql)
    run_due(app, NOV_2_MORNING)

    listed = load(client)['monthly_reports'][0]
    assert (listed['month'], listed['label'], listed['cases']) == ('2026-10', 'October 2026', 1)

    r = client.get('/sticky-notes/monthly/2026-10.xlsx', headers=VIEWER)
    assert r.status_code == 200
    assert table(r.data)[2][0][0] == 'MRN00001'
    assert client.get('/sticky-notes/monthly/2026-09.xlsx', headers=VIEWER).status_code == 404
    assert client.get('/sticky-notes/monthly/nonsense.xlsx', headers=VIEWER).status_code == 404
    assert client.get('/sticky-notes/monthly/2026-10.xlsx', headers={'X-Test-Role': 'tec'}).status_code == 403


def test_only_an_admin_sees_recipients_and_the_mail_state(client, sql):
    viewer = client.get('/sticky-notes/data', headers=VIEWER).get_json()
    assert 'monthly_recipients' not in viewer and 'mail_ready' not in viewer
    assert load(client)['mail_ready'] is False


def test_an_admin_can_generate_a_month_again_and_it_goes_out_again(app, client, sql, relay):
    er_case(sql)
    notes_copied(sql)
    recipients(sql)
    run_due(app, NOV_2_MORNING)
    er_case(sql, uid=2)

    r = client.post('/sticky-notes/monthly/generate', headers=ADMIN, json={'month': '2026-10'})

    assert r.status_code == 200
    assert r.get_json()['cases'] == 2 and r.get_json()['emailed'] is True
    assert kept(app)[0]['case_count'] == 2
    assert len(relay.mails) == 2


def test_a_viewer_cannot_generate(client, sql):
    r = client.post('/sticky-notes/monthly/generate', headers=VIEWER, json={'month': '2026-10'})
    assert r.status_code == 403


@pytest.mark.parametrize('month', ['nonsense', '', None, '2999-01'])
def test_only_a_finished_month_can_be_generated(client, sql, month):
    r = client.post('/sticky-notes/monthly/generate', headers=ADMIN, json={'month': month})
    assert r.status_code == 400


def test_the_current_month_cannot_be_generated(client, sql):
    r = client.post('/sticky-notes/monthly/generate', headers=ADMIN,
                    json={'month': datetime.now().strftime('%Y-%m')})
    assert r.status_code == 400


def test_recipients_are_saved_with_the_settings_and_follow_the_relays_rules(client, sql, relay):
    ok = client.post('/sticky-notes/settings', headers=ADMIN, json={'departments': [], 'recipients': 'JCI@laumc.test; b@laumc.test'})
    assert ok.status_code == 200 and ok.get_json()['recipients'] == ['jci@laumc.test', 'b@laumc.test']
    assert load(client)['monthly_recipients'] == ['jci@laumc.test', 'b@laumc.test']

    bad = client.post('/sticky-notes/settings', headers=ADMIN, json={'departments': ['ICU'], 'recipients': 'x@elsewhere.test'})
    assert bad.status_code == 400 and 'allowed domains' in bad.get_json()['message']
    assert load(client)['urgent_departments'] == []     # nothing saved from a refused form

    assert client.post('/sticky-notes/settings', headers=ADMIN, json={'departments': []}).status_code == 200
    assert load(client)['monthly_recipients'] == ['jci@laumc.test', 'b@laumc.test']     # left alone

    client.post('/sticky-notes/settings', headers=ADMIN, json={'departments': [], 'recipients': ''})
    assert load(client)['monthly_recipients'] == []
