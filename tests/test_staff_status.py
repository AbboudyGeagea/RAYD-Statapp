"""
tests/test_staff_status.py — staff who left (LAUMC to-do item 9, migration 0157,
utils/staff_status.py, Report 39).

The customer: inactive on the RIS and/or PACS -> no KPIs shown, in any period; department totals
keep their studies; no PACS account is not inactive; nothing deleted. A person is judged on all
their RIS rows and on every RIS person sharing their login. While the switch is on, the rh views
every page reads say "Former staff" for them; the real tables keep the names.
"""
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}
DAY = '2026-10-05'


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        sql("DELETE FROM etl_pacs_audit_events")
        yield
        db.session.rollback()
        sql("DELETE FROM etl_pacs_audit_events")
        sql("DELETE FROM std_pacs_user_groups")
        sql("DELETE FROM std_resources_ris")
        sql("REFRESH MATERIALIZED VIEW staff_status")


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def rows(statement, **params):
    from db import db
    out = db.session.execute(text(statement), params).mappings().all()
    db.session.commit()
    return out


def in_request(app, statement, **params):
    """What a web request reads (the rh views)."""
    from db import db
    db.session.commit()
    with app.test_request_context('/'):
        out = db.session.execute(text(statement), params).mappings().all()
        db.session.commit()
    return out


_keys = iter(range(70_000, 99_999))


def ris(person, role, active, resource_id=None, first='First', last='Last', mail=None, site=None):
    key = next(_keys)
    sql("""INSERT INTO std_resources_ris (resource_id_key, person_key, role_code, active, resource_id,
                                          first_name, last_name, primary_email_address, site_id)
           VALUES (:k, :p, :r, :a, :i, :f, :l, :m, (SELECT id FROM sites WHERE code = :s))""",
        k=key, p=person, r=role, a=active, i=resource_id, f=first, l=last, m=mail, s=site)
    return key


def pacs(login, group='radiologists', status='ACTIVE', email=None, domain='ad'):
    key = next(_keys)
    sql("""INSERT INTO std_pacs_user_groups (membership_dbid, user_dbid, login_id, email, group_name,
                                             group_domain, user_status)
           VALUES (:k, :k, :l, :e, :g, :d, :s)""", k=-key, l=login, e=email, g=group, s=status, d=domain)


def refresh():
    sql("REFRESH MATERIALIZED VIEW staff_status")


def switch(on):
    sql("UPDATE settings SET value = :v WHERE key = 'staff_left_hidden'", v='true' if on else 'false')


def status():
    refresh()
    return {r['staff_id']: r for r in rows("SELECT * FROM staff_status")}


def by_login(login):
    return next(r for r in status().values() if login in r['logins'])


def study(uid, signer, reader_id=None, reader_first=None, reader_last=None):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae, study_date,
                                         study_modality, rep_study_last_composed_by, rep_study_last_composed_ts,
                                         reading_physician_id, reading_physician_first_name, reading_physician_last_name)
           VALUES (:u, :u, :a, 'CTX1', :d, 'CT', :s, :t, :ri, :rf, :rl)""",
        u=uid, a=f'A{uid}', d=DAY, s=signer, t=datetime(2026, 10, 5, 12, 0),
        ri=reader_id, rf=reader_first, rl=reader_last)


# ── who left ──────────────────────────────────────────────────────────────────

def test_a_person_with_no_active_ris_row_left_and_one_active_row_keeps_them():
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1')
    ris(1, 'TEC', False, 'gone.rad@ad.umcrh.com_2')
    ris(2, 'RAD', False, 'old.id')                       # an old row of someone still here
    ris(2, 'RAD', True, 'still.here@ad.umcrh.com_3')

    assert by_login('gone.rad')['is_left'] is True
    assert by_login('gone.rad')['left_reason'] == 'Inactive on the RIS'
    assert by_login('still.here')['is_left'] is False


def test_two_ris_people_sharing_a_login_are_judged_together():
    ris(3, 'RAD', False, 'dup.person@ad.umcrh.com_4')    # a duplicate person record, inactive
    ris(4, 'RAD', True, 'dup.person@ad.umcrh.com_5')

    people = [r for r in status().values() if 'dup.person' in r['logins']]

    assert len(people) == 2 and not any(r['is_left'] for r in people)


def test_pacs_not_active_makes_someone_left_and_no_pacs_account_does_not():
    ris(5, 'TEC', True, 'suspended.tec@ad.umcrh.com_6')
    pacs('suspended.tec', 'radiographers', 'SUSPENDED')
    ris(6, 'TEC', True, 'no.account@ad.umcrh.com_7')

    assert by_login('suspended.tec')['left_reason'] == 'Not active on PACS'
    assert by_login('no.account')['is_left'] is False
    assert by_login('no.account')['pacs_active'] is None


def test_a_pacs_lock_after_wrong_passwords_is_not_leaving_and_the_list_can_change():
    ris(5, 'RAD', True, 'locked.rad@ad.umcrh.com_9')
    pacs('locked.rad', 'radiologists', 'LOCKED')
    pacs('timeout.res', 'residents', 'TIMEOUT')

    assert by_login('locked.rad')['is_left'] is False
    assert status()['pacs:timeout.res']['is_left'] is False

    sql("UPDATE settings SET value = 'SUSPENDED, locked' WHERE key = 'staff_pacs_left_statuses'")
    assert by_login('locked.rad')['left_reason'] == 'Not active on PACS'


def test_the_mammography_group_reads_as_radiologists_and_cardiologists_are_out():
    from utils.pacs_roles import role_lookup_cte
    pacs('mammo.reader', 'mammography')
    pacs('cardio.doc', 'cardiologists')

    got = {r['login_id']: r['role'] for r in rows(f"WITH {role_lookup_cte()} SELECT * FROM role_lookup")}

    assert got.get('MAMMO.READER') == 'radiologists' and 'CARDIO.DOC' not in got


def test_a_pacs_login_is_linked_by_email_when_the_ris_id_holds_no_login():
    ris(7, 'RAD', False, 'kt', mail='Kamal.T@umcrh.com')
    pacs('kamal.t', email='kamal.t@umcrh.com')

    row = by_login('kamal.t')

    assert row['person_key'] == 7 and row['linked_by'] == 'e-mail' and row['is_left'] is True


def test_pacs_only_logins_count_and_referrers_and_the_patient_portal_do_not():
    pacs('old.resident', 'residents', 'DISABLED')
    pacs('portal.patient', 'web_patients', 'SUSPENDED')
    ris(8, 'DOC', False, 'ref.doc@ad.umcrh.com_8')

    out = status()

    assert out['pacs:old.resident']['is_left'] is True and out['pacs:old.resident']['person_key'] is None
    assert 'pacs:portal.patient' not in out
    assert 'ris:8' not in out


# ── what every page reads ─────────────────────────────────────────────────────

def test_with_the_switch_on_a_leaver_reads_former_staff_and_every_count_stays(app):
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1', first='Gone', last='Rad')
    ris(2, 'RAD', True, 'still.here@ad.umcrh.com_3', first='Still', last='Here')
    study(1, 'gone.rad@ad', 'gone.rad@ad.umcrh.com_1', 'Gone', 'Rad')
    study(2, 'gone.rad@ad')
    study(3, 'still.here@ad', reader_first='Gone', reader_last='Rad')      # matched by name
    refresh()
    q = ("SELECT study_db_uid AS uid, rep_study_last_composed_by AS signer, reading_physician_id AS rid, "
         "reading_physician_first_name AS rf, reading_physician_last_name AS rl "
         "FROM etl_didb_studies ORDER BY 1")

    off = in_request(app, q)
    switch(True)
    on = in_request(app, q)

    assert [r['signer'] for r in off] == ['gone.rad@ad', 'gone.rad@ad', 'still.here@ad']
    assert [r['signer'] for r in on] == ['Former staff', 'Former staff', 'still.here@ad']
    assert (on[0]['rid'], on[0]['rf'], on[0]['rl']) == ('Former staff', None, 'Former staff')
    assert (on[2]['rf'], on[2]['rl']) == (None, 'Former staff')
    assert [r['signer'] for r in rows(q)] == ['gone.rad@ad', 'gone.rad@ad', 'still.here@ad']   # real table


def test_ris_staff_rows_keep_their_roles_and_keys_with_the_name_replaced(app):
    gone = ris(1, 'TEC', False, 'gone.tec@ad.umcrh.com_1', first='Gone', last='Tec')
    here = ris(2, 'TEC', True, 'here.tec@ad.umcrh.com_2', first='Here', last='Tec')
    refresh()
    switch(True)

    got = {r['k']: r for r in in_request(
        app, "SELECT resource_id_key AS k, role_code, CONCAT(first_name, ' ', last_name) AS name FROM std_resources_ris")}

    assert got[gone]['role_code'] == 'TEC' and got[gone]['name'].strip() == 'Former staff'
    assert got[here]['name'] == 'Here Tec'


def test_a_name_a_current_person_also_holds_is_never_replaced(app):
    ris(1, 'RAD', False, 'maria.k@ad.umcrh.com_1', first='Maria', last='Khoury')
    ris(2, 'TEC', True, 'maria.k2@ad.umcrh.com_2', first='Maria', last='Khoury')
    study(1, 'someone@ad', reader_first='Maria', reader_last='Khoury')
    refresh()
    switch(True)

    assert in_request(app, "SELECT reading_physician_last_name AS l FROM etl_didb_studies")[0]['l'] == 'Khoury'


# ── Report 39 ─────────────────────────────────────────────────────────────────

def test_report_39_lists_everyone_and_flags_a_leaver_who_still_signed(client):
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1', first='Gone', last='Rad')
    ris(2, 'RAD', True, 'still.here@ad.umcrh.com_3', first='Still', last='Here')
    pacs('gone.rad')
    pacs('still.here')
    study(1, 'gone.rad@ad')
    refresh()

    html = client.get('/report/39', headers=ADMIN,
                      query_string={'start_date': DAY, 'end_date': DAY}).get_data(as_text=True)
    working = client.get('/report/39', headers=ADMIN, query_string={
        'start_date': DAY, 'end_date': DAY, 'status': 'working', 'status_enabled': 'on'}).get_data(as_text=True)

    assert 'Staff Status (RIS and PACS)' in html and 'Gone Rad' in html and 'Still Here' in html
    assert 'Left, still working' in html and 'Show them as' in html
    assert 'Gone Rad' in working and 'Still Here' not in working


def test_report_39_excel_and_access(client):
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1', first='=Gone', last='Rad')
    refresh()

    xlsx = client.post('/report/39/export.xlsx', headers=ADMIN, data={'start_date': DAY, 'end_date': DAY})
    from io import BytesIO
    from openpyxl import load_workbook
    ws = load_workbook(BytesIO(xlsx.data)).active
    cells = [ws.cell(row=r, column=1) for r in range(5, ws.max_row + 1)]

    assert xlsx.status_code == 200
    assert [(c.value, c.data_type) for c in cells] == [('=Gone Rad', 's')]     # text, never a formula
    assert client.get('/report/39', headers={'X-Test-Role': 'user'}).status_code == 403


def _hospital_staff():
    """RH's and SJH's people, one at both, one no hospital names who signed, one idle."""
    ris(11, 'RAD', True, 'rh.rad@ad.umcrh.com_11', first='Rh', last='Radiologist', site='RH')
    pacs('rh.rad', domain='ad')
    ris(12, 'RAD', True, 'sjh.rad@sj_12', first='Sjh', last='Radiologist', site='SJH')
    pacs('sjh.rad', domain='SJ')
    pacs('both.rad', domain='ad')                          # a PACS login in both domains
    pacs('both.rad', 'radiographers', domain='SJ')
    ris(13, 'TEC', True, 'idle.tec@x_13', first='Idle', last='Tech')        # no hospital, no work
    ris(14, 'RAD', True, 'worker.rad@x_14', first='Worker', last='Rad')     # no hospital, signed
    study(1, 'worker.rad@ad')
    refresh()


def test_report_39_lists_only_the_hospital_picked(monkeypatch):
    from utils import site_db, staff_status
    _hospital_staff()
    sjh = rows("SELECT id FROM sites WHERE code = 'SJH'")[0]['id']

    rh_rows, rh = staff_status.build(DAY, DAY)
    monkeypatch.setattr(site_db, 'served_site_id', lambda: sjh)
    sjh_rows, sjh_summary = staff_status.build(DAY, DAY)

    def names(rs):
        return {r['name']: r['hospital'] for r in rs}

    assert names(rh_rows) == {'Rh Radiologist': 'RH', 'both.rad': 'RH + SJH',
                              'Idle Tech': 'Not known', 'Worker Rad': 'Not known'}
    assert names(sjh_rows) == {'Sjh Radiologist': 'SJH', 'both.rad': 'RH + SJH',
                               'Worker Rad': 'Not known'}          # worked there in the period
    assert rh['hospital'] == 'RH' and rh['on_main'] and rh['not_known'] == 2
    assert sjh_summary['hospital'] == 'SJH' and not sjh_summary['on_main']
    assert rh['domain_rule'] == 'ad = RH, sj = SJH'


def test_the_ris_hospital_counts_when_the_pacs_domain_is_not_listed_and_the_rule_can_change(monkeypatch):
    from utils import site_db, staff_status
    ris(15, 'TEC', True, 'portal.only@x_15', first='Ris', last='Sjh', site='SJH')
    pacs('portal.only', 'administrator', domain='lo')      # a domain no hospital owns
    refresh()

    assert [r['hospital'] for r in staff_status.build(DAY, DAY)[0]] == []
    sjh = rows("SELECT id FROM sites WHERE code = 'SJH'")[0]['id']
    monkeypatch.setattr(site_db, 'served_site_id', lambda: sjh)
    assert [r['hospital'] for r in staff_status.build(DAY, DAY)[0]] == ['SJH']

    sql("INSERT INTO settings (key, value) VALUES ('staff_pacs_domain_sites', 'lo=RH') "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    try:
        assert [r['hospital'] for r in staff_status.build(DAY, DAY)[0]] == ['RH + SJH']
    finally:
        sql("DELETE FROM settings WHERE key = 'staff_pacs_domain_sites'")


def test_report_39_shows_the_hospital_column(client):
    _hospital_staff()

    html = client.get('/report/39', headers=ADMIN,
                      query_string={'start_date': DAY, 'end_date': DAY}).get_data(as_text=True)

    assert '<th scope="col">Hospital</th>' in html
    assert 'Rh Radiologist' in html and 'Sjh Radiologist' not in html
    assert "This hospital's staff (RH)" in html and '(ad = RH, sj = SJH)' in html


def test_the_switch_is_for_administrators_and_rebuilds_the_list_first(client, app):
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1')
    study(1, 'gone.rad@ad')                     # the list is not rebuilt yet

    denied = client.post('/report/39/switch', headers={'X-Test-Role': 'user'}, json={'on': True})
    on = client.post('/report/39/switch', headers=ADMIN, json={'on': True})
    shown = in_request(app, "SELECT rep_study_last_composed_by AS s FROM etl_didb_studies")[0]['s']
    off = client.post('/report/39/switch', headers=ADMIN, json={'on': False})

    assert denied.status_code == 403
    assert on.get_json() == {'status': 'success', 'on': True} and shown == 'Former staff'
    assert off.status_code == 200
    assert in_request(app, "SELECT rep_study_last_composed_by AS s FROM etl_didb_studies")[0]['s'] == 'gone.rad@ad'


def test_a_report_cached_before_the_switch_is_not_served_after_it(app):
    from routes import report_cache
    with app.test_request_context('/'):
        off = report_cache._make_key(35, {'start_date': DAY})
        switch(True)
        on = report_cache._make_key(35, {'start_date': DAY})

    assert off != on


# ── where the views do not reach ──────────────────────────────────────────────

def test_report_32_turnaround_names_a_signer_who_left_former_staff():
    from utils import sign_tat
    sql("INSERT INTO aetitle_modality_map (aetitle, modality, site_id) "
        "VALUES ('CTX1', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))")
    sql("INSERT INTO settings (key, value) VALUES ('pacs_audit_copied_from', '2026-10-01'), "
        "('pacs_audit_copied_until', :u) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        u=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    ris(1, 'RAD', False, 'gone.rad@ad.umcrh.com_1')
    for uid, login in ((1, 'gone.rad@ad'), (2, 'still.here@ad')):
        sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, study_instance_uid, storing_ae,
                                             study_date, study_modality, insert_time)
               VALUES (:u, :u, :x, 'CTX1', :d, 'CT', :t)""",
            u=uid, x=f'1.2.9.{uid}', d=DAY, t=datetime(2026, 10, 5, 8, 0))
        sql("""INSERT INTO etl_pacs_audit_events (log_dbid, event_at, kind, user_name, study_instance_uid)
               VALUES (:i, :t, 'approve', :l, :x)""", i=uid, t=datetime(2026, 10, 5, 10, 0), l=login, x=f'1.2.9.{uid}')
    refresh()

    before = sorted(r['name'] for r in sign_tat.build(DAY, DAY)['by_rad'])
    switch(True)
    after = sign_tat.build(DAY, DAY)

    assert before == ['Gone Rad', 'Still Here']
    assert sorted(r['name'] for r in after['by_rad']) == ['Former staff', 'Still Here']
    assert after['signed'] == 2                   # the department figure keeps the study


def test_the_nightly_pacs_copy_brings_the_account_status(monkeypatch):
    from db import db
    from ETL_JOBS import etl_pacs_user_groups as groups
    since = datetime(2026, 3, 4, 12, 0)

    class Cursor:
        def execute(self, query, *args):
            assert 'STATUS' in query and 'IN_STATE_SINCE' in query
            assert 'PASSWORD' not in query.upper() and 'RECOVERY' not in query.upper()
            self.batches = [[(-11, 11, 'gone.rad', None, 5, 'radiologists', 'ad', 'SUSPENDED', since)], []]

        def fetchmany(self, size):
            return self.batches.pop(0)

        def close(self):
            pass

    conn = SimpleNamespace(cursor=lambda: Cursor(), close=lambda: None)
    monkeypatch.setattr(groups, 'OracleConnector', SimpleNamespace(get_connection=lambda source: conn))

    assert groups.run_pacs_user_groups_etl(db.engine, 'oracle_PACS') == 1
    got = rows("SELECT login_id, user_status, status_since FROM std_pacs_user_groups")
    assert [tuple(r.values()) for r in got] == [('gone.rad', 'SUSPENDED', since)]
