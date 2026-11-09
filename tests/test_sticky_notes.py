"""
tests/test_sticky_notes.py
────────────────────────────────────────────────────────────────
Report 37 (routes/report_37.py, the former Sticky Notes page): the JCI measure on urgent ER
and inpatient cases, reading only what the nightly ETL copied into Postgres. A report like
every other since 2026-11-09: shared filter panel, report access, report_template row 37.
"""
import json
import os
from datetime import date, datetime, timedelta

import pytest
from flask import render_template_string
from sqlalchemy import text

GRANTED, REFUSED, NO_ROW = 9371, 9372, 9373     # users with report 37 on, off, never set
ADMIN = {'X-Test-Role': 'admin'}
VIEWER = {'X-Test-Role': 'viewer', 'X-Test-Uid': str(GRANTED)}

T0 = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0) - timedelta(days=1)
DAY = T0.date().isoformat()
PERIOD = {'start_date': DAY, 'end_date': DAY}


def m(minutes):
    return T0 + timedelta(minutes=minutes)


@pytest.fixture
def sql(app):
    from db import db

    def run(statement, **params):
        with app.app_context():
            db.session.execute(text(statement), params)
            db.session.commit()
    run("DELETE FROM etl_sticky_notes")
    run("DELETE FROM study_access_history")
    run("DELETE FROM std_ordering_organizations")
    run("DELETE FROM physician_alias_map")
    run("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CT_T', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('MR_T', 'MR', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('SR_T', 'SR', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    access(run, GRANTED, 'viewer', True)
    yield run
    run("DELETE FROM etl_sticky_notes")
    run("DELETE FROM study_access_history")
    run("DELETE FROM std_ordering_organizations")
    run("DELETE FROM physician_alias_map")


def access(sql, uid, role, enabled):
    """A real user (report access points at users) and, unless None, its report 37 access."""
    sql("""INSERT INTO users (id, username, password_hash, role) VALUES (:u, :n, 'x', :r)
           ON CONFLICT (id) DO NOTHING""", u=uid, n=f'r37-{uid}', r=role)
    if enabled is not None:
        sql("""INSERT INTO report_access_control (user_id, report_template_id, is_enabled)
               VALUES (:u, 37, :e)
               ON CONFLICT (user_id, report_template_id) DO UPDATE SET is_enabled = EXCLUDED.is_enabled""",
            u=uid, e=enabled)


def study(sql, uid, location='ER', full=None, ae='CT_T', site_label='0', arrived=None):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae,
               study_date, patient_location, patient_location_full, pacs_site_id_raw, insert_time)
           VALUES (:u, :u, :a, :ae, :d, :l, :f, :s, :t)""",
        u=uid, a=f'10050{uid}', ae=ae, d=T0.date(), l=location, f=full, s=site_label, t=arrived or m(0))


def note(sql, uid, minutes, author='res.one@ad', deleted='N', data_len=40, data_uid=None):
    sql("""INSERT INTO etl_sticky_notes (data_db_uid, study_db_uid, user_name, date_created,
               delete_indication, data_len)
           VALUES (:d, :u, :a, :t, :x, :n)""",
        d=data_uid or uid, u=uid, a=author, t=m(minutes), x=deleted, n=data_len)


def organization(sql, order_key, uid, org_key, name):
    sql("""INSERT INTO std_ordering_organizations (ordering_organization_key, name)
           VALUES (:k, :n) ON CONFLICT DO NOTHING""", k=org_key, n=name)
    sql("""INSERT INTO etl_orders (order_dbid, study_db_uid, ordering_organization_key)
           VALUES (:o, :u, :k)""", o=order_key, u=uid, k=org_key)


def settings(client, departments=(), limit=None):
    r = client.post('/report/37/settings', headers=ADMIN,
                    json={'departments': list(departments), 'limit_minutes': limit})
    assert r.status_code == 200, r.get_data(as_text=True)


def load(client, **extra):
    r = client.get('/report/37/data', headers=ADMIN, query_string={**PERIOD, **extra})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def by_accession(data):
    return {c['accession']: c for c in data['cases']}


# ── who can see it ────────────────────────────────────────────────────────────

@pytest.mark.parametrize('who, expected', [('admin', 200), ('granted', 200), ('refused', 403),
                                           ('no_row', 403)])
def test_report_access_decides_who_opens_it(client, sql, who, expected):
    """As /viewer/37 decides it: an admin, or a user whose access to report 37 is on. The
    data and the export check it too, so they cannot be reached around the page."""
    access(sql, REFUSED, 'viewer', False)
    access(sql, NO_ROW, 'finance', None)
    headers = {'admin': ADMIN, 'granted': VIEWER,
               'refused': {'X-Test-Role': 'viewer', 'X-Test-Uid': str(REFUSED)},
               'no_row': {'X-Test-Role': 'finance', 'X-Test-Uid': str(NO_ROW)}}[who]
    assert client.get('/report/37', headers=headers).status_code == expected
    assert client.get('/report/37/data', headers=headers).status_code == expected
    assert client.get('/report/37/export.csv', headers=headers).status_code == expected


def test_the_old_address_leads_to_report_37(client, sql):
    r = client.get('/sticky-notes', headers=VIEWER)
    assert r.status_code == 302 and r.headers['Location'].endswith('/viewer/37')


def test_page_needs_a_login(client, sql):
    assert client.get('/report/37/data').status_code in (302, 401)


def test_only_an_admin_sees_and_changes_the_settings(client, sql):
    assert b'limit-input' in client.get('/report/37', headers=ADMIN).data
    assert b'limit-input' not in client.get('/report/37', headers=VIEWER).data
    r = client.post('/report/37/settings', headers=VIEWER, json={'departments': ['ICU'], 'limit_minutes': 5})
    assert r.status_code == 403
    assert load(client)['urgent_departments'] == []
    assert load(client)['limit_minutes'] is None


def test_page_states_what_the_clock_really_is(client, sql):
    page = client.get('/report/37', headers=VIEWER).get_data(as_text=True)
    assert 'images reach pacs to first sticky note' in page.lower()
    assert 'PACS keeps only the latest open' in page
    assert 'cannot show who received a note' in page


# ── which cases count ─────────────────────────────────────────────────────────

def test_er_always_counts_and_other_departments_only_once_ticked(client, sql):
    study(sql, 1, location='ER')
    study(sql, 2, location='ICU')

    assert set(by_accession(load(client))) == {'100501'}

    settings(client, departments=['ICU'])

    assert set(by_accession(load(client))) == {'100501', '100502'}


def test_a_ris_emergency_organization_counts_as_er(client, sql):
    study(sql, 1, location='DEF')
    organization(sql, 501, 1, 7, 'Emergency room B1')

    assert by_accession(load(client))['100501']['department'] == 'Emergency room B1'


@pytest.mark.parametrize('ae, site_label', [('SR_T', '0'), ('UNMAPPED', '1'), ('LAUMCWFM1AR', '0')])
def test_structured_reports_other_sites_and_imageless_devices_are_left_out(client, sql, ae, site_label):
    study(sql, 1, location='ER', ae=ae, site_label=site_label)

    assert load(client)['cases'] == []


def test_studies_outside_the_period_are_left_out(client, sql):
    study(sql, 1, arrived=m(-60 * 24 * 5))

    assert load(client)['cases'] == []


# ── the department ────────────────────────────────────────────────────────────

def test_department_prefers_ris_then_pacs_and_keeps_unknown_visible(client, sql):
    settings(client, departments=['Neuro ward', 'MSU7', 'MSU', 'Department unknown'])
    study(sql, 1, location='DEF', full='DEFLOC')
    organization(sql, 501, 1, 7, 'Neuro ward')
    study(sql, 2, location='MSU', full='MSU7')
    organization(sql, 502, 2, 8, 'Generic Orgnization')
    study(sql, 3, location='MSU')
    study(sql, 4, location='DEF', full='DEFLOC')

    cases = by_accession(load(client))

    assert cases['100501']['department'] == 'Neuro ward'
    assert cases['100502']['department'] == 'MSU7'
    assert cases['100503']['department'] == 'MSU'
    assert cases['100504']['department'] == 'Department unknown'


def test_settings_list_real_departments_without_er_or_defaults(client, sql):
    study(sql, 1, location='MSU', full='MSU7')
    study(sql, 2, location='ER', full='ER')
    study(sql, 3, location='DEF', full='DEFLOC')
    study(sql, 4, location='DEF', full='DEFLOC')
    organization(sql, 501, 4, 7, 'Emergency room B1')
    organization(sql, 502, 3, 8, 'Generic Orgnization')
    organization(sql, 503, 1, 9, 'Neuro ward')
    sql("UPDATE etl_orders SET scheduled_datetime = :t", t=m(0))

    names = [o['name'] for o in load(client)['department_options']]

    assert names == ['MSU7', 'Neuro ward']


def test_settings_do_not_offer_another_sites_locations(client, sql):
    study(sql, 1, location='MSU', full='MSU7')
    study(sql, 2, location='SJH', full='SJHWARD', ae='UNMAPPED', site_label='1')

    assert [o['name'] for o in load(client)['department_options']] == ['MSU7']


# ── the times ─────────────────────────────────────────────────────────────────

def test_time_to_note_uses_the_first_real_note(client, sql):
    study(sql, 1)
    note(sql, 1, 5, deleted='Y', data_len=1, data_uid=11)       # deleted
    note(sql, 1, 6, data_len=1, data_uid=12)                    # blank
    note(sql, 1, 30, author='res.one@ad', data_uid=13)
    note(sql, 1, 90, author='res.two@ad', data_uid=14)

    case = load(client)['cases'][0]

    assert case['minutes_to_note'] == 30
    assert case['note_author'] == 'res.one@ad'


def test_a_note_before_a_late_arrival_time_is_not_a_negative_wait(client, sql):
    study(sql, 1)
    note(sql, 1, -10)

    assert load(client)['cases'][0]['minutes_to_note'] == 0


def test_author_role_comes_from_the_shared_lookup(client, sql):
    sql("""INSERT INTO pacs_user_role_override (login_id, role) VALUES ('RES.ONE', 'residents')
           ON CONFLICT DO NOTHING""")
    study(sql, 1)
    note(sql, 1, 30)

    assert load(client)['cases'][0]['author_role'] == 'residents'


def test_exam_done_and_first_signature_across_report_versions(client, sql):
    study(sql, 1)
    sql("INSERT INTO etl_orders (order_dbid, study_db_uid) VALUES (501, 1)")
    sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at)
           VALUES (501, :a), (501, :b)""", a=m(-10), b=m(-5))
    sql("""INSERT INTO std_resources_ris (resource_id_key, first_name, last_name)
           VALUES (61, 'First', 'Signer'), (62, 'Later', 'Signer')""")
    sql("""INSERT INTO std_reports (report_key, version, reported_acc_number, is_max_version,
                                    verified1_date, verified1_by_resource_id_key)
           VALUES (900, 1, '100501', FALSE, :a, 61), (900, 2, '100501', TRUE, :b, 62)""",
        a=m(110), b=m(300))

    case = load(client)['cases'][0]

    assert case['signed_by'] == 'First Signer'
    assert case['minutes_exam_to_sign'] == 120
    assert load(client)['kpi']['median_exam_to_sign'] == 120


# ── first open ────────────────────────────────────────────────────────────────

def history(sql, uid, captured_minutes, access_minutes, login):
    sql("""INSERT INTO study_access_history (study_db_uid, captured_at, last_accessed_time, rep_last_accessed_by)
           VALUES (:u, :c, :a, :l)""",
        u=uid, c=m(captured_minutes), a=m(access_minutes) if access_minutes is not None else None, l=login)


def test_first_open_is_the_earliest_copy_and_shows_whoever_opened_it(client, sql):
    study(sql, 1)
    note(sql, 1, 30, author='res.one@ad')
    history(sql, 1, 600, 10, 'rad.two@ad')              # someone other than the note's author
    history(sql, 1, 2000, 1900, 'res.one@ad')           # a later reopening

    data = load(client)
    case = data['cases'][0]

    assert case['open_at'] == m(10).strftime('%Y-%m-%d %H:%M')
    assert case['opened_by_login'] == 'rad.two@ad'
    assert case['opened_by'] == 'Rad Two'
    assert case['minutes_open_to_note'] == 20
    assert data['kpi']['open_seen'] == 1


def test_a_study_with_no_note_still_shows_its_first_open(client, sql):
    study(sql, 1)
    history(sql, 1, 600, 10, 'res.one@ad')

    case = load(client)['cases'][0]

    assert case['open_at'] == m(10).strftime('%Y-%m-%d %H:%M')
    assert case['outcome'] == 'no_note'


def test_an_open_after_the_note_is_not_passed_off_as_the_first(client, sql):
    study(sql, 1)
    note(sql, 1, 30)
    history(sql, 1, 600, 40, 'rad.two@ad')

    data = load(client)
    case = data['cases'][0]

    assert case['open_at'] is None
    assert case['opened_by'] is None
    assert case['open_overwritten'] is True
    assert data['kpi']['open_seen'] == 0
    assert data['kpi']['open_overwritten'] == 1


def test_a_copy_with_no_login_or_no_time_is_not_an_open(client, sql):
    study(sql, 1)
    history(sql, 1, 100, 5, None)
    history(sql, 1, 200, None, 'res.one@ad')

    assert load(client)['cases'][0]['open_at'] is None


HOUR = 60


def test_an_open_caught_days_after_arrival_is_not_passed_off_as_the_first(client, sql):
    """What happened to 29-31 October 2026: the first nightly copy reached back 10 days, so
    its open is the latest one PACS kept by then, not the first."""
    study(sql, 1)
    history(sql, 1, 9 * 24 * HOUR, 8 * 24 * HOUR, 'rad.two@ad')

    data = load(client)
    case = data['cases'][0]

    assert case['open_at'] is None and case['opened_by'] is None
    assert case['open_caught_late'] is True and case['open_overwritten'] is False
    assert case['open_blank_reason'] == 'Caught over 36 h after last seen unopened'
    assert data['kpi']['open_seen'] == 0
    assert data['kpi']['open_caught_late'] == 1


def test_an_open_caught_the_night_after_an_unopened_copy_is_still_the_first(client, sql):
    """Nobody had opened it by the first copy, so the open the next copy caught is the first,
    though it came 44 hours after the images."""
    study(sql, 1)
    history(sql, 1, 20 * HOUR, None, None)
    history(sql, 1, 44 * HOUR, 30 * HOUR, 'res.one@ad')

    case = load(client)['cases'][0]

    assert case['open_at'] == m(30 * HOUR).strftime('%Y-%m-%d %H:%M')
    assert case['opened_by_login'] == 'res.one@ad'
    assert case['open_caught_late'] is False and case['open_blank_reason'] is None


def test_a_missed_night_blanks_the_open_it_finally_caught(client, sql):
    study(sql, 1)
    history(sql, 1, 20 * HOUR, None, None)
    history(sql, 1, 68 * HOUR, 50 * HOUR, 'res.one@ad')    # 48 hours after the last copy

    case = load(client)['cases'][0]

    assert case['open_at'] is None and case['open_caught_late'] is True


def test_the_export_says_why_a_first_open_is_blank(client, sql):
    study(sql, 1)
    history(sql, 1, 9 * 24 * HOUR, 8 * 24 * HOUR, 'rad.two@ad')
    study(sql, 2)
    note(sql, 2, 30)
    history(sql, 2, 600, 40, 'rad.two@ad')
    study(sql, 3)

    body = client.get('/report/37/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)
    lines = {line.split(',')[1]: line for line in body.splitlines() if line.count(',') > 5}

    assert 'First open blank because' in lines['Accession']
    assert 'Caught over 36 h after last seen unopened' in lines['100501']
    assert 'Reopened after the note' in lines['100502']
    assert 'No open caught' in lines['100503']


def test_the_admin_buttons_only_report_what_the_server_confirmed(client, sql):
    """An expired session or page token is answered with a redirect to the login page, which
    fetch follows and reports as 200: Generate said "Done: undefined cases." and saved
    nothing (LAUMC, 2026-11-09). Runs the page's own postJson against stand-in answers."""
    import re
    import shutil
    import subprocess
    if not shutil.which('node'):
        pytest.skip('node not installed')
    page = client.get('/report/37', headers=ADMIN).get_data(as_text=True)
    helper = re.search(r'(const SESSION_EXPIRED = .*?\n)(async function postJson.*?\n\})', page, re.S)
    assert helper, 'postJson is missing from the page'
    script = helper.group(1) + helper.group(2) + """
const answers = {
    login: { ok: true, redirected: true, json: async () => { throw new Error('html'); } },
    html: { ok: true, redirected: false, json: async () => { throw new Error('html'); } },
    done: { ok: true, redirected: false, json: async () => ({ status: 'success', cases: 1430 }) },
    refused: { ok: false, redirected: false, json: async () => ({ status: 'error', message: 'Only a finished month can be generated.' }) },
};
(async () => {
    const out = {};
    for (const [name, answer] of Object.entries(answers)) {
        globalThis.fetch = async () => answer;
        out[name] = await postJson('/x', {}, 'Could not generate.');
    }
    globalThis.fetch = async () => { throw new Error('offline'); };
    out.offline = await postJson('/x', {}, 'Could not generate.');
    console.log(JSON.stringify(out));
})();
"""
    tmp = os.path.join(os.path.dirname(__file__), '_sticky_post_json.js')
    try:
        open(tmp, 'w', encoding='utf-8').write(script)
        done = subprocess.run(['node', tmp], capture_output=True, text=True)
    finally:
        os.remove(tmp)
    assert done.returncode == 0, done.stderr
    out = json.loads(done.stdout)
    assert out['login']['done'] is False and 'expired' in out['login']['message']
    assert out['html']['done'] is False and 'expired' in out['html']['message']
    assert out['done']['done'] is True and out['done']['cases'] == 1430
    assert out['refused']['done'] is False and out['refused']['message'] == 'Only a finished month can be generated.'
    assert out['offline']['done'] is False and out['offline']['message'] == 'Could not generate.'


# ── names and the patient ─────────────────────────────────────────────────────

def test_patient_id_is_on_the_page_and_in_the_export(client, sql):
    study(sql, 1)
    sql("INSERT INTO etl_patient_view (patient_db_uid, fallback_id) VALUES (1, 'MRN00123')")

    assert load(client)['cases'][0]['patient_id'] == 'MRN00123'
    body = client.get('/report/37/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)
    assert 'Patient ID,Accession' in body
    assert 'MRN00123,100501' in body


@pytest.mark.parametrize('login, alias, expected', [
    ('first.last@ad', None, 'First Last'),
    ('first.middle.last@ad', None, 'First Middle Last'),
    ('TRAD@AD', None, 'TRAD'),
    ('first.last@ad', 'Dr First Last', 'Dr First Last'),
    (None, None, None),
])
def test_person_name(login, alias, expected):
    from utils.sticky_notes import person_name
    assert person_name(alias, login) == expected


def test_a_curated_name_is_used_when_there_is_one(client, sql):
    sql("INSERT INTO physician_alias_map (alias, canonical_name) VALUES ('res.one@ad', 'Dr Resident One')")
    study(sql, 1)
    note(sql, 1, 30)
    history(sql, 1, 600, 10, 'res.one@ad')

    case = load(client)['cases'][0]

    assert (case['note_by'], case['opened_by']) == ('Dr Resident One', 'Dr Resident One')
    assert case['note_author'] == 'res.one@ad'


def test_the_period_starts_on_the_preceding_month(client, sql):
    from datetime import date
    from utils.sticky_notes import previous_month
    assert previous_month(date(2026, 11, 7)) == (date(2026, 10, 1), date(2026, 10, 31))
    assert previous_month(date(2027, 1, 1)) == (date(2026, 12, 1), date(2026, 12, 31))
    assert previous_month(date(2028, 3, 31)) == (date(2028, 2, 1), date(2028, 2, 29))
    first, last = previous_month()
    page = client.get('/report/37', headers=VIEWER).get_data(as_text=True)
    assert f'value="{first.isoformat()}"' in page and f'value="{last.isoformat()}"' in page


# ── the limit and the outcomes ────────────────────────────────────────────────

def three_cases(sql):
    study(sql, 1)
    note(sql, 1, 20)
    study(sql, 2)
    note(sql, 2, 90)
    study(sql, 3)


def test_no_case_passes_or_fails_until_a_limit_is_set(client, sql):
    three_cases(sql)

    data = load(client)

    assert sorted(c['outcome'] for c in data['cases']) == ['no_note', 'noted', 'noted']
    assert (data['kpi']['within'], data['kpi']['over']) == (0, 0)
    assert data['limit_minutes'] is None


def test_outcomes_once_a_limit_is_set(client, sql):
    three_cases(sql)
    settings(client, limit=60)

    data = load(client)
    cases = by_accession(data)

    assert cases['100501']['outcome'] == 'within'
    assert cases['100502']['outcome'] == 'over'
    assert cases['100503']['outcome'] == 'no_note'
    assert (data['kpi']['cases'], data['kpi']['noted'], data['kpi']['no_note']) == (3, 2, 1)
    assert (data['kpi']['within'], data['kpi']['over']) == (1, 1)
    assert data['kpi']['median_to_note'] == 55


def test_exactly_at_the_limit_is_within(client, sql):
    study(sql, 1)
    note(sql, 1, 60)
    settings(client, limit=60)

    assert load(client)['cases'][0]['outcome'] == 'within'


@pytest.mark.parametrize('bad', [0, -5, 'soon'])
def test_a_bad_limit_is_refused(client, sql, bad):
    r = client.post('/report/37/settings', headers=ADMIN, json={'departments': [], 'limit_minutes': bad})
    assert r.status_code == 400


def test_clearing_the_limit_goes_back_to_no_judgement(client, sql):
    three_cases(sql)
    settings(client, limit=60)
    settings(client, limit='')

    assert load(client)['limit_minutes'] is None


# ── the panel's filters ───────────────────────────────────────────────────────

def test_every_modality_starts_counted_and_the_filter_changes_the_figures(client, sql):
    study(sql, 1, ae='CT_T')
    note(sql, 1, 20)
    study(sql, 2, ae='MR_T')

    everything = load(client)
    ct_only = load(client, modality=['CT'], mod_enabled='on')

    assert everything['choices']['modality'] == ['CT', 'MR']
    assert everything['kpi']['noted_pct'] == 50
    assert ct_only['choices']['modality'] == ['CT', 'MR']   # MR stays on the list to choose again
    assert ct_only['filters'] == {'modality': ['CT']}
    assert ct_only['kpi']['noted_pct'] == 100
    assert [r['modality'] for r in ct_only['by_modality']] == ['CT']


def test_a_choice_counts_only_while_apply_filter_is_ticked(client, sql):
    """As on every report: an unticked "Apply filter" sends no *_enabled, and hides nothing."""
    study(sql, 1, ae='CT_T')
    study(sql, 2, ae='MR_T')

    data = load(client, modality=['CT'])

    assert data['kpi']['cases'] == 2 and data['filters'] == {}


def test_department_and_outcome_filter_like_modality(client, sql):
    settings(client, departments=['ICU'], limit=60)
    study(sql, 1, location='ER')
    note(sql, 1, 20)                                    # within
    study(sql, 2, location='ICU')
    note(sql, 2, 300)                                   # over
    study(sql, 3, location='ER')                        # no note

    icu = load(client, department=['ICU'], dept_enabled='on')
    late_or_missing = load(client, outcome=['over', 'no_note'], outcome_enabled='on')

    assert sorted(by_accession(icu)) == ['100502']
    assert icu['choices']['department'] == ['ER', 'ICU']
    assert sorted(by_accession(late_or_missing)) == ['100502', '100503']


def test_the_report_uses_the_shared_filter_panel(client, sql):
    """The left panel every report uses (templates/_report_filter_panel.html), with the
    monthly workbook under it, not a filter strip of its own."""
    page = client.get('/report/37', headers=ADMIN, query_string={**PERIOD, 'modality': 'CT', 'mod_enabled': 'on'})
    html = page.get_data(as_text=True)
    panel = html[html.index('aria-label="Report filters"'):html.index('</aside>')]

    assert page.status_code == 200
    assert f'name="start_date" value="{DAY}"' in panel and f'name="end_date" value="{DAY}"' in panel
    for name, toggle in (('modality', 'mod_enabled'), ('department', 'dept_enabled'), ('outcome', 'outcome_enabled')):
        assert f'name="{name}" multiple' in panel and f'name="{toggle}"' in panel
    assert '<option value="CT" selected>CT</option>' in panel           # the choice shows at once
    assert 'class="rfp-generate">Generate</button>' in panel
    assert 'id="monthly-list"' in panel and 'Regenerate' in panel
    assert 'Regenerate' not in client.get('/report/37', headers=VIEWER).get_data(as_text=True)


def test_the_period_starts_on_the_preceding_month(client, sql):
    from utils.sticky_notes import previous_month
    first, last = previous_month()
    html = client.get('/report/37', headers=VIEWER).get_data(as_text=True)
    assert f'name="start_date" value="{first.isoformat()}"' in html
    assert f'name="end_date" value="{last.isoformat()}"' in html


def test_breakdowns_add_up_to_the_page(client, sql):
    settings(client, departments=['ICU'])
    study(sql, 1, location='ER')
    note(sql, 1, 20)
    study(sql, 2, location='ICU')
    study(sql, 3, location='ICU')
    note(sql, 3, 40)

    data = load(client)
    rows = {r['department']: r for r in data['by_department']}

    assert rows['ICU']['cases'] == 2 and rows['ICU']['noted_pct'] == 50
    assert rows['ER']['cases'] == 1
    assert sum(r['cases'] for r in data['by_department']) == data['kpi']['cases']


# ── the charts that replace the case list ─────────────────────────────────────

def test_time_to_note_bars_count_each_case_once(client, sql):
    settings(client, limit=45)
    for uid, minutes in ((1, 10), (2, 20), (3, 50), (4, 90), (5, 600), (6, None)):
        study(sql, uid)
        if minutes is not None:
            note(sql, uid, minutes)

    data = load(client)
    bars = {b['key']: b for b in data['by_bucket']}

    assert [b['key'] for b in data['by_bucket']] == ['< 15 min', '15–30 min', '30–60 min', '1–2 h',
                                                     '2–4 h', '4–8 h', '8 h or more', 'No note']
    assert (bars['< 15 min']['within'], bars['15–30 min']['within']) == (1, 1)
    assert (bars['30–60 min']['over'], bars['1–2 h']['over'], bars['8 h or more']['over']) == (1, 1, 1)
    assert bars['No note']['no_note'] == 1
    assert sum(b['cases'] for b in data['by_bucket']) == data['kpi']['cases'] == 6
    assert {c['bucket'] for c in data['cases']} == {'< 15 min', '15–30 min', '30–60 min', '1–2 h',
                                                    '8 h or more', 'No note'}


def test_the_trend_has_a_bar_for_every_day_including_empty_ones(client, sql):
    study(sql, 1)
    note(sql, 1, 20)
    day_before = (T0 - timedelta(days=1)).date().isoformat()

    data = load(client, start_date=day_before, end_date=DAY)

    assert data['trend_granularity'] == 'day'
    assert [(r['key'], r['cases'], r['noted']) for r in data['trend']] == [(day_before, 0, 0), (DAY, 1, 1)]
    assert data['cases'][0]['trend_key'] == DAY


def test_a_long_period_is_drawn_week_by_week(client, sql):
    study(sql, 1)
    start = (T0 - timedelta(days=90)).date()

    data = load(client, start_date=start.isoformat(), end_date=DAY)

    week_of_case = (T0.date() - timedelta(days=T0.weekday())).isoformat()
    assert data['trend_granularity'] == 'week'
    assert all(date.fromisoformat(r['key']).weekday() == 0 for r in data['trend'])
    assert {r['key']: r['cases'] for r in data['trend']}[week_of_case] == 1
    assert data['cases'][0]['trend_key'] == week_of_case


def test_the_page_draws_charts_instead_of_listing_every_case(client, sql):
    page = client.get('/report/37', headers=VIEWER).get_data(as_text=True)
    for chart in ('chart-buckets', 'chart-departments', 'chart-modalities', 'chart-trend'):
        assert f'id="{chart}"' in page
    assert 'id="cases-body"' not in page
    assert 'Click any bar to see its cases' in page


# ── the export ────────────────────────────────────────────────────────────────

def test_export_states_what_was_counted(client, sql):
    study(sql, 1, ae='CT_T')
    note(sql, 1, 20)
    study(sql, 2, ae='MR_T')
    settings(client, departments=['ICU'], limit=60)

    r = client.get('/report/37/export.csv', headers=VIEWER,
                   query_string={**PERIOD, 'modality': 'CT', 'mod_enabled': 'on'})
    body = r.get_data(as_text=True)

    assert r.mimetype == 'text/csv'
    assert 'Modalities,CT' in body
    assert 'Departments shown,all urgent departments' in body
    assert 'Outcomes shown,all' in body
    assert '"ER (always), ICU"' in body
    assert 'Limit (minutes),60.0' in body
    assert 'Images reach PACS to first sticky note' in body
    assert '100501' in body
    assert '100502' not in body


def test_export_cannot_run_a_cell_as_a_formula(client, sql):
    study(sql, 1)
    organization(sql, 501, 1, 7, '=Emergency()')

    body = client.get('/report/37/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)

    assert "'=Emergency()" in body


def test_bad_dates_are_refused(client, sql):
    r = client.get('/report/37/data', headers=ADMIN, query_string={'start_date': 'yesterday'})
    assert r.status_code == 400


# ── the card ──────────────────────────────────────────────────────────────────

def _card():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'templates', 'viewer_dashboard.html')
    with open(path, encoding='utf-8') as f:
        return f.read()


def _report_37_card(source):
    start = source.index('{% for report in reports if report.report_id == 37 %}')
    return source[start:source.index('{% endfor %}', start) + len('{% endfor %}')]


def test_card_sits_after_the_er_card_in_the_clinical_tab_and_nowhere_else():
    source = _card()
    assert (source.index('id="grid-clinical"') < source.index('<!-- ER Dashboard card -->')
            < source.index('report.report_id == 37') < source.index('id="grid-it"'))
    assert 'not in [22,23,25,27,28,30,35,36,37]' in source      # not repeated in the IT tab


@pytest.mark.parametrize('has_access', [True, False])
def test_card_is_the_standard_report_card_for_whoever_has_report_37(app, sql, has_access):
    """viewer_dashboard passes only the reports a user may open, as for every report."""
    class Row:
        report_id, report_name, long_description = 37, 'Urgent Emergent: First Open & Sticky Note', 'JCI'
    with app.test_request_context('/', headers=ADMIN):
        html = render_template_string(_report_37_card(_card()),
                                      reports=[Row] if has_access else [], user_favorites=[])
    assert ('Open Analysis' in html) is has_access
    assert ('Urgent Emergent: First Open &amp; Sticky Note' in html) is has_access


def test_report_access_replaced_the_page_permission():
    from db import ALL_FEATURE_KEYS
    assert 'sticky_notes' not in ALL_FEATURE_KEYS


# ── its registration (migration 0147) ─────────────────────────────────────────

MIGRATION = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         'migrations', '0147_register_report_37.sql')


def _statements():
    body = '\n'.join(l for l in open(MIGRATION, encoding='utf-8').read().splitlines()
                     if not l.strip().startswith('--'))
    return [s.strip() for s in body.split(';') if s.strip()]


def test_report_37_is_in_the_report_catalogue(app):
    from db import db
    with app.app_context():
        row = db.session.execute(text(
            "SELECT report_name, is_base FROM report_template WHERE report_id = 37")).first()
    assert row == ('Urgent Emergent: First Open & Sticky Note', True)


def test_the_licence_gains_report_37_once(app, sql):
    from db import db
    update = next(s for s in _statements() if s.startswith('UPDATE settings'))
    with app.app_context():
        db.session.execute(text("""INSERT INTO settings (key, value) VALUES ('license', '{"reports": [22, 36]}')
                                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"""))
        db.session.execute(text(update))
        db.session.execute(text(update))
        reports = db.session.execute(text("SELECT value::jsonb->'reports' FROM settings WHERE key = 'license'")).scalar()
        db.session.rollback()
    assert reports == [22, 36, 37]


def test_access_carries_over_who_had_the_page(app, sql):
    """An explicit grant or block of the old page stays; otherwise the role decides, as the
    old page permission did (viewer yes, tec no)."""
    from db import db
    grant = next(s for s in _statements() if s.startswith('INSERT INTO report_access_control'))
    people = {9381: ('viewer', None), 9382: ('tec', None), 9383: ('viewer', False), 9384: ('tec', True)}
    with app.app_context():
        for uid, (role, page) in people.items():
            db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (:u, :n, 'x', :r)"),
                               {'u': uid, 'n': f'r37m-{uid}', 'r': role})
            if page is not None:
                db.session.execute(text("""INSERT INTO user_page_permissions (user_id, page_key, is_enabled)
                                           VALUES (:u, 'sticky_notes', :e)"""), {'u': uid, 'e': page})
        db.session.execute(text(grant))
        got = dict(db.session.execute(text("""SELECT user_id, is_enabled FROM report_access_control
                                              WHERE report_template_id = 37 AND user_id BETWEEN 9381 AND 9384""")).fetchall())
        db.session.rollback()
    assert got == {9381: True, 9382: False, 9383: False, 9384: True}
