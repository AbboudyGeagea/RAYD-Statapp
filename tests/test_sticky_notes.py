"""
tests/test_sticky_notes.py
────────────────────────────────────────────────────────────────
The Sticky Notes page (routes/sticky_notes.py): the JCI measure on urgent ER and inpatient
cases, reading only what the nightly ETL copied into Postgres.
"""
import json
import os
from datetime import datetime, timedelta

import pytest
from flask import render_template_string
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'admin'}
VIEWER = {'X-Test-Role': 'viewer'}

T0 = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0) - timedelta(days=1)
DAY = T0.date().isoformat()
PERIOD = {'start': DAY, 'end': DAY}


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
    run("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CT_T', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('MR_T', 'MR', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('SR_T', 'SR', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    yield run
    run("DELETE FROM etl_sticky_notes")
    run("DELETE FROM study_access_history")
    run("DELETE FROM std_ordering_organizations")


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
    r = client.post('/sticky-notes/settings', headers=ADMIN,
                    json={'departments': list(departments), 'limit_minutes': limit})
    assert r.status_code == 200, r.get_data(as_text=True)


def load(client, **extra):
    r = client.get('/sticky-notes/data', headers=ADMIN, query_string={**PERIOD, **extra})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def by_accession(data):
    return {c['accession']: c for c in data['cases']}


# ── who can see it ────────────────────────────────────────────────────────────

@pytest.mark.parametrize('role, expected', [('admin', 200), ('viewer', 200), ('viewer2', 200),
                                            ('tec', 403), ('finance', 403)])
def test_page_permission(client, sql, role, expected):
    headers = {'X-Test-Role': role}
    assert client.get('/sticky-notes', headers=headers).status_code == expected
    assert client.get('/sticky-notes/data', headers=headers).status_code == expected
    assert client.get('/sticky-notes/export.csv', headers=headers).status_code == expected


def test_page_needs_a_login(client, sql):
    assert client.get('/sticky-notes/data').status_code in (302, 401)


def test_only_an_admin_sees_and_changes_the_settings(client, sql):
    assert b'limit-input' in client.get('/sticky-notes', headers=ADMIN).data
    assert b'limit-input' not in client.get('/sticky-notes', headers=VIEWER).data
    r = client.post('/sticky-notes/settings', headers=VIEWER, json={'departments': ['ICU'], 'limit_minutes': 5})
    assert r.status_code == 403
    assert load(client)['urgent_departments'] == []
    assert load(client)['limit_minutes'] is None


def test_page_states_what_the_clock_really_is(client, sql):
    page = client.get('/sticky-notes', headers=VIEWER).get_data(as_text=True)
    assert 'Images reach PACS to first sticky note' in page
    assert 'cannot yet time from the resident opening the images' in page
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


# ── open seen ─────────────────────────────────────────────────────────────────

def history(sql, uid, captured_minutes, access_minutes, login):
    sql("""INSERT INTO study_access_history (study_db_uid, captured_at, last_accessed_time, rep_last_accessed_by)
           VALUES (:u, :c, :a, :l)""", u=uid, c=m(captured_minutes), a=m(access_minutes), l=login)


def test_open_seen_is_the_authors_open_before_the_note_in_the_next_copy(client, sql):
    study(sql, 1)
    note(sql, 1, 30)
    history(sql, 1, -60, -70, 'someone.else@ad')    # a copy from before the note
    history(sql, 1, 600, 10, 'RES.ONE@AD')          # the next copy; case of the login differs
    history(sql, 1, 2000, 1900, 'someone.else@ad')  # a later reopening, overwritten by then

    data = load(client)
    case = data['cases'][0]

    assert case['minutes_open_to_note'] == 20
    assert data['kpi']['open_seen'] == 1


@pytest.mark.parametrize('access_minutes, login', [(10, 'someone.else@ad'), (40, 'res.one@ad')])
def test_open_not_seen_when_another_login_or_after_the_note(client, sql, access_minutes, login):
    study(sql, 1)
    note(sql, 1, 30)
    history(sql, 1, 600, access_minutes, login)

    case = load(client)['cases'][0]

    assert case['open_at'] is None
    assert case['minutes_open_to_note'] is None


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
    r = client.post('/sticky-notes/settings', headers=ADMIN, json={'departments': [], 'limit_minutes': bad})
    assert r.status_code == 400


def test_clearing_the_limit_goes_back_to_no_judgement(client, sql):
    three_cases(sql)
    settings(client, limit=60)
    settings(client, limit='')

    assert load(client)['limit_minutes'] is None


# ── the modality filter ───────────────────────────────────────────────────────

def test_every_modality_starts_counted_and_the_filter_changes_the_figures(client, sql):
    study(sql, 1, ae='CT_T')
    note(sql, 1, 20)
    study(sql, 2, ae='MR_T')

    everything = load(client)
    ct_only = load(client, unticked='MR')

    assert everything['modalities'] == ['CT', 'MR']
    assert everything['kpi']['noted_pct'] == 50
    assert ct_only['modalities'] == ['CT', 'MR']        # MR stays on the list to tick again
    assert ct_only['unticked'] == ['MR']
    assert ct_only['kpi']['noted_pct'] == 100
    assert [r['modality'] for r in ct_only['by_modality']] == ['CT']


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


# ── the export ────────────────────────────────────────────────────────────────

def test_export_states_what_was_counted(client, sql):
    study(sql, 1, ae='CT_T')
    note(sql, 1, 20)
    study(sql, 2, ae='MR_T')
    settings(client, departments=['ICU'], limit=60)

    r = client.get('/sticky-notes/export.csv', headers=VIEWER, query_string={**PERIOD, 'unticked': 'MR'})
    body = r.get_data(as_text=True)

    assert r.mimetype == 'text/csv'
    assert 'Modalities counted,CT' in body
    assert 'Modalities left out,MR' in body
    assert '"ER (always), ICU"' in body
    assert 'Limit (minutes),60.0' in body
    assert 'Images reach PACS to first sticky note' in body
    assert '100501' in body
    assert '100502' not in body


def test_export_cannot_run_a_cell_as_a_formula(client, sql):
    study(sql, 1)
    organization(sql, 501, 1, 7, '=Emergency()')

    body = client.get('/sticky-notes/export.csv', headers=ADMIN, query_string=PERIOD).get_data(as_text=True)

    assert "'=Emergency()" in body


def test_bad_dates_are_refused(client, sql):
    r = client.get('/sticky-notes/data', headers=ADMIN, query_string={'start': 'yesterday'})
    assert r.status_code == 400


# ── the card ──────────────────────────────────────────────────────────────────

def _card():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'templates', 'viewer_dashboard.html')
    with open(path, encoding='utf-8') as f:
        return f.read()


def test_card_sits_after_the_er_card_in_the_clinical_tab():
    source = _card()
    assert (source.index('id="grid-clinical"') < source.index('<!-- ER Dashboard card -->')
            < source.index('<!-- Sticky Notes card -->') < source.index('id="grid-it"'))


@pytest.mark.parametrize('role, shown', [('admin', True), ('viewer', True), ('tec', False)])
def test_card_follows_the_page_permission(app, sql, role, shown):
    source = _card()
    fragment = source[source.index('<!-- Sticky Notes card -->'):source.index('<!-- /Sticky Notes card -->')]
    with app.test_request_context('/', headers={'X-Test-Role': role}):
        html = render_template_string(fragment)
    assert ('Open Sticky Notes' in html) is shown


def test_the_page_key_can_be_switched_per_user():
    from db import ALL_FEATURE_KEYS, ROLE_PAGE_DEFAULTS
    assert 'sticky_notes' in ALL_FEATURE_KEYS
    assert 'sticky_notes' in ROLE_PAGE_DEFAULTS['viewer']
    assert 'sticky_notes' not in ROLE_PAGE_DEFAULTS['tec']
