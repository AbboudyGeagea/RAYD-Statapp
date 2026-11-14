"""
tests/test_report_38.py — Report 38, the ER dashboard (until 2026-11-14 the page /er): a
report like every other (CLAUDE.md rule 8, to-do item 25). Per-user report access, the old
address redirects, the shared left panel's filters reach the figures, and migration 0150
adds the catalogue row, the licence entry and access for whoever had the page.
"""
import os
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'admin'}
GRANTED, REFUSED, NO_ROW = 9481, 9482, 9483
VIEWER = {'X-Test-Role': 'viewer', 'X-Test-Uid': str(GRANTED)}
DAY = date(2026, 10, 15)
PERIOD = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}
MIGRATION = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         'migrations', '0150_report_38_er_dashboard.sql')


@pytest.fixture
def sql(app):
    from db import db
    with app.app_context():
        def run(statement, **params):
            db.session.execute(text(statement), params)
            db.session.commit()
        run("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
               VALUES ('CT_ER', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                      ('MR_ER', 'MR', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
        access(run, GRANTED, 'viewer', True)
        access(run, REFUSED, 'viewer', False)
        access(run, NO_ROW, 'finance', None)
        yield run
        db.session.rollback()


def access(sql, uid, role, enabled):
    sql("""INSERT INTO users (id, username, password_hash, role) VALUES (:u, :n, 'x', :r)
           ON CONFLICT (id) DO NOTHING""", u=uid, n=f'r38-{uid}', r=role)
    if enabled is not None:
        sql("""INSERT INTO report_access_control (user_id, report_template_id, is_enabled)
               VALUES (:u, 38, :e)
               ON CONFLICT (user_id, report_template_id) DO UPDATE SET is_enabled = EXCLUDED.is_enabled""",
            u=uid, e=enabled)


def study(sql, uid, ae, location='ER', minutes=45):
    """A reported study: in the PACS at 10:00, report composed `minutes` later."""
    arrived = datetime(DAY.year, DAY.month, DAY.day, 10, 0)
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, storing_ae,
               study_date, patient_location, pacs_site_id_raw, insert_time, rep_study_last_composed_ts)
           VALUES (:u, :u, :a, :ae, :d, :l, (SELECT pacs_site_id FROM sites WHERE is_default LIMIT 1), :t, :r)""",
        u=uid, a=f'38{uid}', ae=ae, d=DAY, l=location, t=arrived, r=arrived + timedelta(minutes=minutes))


def load(client, **extra):
    r = client.get('/report/38/data', headers=ADMIN, query_string={**PERIOD, **extra})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


# ── who can see it ────────────────────────────────────────────────────────────

@pytest.mark.parametrize('who, expected', [('admin', 200), ('granted', 200), ('refused', 403),
                                           ('no_row', 403)])
def test_report_access_decides_who_opens_it(client, sql, who, expected):
    """As /viewer/38 decides it; the data checks it too, so it cannot be reached around the page."""
    headers = {'admin': ADMIN, 'granted': VIEWER,
               'refused': {'X-Test-Role': 'viewer', 'X-Test-Uid': str(REFUSED)},
               'no_row': {'X-Test-Role': 'finance', 'X-Test-Uid': str(NO_ROW)}}[who]
    assert client.get('/report/38', headers=headers).status_code == expected
    assert client.get('/report/38/data', headers=headers).status_code == expected


def test_the_old_address_leads_to_report_38(client, sql):
    r = client.get('/er', headers=VIEWER)
    assert r.status_code == 302 and r.headers['Location'].endswith('/viewer/38')


# ── what the panel sends reaches the figures ─────────────────────────────────

def test_only_er_studies_count(client, sql):
    study(sql, 1, 'CT_ER')
    study(sql, 2, 'MR_ER')
    study(sql, 3, 'CT_ER', location='OPD')
    assert load(client)['kpi']['total'] == 2


def test_the_modality_dropdown_filters_while_ticked(client, sql):
    study(sql, 1, 'CT_ER')
    study(sql, 2, 'MR_ER')
    assert load(client, modality='CT', mod_enabled='on')['kpi']['total'] == 1
    assert load(client, modality='ct ', mod_enabled='on')['kpi']['total'] == 1   # any case or padding
    assert load(client, modality='CT')['kpi']['total'] == 2                      # tick off: not applied


def test_the_target_is_set_in_hours(client, sql):
    study(sql, 1, 'CT_ER', minutes=45)
    study(sql, 2, 'MR_ER', minutes=90)
    default = load(client)['kpi']
    assert default['sla_limit'] == 60 and default['within_sla'] == 1 and default['breached'] == 1
    two_hours = load(client, sla_hours='2')['kpi']
    assert two_hours['sla_limit'] == 120 and two_hours['within_sla'] == 2 and two_hours['breached'] == 0
    assert load(client, sla_hours='0')['kpi']['sla_limit'] == 60                 # not positive: 1 h


def test_the_page_opens_on_the_last_30_days(client, sql):
    html = client.get('/report/38', headers=ADMIN).get_data(as_text=True)
    first = (date.today() - timedelta(days=30)).isoformat()
    assert f'name="start_date" value="{first}"' in html
    assert f'name="end_date" value="{date.today().isoformat()}"' in html
    assert 'name="sla_hours" value="1"' in html
    assert "url_for" not in html and '/report/38/data' in html


# ── migration 0150 ────────────────────────────────────────────────────────────

def _statements():
    body = '\n'.join(l for l in open(MIGRATION, encoding='utf-8').read().splitlines()
                     if not l.strip().startswith('--'))
    return [s.strip() for s in body.split(';') if s.strip()]


def test_report_38_is_in_the_report_catalogue(app):
    from db import db
    with app.app_context():
        row = db.session.execute(text(
            "SELECT report_name, is_base FROM report_template WHERE report_id = 38")).first()
    assert row == ('ER Analytics Dashboard', True)


@pytest.mark.parametrize('licence, expected', [
    ('{"reports": [22, 37]}', [22, 37, 38]),
    ('{"reports": [22, 37], "er_dashboard": true}', [22, 37, 38]),
    ('{"reports": [22, 37], "er_dashboard": false}', [22, 37]),   # the ER page was switched off
])
def test_the_licence_gains_report_38_once(app, licence, expected):
    from db import db
    update = next(s for s in _statements() if s.startswith('UPDATE settings'))
    with app.app_context():
        db.session.execute(text("""INSERT INTO settings (key, value) VALUES ('license', :v)
                                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"""), {'v': licence})
        db.session.execute(text(update))
        db.session.execute(text(update))
        reports = db.session.execute(text("SELECT value::jsonb->'reports' FROM settings WHERE key = 'license'")).scalar()
        db.session.rollback()
    assert reports == expected


def test_access_carries_over_who_had_the_page(app):
    """Every role but tec could open /er; an existing row stays as it is."""
    from db import db
    grant = next(s for s in _statements() if s.startswith('INSERT INTO report_access_control'))
    people = {9491: 'viewer', 9492: 'tec', 9493: 'finance', 9494: 'viewer'}
    with app.app_context():
        for uid, role in people.items():
            db.session.execute(text("INSERT INTO users (id, username, password_hash, role) VALUES (:u, :n, 'x', :r)"),
                               {'u': uid, 'n': f'r38m-{uid}', 'r': role})
        db.session.execute(text("""INSERT INTO report_access_control (user_id, report_template_id, is_enabled)
                                   VALUES (9494, 38, FALSE)"""))
        db.session.execute(text(grant))
        got = dict(db.session.execute(text("""SELECT user_id, is_enabled FROM report_access_control
                                              WHERE report_template_id = 38 AND user_id BETWEEN 9491 AND 9494""")).fetchall())
        db.session.rollback()
    assert got == {9491: True, 9492: False, 9493: True, 9494: False}


def test_the_home_page_card_opens_report_38():
    src = open(os.path.join(os.path.dirname(MIGRATION), '..', 'templates', 'viewer_dashboard.html'),
               encoding='utf-8').read()
    assert "url_for('er.er_page')" not in src
    assert 'report.report_id == 38' in src
