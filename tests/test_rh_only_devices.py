"""
tests/test_rh_only_devices.py
────────────────────────────────────────────────────────────────
Operator rule, 2026-11-05: SJH appears in nothing, RH only, and no SJH data is deleted.
These cover what is read from aetitle_modality_map and sites themselves: device lists and
site pickers, plus fact_exam, which records its exclusions rather than dropping them.
"""
import pytest
from sqlalchemy import text

DAY = '2026-10-15'
ROUTER = 'LAUMCWFM1AR'


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


def values(statement, **params):
    from db import db
    rows = db.session.execute(text(statement), params).scalars().all()
    db.session.commit()
    return rows


def site_id(code):
    return values("SELECT id FROM sites WHERE code = :c", c=code)[0]


def device(aetitle, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id) VALUES (:a, :m, :s)
           ON CONFLICT (aetitle) DO UPDATE SET modality = EXCLUDED.modality, site_id = EXCLUDED.site_id""",
        a=aetitle, m=modality, s=site)


@pytest.fixture
def devices():
    rh, sjh = site_id('RH'), site_id('SJH')
    device('RH_SCANNER', 'CT', rh)
    device('SENO1', 'MG', rh)
    device('SJH_SCANNER', 'CT', sjh)
    device(ROUTER, 'PACS', None)


def test_device_lists_leave_out_other_sites(devices):
    from utils.report_filters import rh_device_sql
    rows = set(values(f"""SELECT aetitle FROM aetitle_modality_map m
                          WHERE {rh_device_sql('m')} AND aetitle IN
                          ('RH_SCANNER', 'SJH_SCANNER', 'SENO1', 'LAUMCWFM1AR')"""))
    assert rows == {'RH_SCANNER', 'SENO1', ROUTER}


def test_the_department_board_leaves_out_other_sites(client, devices):
    """The board (item 38) shows the RIS rooms of one hospital, among the account's."""
    from datetime import datetime
    sql("""UPDATE aetitle_modality_map SET ris_modality_key = 990000 + length(aetitle), room_code = aetitle
            WHERE aetitle IN ('RH_SCANNER', 'SJH_SCANNER')""")
    for room in ('RH_SCANNER', 'SJH_SCANNER'):             # a booking today on each
        sql("""INSERT INTO brayn_study_state (accession_number, ris_room, appointment_at, current_rank,
                                              event_count, is_closed, first_seen_at, updated_at)
               VALUES (:a, :r, :t, 40, 1, FALSE, now(), now())""", a='ACC-' + room, r=room, t=datetime.now())
    r = client.get('/viewer/department/api/board', headers={'X-Test-Role': 'administrator', 'X-Test-Uid': '1'})
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert 'RH_SCANNER' in body
    assert 'SJH_SCANNER' not in body


def test_the_department_board_offers_no_other_site(client, devices):
    board = client.get('/viewer/department/api/board',
                       headers={'X-Test-Role': 'administrator', 'X-Test-Uid': '1'}).get_json()
    assert [s['code'] for s in board['sites']] == ['RH']
    assert 'plans' not in board                      # the floor plan is gone (2026-11-19)


def test_the_floor_plan_page_is_gone(client):
    """Operator, 2026-11-19: Admin > Floor Plan removed with everything related."""
    r = client.get('/admin/floor-plan/', headers={'X-Test-Role': 'superuser'})
    assert r.status_code == 404
    page = client.get('/viewer/department', headers={'X-Test-Role': 'superuser'}).get_data(as_text=True)
    assert 'floor_plan' not in page and 'plan-btn' not in page


def test_fact_exam_marks_sjh_as_not_counted(devices):
    from db import db
    from ETL_JOBS.etl_fact_exam import _DERIVE_SQL
    for uid, ae, label in ((1, 'RH_SCANNER', '0'), (2, 'SJH_SCANNER', '1')):
        sql("""INSERT INTO fact_exam (study_db_uid, storing_ae, pacs_site_id, study_modality_raw,
                                      study_ts, number_of_images)
               VALUES (:u, :ae, :l, '\\CT\\', :d, 1)""", u=uid, ae=ae, l=label, d=DAY)
    db.session.execute(text(_DERIVE_SQL))
    db.session.commit()
    rows = dict(db.session.execute(text("SELECT study_db_uid, reason FROM fact_exam")).fetchall())
    assert rows == {1: None, 2: 'SJH (RH only)'}
