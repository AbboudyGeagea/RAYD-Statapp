"""
tests/test_unread_assigned.py — item 45: exams waiting to be read per assigned radiologist
(utils/unread_assigned.py, Report 36's tab "Waiting to be read").

The assignment is SITE_PPS.ASSIGNED_TO (std_site_pps_ext.assigned_to, a resource key as text) on the
exam's step, found through std_worklist_exam_done; "waiting" = the exam's current RIS status has the
stage exam_done; one radiologist with several resource keys is counted once.
"""
from datetime import date, datetime

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}
TODAY = date(2026, 11, 27)


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    from routes.report_cache import cache_invalidate
    cache_invalidate('36-unread')
    with app.app_context():
        yield
        db.session.rollback()
    cache_invalidate('36-unread')


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def exam(key, status, done_at=None, assigned=None, modality='CT'):
    """One RIS worklist row; its step (pps_key = key + 1000) carries the assignment."""
    sql("""INSERT INTO etl_orders (order_dbid, status_key, order_status, modality)
           VALUES (:k, :s, 'CM', :m)""", k=key, s=status, m=modality)
    if done_at is not None:
        sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at)
               VALUES (:k, :p, :t)""", k=key, p=key + 1000, t=done_at)
        sql("INSERT INTO std_site_pps_ext (pps_key, assigned_to) VALUES (:p, :a)", p=key + 1000, a=assigned)


@pytest.fixture
def data():
    for key, first, last in ((63586, 'Sahar', 'Semaan'),
                             (81778, 'Perla', 'Audi'), (81203, 'PERLA', 'AUDI'),   # one person, two keys
                             (63588, 'Tamina', 'Elias-Rizk')):
        sql("INSERT INTO std_resources_ris (resource_id_key, first_name, last_name) VALUES (:k, :f, :l)",
            k=key, f=first, l=last)
    exam(1, 100, datetime(2026, 11, 27, 9, 0), '63586', 'MR')     # Semaan, done today
    exam(2, 2022, datetime(2026, 11, 25, 9, 0), ' 63586 ')        # Semaan, Pending, 2 days
    exam(3, 100, datetime(2026, 11, 10, 9, 0), '81778')           # Audi, 17 days
    exam(4, 100, datetime(2026, 9, 1, 9, 0), '81203')             # Audi (other key), 87 days
    exam(5, 100, datetime(2026, 11, 26, 9, 0), None)              # nobody assigned
    exam(6, 100, datetime(2026, 11, 26, 9, 0), '99999')           # a key that names nobody
    exam(7, 100)                                                  # no exam-done row, no step
    exam(8, 160, datetime(2026, 11, 26, 9, 0), '63588')           # approved: not waiting
    exam(9, 40, None)                                             # scheduled: not waiting


def build(app):
    from db import db
    from utils import unread_assigned
    db.session.commit()
    with app.test_request_context('/'):                           # through the views, as a page reads
        out = unread_assigned.build(today=TODAY)
        db.session.commit()
    return out


def test_waiting_exams_per_assigned_radiologist(app, data):
    out = build(app)
    rows = {r['name']: r for r in out['rows']}

    assert out['total'] == 7 and out['assigned'] == 4 and out['not_assigned'] == 3
    assert out['radiologists'] == 2 and 'Tamina Elias-Rizk' not in rows        # approved exam left out
    assert out['over_7'] == 2                                                  # Audi's 17 and 87 days

    semaan = rows['Sahar Semaan']
    assert (semaan['n'], semaan['today'], semaan['d1_7'], semaan['d8_30'], semaan['over30']) == (2, 1, 1, 0, 0)
    assert dict(semaan['modalities']) == {'MR': 1, 'CT': 1}

    audi = rows['Perla Audi']                                   # two keys, one person, lower-case spelling
    assert 'PERLA AUDI' not in rows
    assert (audi['n'], audi['d8_30'], audi['over30']) == (2, 1, 1)
    assert audi['oldest'] == '01/09/2026' and audi['oldest_days'] == 87

    nobody = rows['Not assigned']                               # empty, unknown key, no step at all
    assert nobody['n'] == 3 and not nobody['assigned'] and nobody['no_date'] == 1
    assert out['rows'][-1]['name'] == 'Not assigned'            # listed last
    assert [r['name'] for r in out['rows'][:2]] == ['Perla Audi', 'Sahar Semaan']   # most first, then by name


def test_latest_exam_done_gives_the_step(app, data):
    """An exam done twice: the later exam done's step holds the assignment that counts."""
    sql("INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at) VALUES (5, 5555, '2026-11-26 10:00')")
    sql("INSERT INTO std_site_pps_ext (pps_key, assigned_to) VALUES (5555, '63588')")
    rows = {r['name']: r for r in build(app)['rows']}
    assert rows['Tamina Elias-Rizk']['n'] == 1 and rows['Not assigned']['n'] == 2


def test_the_tab_on_report_36(app, client, data):
    page = client.get('/report/36', headers=ADMIN).get_data(as_text=True)
    assert 'data-tab="unread"' in page and 'data-part-template="_report_36_unread.html"' in page

    r = client.get('/report/36/part/unread', headers=ADMIN,
                   query_string={'start_date': '2020-01-01', 'end_date': '2020-01-02'})   # dates do not apply
    html = r.get_data(as_text=True)
    assert r.status_code == 200, html[:400]
    assert 'Waiting to be read, per assigned radiologist' in html
    assert 'Sahar Semaan' in html and 'Perla Audi' in html and 'Not assigned' in html
    for key in ('r36-unread-total', 'r36-unread-assigned', 'r36-unread-not-assigned',
                'r36-unread-over-7', 'r36-unread-table'):
        assert f'data-explain="{key}"' in html


def test_every_unread_explanation_exists():
    import pathlib
    js = (pathlib.Path(__file__).resolve().parent.parent / 'static' / 'js' / 'chart-explanations.js').read_text(encoding='utf-8')
    for key in ('r36-unread-total', 'r36-unread-assigned', 'r36-unread-not-assigned',
                'r36-unread-over-7', 'r36-unread-table'):
        assert f"'{key}'" in js
