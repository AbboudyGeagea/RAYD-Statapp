"""
tests/test_yesterday_reconciliation.py — LAUMC to-do item 23: the home page's Yesterday tab,
"Orders <-> PACS Reconciliation". Yesterday's RIS exams done are grouped by the RIS's own
linked group (Report 27's method), so one exam in several parts is one expected study, and
compared with the PACS studies of the same day. It used to group on the procedure code and
reported "orders placed" equal to "expected studies" whatever the data.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'admin'}


@pytest.fixture
def sql(app):
    from db import db
    with app.app_context():
        def run(statement, **params):
            db.session.execute(text(statement), params)
            db.session.commit()
        run("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
               VALUES ('CT_Y', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
        yield run
        db.session.rollback()


def yesterday_at(hour):
    d = date.today() - timedelta(days=1)          # the test runner is on hospital time
    return datetime(d.year, d.month, d.day, hour, 0)


def order(sql, key, status='CM', linked=None, modality='CT', when=None):
    sql("""INSERT INTO etl_orders (order_dbid, order_status, linked_id, modality, scheduled_datetime)
           VALUES (:k, :s, :l, :m, :t)""",
        k=key, s=status, l=linked, m=modality, t=when or yesterday_at(9))


def study(sql, uid):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date, insert_time)
           VALUES (:u, :u, 'CT_Y', :d, :t)""", u=uid, d=yesterday_at(9).date(), t=yesterday_at(10))


def reconciliation(client):
    r = client.get('/viewer/yesterday', headers=ADMIN)
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()['orders_link']


def test_one_exam_in_several_parts_is_one_expected_study(client, sql):
    order(sql, 1, linked=500)          # CT abdomen ...
    order(sql, 2, linked=500)          # ... and pelvis: one study
    order(sql, 3)                      # no linked group: a group of its own
    order(sql, 4, linked=600)          # a linked group of one
    study(sql, 11)
    study(sql, 12)
    ol = reconciliation(client)
    assert ol['total_orders'] == 4
    assert (ol['linked_groups'], ol['orders_in_linked'], ol['standalone_orders']) == (1, 2, 2)
    assert ol['expected_studies'] == 3
    assert (ol['pacs_studies'], ol['delta'], ol['delta_pct']) == (2, 1, 33.3)


def test_only_exams_done_count(client, sql):
    order(sql, 1)                          # done
    order(sql, 2, status='SC')             # still scheduled: never done, not a missing study
    order(sql, 3, status='CA')             # cancelled
    order(sql, 4, modality='SR')           # not an exam
    order(sql, 5, when=yesterday_at(9) - timedelta(days=1))   # the day before
    ol = reconciliation(client)
    assert ol['total_orders'] == 1 and ol['expected_studies'] == 1


def test_orders_placed_no_longer_mirror_expected_studies(client, sql):
    """The old grouping counted groups as orders, so the two figures were always equal."""
    order(sql, 1, linked=700)
    order(sql, 2, linked=700)
    order(sql, 3, linked=700)
    ol = reconciliation(client)
    assert (ol['total_orders'], ol['expected_studies']) == (3, 1)
