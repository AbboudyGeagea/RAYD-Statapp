"""
tests/test_his_duplicate_orders.py — HIS duplicates left "Scheduled" (migration 0162, item 7).

The HIS sends every order to the RIS twice; one copy goes on to be reported, the other stays
Scheduled and the RIS screen filters it out (operator, 2026-11-17). RAYD does the same: an order
still Scheduled once its day has passed (hospital time) is left out of what every page reads,
with its scheduling events. Today's and future bookings stay, and so does every order that moved
on. The real table keeps every row.
"""
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text


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


def in_request(app, statement, **params):
    from db import db
    db.session.commit()
    with app.test_request_context('/'):
        out = db.session.execute(text(statement), params).scalars().all()
        db.session.commit()
    return out


def outside(statement, **params):
    from db import db
    out = db.session.execute(text(statement), params).scalars().all()
    db.session.commit()
    return out


def order(key, status, when):
    sql("""INSERT INTO etl_orders (order_dbid, patient_dbid, order_status, scheduled_datetime, modality,
                                   order_control, accession_number)
           VALUES (:k, '77', :s, :t, 'MR', 'SAP_PROD', :a)""", k=key, s=status, t=when, a=f'ACC{key}')
    sql("INSERT INTO std_worklist_scheduled (site_worklist_key, pps_key, scheduled_at) VALUES (:k, :k, :t)",
        k=key, t=when)


def test_a_past_order_still_scheduled_is_the_his_duplicate_and_leaves_every_page(app):
    now = datetime.now()
    yesterday, later_today, next_week = now - timedelta(days=1), now.replace(hour=23, minute=0), now + timedelta(days=7)
    order(1, 'scheduled', yesterday)          # the copy the HIS left behind
    order(2, 'CM', yesterday)                 # its twin, reported
    order(3, 'scheduled', later_today)        # booked for today: still to come
    order(4, 'scheduled', next_week)
    order(5, 'CA', yesterday)                 # cancelled is not a duplicate

    assert sorted(in_request(app, "SELECT order_dbid FROM etl_orders")) == [2, 3, 4, 5]
    assert sorted(in_request(app, "SELECT site_worklist_key FROM std_worklist_scheduled")) == [2, 3, 4, 5]
    assert sorted(outside("SELECT order_dbid FROM etl_orders")) == [1, 2, 3, 4, 5]        # nothing deleted


def test_an_order_with_no_status_or_no_date_is_never_a_duplicate(app):
    sql("INSERT INTO etl_orders (order_dbid, scheduled_datetime) VALUES (7, now() - interval '3 days')")
    sql("INSERT INTO etl_orders (order_dbid, order_status) VALUES (8, 'scheduled')")

    assert sorted(in_request(app, "SELECT order_dbid FROM etl_orders WHERE order_dbid IN (7, 8)")) == [7, 8]


def test_the_rule_moves_on_by_itself_each_day(app):
    order(6, 'scheduled', datetime.now().replace(hour=0, minute=1, second=0, microsecond=0))
    # earlier today still counts as today: the day has not passed yet
    assert in_request(app, "SELECT order_dbid FROM etl_orders WHERE order_dbid = 6") == [6]
    sql("UPDATE etl_orders SET scheduled_datetime = scheduled_datetime - interval '1 day' WHERE order_dbid = 6")
    assert in_request(app, "SELECT order_dbid FROM etl_orders WHERE order_dbid = 6") == []
