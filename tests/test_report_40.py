"""
tests/test_report_40.py — Report 40, Patient Schedule Lead Time (item 21, migration 0167,
utils/schedule_lead_time.py).

Booked = the order's creation on the RIS (etl_orders.order_created_at); lead time = appointment
day minus booking day; an appointment before its booking is counted aside; cancelled and
"scheduled more than once" from the RIS; through the site's views (no other site's orders, no
HIS duplicates).
"""
from datetime import date, datetime

import pytest
from sqlalchemy import text

from utils import schedule_lead_time as lead

ADMIN = {'X-Test-Role': 'administrator'}
OCT = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}
BASE = 88000


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


def order(n, booked, appointment, modality='CT', status='CM', issuer='SAP_PROD', org=BASE + 1, times=1):
    sql("""INSERT INTO etl_orders (order_dbid, order_control, order_status, modality, scheduled_datetime,
                                   order_created_at, ordering_organization_key, accession_number, proc_id, proc_text)
           VALUES (:k, :i, :s, :m, :a, :b, :o, :acc, 'P1', 'Procedure one')""",
        k=BASE + n, i=issuer, s=status, m=modality, a=appointment, b=booked, o=org, acc=f'ACC{BASE + n}')
    for t in range(times):
        sql("INSERT INTO std_worklist_scheduled (site_worklist_key, scheduled_at) VALUES (:k, :t)",
            k=BASE + n, t=datetime(2026, 9, 1, 8, t))


@pytest.fixture
def october():
    sql("""INSERT INTO std_ordering_organizations (ordering_organization_key, code, name)
           VALUES (:k, 'ER', 'Emergency room B1')
           ON CONFLICT (ordering_organization_key) DO UPDATE SET name = EXCLUDED.name""", k=BASE + 1)
    order(1, datetime(2026, 10, 1, 9), datetime(2026, 10, 1, 14))                       # same day
    order(2, datetime(2026, 9, 20, 10), datetime(2026, 10, 5, 8), modality='MR', times=2)  # 15 days, rescheduled
    order(3, datetime(2026, 10, 2, 8), datetime(2026, 10, 3, 8), status='CA')            # 1 day, cancelled
    order(4, datetime(2026, 10, 10, 12), datetime(2026, 10, 10, 9))                      # booked afterwards
    order(5, datetime(2026, 10, 20, 8), datetime(2026, 11, 2, 8))                        # appointment in November
    order(6, None, datetime(2026, 10, 6, 8))                                             # no booking time
    order(7, datetime(2026, 10, 1, 8), datetime(2026, 10, 7, 8), issuer='SAP_SJH')       # the other site
    order(8, datetime(2026, 10, 1, 8), datetime(2026, 10, 8, 8), status='scheduled')     # an HIS duplicate
    yield


@pytest.mark.parametrize('days, bucket', [(-1, 'after'), (0, '0'), (1, '1'), (2, '2-3'), (7, '4-7'),
                                          (8, '8-14'), (30, '15-30'), (31, '31-60'), (61, '61+')])
def test_buckets(days, bucket):
    assert lead.bucket_of(days) == bucket


def test_the_figures_for_a_month(october):
    from utils.rh_scope import rh_scope
    with rh_scope():                                 # as a web request reads: the site's views
        cases, summary = lead.build(date(2026, 10, 1), date(2026, 10, 31))
    assert sorted(c['exam_id'] - BASE for c in cases) == [1, 2, 3, 4]
    k = summary['kpi']
    assert k['exams'] == 4 and k['after'] == 1
    assert k['median_days'] == 1                     # of 0, 15 and 1; the walk-in left out
    assert k['p90_days'] == 15
    assert k['same_day_pct'] == 33.3 and k['ahead_pct'] == 33.3
    assert k['cancelled_pct'] == 25.0 and k['rescheduled_pct'] == 25.0
    assert {b['key']: b['exams'] for b in summary['buckets'] if b['exams']} == {'after': 1, '0': 1, '1': 1, '15-30': 1}
    by_modality = {r['value']: r['exams'] for r in summary['breakdowns']['modality']['rows']}
    assert by_modality == {'CT': 3, 'MR': 1}
    assert summary['breakdowns']['department']['rows'][0]['value'] == 'Emergency room B1'
    assert sum(map(sum, summary['heat'])) == 4


def test_a_filter_keeps_its_values(october):
    from utils.rh_scope import rh_scope
    with rh_scope():
        cases, summary = lead.build(date(2026, 10, 1), date(2026, 10, 31), {'modality': {'MR'}})
    assert [c['exam_id'] - BASE for c in cases] == [2]
    assert summary['options']['modality'] == ['CT', 'MR']    # the dropdown still offers both


def test_the_page_and_the_excel(client, october):
    r = client.get('/report/40', headers=ADMIN, query_string=OCT)
    assert r.status_code == 200, r.get_data(as_text=True)[:400]
    page = r.get_data(as_text=True)
    assert 'Patient Schedule Lead Time' in page and 'Emergency room B1' in page
    assert 'could not be loaded' not in page

    r = client.post('/report/40/export.xlsx', headers=ADMIN, data=OCT)
    assert r.status_code == 200 and r.mimetype.endswith('spreadsheetml.sheet')
    from openpyxl import load_workbook
    import io
    sheet = load_workbook(io.BytesIO(r.data)).active
    accessions = [sheet.cell(row=i, column=1).value for i in range(6, sheet.max_row + 1)]
    assert sorted(accessions) == [f'ACC{BASE + n}' for n in (1, 2, 3, 4)]


def test_report_40_is_in_the_catalogue_and_the_licence():
    from db import db
    assert db.session.execute(text("SELECT report_name FROM report_template WHERE report_id = 40")).scalar() \
        == 'Patient Schedule Lead Time'
    licence = db.session.execute(text("SELECT value FROM settings WHERE key = 'license'")).scalar()
    if licence and '"reports"' in licence:
        import json
        assert 40 in json.loads(licence)['reports']
