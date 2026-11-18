"""
tests/test_report_27_his_ris.py — item 1, HIS orders vs RIS (Report 27's second tab,
utils/his_ris.py).

One HIS order per HIS order number (its latest message, ordered at its first), matched to the
RIS orders carrying that number; outcome; the hospital from the RIS order, else from the HIS
location; an order of the other hospital left out, one of unknown hospital only counted; RIS
orders with no HIS order; "not copied yet" after the last nightly RIS copy.
"""
from datetime import date, datetime

import pytest
from sqlalchemy import text

from utils import his_ris

ADMIN = {'X-Test-Role': 'administrator'}
PERIOD = {'start_date': '2026-11-18', 'end_date': '2026-11-19'}
FAR = datetime(2030, 1, 1, 9)          # an appointment never in the past (the RH views drop past Scheduled)


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


def site(code):
    from db import db
    return db.session.execute(text("SELECT id FROM sites WHERE code = :c"), {'c': code}).scalar()


_n = iter(range(1, 10_000))


def his(number, control, at, location='CTSCAN', app='SAP_P', cls='O'):
    ctrl = f'MSG{next(_n)}'
    raw = (f"MSH|^~\\&|{app}||RAYD||{at:%Y%m%d%H%M%S}||ORM^O01|{ctrl}|P|2.3\r"
           f"PID|1||P0001^^^HIS\r"
           f"ORC|{control}|{number}^HIS||||||||||\r"
           f"OBR|1|{number}^HIS||J17A-01^CT SCAN, BRAIN\r")
    sql("""INSERT INTO hl7_orders (message_id, message_datetime, message_type, patient_id, placer_order_number,
                                   procedure_code, procedure_text, patient_location, patient_class, raw_message, received_at)
           VALUES (:m, :at, 'ORM^O01', 'P0001', :n, 'J17A-01', 'CT SCAN, BRAIN', :loc, :cls, :raw, :at)""",
        m=ctrl, at=at, n=number, loc=location, cls=cls, raw=raw)


def ris(key, number, created, status='CM', issuer='SAP_PROD', appointment=FAR, done_at=None):
    sql("""INSERT INTO etl_orders (order_dbid, order_control, order_status, placer_order_number, order_created_at,
                                   scheduled_datetime, accession_number)
           VALUES (:k, :i, :s, :n, :c, :a, :acc)""",
        k=key, i=issuer, s=status, n=number, c=created, a=appointment, acc=f'ACC{key}')
    if done_at:
        sql("INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at) VALUES (:k, :t)", k=key, t=done_at)


@pytest.fixture
def feeds():
    d = lambda h, m=0, day=18: datetime(2026, 11, day, h, m)
    sql("INSERT INTO etl_job_log (job_name, status, start_time) VALUES ('ORDERS_ETL', 'SUCCESS', :t)", t=d(5, day=19))
    his('0019400001', 'NW', d(10));                ris(66001, '0019400001', d(10, 20), done_at=d(12))
    his('0019400002', 'NW', d(10, 30)); his('0019400002', 'RQ', d(10, 45))
    ris(66002, '0019400002', d(11), status='scheduled')
    his('0019400003', 'NW', d(11)); his('0019400003', 'CA', d(11, 30))
    his('0019400004', 'NW', d(12))                                            # never registered
    his('0019400005', 'NW', d(12, 30), location='EM-J'); ris(66005, '0019400005', d(12, 40), issuer='SAP_SJH')
    his('0019400006', 'NW', d(13), location='EM-J')                           # SJH by its location
    his('0019400007', 'NW', d(13, 30), location='NEWWARD')                    # hospital unknown
    his('0019400008', 'NW', d(6, day=19))                                     # after the last RIS copy
    his('0019400009', 'NW', d(14), app='OTHERAPP')                            # not the HIS
    ris(66010, None, d(15))                                                   # created on the RIS
    ris(66011, '0019499999', d(15, 30))                                       # a number the HIS never sent
    yield


def test_the_tab_for_rh(feeds):
    from utils.rh_scope import rh_scope
    with rh_scope():
        cases, s = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('RH'))
    got = {c['his_no']: c['outcome'] for c in cases}
    assert got == {'0019400001': 'done', '0019400002': 'open', '0019400003': 'cancelled_his',
                   '0019400004': 'not_on_ris', '0019400008': 'pending'}
    k = s['kpi']
    assert k['orders'] == 5 and k['on_ris_pct'] == 50.0 and k['not_on_ris_pct'] == 25.0
    assert k['median_to_ris_min'] == 25.0            # 20 and 30 minutes
    assert k['median_to_done_h'] == 2.0
    assert s['unknown_site'] == 1                    # NEWWARD: not listed, only counted
    assert s['ris_only'] == {'no_his_number': 1, 'not_sent': 1, 'created': 4}
    two = next(c for c in cases if c['his_no'] == '0019400002')
    assert two['messages'] == 2 and two['control'] == 'RQ' and two['ordered'] == '2026-11-18 10:30'
    assert [c['his_no'] for c in s['not_on_ris']] == ['0019400004']


def test_the_tab_for_sjh_takes_the_other_orders(feeds):
    cases, s = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('SJH'))
    assert {c['his_no']: c['outcome'] for c in cases} == {'0019400005': 'done', '0019400006': 'not_on_ris'}


def test_the_page_and_the_excel(client, feeds):
    r = client.get('/report/27', headers=ADMIN, query_string={**PERIOD, 'tab': 'his'})
    assert r.status_code == 200, r.get_data(as_text=True)[:400]
    page = r.get_data(as_text=True)
    assert 'HIS orders vs RIS' in page and 'could not be loaded' not in page
    assert '0019400004' in page and '0019400006' not in page      # SJH's never on RH's page

    r = client.post('/report/27/export-his-ris', headers=ADMIN, data=PERIOD)
    assert r.status_code == 200 and r.mimetype.endswith('spreadsheetml.sheet')
    import io
    from openpyxl import load_workbook
    sheet = load_workbook(io.BytesIO(r.data)).active
    numbers = sorted(sheet.cell(row=i, column=1).value for i in range(6, sheet.max_row + 1))
    assert numbers == ['0019400001', '0019400002', '0019400003', '0019400004', '0019400008']


def test_the_ris_tab_shows_both_tabs_and_an_empty_range_says_so(client):
    r = client.get('/report/27', headers=ADMIN, query_string=PERIOD)
    assert r.status_code == 200
    page = r.get_data(as_text=True)
    assert 'RIS orders vs PACS' in page and 'HIS orders vs RIS' in page
    assert 'No RIS order in this range' in page
