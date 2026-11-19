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


def his(number, control, at, location='CTSCAN', app='SAP_P', cls='O', facility='1000', entering='', proc='J17A-01'):
    """An HIS ORM as SAP_P sends it: PV1-3 = location^room^bed^^^^facility (1000 RH, 2000 SJH),
    ORC-17 = the entering unit."""
    ctrl = f'MSG{next(_n)}'
    orc = '|'.join(['ORC', control, f'{number}^HIS'] + [''] * 14 + [f'{entering}^^'])
    raw = (f"MSH|^~\\&|{app}||RAYD||{at:%Y%m%d%H%M%S}||ORM^O01|{ctrl}|P|2.3\r"
           f"PID|1||P0001^^^HIS\r"
           f"PV1|1|{cls}|{location}^^^^^^{facility}\r"
           f"{orc}\r"
           f"OBR|1|{number}^HIS||{proc}^CT SCAN, BRAIN\r")
    sql("""INSERT INTO hl7_orders (message_id, message_datetime, message_type, patient_id, placer_order_number,
                                   procedure_code, procedure_text, patient_location, patient_class, raw_message, received_at)
           VALUES (:m, :at, 'ORM^O01', 'P0001', :n, :proc, 'CT SCAN, BRAIN', :loc, :cls, :raw, :at)""",
        m=ctrl, at=at, n=number, loc=location, cls=cls, raw=raw, proc=proc)


def ris(key, number, created, status='CM', issuer='SAP_PROD', appointment=FAR, done_at=None):
    sql("""INSERT INTO etl_orders (order_dbid, order_control, order_status, placer_order_number, order_created_at,
                                   scheduled_datetime, accession_number)
           VALUES (:k, :i, :s, :n, :c, :a, :acc)""",
        k=key, i=issuer, s=status, n=number, c=created, a=appointment, acc=f'ACC{key}')
    if done_at:
        sql("INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at) VALUES (:k, :t)", k=key, t=done_at)


@pytest.fixture
def rooms():
    """The RIS lookup: procedure J17A-01 bookable on two RH CT rooms and SJH's CT (MODALITY_SCHEDULE)."""
    sql("""CREATE TABLE IF NOT EXISTS public.ris_modality_schedule_stage
           (sps_code_key BIGINT, modality_key BIGINT, schedule_template_key BIGINT)""")
    sql("""INSERT INTO procedure_duration_map (procedure_code, duration_minutes, clinical_rvu, technical_rvu, ris_sps_code_key)
           VALUES ('J17A-01', 15, 1, 1, 990001)""")
    for ae, room, key, code in (('HR_CT1', 'CT Room 1', 880001, 'RH'), ('HR_CT2', 'CT128 ER', 880002, 'RH'),
                                ('HR_CTJ', 'CTScanner', 880003, 'SJH')):
        sql("""INSERT INTO aetitle_modality_map (aetitle, modality, station_name, ris_modality_key, site_id)
               SELECT :a, 'CT', :r, :k, id FROM sites WHERE code = :c""", a=ae, r=room, k=key, c=code)
        sql("INSERT INTO public.ris_modality_schedule_stage (sps_code_key, modality_key) VALUES (990001, :k)", k=key)
    # the last 90 days at RH: J17A-01 performed twice in CT Room 1, once in CT128 ER
    from datetime import datetime as _dt, timedelta as _td
    for key, ae in ((770001, 'HR_CT1'), (770002, 'HR_CT1'), (770003, 'HR_CT2')):
        sql("""INSERT INTO std_pps (pps_key, procedure_code, performing_ae_title, start_datetime)
               VALUES (:k, 'J17A-01', :a, :t)""", k=key, a=ae, t=_dt.now() - _td(days=5))
    yield
    sql("DELETE FROM std_pps WHERE pps_key BETWEEN 770001 AND 770003")
    sql("DELETE FROM public.ris_modality_schedule_stage WHERE sps_code_key = 990001")
    sql("DELETE FROM aetitle_modality_map WHERE aetitle IN ('HR_CT1', 'HR_CT2', 'HR_CTJ')")
    sql("DELETE FROM procedure_duration_map WHERE procedure_code = 'J17A-01'")


@pytest.fixture
def feeds(rooms):
    d = lambda h, m=0, day=18: datetime(2026, 11, day, h, m)
    sql("INSERT INTO etl_job_log (job_name, status, start_time) VALUES ('ORDERS_ETL', 'SUCCESS', :t)", t=d(5, day=19))
    his('0019400001', 'NW', d(10));                ris(66001, '0019400001', d(10, 20), done_at=d(12))
    # performed: its PACS study came from the CT128 ER device, so that is its room
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date, study_modality)
           VALUES (66001, 66001, 'HR_CT2', '2026-11-18', 'CT')""")
    sql("UPDATE etl_orders SET study_db_uid = 66001 WHERE order_dbid = 66001")
    his('0019400002', 'NW', d(10, 30)); his('0019400002', 'RQ', d(10, 45))
    ris(66002, '0019400002', d(11), status='scheduled')
    his('0019400003', 'NW', d(11)); his('0019400003', 'CA', d(11, 30))
    his('0019400004', 'NW', d(12))                                            # never registered
    his('0019400005', 'NW', d(12, 30), location='EM-J', facility='2000'); ris(66005, '0019400005', d(12, 40), issuer='SAP_SJH')
    his('0019400006', 'NW', d(13), location='EM-J', facility='2000')          # SJH: the HIS says 2000
    his('0019400007', 'NW', d(13, 30), location='NEWWARD', facility='')      # hospital unknown
    his('0019400012', 'NW', d(13, 40), location='', facility='', entering='ENDOS-J')   # SJH by its unit
    his('0019400013', 'NW', d(13, 50), location='WARD9', facility='1000')    # RH: the HIS says 1000
    his('0019400014', 'RQ', d(14, 10)); his('0019400014', 'RQ', d(14, 20))   # never sent as new: not billed
    his('0019400015', 'NW', d(14, 30), proc='ZZ-99')                          # no booking rule on the RIS
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
                   '0019400004': 'not_on_ris', '0019400008': 'pending', '0019400013': 'not_on_ris',
                   '0019400015': 'not_on_ris'}
    k = s['kpi']
    assert k['orders'] == 7 and k['on_ris_pct'] == 33.3 and k['not_on_ris_pct'] == 50.0
    assert s['site_from_his'] == 5                   # 3, 4, 8, 13, 15: no RIS order, the HIS names RH
    assert s['not_billed'] == 1                      # 14: only changes (RQ), never a new order (NW)
    assert got['0019400003'] == 'cancelled_his'      # billed (NW), then cancelled: still counted
    by_no = {c['his_no']: c for c in cases}
    assert by_no['0019400001']['room'] == 'CT128 ER'                  # performed: the room the RIS used
    assert by_no['0019400002']['room'] == 'CT Room 1'                 # not yet: RH's most used for J17A-01
    assert by_no['0019400002']['room_how'].startswith('not performed yet: most used')
    assert by_no['0019400015']['room'] == his_ris.NO_ROOM
    assert [r['value'] for r in s['breakdowns']['room']['rows']] == ['CT Room 1', 'CT128 ER', his_ris.NO_ROOM]
    assert not any(' / ' in r['value'] for r in s['breakdowns']['room']['rows'])   # each room alone
    assert (s['room_from_ris'], s['room_from_usual'], s['room_from_lookup'], s['room_unknown']) == (1, 5, 0, 1)
    assert his_ris.DIMENSIONS[0][0] == 'room'                         # the breakdown opens on Room
    assert k['median_to_ris_min'] == 25.0            # 20 and 30 minutes
    assert k['median_to_done_h'] == 2.0
    assert s['unknown_site'] == 1                    # NEWWARD: not listed, only counted
    assert s['ris_only'] == {'no_his_number': 1, 'not_sent': 1, 'created': 4}
    two = next(c for c in cases if c['his_no'] == '0019400002')
    assert two['messages'] == 2 and two['control'] == 'RQ' and two['ordered'] == '2026-11-18 10:30'
    assert [c['his_no'] for c in s['not_on_ris']] == ['0019400004', '0019400013', '0019400015']


def test_an_order_not_performed_yet_takes_the_room_the_ris_booked(feeds):
    """Migration 0172: the booked room before the 90-day guess; the room it was performed in
    still wins over its booking."""
    sql("UPDATE etl_orders SET booked_ae_title = 'hr_ct2' WHERE order_dbid = 66002")    # open, booked on CT128 ER
    sql("UPDATE etl_orders SET booked_ae_title = 'HR_CT1' WHERE order_dbid = 66001")    # done in CT128 ER
    from utils.rh_scope import rh_scope
    with rh_scope():
        cases, s = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('RH'))
    by_no = {c['his_no']: c for c in cases}
    assert (by_no['0019400002']['room'], by_no['0019400002']['room_from']) == ('CT128 ER', 'booked')
    assert by_no['0019400002']['room_how'] == 'not performed yet: the room the RIS booked'
    assert (by_no['0019400001']['room'], by_no['0019400001']['room_from']) == ('CT128 ER', 'ris')
    assert (s['room_from_ris'], s['room_from_booked'], s['room_from_usual'], s['room_unknown']) == (1, 1, 4, 1)


def test_a_booked_device_rayd_does_not_map_shows_its_ae_title(feeds):
    sql("UPDATE etl_orders SET booked_ae_title = 'NEWCT9' WHERE order_dbid = 66002")
    from utils.rh_scope import rh_scope
    with rh_scope():
        cases, _ = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('RH'))
    assert next(c['room'] for c in cases if c['his_no'] == '0019400002') == 'NEWCT9'


def test_the_tab_for_sjh_takes_the_other_orders(feeds):
    cases, s = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('SJH'))
    assert {c['his_no']: c['outcome'] for c in cases} == {'0019400005': 'done', '0019400006': 'not_on_ris',
                                                          '0019400012': 'not_on_ris'}
    assert {c['room'] for c in cases} == {'CTScanner'}                # SJH: no recent exam, its only bookable room
    assert {c['room_from'] for c in cases if c['room'] == 'CTScanner'} == {'lookup'}


def test_on_sjhs_own_database_the_his_orders_still_come_from_the_main_one(feeds):
    """Item 13, before the move: the listener writes every HIS order to the main database; SJH's
    own database gets none until BRAYN routes by site. The tab reads them from the main one."""
    from utils import site_db
    if not site_db.has_site_databases():
        pytest.skip('no site database configured')
    with site_db.use_site(site('SJH')):
        cases, s = his_ris.build(date(2026, 11, 18), date(2026, 11, 19), site('SJH'))
    got = {c['his_no'] for c in cases}
    assert {'0019400006', '0019400012'} <= got           # SJH's by PV1-3.7 = 2000 and by its -J unit
    assert not got & {'0019400001', '0019400004'}         # RH's stay out
    assert s['feed_start'] is not None


def test_the_hl7_orders_page_keeps_the_picked_hospital(client, feeds):
    """2026-11-19: HL7 Orders read the main database and keep the header's hospital by the code
    the HIS sends (it showed both hospitals' orders on RH)."""
    day = '2026-11-18'
    assert client.get('/hl7-orders', headers=ADMIN, query_string={'date': day}).status_code == 200
    data = client.get('/hl7-orders/data', headers=ADMIN, query_string={'date': day}).get_json()
    numbers = {o['placer_order_number'] for o in data['orders']}
    assert {'0019400004', '0019400013'} <= numbers               # RH: PV1-3.7 = 1000
    assert not numbers & {'0019400005', '0019400006', '0019400012'}   # SJH: 2000, and its -J unit
    assert data['total'] == len([o for o in data['orders'] if o.get('order_status') != 'CA']) or data['total'] >= 1


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
    assert numbers == ['0019400001', '0019400002', '0019400003', '0019400004', '0019400008', '0019400013',
                       '0019400015']


def test_the_ris_tab_shows_both_tabs_and_an_empty_range_says_so(client):
    r = client.get('/report/27', headers=ADMIN, query_string=PERIOD)
    assert r.status_code == 200
    page = r.get_data(as_text=True)
    assert 'RIS orders vs PACS' in page and 'HIS orders vs RIS' in page
    assert 'No RIS order in this range' in page


def test_a_tab_opens_without_running_until_run_analysis(client, monkeypatch):
    """Operator, 19 Nov: no query when the page or a tab opens; only Run Analysis runs it."""
    def must_not_run(*a, **k):
        raise AssertionError('the HIS vs RIS query ran on opening')
    monkeypatch.setattr(his_ris, 'build', must_not_run)
    r = client.get('/report/27', headers=ADMIN, query_string={'tab': 'his', 'd_start': '2026-11-18', 'd_end': '2026-11-19'})
    assert r.status_code == 200
    page = r.get_data(as_text=True)
    assert 'Select Analysis Range' in page
    assert 'value="2026-11-18"' in page                    # the dates still fill the form
    assert 'start_date=' not in page.split('aria-label="Report 27 views"')[1].split('</nav>')[0]


def test_each_room_downloads_its_own_orders(client, feeds):
    r = client.post('/report/27/export-his-ris', headers=ADMIN, data={**PERIOD, 'room': 'CT128 ER'})
    assert r.status_code == 200 and 'CT128_ER' in r.headers['Content-Disposition']
    import io
    from openpyxl import load_workbook
    sheet = load_workbook(io.BytesIO(r.data)).active
    assert 'room CT128 ER' in sheet.cell(row=1, column=1).value
    assert [sheet.cell(row=i, column=1).value for i in range(6, sheet.max_row + 1)] == ['0019400001']
    page = client.get('/report/27', headers=ADMIN, query_string={**PERIOD, 'tab': 'his'}).get_data(as_text=True)
    assert page.count('name="room" value=') == 3          # one Excel button per room row
