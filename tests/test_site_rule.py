"""
tests/test_site_rule.py
────────────────────────────────────────────────────────────────
Which site a PACS study belongs to, settled against production on 2026-11-05.

The PACS SITE_ID label is not enough on its own:
    SENO1         An RH mammography device. Double reading stamps most of its studies
                  with PACS site '1', though their RIS orders are all RH.
    LAUMCWFM1AR   Reports without images on PACS site '2', which is not a real site.
                  Counted aside, in no site's totals.

And the device map is not enough on its own:
    gateways      Carry traffic for both sites, so the PACS label decides each study.
    unmapped      Have no mapped site at all, and must not be lost.

These drive the real report code against the throwaway database.
"""
import os
import re

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

pytest.importorskip('pandas')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START, END = '2026-10-01', '2026-10-31'
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


def scalar(statement, **params):
    from db import db
    return db.session.execute(text(statement), params).scalar()


def site_id(code):
    return scalar("SELECT id FROM sites WHERE code = :c", c=code)


def device(aetitle, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES (:a, :m, :s)
           ON CONFLICT (aetitle) DO UPDATE
           SET modality = EXCLUDED.modality, site_id = EXCLUDED.site_id""",
        a=aetitle, m=modality, s=site)


def study(uid, ae, pacs_site, modality='CT', status='APPROVED'):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, pacs_site_id_raw, study_status)
           VALUES (:u, :u, :iuid, :a, :ae, :d, :m, :site, :st)""",
        u=uid, iuid=f'1.2.3.{uid}', a=f'A{uid}', ae=ae, d=DAY, m=modality, site=pacs_site, st=status)


@pytest.fixture
def every_case():
    """One study per case. The uid says which case it is."""
    rh, sjh = site_id('RH'), site_id('SJH')
    device('RH_SCANNER', 'CT', rh)
    device('SENO1', 'MG', rh)
    device('SJH_SCANNER', 'CT', sjh)
    device('GATEWAY_T', 'PACS', rh)
    device(ROUTER, 'PACS', None)
    study(1, 'RH_SCANNER', '0')             # RH, the plain case
    study(2, 'SENO1', '1', modality='MG')   # RH: double read
    study(3, 'SENO1', '0', modality='MG')   # RH
    study(4, 'SJH_SCANNER', '1')            # SJH
    study(5, 'SJH_SCANNER', '0')            # SJH: the device decides
    study(6, 'GATEWAY_T', '0')              # RH: the label decides
    study(7, 'GATEWAY_T', '1')              # SJH: the label decides
    study(8, 'UNMAPPED_AE', '0')            # RH: the label decides
    study(9, 'UNMAPPED_AE', '1')            # SJH: the label decides
    study(10, ROUTER, '2', modality='SR')   # no site: counted aside


# ── the rule itself ───────────────────────────────────────────────────────────

RH_CASES = {1, 2, 3, 6, 8}


def test_the_rule_decides_every_case(every_case):
    from db import db
    from utils.report_filters import study_site_sql
    rows = db.session.execute(text(f"""
        SELECT s.study_db_uid
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
        WHERE {study_site_sql('rh')}
    """), {'rh': site_id('RH')}).scalars().all()
    assert set(rows) == RH_CASES


def test_the_other_site_gets_the_rest_but_not_the_router(every_case):
    from db import db
    from utils.report_filters import study_site_sql
    rows = db.session.execute(text(f"""
        SELECT s.study_db_uid
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
        WHERE {study_site_sql('sjh')}
    """), {'sjh': site_id('SJH')}).scalars().all()
    assert set(rows) == {4, 5, 7, 9}


# ── Report 23 ─────────────────────────────────────────────────────────────────

def report_23_uids():
    from db import db
    from routes.report_23 import get_report_config
    form = MultiDict({'start_date': START, 'end_date': END})
    base_sql, _start, _end, params, extra = get_report_config(form)
    assert base_sql, 'report_template row 23 is missing'
    return db.session.execute(text(f"""
        WITH base_data AS ({base_sql})
        SELECT modality, COUNT(*) FROM base_data
        WHERE study_date BETWEEN :s AND :e{extra}
        GROUP BY 1
    """), params).fetchall()


def test_report_23_keeps_double_read_mammography(every_case):
    counts = dict(report_23_uids())
    assert counts['MG'] == 2


def test_report_23_applies_the_rule_to_every_other_case(every_case):
    counts = dict(report_23_uids())
    # RH_SCANNER, plus the unmapped device on RH's label, whose modality comes from the
    # study; the RH study on the gateway is the one 'PACS' row
    assert counts['CT'] == 2
    assert counts['PACS'] == 1
    assert sum(counts.values()) == len(RH_CASES)


def test_report_23_page_shows_the_router_aside(client, every_case):
    response = client.get('/report/23', query_string={'start_date': START, 'end_date': END},
                          headers={'X-Test-Role': 'admin'})
    assert response.status_code == 200
    assert re.search(r'Reports Without Images</p>\s*<p[^>]*>1</p>', response.get_data(as_text=True))


# ── Report 27 ─────────────────────────────────────────────────────────────────

def report_27_pacs_total():
    from routes.report_27 import _build_system_agreement
    months = _build_system_agreement(START, END)['months']
    return months[0]['pacs_total'] if months else 0


def test_report_27_pacs_side_applies_the_rule_to_every_case(every_case):
    assert report_27_pacs_total() == len(RH_CASES)


# One case at a time: on the PACS label alone these two errors cancel out in a total.
def test_report_27_pacs_side_keeps_double_read_mammography():
    device('SENO1', 'MG', site_id('RH'))
    study(1, 'SENO1', '1', modality='MG')
    assert report_27_pacs_total() == 1


def test_report_27_pacs_side_leaves_out_a_device_at_the_other_site():
    device('SJH_SCANNER', 'CT', site_id('SJH'))
    study(1, 'SJH_SCANNER', '0')
    assert report_27_pacs_total() == 0


# ── reports that resolve site through the device map ──────────────────────────

def test_migration_takes_the_router_out_of_every_site(client):
    """Read from what the migrations left, not from a test's own row."""
    row = scalar("SELECT site_id FROM aetitle_modality_map WHERE UPPER(TRIM(aetitle)) = :a", a=ROUTER)
    assert row is None
    assert scalar("SELECT COUNT(*) FROM aetitle_modality_map WHERE UPPER(TRIM(aetitle)) = :a", a=ROUTER) == 1


def test_the_shared_sidebar_filter_leaves_the_router_out():
    """The site clause reports 25, 36 and others build on. Uses the migrated map row."""
    from db import db
    from utils.report_filters import sidebar_filters
    device('RH_SCANNER', 'CT', site_id('RH'))
    study(1, 'RH_SCANNER', '0')
    study(2, ROUTER, '2', modality='SR')
    params, clause, _s, _e = sidebar_filters(MultiDict({'start_date': START, 'end_date': END}))
    rows = db.session.execute(text(f"""
        SELECT s.study_db_uid
        FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :start AND :end {clause}
    """), params).scalars().all()
    assert rows == [1]


# ── fact_exam ─────────────────────────────────────────────────────────────────

def fact(uid, ae, pacs_site, raw='\\CT\\', workitem=None, linked=False):
    sql("""INSERT INTO fact_exam (study_db_uid, workitem_db_uid, is_linked, storing_ae,
                                  pacs_site_id, study_modality_raw, study_ts, number_of_images)
           VALUES (:u, :w, :l, :ae, :s, :raw, :d, 1)""",
        u=uid, w=workitem, l=linked, ae=ae, s=pacs_site, raw=raw, d=DAY)


def fact_sites():
    from db import db
    from ETL_JOBS.etl_fact_exam import _DERIVE_SQL
    db.session.execute(text(_DERIVE_SQL))
    db.session.commit()
    return dict(db.session.execute(text("SELECT study_db_uid, site_id FROM fact_exam")).fetchall())


def test_fact_exam_site_follows_the_rule():
    rh, sjh = site_id('RH'), site_id('SJH')
    device('SENO1', 'MG', rh)
    device('SJH_SCANNER', 'CT', sjh)
    device('GATEWAY_T', 'PACS', rh)
    device(ROUTER, 'PACS', None)
    fact(1, 'SENO1', '1', raw='\\MG\\')
    fact(2, 'SJH_SCANNER', '0')
    fact(3, 'GATEWAY_T', '1')
    fact(4, 'UNMAPPED_AE', '0')
    fact(5, ROUTER, '2')
    assert fact_sites() == {1: rh, 2: sjh, 3: sjh, 4: rh, 5: None}


def test_a_linked_report_takes_the_site_of_the_device_holding_the_images():
    rh = site_id('RH')
    device('SENO1', 'MG', rh)
    device(ROUTER, 'PACS', None)
    fact(1, 'SENO1', '1', raw='\\MG\\', workitem=77, linked=True)
    fact(2, ROUTER, '2', raw='\\SR\\', workitem=77, linked=True)
    assert fact_sites() == {1: rh, 2: rh}


def test_the_migration_marks_only_rows_that_disagree_with_their_device():
    from db import db
    rh, sjh = site_id('RH'), site_id('SJH')
    device('SENO1', 'MG', rh)
    device('GATEWAY_T', 'PACS', rh)
    for uid, ae, site in ((1, 'SENO1', sjh), (2, 'SENO1', rh), (3, 'GATEWAY_T', sjh), (4, 'UNKNOWN', sjh)):
        fact(uid, ae, '1')
        sql("UPDATE fact_exam SET device = :ae, site_id = :s, derived_at = NOW() WHERE study_db_uid = :u",
            ae=ae, s=site, u=uid)
    with open(os.path.join(ROOT, 'migrations', '0141_laumcwfm1ar_no_site.sql'), encoding='utf-8') as f:
        db.session.execute(text(f.read()))
    db.session.commit()
    marked = db.session.execute(
        text("SELECT study_db_uid FROM fact_exam WHERE derived_at IS NULL")).scalars().all()
    assert marked == [1]
