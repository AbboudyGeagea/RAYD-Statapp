"""
tests/test_ae_filter_reports.py
────────────────────────────────────────────────────────────────
The AE title filter of Reports 25, 34 and 36 (meeting of 2026-11-13, Report 22's
"filtering on AE titles shows 0"; Report 22 itself is in test_report_22_filters.py).

All three share the sidebar dropdown, which offers one entry per device in the mapping's
spelling. They used to compare it raw: Reports 25 and 34 against UPPER(TRIM(storing_ae))
(an AE picked in lower case matched nothing, so the page was empty), and their other
sections, and Report 36 through utils.report_filters.sidebar_filters, against the raw
storing_ae (a study stored as 'ct_er_test ' was missing from them).
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

pd = pytest.importorskip('pandas')

TODAY = date.today()
TEN = datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=10)
STORED = 'ct_er_test '     # how PACS stored it on the study
MAPPED = 'CT_ER_TEST'      # how the device is mapped, and what the dropdown offers


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


@pytest.fixture
def one_unread_ct():
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES (:a, 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))""", a=MAPPED)
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, procedure_code, patient_class, patient_location,
                insert_time, rep_study_last_composed_by, rep_study_last_composed_ts, study_status)
           VALUES (1, 1, '1.2.3.1', '100500001', :ae, :d, 'CT', 'CT01', 'O', 'DEF',
                   :ins, 'dr.test@ad', :comp, 'UNREAD')""",
        ae=STORED, d=TODAY, ins=TEN, comp=TEN + timedelta(hours=2))


def form(*aes):
    return MultiDict([('start_date', TODAY.isoformat()), ('end_date', TODAY.isoformat()),
                      ('ae_enabled', 'on')] + [('aetitle', a) for a in aes])


def test_the_dropdown_offers_the_device_once_in_its_mapped_spelling(client, one_unread_ct):
    response = client.get('/api/filter-options', headers={'X-Test-Role': 'admin'})
    assert response.get_json()['aetitles'] == [MAPPED]


@pytest.mark.parametrize('pick', [MAPPED, 'ct_er_test', STORED])
def test_report_25_finds_the_study_whatever_the_case(one_unread_ct, pick):
    from routes.report_25 import get_gold_standard_data
    data, _start, _end = get_gold_standard_data(form(pick))
    assert data is not None, f'{pick!r} gave an empty page'
    # a section built on the secondary filters, which compared the raw storing_ae
    assert data['unread_aging'] == [{'bucket': '0-24h', 'modality': 'CT', 'cnt': 1}]


def test_report_25_still_filters_out_other_devices(one_unread_ct):
    from routes.report_25 import get_gold_standard_data
    data, _start, _end = get_gold_standard_data(form('OTHER_AE'))
    assert data is None


@pytest.mark.parametrize('pick', [MAPPED, 'ct_er_test'])
def test_report_34_finds_the_study_whatever_the_case(one_unread_ct, pick):
    from routes.report_34 import get_device_utilization_data
    data, _start, _end = get_device_utilization_data(form(pick))
    assert data is not None, f'{pick!r} gave an empty page'


def _sidebar_count(f):
    from db import db
    from utils.report_filters import sidebar_filters
    params, clause, _start, _end = sidebar_filters(f)
    return db.session.execute(text(f"""
        SELECT COUNT(*) FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :start AND :end {clause}"""), params).scalar()


@pytest.mark.parametrize('pick', [MAPPED, 'ct_er_test', STORED])
def test_the_shared_sidebar_filter_finds_the_study(one_unread_ct, pick):
    """What Report 36 and Report 25's per-section queries run."""
    assert _sidebar_count(form(pick)) == 1


def test_the_shared_sidebar_filter_ignores_a_blank_pick(one_unread_ct):
    assert _sidebar_count(form('', ' ')) == 1
    assert _sidebar_count(form('OTHER_AE')) == 0
