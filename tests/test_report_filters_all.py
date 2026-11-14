"""
tests/test_report_filters_all.py
────────────────────────────────────────────────────────────────
Item 3 of the meeting of 2026-11-13, the rest of it: after the AE title filter
(test_report_22_filters.py, test_ae_filter_reports.py), every other sidebar filter on
every report, so that "a filter shows 0" does not come back.

The dropdowns offer TRIM'd values in the case they are stored in, and the data carries
what PACS sends: a patient class stored as 'o ' and a location stored as 'er ', which the
reports compared raw or with UPPER() alone, so a pick of 'O' or 'ER' matched nothing.
Report 37 is not here: its dropdowns list the values the period itself holds and compare
them exactly (tests/test_sticky_notes.py).
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

pd = pytest.importorskip('pandas')

TODAY = date.today()
TEN = datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=10)
ADMIN = {'X-Test-Role': 'admin'}


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


def scalar(statement, params):
    from db import db
    return db.session.execute(text(statement), params).scalar()


def device(aetitle, modality, site='RH'):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES (:a, :m, (SELECT id FROM sites WHERE code = :s))""", a=aetitle, m=modality, s=site)


def study(uid, ae, patient_class, location, status='UNREAD'):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, procedure_code, patient_class, patient_location,
                insert_time, rep_study_last_composed_by, rep_study_last_composed_ts,
                rep_final_timestamp, rep_final_signed_by, study_status)
           VALUES (:u, :u, :iuid, :acc, :ae, :d, 'CT', 'CT01', :c, :l, :ins, 'dr.test@ad', :comp,
                   :comp, 'dr', :st)""",
        u=uid, iuid=f'1.2.3.{uid}', acc=f'10050{uid:04d}', ae=ae, d=TODAY, c=patient_class,
        l=location, ins=TEN, comp=TEN + timedelta(hours=2), st=status)
    sql("""INSERT INTO etl_patient_view (patient_db_uid, sex) VALUES (:u, 'M')
           ON CONFLICT DO NOTHING""", u=uid)


@pytest.fixture
def two_ct_studies():
    device('CTX1', 'CT')
    study(1, 'CTX1', 'o ', 'er ')      # stored in lower case, with a trailing space
    study(2, 'CTX1', 'I', 'ICU')


def period(**filters):
    items = [('start_date', TODAY.isoformat()), ('end_date', TODAY.isoformat())]
    for k, v in filters.items():
        for one in (v if isinstance(v, list) else [v]):
            items.append((k, one))
    return MultiDict(items)


# ── the shared sidebar filter: Report 36, and Report 25's sections ──────────────

def sidebar_count(form):
    from utils.report_filters import sidebar_filters
    params, clause, _s, _e = sidebar_filters(form)
    return scalar(f"""
        SELECT COUNT(*) FROM etl_didb_studies s
        LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
        WHERE s.study_date BETWEEN :start AND :end {clause}""", params)


@pytest.mark.parametrize('pick', ['O', 'o', ' o '])
def test_sidebar_patient_class_matches_whatever_the_case(two_ct_studies, pick):
    assert sidebar_count(period(class_enabled='on', patient_class=pick)) == 1


@pytest.mark.parametrize('pick', ['ER', 'er'])
def test_sidebar_location_matches_whatever_the_case(two_ct_studies, pick):
    assert sidebar_count(period(loc_enabled='on', patient_location=pick)) == 1


def test_sidebar_modality_matches_whatever_the_case(two_ct_studies):
    assert sidebar_count(period(mod_enabled='on', modality='ct')) == 2


def test_sidebar_values_add_up_to_the_total(two_ct_studies):
    total = sidebar_count(period())
    classes = sidebar_count(period(class_enabled='on', patient_class='O')) + \
        sidebar_count(period(class_enabled='on', patient_class='I'))
    assert total == classes == 2


# ── Reports 25 and 34 (the gold-standard template) ─────────────────────────────

@pytest.mark.parametrize('field,enabled,pick', [
    ('patient_class', 'class_enabled', 'O'),
    ('patient_location', 'loc_enabled', 'ER'),
])
def test_report_25_finds_the_study(two_ct_studies, field, enabled, pick):
    from routes.report_25 import get_gold_standard_data
    data, _s, _e = get_gold_standard_data(period(**{enabled: 'on', field: pick}))
    assert data is not None, f'{field}={pick!r} gave an empty page'
    # a section built on the secondary filters
    assert sum(r['cnt'] for r in data['unread_aging']) == 1


def test_report_25_modality_in_lower_case(two_ct_studies):
    from routes.report_25 import get_gold_standard_data
    data, _s, _e = get_gold_standard_data(period(mod_enabled='on', modality='ct'))
    assert sum(r['cnt'] for r in data['unread_aging']) == 2


@pytest.mark.parametrize('field,enabled,pick', [
    ('patient_class', 'class_enabled', 'O'),
    ('patient_location', 'loc_enabled', 'er'),
    ('modality', 'mod_enabled', 'ct'),
])
def test_report_34_finds_the_study(two_ct_studies, field, enabled, pick):
    from routes.report_34 import get_device_utilization_data
    data, _s, _e = get_device_utilization_data(period(**{enabled: 'on', field: pick}))
    assert data is not None, f'{field}={pick!r} gave an empty page'


# ── Report 35 (technologist turnaround) ─────────────────────────────────────────

def test_report_35_modality_in_lower_case():
    from routes.report_35 import get_technician_tat_data
    device('CTX1', 'CT')
    study(1, 'CTX1', 'O', 'DEF')
    sql("""INSERT INTO std_pps (pps_key, study_instance_uid, study_db_uid, performing_ae_title,
                                start_datetime, end_datetime, procedure_code)
           VALUES (1, '1.2.3.1', 1, 'CTX1', :a, :d, 'CT01')""",
        a=TEN, d=TEN + timedelta(minutes=20))
    sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
           VALUES (1, 1, '100500001', :a)""", a=TEN - timedelta(minutes=10))
    sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at)
           VALUES (1, 1, :d)""", d=TEN + timedelta(minutes=20))
    picked = get_technician_tat_data(period(mod_enabled='on', modality='ct'))['tech_data']
    other = get_technician_tat_data(period(mod_enabled='on', modality='MR'))['tech_data']
    assert picked['by_modality'], 'a lower-case pick found no exam'
    assert not other['by_modality']


# ── Reports 23, 31 and 32 (one value per filter) ───────────────────────────────

def test_report_23_patient_class_and_sex(two_ct_studies):
    from routes.report_23 import get_report_config
    for form, want in ((period(f_class_active='on', f_class='O'), 1),
                       (period(f_class_active='on', f_class='i'), 1),
                       (period(f_sex_active='on', f_sex='m'), 2)):
        base, start, end, params, extra = get_report_config(form)
        n = scalar(f"WITH base_data AS ({base}) SELECT COUNT(DISTINCT study_db_uid) FROM base_data "
                   f"WHERE study_date BETWEEN :s AND :e{extra}", params)
        assert n == want, form


def test_report_31_patient_class(two_ct_studies):
    from routes.report_31 import get_report_config
    base, start, end, params, extra, _site = get_report_config(
        period(f_class_active='on', f_class='O'))
    assert scalar(f"WITH base_data AS ({base}) SELECT COUNT(*) FROM base_data WHERE 1=1{extra}",
                  params) == 1


def test_report_32_patient_class_main_and_secondary(two_ct_studies):
    from routes.report_32 import get_report_config
    base, start, end, params, extra, sec, _join = get_report_config(
        period(f_class_active='on', f_class='O', f_mod_active='on', f_mod='ct'))
    assert scalar(f"WITH base_data AS ({base}) SELECT COUNT(*) FROM base_data "
                  f"WHERE study_date BETWEEN :start AND :end {extra}", params) == 1
    assert scalar(f"""SELECT COUNT(*) FROM etl_didb_studies s
                      LEFT JOIN aetitle_modality_map m
                        ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
                      WHERE s.study_date BETWEEN :start AND :end {sec}""", params) == 1


# ── what the dropdowns offer ────────────────────────────────────────────────────

def test_the_modality_list_leaves_out_the_other_sites_modalities(client):
    device('CTX1', 'CT')
    device('SJH_ONLY', 'XQ', site='SJH')
    mods = client.get('/api/filter-options', headers=ADMIN).get_json()['modalities']
    assert 'CT' in mods
    assert 'XQ' not in mods


def test_the_super_report_offers_only_aes_it_can_count(client, two_ct_studies):
    study(3, 'NOT_MAPPED', 'O', 'DEF')
    aes = client.get('/viewer/super-report/filters', headers=ADMIN).get_json()['storing_ae']
    assert aes == ['CTX1']
