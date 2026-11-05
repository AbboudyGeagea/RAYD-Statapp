"""
tests/test_er_detection.py
────────────────────────────────────────────────────────────────
ER studies on LAUMC are identified by patient_location = 'ER'. Reports 25, 33 and 35
looked only for the accession prefix '2XE', which matched no LAUMC study in the 12
months measured on 2026-11-05 (LAUMC accessions start 100500), so:

    Report 33   ER studies landed in the Outpatient block, against the 48 h target
                instead of the 24 h urgent one, and the Urgent block never appeared
    Report 35   the "ER overlap" lookup on too-late exams always came back empty
    Report 25   "ER Avg TAT" always showed 0m

These drive the real report functions against the throwaway database. Nearly all ER
studies on LAUMC are patient class O, which is what makes the misclassification real.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

pd = pytest.importorskip('pandas')

from utils import tat_sla  # noqa: E402

TODAY = date.today()
TEN = datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=10)


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    from routes import report_cache
    report_cache._store.clear()
    with app.app_context():
        yield
        db.session.rollback()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def device(aetitle='CT_ER_TEST', modality='CT'):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES (:a, :m, (SELECT id FROM sites WHERE is_default LIMIT 1))""",
        a=aetitle, m=modality)


def study(uid, accession, patient_class, location, composed_after_min=120):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, procedure_code, patient_class, patient_location,
                insert_time, rep_study_last_composed_by, rep_study_last_composed_ts)
           VALUES (:u, :u, :iuid, :a, 'CT_ER_TEST', :d, 'CT', 'CT01', :c, :l, :ins,
                   'dr.test@ad', :comp)""",
        u=uid, iuid=f'1.2.3.{uid}', a=accession, d=TODAY, c=patient_class, l=location,
        ins=TEN, comp=TEN + timedelta(minutes=composed_after_min))


def exam(key, accession, patient_class, location, arrived, done):
    """A RIS exam with its PACS study, as Report 35 reads it."""
    study(key, accession, patient_class, location)
    sql("""INSERT INTO std_pps (pps_key, study_instance_uid, performing_ae_title,
                                start_datetime, end_datetime, procedure_code)
           VALUES (:k, :iuid, 'CT_ER_TEST', :a, :d, 'CT01')""",
        k=key, iuid=f'1.2.3.{key}', a=arrived, d=done)
    sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
           VALUES (:k, :k, :acc, :a)""", k=key, acc=accession, a=arrived)
    sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at)
           VALUES (:k, :k, :d)""", k=key, d=done)


def period():
    return MultiDict({'start_date': TODAY.isoformat(), 'end_date': TODAY.isoformat()})


# ── the shared definition ─────────────────────────────────────────────────────

@pytest.mark.parametrize('accession, location, expected', [
    ('100500001', 'ER', True),
    ('100500001', ' er ', True),
    ('2XE123', None, True),
    ('2xe123', 'ICU', True),
    ('100500001', 'DEF', False),
    ('100500001', 'ERX', False),
    (None, None, False),
])
def test_is_er(accession, location, expected):
    assert tat_sla.is_er(accession, location) is expected


def test_er_mask_matches_is_er_row_by_row():
    df = pd.DataFrame({
        'accession_number': ['100500001', '2XE123', '100500003', None, '100500005'],
        'patient_location': ['ER', 'ICU', 'DEF', None, ' er '],
    })
    assert tat_sla.er_mask(df).tolist() == [True, True, False, False, True]


def test_er_mask_tolerates_a_missing_column():
    # Report 25's template has no accession_number column.
    assert tat_sla.er_mask(pd.DataFrame({'patient_location': ['ER', 'ICU']})).tolist() == [True, False]
    assert tat_sla.er_mask(pd.DataFrame({'accession_number': ['2XE1', '1005']})).tolist() == [True, False]
    assert tat_sla.er_mask(pd.DataFrame({'other': [1, 2]})).tolist() == [False, False]


def test_classify_still_prefers_er_over_patient_class():
    vocab = tat_sla.get_class_vocab()
    assert tat_sla.classify('O', '100500001', 'ER', vocab) == tat_sla.BUCKET_URG
    assert tat_sla.classify('I', '2XE1', 'ICU', vocab) == tat_sla.BUCKET_URG
    assert tat_sla.classify('O', '100500001', 'DEF', vocab) == tat_sla.BUCKET_OUT
    assert tat_sla.classify('I', '100500001', 'ICU', vocab) == tat_sla.BUCKET_IN


# ── Report 33 ─────────────────────────────────────────────────────────────────

def test_report_33_er_study_lands_in_the_urgent_block_with_its_target():
    from routes.report_33 import get_kpi_detailed_reading
    device()
    study(1, '100500001', 'O', 'ER')
    study(2, '100500002', 'O', 'DEF')
    study(3, '100500003', 'I', 'ICU')

    blocks = {b['label']: b for b in get_kpi_detailed_reading(period())}

    assert set(blocks) == {'CT-Urg', 'CT-Out', 'CT-IN'}
    assert blocks['CT-Urg']['sla_target_label'] == '24h'
    assert blocks['CT-Out']['sla_target_label'] == '48h'


# ── Report 35 ─────────────────────────────────────────────────────────────────

def test_report_35_too_late_exam_lists_the_er_exam_that_overlapped_it():
    from routes.report_35 import get_technician_tat_data
    device()
    exam(11, '100500011', 'O', 'DEF', TEN, TEN + timedelta(minutes=120))              # too late
    exam(12, '100500012', 'O', 'ER', TEN + timedelta(minutes=30), TEN + timedelta(minutes=55))
    exam(13, '100500013', 'O', 'DEF', TEN + timedelta(minutes=40), TEN + timedelta(minutes=65))

    flagged = {f['accession']: f for f in get_technician_tat_data(period())['tech_data']['flagged']}

    assert 'too_late' in flagged['100500011']['flags']
    assert [e['accession'] for e in flagged['100500011']['er_concurrent']] == ['100500012']


# ── Report 25 ─────────────────────────────────────────────────────────────────

def test_report_25_er_avg_tat_averages_er_studies_only():
    from routes.report_25 import get_gold_standard_data
    device()
    study(21, '100500021', 'O', 'ER', composed_after_min=60)
    study(22, '100500022', 'I', 'ER', composed_after_min=120)
    study(23, '100500023', 'O', 'DEF', composed_after_min=900)

    data, _start, _end = get_gold_standard_data(period())

    assert data['summary']['er_wait'] == '90.0m'


def test_report_25_unread_aging_loads_in_bucket_order():
    # The query used to fail every time, on a SELECT alias inside an ORDER BY expression,
    # and the error was only logged, so the page showed no aging data at all.
    from routes.report_25 import get_gold_standard_data
    device()
    study(41, '100500041', 'O', 'DEF')
    sql("UPDATE etl_didb_studies SET study_status = 'UNREAD' WHERE study_db_uid = 41")
    study(42, '100500042', 'O', 'DEF')
    sql("""UPDATE etl_didb_studies SET study_status = 'UNREAD', study_date = :d
           WHERE study_db_uid = 42""", d=TODAY - timedelta(days=5))

    period_with_old_study = MultiDict({'start_date': (TODAY - timedelta(days=5)).isoformat(),
                                       'end_date': TODAY.isoformat()})
    data, _start, _end = get_gold_standard_data(period_with_old_study)

    assert data['unread_aging'] == [
        {'bucket': '0-24h', 'modality': 'CT', 'cnt': 1},
        {'bucket': '72h+', 'modality': 'CT', 'cnt': 1},
    ]


def test_report_25_er_avg_tat_is_zero_without_er_studies():
    from routes.report_25 import get_gold_standard_data
    device()
    study(31, '100500031', 'O', 'DEF')

    data, _start, _end = get_gold_standard_data(period())

    assert data['summary']['er_wait'] == '0m'
