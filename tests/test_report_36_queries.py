"""
tests/test_report_36_queries.py — Report 36's residents / radiologists TAT, modality TAT and
patient wait (routes/report_36.py), on known studies. 2026-11-19 the RIS steps (exam done,
scheduled, arrived) are looked up for the studies in range only instead of grouped over every
row ever recorded: these pin the figures so that change keeps them.
"""
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from werkzeug.datastructures import MultiDict

T = datetime(2026, 10, 5, 8, 0)
FORM = MultiDict({'start_date': '2026-10-05', 'end_date': '2026-10-05'})


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


def m(minutes):
    return T + timedelta(minutes=minutes)


def study(uid, day=T.date(), cls='O', location='OPD'):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae, study_date,
                study_modality, patient_class, patient_location, insert_time,
                rep_prelim_signed_by, rep_prelim_timestamp, rep_study_last_composed_by, rep_study_last_composed_ts)
           VALUES (:u, :u, :x, :a, 'CT36', :d, 'CT', :c, :l, :ins, 'res.one@ad', :pre, 'rad.two@ad', :fin)""",
        u=uid, x=f'1.2.36.{uid}', a=f'A36{uid}', d=day, c=cls, l=location,
        ins=m(0), pre=m(70), fin=m(190))
    sql("INSERT INTO std_pps (pps_key, study_instance_uid) VALUES (:k, :x)", k=3600 + uid, x=f'1.2.36.{uid}')


def ris_steps(uid):
    k = 3600 + uid
    for kind, table, minutes in (('exam_done_at', 'std_worklist_exam_done', (5, 10)),   # the last exam done
                                 ('scheduled_at', 'std_worklist_scheduled', (-60, -30)),  # the first scheduling
                                 ('arrived_at', 'std_worklist_arrivals', (-15, -5))):     # the first arrival
        for i, mins in enumerate(minutes):
            sql(f"INSERT INTO {table} (site_worklist_key, pps_key, {kind}) VALUES (:w, :k, :t)", w=k * 10 + i, k=k, t=m(mins))


@pytest.fixture
def studies():
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CT36', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    sql("""INSERT INTO pacs_user_role_override (login_id, role)
           VALUES ('RES.ONE', 'residents'), ('RAD.TWO', 'radiologists') ON CONFLICT DO NOTHING""")
    study(1); ris_steps(1)
    study(2, day=T.date() - timedelta(days=3)); ris_steps(2)          # another day: out
    study(3, cls='I')                                                 # no RIS steps: PACS clock only


def test_residents_and_radiologists_tat_from_the_last_exam_done_and_from_pacs(studies):
    from routes.report_36 import get_resident_radiologist_tat
    out = get_resident_radiologist_tat(FORM)
    res = {r['patient_class_bucket']: r for r in out['ris']['residents']}
    rad = {r['patient_class_bucket']: r for r in out['ris']['radiologists']}
    assert set(res) == {'Outpatient'} and res['Outpatient']['median_tat_h'] == 1.0      # 70 - 10 min
    assert rad['Outpatient']['median_tat_h'] == 3.0                                     # 190 - 10 min
    pacs_res = {r['patient_class_bucket']: r for r in out['pacs']['residents']}
    assert float(pacs_res['Outpatient']['median_tat_h']) == round(70 / 60, 2)           # from the PACS arrival
    assert pacs_res['Inpatient']['n'] == 1                                              # study 3: PACS only


def test_modality_tat_every_signer_together(studies):
    from routes.report_36 import get_modality_tat
    out = get_modality_tat(FORM)
    ris = {(r['modality'], r['patient_class_bucket']): r for r in out['ris']}
    assert ris == {('CT', 'Outpatient'): {'modality': 'CT', 'patient_class_bucket': 'Outpatient', 'n': 2,
                                          'avg_tat_h': 2.0, 'median_tat_h': 2.0}}        # 1 h and 3 h
    assert {(r['modality'], r['patient_class_bucket']) for r in out['pacs']} == {('CT', 'Outpatient'), ('CT', 'Inpatient')}


def test_patient_wait_from_the_first_scheduling_to_the_first_arrival(studies):
    from routes.report_36 import get_patient_wait_time
    rows = get_patient_wait_time(FORM)
    assert [(r['modality'], r['patient_class_bucket'], r['n'], float(r['median_wait_min'])) for r in rows] == \
           [('CT', 'Outpatient', 1, 45.0)]                                               # -60 to -15 min
