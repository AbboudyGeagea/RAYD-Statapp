"""
tests/test_pps_study_link.py — a RIS performed step linked to its PACS study
(ETL_JOBS/etl_ris_pps.run_pps_study_enrichment).

By study UID first. A device that writes its own study UID instead of the worklist's (SJH's
Canon CT, found 2026-11-17: none of its RIS steps had ever linked) is linked through the step's
order and its accession, but only when the accession names exactly one PACS study that is not a
structured report.
"""
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


def study(uid, accession, study_uid, modality='CT', ae='SJHCSAPWFMFIR'):
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, accession_number, study_instance_uid,
                                         storing_ae, study_date, study_modality)
           VALUES (:u, :u, :a, :x, :ae, '2026-11-10', :m)""", u=uid, a=accession, x=study_uid, ae=ae, m=modality)


def step(pps, study_uid, order, accession, table='std_worklist_arrivals', at_col='arrived_at'):
    sql("INSERT INTO std_pps (pps_key, study_instance_uid, performing_ae_title) VALUES (:p, :x, 'CM_CT_CMW_V1')",
        p=pps, x=study_uid)
    sql("""INSERT INTO etl_orders (order_dbid, accession_number, order_status, modality)
           VALUES (:o, :a, 'CM', 'CT') ON CONFLICT DO NOTHING""", o=order, a=accession)
    sql(f"INSERT INTO {table} (site_worklist_key, pps_key, {at_col}) VALUES (:o, :p, '2026-11-10 09:00')",
        o=order, p=pps)


def links():
    from db import db
    out = dict(db.session.execute(text("SELECT pps_key, study_db_uid FROM std_pps")).all())
    db.session.commit()
    return out


def run():
    from db import db
    from ETL_JOBS.etl_ris_pps import run_pps_study_enrichment
    return run_pps_study_enrichment(db.engine)


def test_a_step_is_linked_by_study_uid_and_by_accession_when_the_device_wrote_its_own_uid():
    study(1, 'ACC1', '1.2.840.113564.1')                                  # worklist UID kept
    step(10, '1.2.840.113564.1', 100, 'ACC1')
    study(2, 'ACC2', '1.2.392.200036.9116.2')                             # the CT's own UID
    step(20, '1.2.840.113564.9.1.ACC2', 200, 'ACC2', 'std_worklist_exam_done', 'exam_done_at')

    assert run() == 2
    assert links() == {10: 1, 20: 2}


def test_an_accession_naming_two_studies_is_left_alone_and_a_report_object_does_not_count():
    study(3, 'ACC3', '1.2.392.3a')
    study(4, 'ACC3', '1.2.392.3b')                                        # two image studies
    step(30, '1.2.840.113564.3', 300, 'ACC3')
    study(5, 'ACC5', '1.2.392.5')
    study(6, 'ACC5', '1.2.392.5sr', modality='SR')                        # its structured report
    step(50, '1.2.840.113564.5', 500, 'ACC5')

    run()

    assert links() == {30: None, 50: 5}


def test_a_step_already_linked_keeps_its_study():
    study(7, 'ACC7', '1.2.840.113564.7')
    study(8, 'ACC8', '1.2.392.8')
    step(70, '1.2.840.113564.7', 800, 'ACC8')       # its UID names study 7, its order names 8

    run()

    assert links() == {70: 7}
