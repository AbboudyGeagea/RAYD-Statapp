"""
tests/test_daily_briefing.py — LAUMC to-do item 22: the nightly Daily Briefing snapshot
(ETL_JOBS/daily_analytics.py, the Super Report's Daily Briefing card) counts as every report
counts: Rizk Hospital only, also when run by hand; structured reports, other objects and bone
densitometry left out; TAT timed to the report time (last composed, else PACS final).
The scheduler's own run through rh_scope() is covered in tests/test_rh_only_views.py.
"""
import json
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

YESTERDAY = date.today() - timedelta(days=1)


@pytest.fixture
def sql(app):
    from db import db
    with app.app_context():
        def run(statement, **params):
            db.session.execute(text(statement), params)
            db.session.commit()
        yield run
        db.session.rollback()
        run("DELETE FROM analytics_snapshots")


def sites():
    from db import db
    rows = db.session.execute(text("SELECT id, is_default FROM sites")).fetchall()
    return next(r[0] for r in rows if r[1]), next(r[0] for r in rows if not r[1])


def study(sql, uid, modality='CT', site=None, composed=None, final=None, day=YESTERDAY):
    arrived = datetime(day.year, day.month, day.day, 10, 0)
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date,
               study_modality, site_id, insert_time, rep_study_last_composed_ts, rep_final_timestamp)
           VALUES (:u, :u, 'NOT_MAPPED', :d, :m, :site, :t, :c, :f)""",
        u=uid, d=day, m=modality, site=site, t=arrived,
        c=arrived + timedelta(minutes=composed) if composed is not None else None,
        f=arrived + timedelta(minutes=final) if final is not None else None)


def collect():
    from db import db
    from ETL_JOBS.daily_analytics import _collect
    from utils.rh_scope import rh_scope
    with rh_scope():
        with db.engine.begin() as conn:
            return _collect(conn, YESTERDAY, YESTERDAY)


def test_structured_reports_and_other_objects_are_left_out(sql):
    study(sql, 1)
    study(sql, 2, modality='MR')
    study(sql, 3, modality='SR')
    study(sql, 4, modality='OT')
    study(sql, 5, modality='BMD')
    numbers = collect()
    assert numbers['total_studies'] == 2
    assert {m['name'] for m in numbers['modalities']} == {'CT', 'MR'}


def test_tat_is_timed_to_the_last_composition(sql):
    """The PACS final signature is blank on many LAUMC studies; the composition is not."""
    study(sql, 1, composed=60)                  # no final signature at all
    study(sql, 2, composed=120, final=500)      # composition first
    study(sql, 3, modality='SR', composed=5)    # not a study
    assert collect()['median_tat_min'] == 90.0


def test_a_run_by_hand_counts_rizk_hospital_only(sql, database_url):
    """python ETL_JOBS/daily_analytics.py builds its own engine, which the app never hooked."""
    rh, other = sites()
    study(sql, 1, site=rh)
    study(sql, 2)                               # site not resolved yet: shown, as in reports
    study(sql, 3, site=other)
    from ETL_JOBS.daily_analytics import run_rh_only
    engine = create_engine(database_url)
    try:
        run_rh_only(engine)
    finally:
        engine.dispose()
    from db import db
    data = db.session.execute(text("""SELECT data_json FROM analytics_snapshots
                                      WHERE period_label = 'last_30d'
                                      ORDER BY computed_at DESC LIMIT 1""")).scalar()
    data = data if isinstance(data, dict) else json.loads(data)
    assert data['total_studies'] == 2
