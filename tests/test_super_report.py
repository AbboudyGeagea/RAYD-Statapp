"""
tests/test_super_report.py — the Super Report's route (routes/super_report.py): its dates, the
ER-delayed count, the comparison window in the narrative, its caches and settings (review of
2026-11-20).
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

ADMIN = {'X-Test-Role': 'administrator'}
D1 = date(2026, 10, 5)


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def device(ae, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           SELECT :ae, :m, id FROM sites WHERE code = :s""", ae=ae, m=modality, s=site)


_uid = iter(range(70_000, 90_000))


def study(day, ae, modality, patient_class='O', tat_min=10):
    uid = next(_uid)
    arrived = datetime.combine(day, datetime.min.time()) + timedelta(hours=8)
    sql("""INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, storing_ae, study_date, study_modality,
                                         patient_class, insert_time, rep_study_last_composed_ts, study_has_report)
           VALUES (:u, :u, :ae, :d, :m, :pc, :t, :c, TRUE)""",
        u=uid, ae=ae, d=day, m=modality, pc=patient_class, t=arrived, c=arrived + timedelta(minutes=tat_min))
    return uid


def site_id(code):
    from db import db
    return db.session.execute(text("SELECT id FROM sites WHERE code = :c"), {'c': code}).scalar()


# ── 1.3 dates ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('query', [
    'start=banana&end=2026-10-11',
    'start=2026-10-05&end=2026-13-40',
    'start=2026-01-10&end=2026-01-01',                                  # end before start
    'start=2026-10-05&end=2026-10-11&cmp_start=2026-09-30&cmp_end=2026-09-01',
    'start=2026-10-05&end=2026-10-11&cmp_start=nope&cmp_end=2026-09-30',
])
def test_a_date_that_does_not_make_sense_is_refused(client, query):
    r = client.get(f'/viewer/super-report?{query}', headers=ADMIN)
    assert r.status_code == 400 and r.get_json()['error']


# ── 1.5 the narrative names the comparison window ────────────────────────────

def test_the_narrative_names_the_comparison_window_not_the_current_one(client, app):
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        for d in (D1, D1 + timedelta(days=1)):
            study(d, 'CTX1', 'CT')
        study(date(2026, 9, 10), 'CTX1', 'CT')
    r = client.get('/viewer/super-report?start=2026-10-05&end=2026-10-11'
                   '&cmp_start=2026-09-01&cmp_end=2026-09-30', headers=ADMIN).get_json()
    overview = next(s for s in r['narrative'] if s['title'] == 'Overview')['bullets'][0]
    assert 'vs prior 30-day period' in overview, overview
    assert (r['delta_days'], r['cmp_days']) == (7, 30)
    # the automatic window: the same length, ending the day before start
    r = client.get('/viewer/super-report?start=2026-10-05&end=2026-10-11', headers=ADMIN).get_json()
    assert (r['cmp_start'], r['cmp_end'], r['cmp_days']) == ('2026-09-28', '2026-10-04', 7)


# ── 1.2 ER-delayed counts only days with ER volume in the page's own selection ─

def test_er_delayed_counts_only_er_days_of_the_selected_site_and_modality(app):
    from routes.super_report import _collect_data
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        device('MRX1', 'MR', 'RH')
        device('CTS1', 'CT', 'SJH')
        d1, d2, d3 = D1, D1 + timedelta(days=1), D1 + timedelta(days=2)
        study(d1, 'CTX1', 'CT', 'E')          # ER CT on RH: a day that counts
        study(d2, 'MRX1', 'MR', 'E')          # ER, but MR
        study(d3, 'CTS1', 'CT', 'E')          # ER, but SJH
        for d in (d1, d2, d3):                # each day: three quick non-ER CT on RH, one slow
            for _ in range(3):
                study(d, 'CTX1', 'CT', 'O', tat_min=10)
            study(d, 'CTX1', 'CT', 'O', tat_min=600)
        data = _collect_data(d1.isoformat(), d3.isoformat(), {'modality': ['CT']}, site_id('RH'))
        assert data['ae_ops']['er_delayed'] == 1        # the slow study of d1 only (was 3)
        assert data['tat']['er_reported_count'] == 1


def test_the_patient_class_lists_are_bound_parameters(app):
    """A class name with a quote in it is matched as data, not spliced into the SQL."""
    from routes.super_report import _collect_data
    with app.app_context():
        device('CTX1', 'CT', 'RH')
        sql("INSERT INTO settings (key, value) VALUES ('pc_emergency', 'E,O''R') "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
        study(D1, 'CTX1', 'CT', "O'R")
        study(D1, 'CTX1', 'CT', 'O')
        data = _collect_data(D1.isoformat(), D1.isoformat(), {}, site_id('RH'))
        assert data['tat']['er_reported_count'] == 1 and data['tat']['non_er_reported_count'] == 1
    import inspect
    from routes import super_report
    assert "replace(\"'\"" not in inspect.getsource(super_report) and 'chr(39)' not in inspect.getsource(super_report)
