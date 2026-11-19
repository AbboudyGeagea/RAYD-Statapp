"""
tests/test_sign_tat.py — radiologist signature turnaround from the PACS audit trail (items 14A
and 14B, utils/sign_tat.py, Report 32).

The signature is the first approval in the audit; four clocks end there: RIS exam done,
images reach PACS, the first reader's open, the signer's own first open.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

pd = pytest.importorskip('pandas')

T = datetime(2026, 10, 5, 8, 0)
DAY = T.date().isoformat()
ADMIN = {'X-Test-Role': 'administrator'}


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        clean()
        yield
        db.session.rollback()
        clean()


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def clean():
    sql("DELETE FROM etl_pacs_audit_events")
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")


def m(minutes):
    return T + timedelta(minutes=minutes)


def setup_site(covered_from=date(2026, 10, 1)):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CTX1', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('SRX1', 'SR', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    sql("""INSERT INTO pacs_user_role_override (login_id, role)
           VALUES ('RES.ONE', 'residents'), ('RAD.TWO', 'radiologists') ON CONFLICT DO NOTHING""")
    sql("""INSERT INTO settings (key, value) VALUES ('pacs_audit_copied_from', :f), ('pacs_audit_copied_until', :u)""",
        f=covered_from.isoformat(), u=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def study(uid, arrived, ae='CTX1'):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae, study_date,
                study_modality, procedure_code, patient_class, patient_location, insert_time,
                rep_study_last_composed_by, rep_study_last_composed_ts, rep_final_timestamp, rep_final_signed_by)
           VALUES (:u, :u, :x, :a, :ae, :d, 'CT', 'CT01', 'I', 'ICU', :t, 'rad.two@ad', :c, :c, 'rad.two@ad')""",
        u=uid, x=f'1.2.9.{uid}', a=f'A{uid}', ae=ae, d=T.date(), t=arrived, c=arrived + timedelta(hours=3))
    sql("INSERT INTO etl_patient_view (patient_db_uid, sex) VALUES (:u, 'M') ON CONFLICT DO NOTHING", u=uid)


def exam_done(uid, at):
    sql("INSERT INTO etl_orders (order_dbid, study_db_uid) VALUES (:o, :u)", o=5000 + uid, u=uid)
    sql("INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at) VALUES (:o, :t)", o=5000 + uid, t=at)


_ids = iter(range(1, 100_000))


def event(uid, at, kind, login):
    sql("""INSERT INTO etl_pacs_audit_events (log_dbid, event_at, kind, user_name, study_instance_uid)
           VALUES (:i, :t, :k, :l, :x)""", i=next(_ids), t=at, k=kind, l=login, x=f'1.2.9.{uid}')


def six_studies():
    setup_site()
    # 1: the whole chain
    study(1, m(0))
    exam_done(1, m(-10))
    event(1, m(30), 'open', 'res.one@ad')
    event(1, m(32), 'wetread', 'res.one@ad')
    event(1, m(120), 'lock', 'rad.two@ad')
    event(1, m(150), 'approve', 'rad.two@ad')
    event(1, m(150), 'approve', 'rad.two@ad')          # ApprovedSign Complete, same second
    # 2: no exam done, nobody's open copied
    study(2, m(60))
    event(2, m(360), 'approve', 'rad.two@ad')
    # 3: a structured report
    study(3, m(0), ae='SRX1')
    event(3, m(10), 'approve', 'rad.two@ad')
    # 4: arrived before the audit copy starts
    study(4, datetime(2026, 9, 30, 23, 0))
    event(4, m(10), 'approve', 'rad.two@ad')
    # 5: not signed yet
    study(5, m(0))
    # 6: exam done entered after the approval; a technologist's open first
    study(6, m(0))
    exam_done(6, m(200))
    event(6, m(5), 'open', 'tech.one@ad')
    event(6, m(100), 'approve', 'rad.two@ad')


def build():
    from utils import sign_tat
    return sign_tat.build(DAY, DAY)


def test_the_four_clocks_end_at_the_radiologists_approval():
    six_studies()

    out = build()

    assert out['studies'] == 4 and out['signed'] == 3          # SR and the pre-copy study out
    o = out['overall']
    assert o['exam_to_sign'] == {'n': 1, 'median_h': round(160 / 60, 2), 'p90_h': round(160 / 60, 2)}
    assert o['arrival_to_sign']['n'] == 3 and o['arrival_to_sign']['median_h'] == 2.5   # 150, 300, 100 min
    assert o['open_to_sign'] == {'n': 1, 'median_h': 2.0, 'p90_h': 2.0}                # from the resident's open
    assert o['signer_open_to_sign'] == {'n': 1, 'median_h': 0.5, 'p90_h': 0.5}


def test_figures_per_signing_radiologist():
    six_studies()

    rad = build()['by_rad']

    assert [(r['name'], r['login'], r['signed']) for r in rad] == [('Rad Two', 'rad.two@ad', 3)]
    assert rad[0]['arrival_to_sign']['median_h'] == 2.5


def test_nothing_without_the_audit_copy():
    six_studies()
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")

    out = build()

    assert out['coverage'] is None and out['signed'] == 0 and out['by_rad'] == []


# ── Report 36: the radiologists' reading TAT and the three-clock comparison ────

def reading_studies():
    """six_studies (1: wet read at 32, the approver's lock at 120, approval at 150 -> 30 min;
    2 and 6: approved, never opened by the approver) and five more."""
    six_studies()
    study(7, m(0))                                     # no wet read: the approver's first open
    event(7, m(40), 'open', 'rad.two@ad')
    event(7, m(110), 'approve', 'rad.two@ad')          # 70 min
    study(8, m(0))                                     # opened before the wet read, again after
    event(8, m(20), 'open', 'rad.two@ad')
    event(8, m(50), 'wetread', 'res.one@ad')
    event(8, m(200), 'open', 'rad.two@ad')
    event(8, m(500), 'approve', 'rad.two@ad')          # 300 min, from the open after the wet read
    study(9, m(0))
    event(9, m(10), 'wetread', 'res.one@ad')
    event(9, m(30), 'lock', 'rad.two@ad')
    event(9, m(430), 'approve', 'rad.two@ad')          # 400 min
    study(10, m(0))                                    # opened only before the wet read
    event(10, m(20), 'open', 'rad.two@ad')
    event(10, m(60), 'wetread', 'res.one@ad')
    event(10, m(90), 'approve', 'rad.two@ad')          # not measured
    study(11, m(0))                                    # a wet read after the approval: none
    event(11, m(40), 'open', 'rad.two@ad')
    event(11, m(50), 'approve', 'rad.two@ad')          # 10 min
    event(11, m(60), 'wetread', 'res.one@ad')


def reading():
    from utils import sign_tat
    return sign_tat.reading(DAY, DAY)


def test_reading_tat_runs_from_the_approvers_first_open_after_the_wet_read():
    reading_studies()

    out = reading()

    assert (out['approved'], out['not_measured']) == (8, 3)          # 2, 6 and 10 not measured
    k = out['kpi']['all']
    assert (k['n'], k['studies'], k['median_h'], k['p90_h']) == (5, 8, round(70 / 60, 2), round(400 / 60, 2))
    assert [(b['label'], b['n'], b['pct']) for b in k['bands']] == \
           [('≤ 1 h', 2, 40.0), ('1–3 h', 1, 20.0), ('3–5 h', 1, 20.0), ('> 5 h', 1, 20.0)]   # 300 min is 3–5 h
    wet, dry = out['kpi']['wet'], out['kpi']['no_wet']
    assert (wet['studies'], wet['n'], wet['median_h']) == (4, 3, 5.0)       # 30, 300, 400 min
    assert (dry['studies'], dry['n'], dry['median_h']) == (4, 2, round(40 / 60, 2))   # 70 and 10 min


def test_reading_tat_per_radiologist_and_modality():
    reading_studies()

    out = reading()

    assert [(r['name'], r['studies'], r['n']) for r in out['by_rad']] == [('Rad Two', 8, 5)]
    assert [(r['modality'], r['studies']) for r in out['by_modality']] == [('CT', 8)]


def test_the_three_clocks_end_at_the_same_approval():
    reading_studies()

    out = reading()

    assert [c['key'] for c in out['clocks']] == ['exam_to_sign', 'arrival_to_sign', 'reading_to_sign']
    c = out['compare']
    assert c['exam_to_sign']['n'] == 1 and c['exam_to_sign']['median_h'] == round(160 / 60, 2)
    assert c['arrival_to_sign']['n'] == 8 and c['reading_to_sign']['n'] == 5
    common = out['common']                                          # only study 1 has all three
    assert common['n'] == 1 and common['reading_to_sign']['median_h'] == 0.5
    assert [r['patient_class'] for r in out['compare_by_class']] == ['Inpatient']
    assert out['compare_by_rad'][0]['arrival_to_sign']['n'] == 8


def test_reading_nothing_without_the_audit_copy():
    reading_studies()
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")

    out = reading()

    assert out['coverage'] is None and out['approved'] == 0 and out['kpi'] == {}


def test_report_36_shows_the_reading_kpi_and_the_comparison_tab(client):
    reading_studies()

    r = client.get('/report/36', headers=ADMIN, query_string={'start_date': DAY, 'end_date': DAY})
    html = r.get_data(as_text=True)

    assert r.status_code == 200, html[:400]
    assert 'Radiologist Reading TAT (KPI)' in html and 'Measured, of 8 approved' in html
    assert 'With a wet read' in html and 'Without a wet read' in html
    assert 'id="r36-tab-compare"' in html and 'TAT comparison: RIS · PACS · reading' in html
    assert 'RIS exam done → approval' in html and 'PACS arrival → approval' in html
    assert 'First open after the wet read → approval' in html
    for key in ('r36-reading-kpi', 'r36-reading-bands', 'r36-cmp-exam', 'r36-cmp-arrival', 'r36-cmp-reading',
                'r36-cmp-modality', 'r36-cmp-class', 'r36-cmp-rad'):
        assert f'data-explain="{key}"' in html


def test_report_32_shows_the_turnaround(client):
    six_studies()

    html = client.get('/report/32', headers=ADMIN, query_string={'start_date': DAY, 'end_date': DAY}).get_data(as_text=True)

    assert 'Signature Turnaround (PACS audit trail)' in html
    assert 'id="chart-sign-exam"' in html and 'id="chart-sign-arrival"' in html and 'id="chart-sign-open"' in html
    assert '3 signed of 4' in html
