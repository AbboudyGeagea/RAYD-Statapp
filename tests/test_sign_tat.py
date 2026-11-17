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


def test_report_32_shows_the_turnaround(client):
    six_studies()

    html = client.get('/report/32', headers=ADMIN, query_string={'start_date': DAY, 'end_date': DAY}).get_data(as_text=True)

    assert 'Signature Turnaround (PACS audit trail)' in html
    assert 'id="chart-sign-exam"' in html and 'id="chart-sign-arrival"' in html and 'id="chart-sign-open"' in html
    assert '3 signed of 4' in html
