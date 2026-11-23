"""
tests/test_tat_bands.py — item 14, the hospital's July request (operator 2026-11-23: "let's
continue a,b,c and d"), utils/tat_bands.py:

  a. Report 36's "Time bands" tab: the wet read and the approval, each study once for its first
     event in the PACS audit trail, timed from RIS exam done and from the PACS arrival, in bands
     the hospital sets (settings, settings capability);
  b. Report 32's "Reports per Radiologist per Day": each study once, on the day of its first
     approval (or wet read), per modality;
  c. Report 25 without the four turnaround copies Report 36 shows.
"""
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

T = datetime(2026, 10, 5, 8, 0)
DAY = T.date().isoformat()
NEXT = (T.date() + timedelta(days=1)).isoformat()
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
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%' OR key LIKE 'tat_bands_%'")


def m(minutes):
    return T + timedelta(minutes=minutes)


def setup_site():
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
           VALUES ('CTX1', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('MRX1', 'MR', (SELECT id FROM sites WHERE is_default LIMIT 1)),
                  ('SRX1', 'SR', (SELECT id FROM sites WHERE is_default LIMIT 1))""")
    sql("""INSERT INTO pacs_user_role_override (login_id, role)
           VALUES ('RES.ONE', 'residents'), ('RAD.TWO', 'radiologists') ON CONFLICT DO NOTHING""")
    sql("INSERT INTO settings (key, value) VALUES ('pacs_audit_copied_from', '2026-10-01'), ('pacs_audit_copied_until', :u)",
        u=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def study(uid, arrived, ae='CTX1', location='ICU', klass='I'):
    # the report fields let Reports 25 and 32 build their own dataset over these studies
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae, study_date,
                study_modality, procedure_code, patient_class, patient_location, insert_time,
                rep_study_last_composed_by, rep_study_last_composed_ts, rep_final_timestamp, rep_final_signed_by,
                study_status)
           VALUES (:u, :u, :x, :a, :ae, :d, 'CT', 'CT01', :c, :l, :t, 'rad.two@ad', :r, :r, 'rad.two@ad', 'UNREAD')""",
        u=uid, x=f'1.2.9.{uid}', a=f'A{uid}', ae=ae, d=arrived.date(), t=arrived, c=klass, l=location,
        r=arrived + timedelta(hours=3))
    sql("INSERT INTO etl_patient_view (patient_db_uid, sex) VALUES (:u, 'M') ON CONFLICT DO NOTHING", u=uid)


def exam_done(uid, at):
    sql("INSERT INTO etl_orders (order_dbid, study_db_uid) VALUES (:o, :u)", o=5000 + uid, u=uid)
    sql("INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at) VALUES (:o, :t)", o=5000 + uid, t=at)


_ids = iter(range(1, 100_000))


def event(uid, at, kind, login):
    sql("""INSERT INTO etl_pacs_audit_events (log_dbid, event_at, kind, user_name, study_instance_uid)
           VALUES (:i, :t, :k, :l, :x)""", i=next(_ids), t=at, k=kind, l=login, x=f'1.2.9.{uid}')


def studies():
    setup_site()
    # 1: exam done, wet read, approved (twice in the audit: the first counts)
    study(1, m(0))
    exam_done(1, m(-10))
    event(1, m(32), 'wetread', 'res.one@ad')
    event(1, m(150), 'approve', 'rad.two@ad')
    event(1, m(150), 'approve', 'rad.two@ad')
    event(1, m(24 * 60 + 30), 'approve', 'rad.two@ad')    # approved again the next day
    # 2: an ER MR study with no exam done, approved only
    study(2, m(60), ae='MRX1', location='ER', klass='E')
    event(2, m(360), 'approve', 'rad.two@ad')
    # 3: a structured report
    study(3, m(0), ae='SRX1')
    event(3, m(10), 'approve', 'rad.two@ad')
    # 4: arrived before the audit copy starts (out of the bands, its approval day still counts)
    study(4, datetime(2026, 9, 30, 23, 0))
    event(4, m(10), 'approve', 'rad.two@ad')
    # 5: not read yet
    study(5, m(0))
    # 6: exam done entered after the approval
    study(6, m(0))
    exam_done(6, m(200))
    event(6, m(100), 'approve', 'rad.two@ad')
    # 7: approved the next day, first time
    study(7, m(0))
    event(7, m(24 * 60 + 60), 'approve', 'rad.two@ad')


# ── the bands ────────────────────────────────────────────────────────────────

def test_the_limits_are_hours_going_up():
    from utils.tat_bands import clean_limits
    assert clean_limits([3, 5, '12', 24.0]) == [3, 5, 12, 24]
    assert clean_limits([0.5, 1]) == [0.5, 1]
    for bad in ([], [5, 3], [3, 3], [0, 4], [800], list(range(1, 10)), ['x'], '3,5', None):
        with pytest.raises(ValueError):
            clean_limits(bad)


def test_each_minute_falls_in_one_band_the_upper_limit_included():
    from utils.tat_bands import band_counts
    assert band_counts([0, 180, 181, 300, 301, 2000, None], [3, 5]) == [2, 2, 2]


def test_the_bands_everyone_starts_with_are_the_settings_else_the_targets(client):
    from utils import tat_bands
    assert tat_bands.load_bands() == {'wetread': [3, 5, 12, 24], 'approve': [6, 24, 48]}
    sql("INSERT INTO settings (key, value) VALUES ('tat_bands_approve', '[4, 8]'), ('tat_bands_wetread', 'nonsense')")
    assert tat_bands.load_bands() == {'wetread': [3, 5, 12, 24], 'approve': [4, 8]}


# ── a. Report 36's Time bands ────────────────────────────────────────────────

def test_each_study_once_for_its_first_wet_read_and_approval_on_both_clocks():
    from utils import tat_bands
    studies()

    out = tat_bands.bands(DAY, DAY)

    assert out['coverage']['from'] == '2026-10-01'
    assert out['studies'] == 5                           # 1, 2, 5, 6, 7: no SR, nothing before the copy
    people = [p['name'] for p in out['people']]
    mods, classes = out['modalities'], out['classes']
    named = lambda rows: sorted(((people[p], mods[mo], classes[c], ris, pacs) for p, mo, c, ris, pacs in rows), key=repr)
    assert named(out['wetread']) == [('Res One', 'CT', 'Inpatient', 42, 32)]
    assert named(out['approve']) == sorted([
        ('Rad Two', 'CT', 'Inpatient', None, 100),       # 6: exam done after the approval, RIS clock out
        ('Rad Two', 'CT', 'Inpatient', None, 1500),      # 7: approved the next day, no exam done
        ('Rad Two', 'CT', 'Inpatient', 160, 150),        # 1: the first approval, not the next day's
        ('Rad Two', 'MR', 'ER', None, 300),              # 2: ER by its location
    ], key=repr)
    assert {p['name']: p['role'] for p in out['people']} == {'Res One': 'Resident', 'Rad Two': 'Radiologist'}
    assert out['wet_studies'] == 1 and out['approved_studies'] == 4


def test_a_device_mapped_twice_in_another_letter_case_counts_its_study_once():
    from utils import tat_bands
    studies()
    sql("INSERT INTO aetitle_modality_map (aetitle, modality, site_id) VALUES ('ctx1', 'CT', (SELECT id FROM sites WHERE is_default LIMIT 1))")

    out = tat_bands.bands(DAY, DAY)

    assert out['studies'] == 5 and out['approved_studies'] == 4


def test_without_the_audit_copy_the_tab_says_so():
    from utils import tat_bands
    studies()
    sql("DELETE FROM settings WHERE key LIKE 'pacs_audit_%'")
    assert tat_bands.bands(DAY, DAY)['coverage'] is None


def test_report_36_has_the_time_bands_tab(client):
    studies()
    q = {'start_date': DAY, 'end_date': DAY}

    page = client.get('/report/36', headers=ADMIN, query_string=q).get_data(as_text=True)
    part = client.get('/report/36/part/bands', headers=ADMIN, query_string=q)

    assert 'data-tab="bands"' in page and 'data-part-template="_report_36_bands.html"' in page
    html = part.get_data(as_text=True)
    assert part.status_code == 200, html[:400]
    assert 'Wet read: turnaround in time bands' in html and 'Approval: turnaround in time bands' in html
    assert 'RIS exam done' in html and 'Images reach PACS' in html
    assert 'Person, modality and patient type' in html
    assert 'Save for everyone' in html                   # an administrator holds the settings capability
    assert '"approve": [6, 24, 48]' in html or '"approve":[6,24,48]' in html.replace(' ', '')
    for key in ('r36-bands-clock', 'r36-bands-wet', 'r36-bands-approve', 'r36-bands-edit'):
        assert f'data-explain="{key}"' in html


def test_saving_the_bands_needs_the_settings_capability(client, monkeypatch):
    from utils import tat_bands

    ok = client.post('/report/36/bands', headers=ADMIN, json={'stage': 'wetread', 'limits': [2, 4, 8]})
    bad = client.post('/report/36/bands', headers=ADMIN, json={'stage': 'wetread', 'limits': [8, 4]})
    assert ok.status_code == 200 and ok.get_json()['limits'] == [2, 4, 8]
    assert bad.status_code == 400 and 'go up' in bad.get_json()['error']
    assert tat_bands.load_bands()['wetread'] == [2, 4, 8]

    import utils.roles
    real = utils.roles.can
    monkeypatch.setattr(utils.roles, 'can', lambda user, cap: False if cap == 'settings' else real(user, cap))
    refused = client.post('/report/36/bands', headers=ADMIN, json={'stage': 'approve', 'limits': [1]})
    part = client.get('/report/36/part/bands', headers=ADMIN, query_string={'start_date': DAY, 'end_date': DAY})
    assert refused.status_code == 403 and tat_bands.load_bands()['approve'] == [6, 24, 48]
    assert 'Save for everyone' not in part.get_data(as_text=True)


# ── b. Report 32's reports per radiologist per day ───────────────────────────

def test_each_study_counts_once_on_the_day_of_its_first_approval():
    from utils import tat_bands
    studies()

    out = tat_bands.per_day('2026-09-01', NEXT)

    assert out['from'] == '2026-10-01' and out['to'] == NEXT       # the copy starts on 1 Oct
    assert out['days'][0] == '2026-10-01' and out['days'][-1] == NEXT and len(out['days']) == 6
    people = [p['name'] for p in out['people']]
    got = sorted((tat_bands.KINDS[k], people[p], out['days'][d], out['modalities'][mo], n)
                 for k, p, d, mo, n in out['rows'])
    assert got == [
        ('approve', 'Rad Two', DAY, 'CT', 3),     # 1, 4 and 6 (3 is a structured report)
        ('approve', 'Rad Two', DAY, 'MR', 1),     # 2
        ('approve', 'Rad Two', NEXT, 'CT', 1),    # 7; 1's second approval is not counted again
        ('wetread', 'Res One', DAY, 'CT', 1),
    ]


def test_the_report_32_filters_apply_to_the_counts():
    from routes.report_32 import get_report_config
    from utils import tat_bands
    studies()
    _b, start, end, params, _x, sec_filters, _j = get_report_config(
        {'start_date': DAY, 'end_date': DAY, 'f_mod_active': 'on', 'f_mod': 'MR'})

    out = tat_bands.per_day(start, end, params, sec_filters)

    assert [(tat_bands.KINDS[k], out['modalities'][mo], n) for k, _p, _d, mo, n in out['rows']] == [('approve', 'MR', 1)]


def test_report_32_shows_the_reports_per_radiologist_per_day(client):
    studies()

    html = client.get('/report/32', headers=ADMIN, query_string={'start_date': DAY, 'end_date': NEXT}).get_data(as_text=True)

    assert 'Reports per Radiologist per Day' in html and 'id="r32-day-card"' in html
    assert 'data-explain="r32-per-day"' in html
    assert 'Approved reports' in html and 'Wet reads' in html and 'All modalities' in html
    assert '05/10/2026 to 06/10/2026' in html


# ── c. Report 25 without the copies ──────────────────────────────────────────

def test_report_25_leaves_the_turnaround_copies_to_report_36(client):
    import routes.report_25 as r25
    from routes import report_cache
    assert not hasattr(r25, 'get_tat_pacs_insert_time') and not hasattr(r25, 'get_tat_ris_exam_done')
    report_cache._store.clear()
    setup_site()
    today = date.today()
    study(99, datetime.combine(today, datetime.min.time()) + timedelta(hours=10))   # Report 25's dataset: today

    html = client.get('/report/25', headers=ADMIN, query_string={'start_date': today.isoformat(), 'end_date': today.isoformat(),
                                                                  'active_tab': 'tat-anchor'}).get_data(as_text=True)

    assert 'TAT: PACS vs RIS' not in html and 'id="tab-tat-anchor"' not in html
    for gone in ('id="chart-1"', 'id="chart-mod-tat"', 'id="chart-tat-hist"', 'data-explain="r25-class-tat"'):
        assert gone not in html
    for what in ('TAT per Patient Class', 'Avg TAT by Modality', 'TAT Distribution'):
        assert f'{what}: now on Report 36' in html
    assert '/report/36?start_date=' in html
    # what stays on Report 25: the device and radiologist charts not on Report 36
    for kept in ('id="chart-ae-tat"', 'id="chart-rvu-tat"', 'Duration vs. TAT Correlation', 'TAT Outlier Detection'):
        assert kept in html
    assert "switchTab('tat-anchor')" not in html and 'active_tab" id="rfp-hidden-active_tab" value="ops"' in html
