"""
tests/test_rh_only_purge.py
────────────────────────────────────────────────────────────────
Operator rule, 2026-11-05: SJH appears in nothing, RH only. ETL_JOBS/etl_rh_only.py removes
SJH rows after each load, and what it must NOT remove matters as much: RH mammography that
double reading labels as SJH, the router counted aside, RH orders, and the clinical record
of a critical result that was already sent.
"""
import pytest
from sqlalchemy import text

from ETL_JOBS.etl_rh_only import (purge_sjh_orders, purge_sjh_ris_detail, purge_sjh_studies,
                                  run_rh_only_purge)

DAY = '2026-10-15'
ROUTER = 'LAUMCWFM1AR'


@pytest.fixture(autouse=True)
def ctx(app):
    from db import db
    with app.app_context():
        yield
        db.session.rollback()


@pytest.fixture
def engine():
    from db import db
    return db.engine


def sql(statement, **params):
    from db import db
    db.session.execute(text(statement), params)
    db.session.commit()


def values(statement, **params):
    from db import db
    rows = db.session.execute(text(statement), params).scalars().all()
    db.session.commit()
    return rows


def site_id(code):
    return values("SELECT id FROM sites WHERE code = :c", c=code)[0]


def device(aetitle, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id) VALUES (:a, :m, :s)
           ON CONFLICT (aetitle) DO UPDATE SET modality = EXCLUDED.modality, site_id = EXCLUDED.site_id""",
        a=aetitle, m=modality, s=site)


def study(uid, ae, label, modality='CT', accession=None):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, pacs_site_id_raw)
           VALUES (:u, :u, :iuid, :a, :ae, :d, :m, :label)""",
        u=uid, iuid=f'1.2.3.{uid}', a=accession or f'A{uid}', ae=ae, d=DAY, m=modality, label=label)


def order(key, issuer, accession, study_uid=None):
    sql("""INSERT INTO etl_orders (order_dbid, order_control, accession_number, study_db_uid, order_status, modality)
           VALUES (:k, :i, :a, :s, 'CM', 'CT')""", k=key, i=issuer, a=accession, s=study_uid)


def study_uids():
    return set(values("SELECT study_db_uid FROM etl_didb_studies"))


@pytest.fixture
def devices():
    rh, sjh = site_id('RH'), site_id('SJH')
    device('RH_SCANNER', 'CT', rh)
    device('SENO1', 'MG', rh)
    device('SJH_SCANNER', 'CT', sjh)
    device('GATEWAY_T', 'PACS', rh)
    device(ROUTER, 'PACS', None)


# ── studies ───────────────────────────────────────────────────────────────────

def test_only_sjh_studies_go(engine, devices):
    study(1, 'RH_SCANNER', '0')
    study(2, 'SENO1', '1', modality='MG')     # RH, double read: must stay
    study(3, 'SJH_SCANNER', '1')
    study(4, 'SJH_SCANNER', '0')              # the device decides, not the label
    study(5, 'GATEWAY_T', '0')
    study(6, 'GATEWAY_T', '1')                # a gateway's map says nothing: the label decides
    study(7, 'UNMAPPED_AE', '0')
    study(8, 'UNMAPPED_AE', '1')
    study(9, ROUTER, '2', modality='SR')      # counted aside: must stay
    assert purge_sjh_studies(engine) == 4
    assert study_uids() == {1, 2, 5, 7, 9}


def test_a_large_purge_finishes_across_batches(engine, devices, monkeypatch):
    from ETL_JOBS import etl_rh_only
    monkeypatch.setattr(etl_rh_only, '_BATCH', 2)
    for uid in range(1, 6):
        study(uid, 'SJH_SCANNER', '1')
    assert purge_sjh_studies(engine) == 5
    assert study_uids() == set()


def test_children_go_with_the_study_and_links_are_cleared(engine, devices):
    study(1, 'SJH_SCANNER', '1')
    sql("INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid) VALUES (10, 1)")
    sql("INSERT INTO std_pps (pps_key, study_db_uid, study_instance_uid) VALUES (20, 1, '1.2.3.1')")
    order(30, 'SAP_PROD', 'A1', study_uid=1)
    purge_sjh_studies(engine)
    assert values("SELECT COUNT(*) FROM etl_didb_serieses") == [0]
    assert values("SELECT study_db_uid FROM std_pps WHERE pps_key = 20") == [None]
    assert values("SELECT has_study FROM etl_orders WHERE order_dbid = 30") == [False]


def test_a_patient_goes_only_with_their_last_study(engine, devices):
    sql("INSERT INTO etl_patient_view (patient_db_uid) VALUES (71), (72)")
    study(1, 'SJH_SCANNER', '1')
    study(2, 'RH_SCANNER', '0')
    study(3, 'SJH_SCANNER', '1')
    sql("UPDATE etl_didb_studies SET patient_db_uid = 71 WHERE study_db_uid = 1")   # SJH only
    sql("UPDATE etl_didb_studies SET patient_db_uid = 72 WHERE study_db_uid IN (2, 3)")  # both
    purge_sjh_studies(engine)
    assert values("SELECT patient_db_uid FROM etl_patient_view") == [72]


def test_every_table_pointing_at_studies_is_handled(engine):
    """A new foreign key with no cascade would stop the purge on production, on real rows."""
    blocking = set(values("""
        SELECT c.conrelid::regclass::text
        FROM pg_constraint c
        WHERE c.confrelid = 'etl_didb_studies'::regclass AND c.contype = 'f' AND c.confdeltype <> 'c'
    """))
    assert blocking == {'std_pps'}


def test_nothing_goes_when_the_map_has_no_rh_device(engine):
    """An empty map would make every double-read mammogram look like SJH."""
    sql("UPDATE aetitle_modality_map SET site_id = NULL")
    study(1, 'SENO1', '1', modality='MG')
    assert purge_sjh_studies(engine) == 0
    assert study_uids() == {1}


def test_running_it_twice_changes_nothing_more(engine, devices):
    study(1, 'SJH_SCANNER', '1')
    study(2, 'RH_SCANNER', '0')
    assert purge_sjh_studies(engine) == 1
    assert purge_sjh_studies(engine) == 0
    assert study_uids() == {2}


# ── orders ────────────────────────────────────────────────────────────────────

def test_sjh_orders_go_and_are_remembered(engine, devices):
    order(1, 'SAP_PROD', 'RH1')
    order(2, 'SAP_SJH', 'SJ1')
    assert purge_sjh_orders(engine) == 1
    assert values("SELECT order_dbid FROM etl_orders") == [1]
    assert values("SELECT accession_number FROM etl_excluded_orders WHERE order_dbid = 2") == ['SJ1']


def test_an_order_with_no_issuer_stays(engine, devices):
    order(1, None, 'X1')
    purge_sjh_orders(engine)
    assert values("SELECT order_dbid FROM etl_orders") == [1]


# ── RIS detail and reports ────────────────────────────────────────────────────

@pytest.fixture
def an_order_at_each_site(devices):
    order(1, 'SAP_PROD', 'RH1')
    order(2, 'SAP_SJH', 'SJ1')
    for pps, key in ((100, 1), (200, 2)):
        sql("INSERT INTO std_pps (pps_key, study_instance_uid) VALUES (:p, :u)", p=pps, u=f'9.{pps}')
        sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
               VALUES (:k, :p, 'S', NOW())""", k=key, p=pps)
        sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at)
               VALUES (:k, :p, NOW())""", k=key, p=pps)
    sql("""INSERT INTO hl7_oru_reports (id, accession_number, report_text)
           VALUES (1, 'RH1', 'rh'), (2, 'SJ1', 'sjh')""")


def test_detail_of_sjh_orders_goes_and_rh_detail_stays(engine, an_order_at_each_site):
    run_rh_only_purge(engine)
    assert values("SELECT site_worklist_key FROM std_worklist_arrivals") == [1]
    assert values("SELECT site_worklist_key FROM std_worklist_exam_done") == [1]
    assert values("SELECT pps_key FROM std_pps") == [100]
    assert values("SELECT id FROM hl7_oru_reports") == [1]


def test_detail_loaded_on_a_later_night_still_goes(engine, an_order_at_each_site):
    """The order is long gone by then, which is why its key is kept."""
    run_rh_only_purge(engine)
    sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
           VALUES (2, 200, 'S', NOW())""")
    purge_sjh_ris_detail(engine)
    assert values("SELECT site_worklist_key FROM std_worklist_arrivals") == [1]


def test_pps_on_an_sjh_device_goes_with_no_worklist_row(engine, devices):
    sql("""INSERT INTO std_pps (pps_key, study_instance_uid, performing_ae_title)
           VALUES (1, '9.1', 'SJH_SCANNER'), (2, '9.2', 'RH_SCANNER'), (3, '9.3', 'SENO1')""")
    purge_sjh_ris_detail(engine)
    assert set(values("SELECT pps_key FROM std_pps")) == {2, 3}


def test_a_report_an_rh_order_also_claims_stays(engine, devices):
    order(1, 'SAP_PROD', 'BOTH')
    order(2, 'SAP_SJH', 'BOTH')
    sql("INSERT INTO hl7_oru_reports (id, accession_number, report_text) VALUES (1, 'BOTH', 'x')")
    run_rh_only_purge(engine)
    assert values("SELECT id FROM hl7_oru_reports") == [1]


def test_a_report_with_a_critical_result_sent_stays(engine, devices):
    """crn_notifications requires its report, and it is the record of a message sent."""
    order(2, 'SAP_SJH', 'SJ1')
    sql("INSERT INTO hl7_oru_reports (id, accession_number, report_text) VALUES (2, 'SJ1', 'x')")
    cols = values("""SELECT column_name FROM information_schema.columns
                     WHERE table_name = 'crn_notifications' AND is_nullable = 'NO'
                       AND column_default IS NULL AND column_name <> 'report_id'""")
    names = ', '.join(cols)
    binds = ', '.join(f"'x{i}'" for i, _ in enumerate(cols))
    from db import db
    try:
        db.session.execute(text(f"INSERT INTO crn_notifications (report_id, {names}) VALUES (2, {binds})"))
        db.session.commit()
    except Exception:
        db.session.rollback()
        pytest.skip('crn_notifications needs typed values this generic insert cannot give')
    run_rh_only_purge(engine)
    assert values("SELECT id FROM hl7_oru_reports") == [2]


def test_analysis_of_a_removed_report_goes_with_it(engine, an_order_at_each_site):
    sql("INSERT INTO hl7_oru_analysis (report_id, affirmed_labels, is_critical) VALUES (2, '{}', FALSE)")
    run_rh_only_purge(engine)
    assert values("SELECT COUNT(*) FROM hl7_oru_analysis") == [0]


# ── the sync ──────────────────────────────────────────────────────────────────

@pytest.fixture
def etl_runner():
    import os
    import sys
    etl_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ETL_JOBS')
    if etl_dir not in sys.path:
        sys.path.insert(0, etl_dir)
    import etl_runner as module
    return module


def test_listing_only_2d_runs_the_purge_with_nothing_from_oracle(engine, devices, monkeypatch, etl_runner):
    """The manual cleanup command, and proof the purge is not behind a phase list."""
    from db import OracleConnector
    opened = []
    monkeypatch.setattr(OracleConnector, 'get_connection', lambda *a, **k: opened.append(a))
    monkeypatch.setenv('RAYD_ETL_PHASES', '2d')
    study(1, 'SJH_SCANNER', '1')
    study(2, 'RH_SCANNER', '0')
    order(3, 'SAP_SJH', 'SJ1')
    etl_runner._perform_migration(engine)
    assert study_uids() == {2}
    assert values("SELECT COUNT(*) FROM etl_orders") == [0]
    assert opened == []


def test_series_are_not_fetched_for_studies_that_went(engine, devices, monkeypatch, etl_runner):
    """Phase 1 hands Phases 2-4 its ids. Those are what cost hours against Oracle."""
    fetched = []

    def load_studies(*_a, **_k):
        study(1, 'SJH_SCANNER', '1')
        study(2, 'RH_SCANNER', '0')
        return 2, [1, 2]
    monkeypatch.setattr(etl_runner, 'run_studies_etl', load_studies)
    monkeypatch.setattr(etl_runner, 'run_series_etl', lambda *a, **k: fetched.extend(a[-1]))
    # Follows Phase 1 and reads Oracle: not what this test is about.
    monkeypatch.setattr(etl_runner, 'run_sticky_notes_etl', lambda *a, **k: 0)
    monkeypatch.setenv('RAYD_ETL_PHASES', '1,2')
    etl_runner._perform_migration(engine)
    assert fetched == [2]


def test_a_failed_purge_shows_in_the_sync_status_and_does_not_stop_it(engine, devices, monkeypatch, etl_runner):
    def boom(_engine):
        raise RuntimeError('deadlock')
    monkeypatch.setattr(etl_runner, 'run_rh_only_purge', boom)
    monkeypatch.setenv('RAYD_ETL_PHASES', '2d')
    etl_runner._perform_migration(engine)
    status, message = values("""SELECT status FROM etl_job_log WHERE job_name = '4TB_SYNC'
                                ORDER BY id DESC LIMIT 1""")[0], values(
        "SELECT error_message FROM etl_job_log WHERE job_name = '4TB_SYNC' ORDER BY id DESC LIMIT 1")[0]
    assert status != 'SUCCESS'
    assert 'RH-only purge' in message


# ── device lists, which removing studies does not reach ───────────────────────

def test_device_lists_leave_out_other_sites(devices):
    from utils.report_filters import rh_device_sql
    rows = set(values(f"""SELECT aetitle FROM aetitle_modality_map m
                          WHERE {rh_device_sql('m')} AND aetitle IN
                          ('RH_SCANNER', 'SJH_SCANNER', 'SENO1', 'LAUMCWFM1AR')"""))
    assert rows == {'RH_SCANNER', 'SENO1', ROUTER}


def test_the_department_board_leaves_out_other_sites(client, devices):
    r = client.get('/viewer/department/api/board', headers={'X-Test-Role': 'admin', 'X-Test-Uid': '1'})
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert 'RH_SCANNER' in body
    assert 'SJH_SCANNER' not in body


def test_the_department_board_offers_no_other_site(client, devices):
    sql("""INSERT INTO floor_plans (site_id, image_path, image_width, image_height)
           VALUES (:s, 'floor_plans/sjh.png', 1600, 900)""", s=site_id('SJH'))
    try:
        board = client.get('/viewer/department/api/board',
                           headers={'X-Test-Role': 'admin', 'X-Test-Uid': '1'}).get_json()
        assert [s['code'] for s in board['sites']] == ['RH']
        assert board['plans'] == []
    finally:
        sql("DELETE FROM floor_plans")


# ── fact_exam records its exclusions instead of dropping them ─────────────────

def test_fact_exam_marks_sjh_as_not_counted(devices):
    from db import db
    from ETL_JOBS.etl_fact_exam import _DERIVE_SQL
    for uid, ae, label in ((1, 'RH_SCANNER', '0'), (2, 'SJH_SCANNER', '1')):
        sql("""INSERT INTO fact_exam (study_db_uid, storing_ae, pacs_site_id, study_modality_raw,
                                      study_ts, number_of_images)
               VALUES (:u, :ae, :l, '\\CT\\', :d, 1)""", u=uid, ae=ae, l=label, d=DAY)
    db.session.execute(text(_DERIVE_SQL))
    db.session.commit()
    rows = dict(db.session.execute(text("SELECT study_db_uid, reason FROM fact_exam")).fetchall())
    assert rows == {1: None, 2: 'SJH (RH only)'}
