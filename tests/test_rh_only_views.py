"""
tests/test_rh_only_views.py
────────────────────────────────────────────────────────────────
Operator rule, 2026-11-05: SJH appears in nothing, and no SJH row is deleted.

Migration 0145 holds a filtered view per table in schema rh, and utils/rh_scope.py points each
web request at them. So every test here asks two things: is SJH gone from what a request reads,
and is it still in the real table? A filter that hid RH mammography, the router counted aside,
or anything the ETL or listener needs would be worse than no filter.
"""
import glob
import json
import os
import re

import pytest
from sqlalchemy import text

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAY = '2026-10-15'
OCTOBER = {'start_date': '2026-10-01', 'end_date': '2026-10-31'}
ROUTER = 'LAUMCWFM1AR'
ADMIN = {'X-Test-Role': 'admin', 'X-Test-Uid': '1'}

ALL_UIDS = set(range(1, 11))
RH_UIDS = {1, 2, 3, 6, 8, 10}


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


def outside(statement, **params):
    """What the ETL, the listener and CRN read."""
    from db import db
    rows = db.session.execute(text(statement), params).scalars().all()
    db.session.commit()
    return rows


def in_request(app, statement, path='/', **params):
    """What a web request reads."""
    from db import db
    db.session.commit()
    with app.test_request_context(path):
        rows = db.session.execute(text(statement), params).scalars().all()
        db.session.commit()
    return rows


def site_id(code):
    return outside("SELECT id FROM sites WHERE code = :c", c=code)[0]


def device(aetitle, modality, site):
    sql("""INSERT INTO aetitle_modality_map (aetitle, modality, site_id) VALUES (:a, :m, :s)
           ON CONFLICT (aetitle) DO UPDATE SET modality = EXCLUDED.modality, site_id = EXCLUDED.site_id""",
        a=aetitle, m=modality, s=site)


def study(uid, ae, label, modality='CT', accession=None):
    sql("""INSERT INTO etl_didb_studies
               (study_db_uid, patient_db_uid, study_instance_uid, accession_number, storing_ae,
                study_date, study_modality, pacs_site_id_raw, study_status)
           VALUES (:u, :u, :iuid, :a, :ae, :d, :m, :label, 'APPROVED')""",
        u=uid, iuid=f'1.2.3.{uid}', a=accession or f'A{uid}', ae=ae, d=DAY, m=modality, label=label)


def order(key, issuer, accession):
    sql("""INSERT INTO etl_orders (order_dbid, order_control, accession_number, order_status, modality)
           VALUES (:k, :i, :a, 'CM', 'CT')""", k=key, i=issuer, a=accession)


def resolve():
    sql("SELECT rh_resolve_study_sites()")


@pytest.fixture
def every_case():
    """One study per case. The uid says which case it is."""
    rh, sjh = site_id('RH'), site_id('SJH')
    device('RH_SCANNER', 'CT', rh)
    device('SENO1', 'MG', rh)
    device('SJH_SCANNER', 'CT', sjh)
    device('GATEWAY_T', 'PACS', rh)
    device(ROUTER, 'PACS', None)
    study(1, 'RH_SCANNER', '0')             # RH, the plain case
    study(2, 'SENO1', '1', modality='MG')   # RH: double read, labelled SJH
    study(3, 'SENO1', '0', modality='MG')   # RH
    study(4, 'SJH_SCANNER', '1')            # SJH
    study(5, 'SJH_SCANNER', '0')            # SJH: the device decides, not the label
    study(6, 'GATEWAY_T', '0')              # RH: a gateway's label decides
    study(7, 'GATEWAY_T', '1')              # SJH: a gateway's label decides
    study(8, 'UNMAPPED_AE', '0')            # RH: an unmapped device's label decides
    study(9, 'UNMAPPED_AE', '1')            # SJH: an unmapped device's label decides
    study(10, ROUTER, '2', modality='SR')   # no site: reports count it aside
    resolve()


# ── studies ───────────────────────────────────────────────────────────────────

def test_a_request_sees_only_rh_studies(app, every_case):
    assert set(in_request(app, "SELECT study_db_uid FROM etl_didb_studies")) == RH_UIDS


def test_nothing_is_deleted(app, every_case):
    in_request(app, "SELECT COUNT(*) FROM etl_didb_studies")
    assert set(outside("SELECT study_db_uid FROM etl_didb_studies")) == ALL_UIDS
    assert set(outside("SELECT study_db_uid FROM public.etl_didb_studies")) == ALL_UIDS


def test_each_study_gets_the_right_site(every_case):
    rh, sjh = site_id('RH'), site_id('SJH')
    sites = dict(zip(*[outside(f"SELECT {c} FROM etl_didb_studies ORDER BY study_db_uid")
                       for c in ('study_db_uid', 'site_id')]))
    assert sites == {1: rh, 2: rh, 3: rh, 4: sjh, 5: sjh, 6: rh, 7: sjh, 8: rh, 9: sjh, 10: None}


def test_moving_a_device_takes_effect_on_the_next_resolve(app, every_case):
    device('SJH_SCANNER', 'CT', site_id('RH'))
    resolve()
    assert {4, 5} <= set(in_request(app, "SELECT study_db_uid FROM etl_didb_studies"))


def test_resolving_twice_changes_nothing(every_case):
    assert outside("SELECT rh_resolve_study_sites()") == [0]


# ── the scope itself ──────────────────────────────────────────────────────────

def test_a_pooled_connection_does_not_keep_the_filter(app, every_case):
    in_request(app, "SELECT COUNT(*) FROM etl_didb_studies")
    assert outside("SELECT COUNT(*) FROM etl_didb_studies") == [len(ALL_UIDS)]


def test_a_second_transaction_in_one_request_is_still_filtered(app, every_case):
    from db import db
    db.session.commit()
    with app.test_request_context('/'):
        db.session.execute(text("SELECT 1"))
        db.session.commit()
        rows = db.session.execute(text("SELECT COUNT(*) FROM etl_didb_studies")).scalar()
        db.session.commit()
    assert rows == len(RH_UIDS)


def test_a_direct_engine_connection_in_a_request_is_filtered(app, every_case):
    """ai_alerts and others use db.engine.connect() rather than the session."""
    from db import db
    with app.test_request_context('/'):
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM etl_didb_studies")).scalar() == len(RH_UIDS)


def test_a_background_job_that_people_read_can_ask_for_the_filter(every_case):
    from db import db
    from utils.rh_scope import rh_scope
    with rh_scope():
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM etl_didb_studies")).scalar() == len(RH_UIDS)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM etl_didb_studies")).scalar() == len(ALL_UIDS)


def test_crn_admin_screens_keep_reading_the_real_tables():
    """A notification already sent must stay workable until the operator decides on CRN."""
    from utils import rh_scope
    assert 'crn_admin' in rh_scope.EXEMPT_BLUEPRINTS


def test_an_exempt_blueprint_reads_the_real_tables(app, every_case, monkeypatch):
    from utils import rh_scope
    monkeypatch.setattr(rh_scope, 'EXEMPT_BLUEPRINTS', {'report_22'})
    assert in_request(app, "SELECT COUNT(*) FROM etl_didb_studies",
                      path='/report/22') == [len(ALL_UIDS)]


def test_it_can_be_turned_off(monkeypatch):
    from utils import rh_scope
    monkeypatch.setenv('RAYD_RH_ONLY', '0')
    assert rh_scope.install(None) is False


def test_tables_that_are_not_filtered_still_resolve_in_a_request(app, every_case):
    assert 'SJH_SCANNER' in in_request(app, "SELECT aetitle FROM aetitle_modality_map")
    assert in_request(app, "SELECT COUNT(*) FROM sites WHERE code = 'SJH'") == [1]


# ── orders and everything hanging off them ────────────────────────────────────

@pytest.fixture
def an_order_at_each_site(every_case):
    order(1, 'SAP_PROD', 'RH1')
    order(2, 'SAP_SJH', 'SJ1')
    for pps, key in ((100, 1), (200, 2)):
        sql("INSERT INTO std_pps (pps_key, study_instance_uid) VALUES (:p, :u)", p=pps, u=f'9.{pps}')
        sql("""INSERT INTO std_worklist_arrivals (site_worklist_key, pps_key, sps_id, arrived_at)
               VALUES (:k, :p, 'S', NOW())""", k=key, p=pps)
        sql("""INSERT INTO std_worklist_exam_done (site_worklist_key, pps_key, exam_done_at)
               VALUES (:k, :p, NOW())""", k=key, p=pps)
    sql("""INSERT INTO hl7_oru_reports (id, accession_number, report_text)
           VALUES (1, 'RH1', 'rh'), (2, 'SJ1', 'sjh'), (3, 'A4', 'on an SJH study'),
                  (4, 'A2', 'double read mammogram'), (5, NULL, 'no accession')""")


def test_a_request_sees_only_rh_orders(app, an_order_at_each_site):
    assert in_request(app, "SELECT order_dbid FROM etl_orders") == [1]
    assert set(outside("SELECT order_dbid FROM etl_orders")) == {1, 2}


def test_status_history_follows_its_order(app, an_order_at_each_site):
    for table in ('std_worklist_arrivals', 'std_worklist_exam_done'):
        assert in_request(app, f"SELECT site_worklist_key FROM {table}") == [1]
        assert set(outside(f"SELECT site_worklist_key FROM {table}")) == {1, 2}


def test_an_order_with_no_issuer_stays_visible(app, every_case):
    order(1, None, 'X1')
    assert in_request(app, "SELECT order_dbid FROM etl_orders") == [1]


def test_reports_follow_their_order_and_study(app, an_order_at_each_site):
    assert set(in_request(app, "SELECT id FROM hl7_oru_reports")) == {1, 4, 5}
    assert set(outside("SELECT id FROM hl7_oru_reports")) == {1, 2, 3, 4, 5}


def test_report_analysis_follows_its_report(app, an_order_at_each_site):
    sql("""INSERT INTO hl7_oru_analysis (report_id, affirmed_labels, is_critical)
           VALUES (1, '{}', FALSE), (2, '{}', FALSE)""")
    sql("INSERT INTO ai_nlp_cache (source_id, classification) VALUES (1, 'x'), (2, 'x')")
    assert in_request(app, "SELECT report_id FROM hl7_oru_analysis") == [1]
    assert in_request(app, "SELECT source_id FROM ai_nlp_cache") == [1]
    assert set(outside("SELECT report_id FROM hl7_oru_analysis")) == {1, 2}


def test_ris_reports_follow_their_order(app, an_order_at_each_site):
    sql("""INSERT INTO std_reports (report_key, version, reported_acc_number)
           VALUES (1, 0, 'RH1'), (2, 0, 'SJ1')""")
    assert in_request(app, "SELECT report_key FROM std_reports") == [1]
    assert set(outside("SELECT report_key FROM std_reports")) == {1, 2}


def test_pps_follows_its_device_and_its_study(app, every_case):
    sql("""INSERT INTO std_pps (pps_key, study_instance_uid, performing_ae_title, study_db_uid)
           VALUES (1, '9.1', 'RH_SCANNER', NULL), (2, '9.2', 'SJH_SCANNER', NULL),
                  (3, '9.3', 'SENO1', 2), (4, '9.4', NULL, 7), (5, '9.5', 'GATEWAY_T', 6)""")
    assert set(in_request(app, "SELECT pps_key FROM std_pps")) == {1, 3, 5}
    assert set(outside("SELECT pps_key FROM std_pps")) == {1, 2, 3, 4, 5}


# ── Report Composer logs in as its own role, so search_path cannot reach it ───

def test_the_composer_reads_only_rh(every_case):
    assert set(outside("SELECT study_uid FROM rpt.studies")) == RH_UIDS


def test_the_composer_reads_only_rh_exams(an_order_at_each_site):
    assert outside("SELECT exam_id FROM rpt.exams") == [1]


def test_no_composer_view_reads_a_filtered_table_directly():
    rows = outside("""
        SELECT DISTINCT c.relname || ' reads ' || t.relname
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class c ON c.oid = r.ev_class
        JOIN pg_class t ON t.oid = d.refobjid
        WHERE c.relnamespace = 'rpt'::regnamespace
          AND t.relnamespace = 'public'::regnamespace
          AND t.relname IN (SELECT relname FROM pg_class WHERE relnamespace = 'rh'::regnamespace)
    """)
    assert rows == []


# ── through real routes ───────────────────────────────────────────────────────

def report_22(client):
    page = client.get('/report/22', query_string=OCTOBER, headers=ADMIN)
    assert page.status_code == 200
    return json.loads(re.search(r'const d = (.*);', page.get_data(as_text=True)).group(1))


def test_report_22_leaves_sjh_out_and_counts_the_router_aside(client, every_case):
    """Uid 7 is the proof: the device rule alone counts an SJH study on an RH-mapped gateway."""
    volume = report_22(client)['volume']
    assert volume['total_studies'] == 4           # 1, 2, 3, 6
    assert volume['reports_without_images'] == 1  # 10


def test_report_23_shows_double_read_mammography(client, every_case):
    page = client.get('/report/23', query_string=OCTOBER, headers=ADMIN)
    assert page.status_code == 200
    assert re.search(r'Reports Without Images</p>\s*<p[^>]*>1</p>', page.get_data(as_text=True))


def test_report_23_conflict_list_leaves_out_sjh_only_patients(client, every_case):
    sql("""INSERT INTO etl_patient_view (patient_db_uid, fallback_id)
           VALUES (1, 'a$$$b'), (4, 'c$$$d'), (999, 'e$$$f')""")   # RH, SJH, no study at all
    r = client.get('/patients/conflicts', headers=ADMIN)
    assert r.get_json()['count'] == 1


def test_department_board_and_a_dropdown_show_no_sjh_device(client, every_case):
    r = client.get('/viewer/department/api/board', headers=ADMIN)
    assert r.status_code == 200
    assert 'SJH_SCANNER' not in r.get_data(as_text=True)


# ── keeping it true ───────────────────────────────────────────────────────────

def test_a_column_added_later_reaches_the_view():
    """Migrations 0143 and 0144 add columns to filtered tables after the views exist."""
    from db import db
    conn = db.engine.connect()
    trans = conn.begin()
    try:
        conn.execute(text("ALTER TABLE etl_didb_studies ADD COLUMN zz_test INTEGER"))
        conn.execute(text("SELECT rh_refresh_views()"))
        assert conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = 'rh' AND table_name = 'etl_didb_studies' AND column_name = 'zz_test'
        """)).scalar() == 1
    finally:
        trans.rollback()
        conn.close()


def test_a_refresh_changes_nothing_in_what_a_request_sees(app, every_case):
    from db import db
    from db_migrations import _refresh_rh_views
    _refresh_rh_views(db)
    assert set(in_request(app, "SELECT study_db_uid FROM etl_didb_studies")) == RH_UIDS


def test_running_a_migration_refreshes_the_views(app, tmp_path, monkeypatch):
    """Through the real runner. The column stays, in this throwaway database."""
    import db_migrations
    (tmp_path / '9999_test_new_column.sql').write_text(
        'ALTER TABLE etl_didb_studies ADD COLUMN IF NOT EXISTS zz_from_migration INTEGER;\n')
    monkeypatch.setattr(db_migrations, 'MIGRATIONS_DIR', str(tmp_path))
    db_migrations.run_migrations(app)
    assert outside("""SELECT COUNT(*) FROM information_schema.columns
                      WHERE table_schema = 'rh' AND table_name = 'etl_didb_studies'
                        AND column_name = 'zz_from_migration'""") == [1]


def test_the_daily_briefing_counts_rh_and_keeps_its_table_in_public(every_case):
    from db import db
    from ETL_JOBS.daily_analytics import _ENSURE_TABLE, _collect
    from utils.rh_scope import rh_scope
    with rh_scope():
        with db.engine.begin() as conn:
            conn.execute(text(_ENSURE_TABLE))
            numbers = _collect(conn, '2026-10-01', '2026-10-31')
    assert numbers['total_studies'] == len(RH_UIDS)
    assert outside("""SELECT COUNT(*) FROM pg_class
                      WHERE relnamespace = 'rh'::regnamespace AND relname = 'analytics_snapshots'""") == [0]


def test_the_app_installs_the_filter_and_runs_the_briefing_through_it():
    """create_app() is not what these tests start, so it is checked where it is written."""
    with open(os.path.join(ROOT, 'app.py'), encoding='utf-8') as f:
        source = f.read()
    assert 'install_rh_scope(db.engine)' in source
    assert re.search(r'with rh_scope\(\):\s+run_analytics\(engine=db\.engine\)', source)


def test_the_views_carry_every_column_of_their_table():
    missing = outside("""
        SELECT c.table_name || '.' || c.column_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name IN (SELECT table_name FROM information_schema.views WHERE table_schema = 'rh')
          AND NOT EXISTS (SELECT 1 FROM information_schema.columns v
                          WHERE v.table_schema = 'rh' AND v.table_name = c.table_name
                            AND v.column_name = c.column_name)
    """)
    assert missing == []


def test_only_views_live_in_rh():
    """An unqualified CREATE TABLE during a request would land here and hide the real table."""
    assert outside("""SELECT relname FROM pg_class WHERE relnamespace = 'rh'::regnamespace
                      AND relkind NOT IN ('v')""") == []


def test_code_that_runs_in_a_request_creates_tables_in_public():
    pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?!public\.|IF\b)\w+', re.I)
    paths = (glob.glob(os.path.join(ROOT, 'routes', '*.py')) + glob.glob(os.path.join(ROOT, 'utils', '*.py'))
             + [os.path.join(ROOT, 'ETL_JOBS', 'daily_analytics.py')])
    offenders = []
    for path in paths:
        with open(path, encoding='utf-8') as f:
            for number, line in enumerate(f, 1):
                if pattern.search(line) and not line.lstrip().startswith('#'):
                    offenders.append(f'{os.path.basename(path)}:{number}')
    assert offenders == []


def test_the_sync_keeps_sites_current_with_nothing_from_oracle(monkeypatch):
    import sys
    from db import OracleConnector, db
    etl_dir = os.path.join(ROOT, 'ETL_JOBS')
    if etl_dir not in sys.path:
        sys.path.insert(0, etl_dir)
    import etl_runner
    opened = []
    monkeypatch.setattr(OracleConnector, 'get_connection', lambda *a, **k: opened.append(a))
    monkeypatch.setenv('RAYD_ETL_PHASES', 'none')
    device('SJH_SCANNER', 'CT', site_id('SJH'))
    study(1, 'SJH_SCANNER', '1')
    assert outside("SELECT site_id FROM etl_didb_studies") == [None]
    etl_runner._perform_migration(db.engine)
    assert outside("SELECT site_id FROM etl_didb_studies") == [site_id('SJH')]
    assert opened == []
