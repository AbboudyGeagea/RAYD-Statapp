"""
tests/test_site_route.py — SJH's rows into SJH's own database (item 13b, option A,
ETL_JOBS/etl_site_route.py).

The ETL reads both hospitals into RH's database; this job stores SJH's rows in SJH's
database, matched on natural keys, verified, and only then deleted from RH. Rows with no
site stay on RH, flagged; devices, catalogs and RIS patients stay on RH too.
"""
import re
from datetime import datetime

import pytest
from sqlalchemy import text

from ETL_JOBS import etl_site_route as route

T = datetime(2026, 10, 5, 9, 0)
RH, SJH = 1, 2

# every table the job writes, children first, cleaned in both databases around each test
DATA = ('hl7_oru_rule_cache', 'hl7_oru_analysis', 'ai_nlp_cache', 'crn_events', 'crn_recipients',
        'crn_notifications', 'hl7_oru_reports', 'std_site_pps_ext', 'std_pps_person_reference', 'std_pps',
        'std_patient_ids', 'std_patients_ris', 'std_visits', 'std_reports', 'worklist_status_history',
        'std_worklist_scheduled', 'std_worklist_arrivals', 'std_worklist_exam_done', 'std_worklist_cancellations',
        'etl_orders', 'fact_exam', 'cd_print_log', 'etl_pacs_audit_events', 'study_access_history',
        'etl_sticky_notes', 'etl_image_locations', 'etl_didb_raw_images', 'etl_didb_serieses',
        'etl_didb_studies', 'etl_patient_view', 'hl7_orders')
CATALOGS = tuple(t.name for t in route.TABLES if t.where == 'TRUE')
DEVICES = ('CT_RH_T', 'CT_SJ_T')


def run(engine, statement, **params):
    with engine.begin() as c:
        return c.execute(text(statement), params)


def rows(engine, statement, **params):
    with engine.connect() as c:
        return [tuple(r) for r in c.execute(text(statement), params)]


def keys(engine, statement):
    return sorted(r[0] for r in rows(engine, statement))


def wipe(engine):
    run(engine, f"TRUNCATE {', '.join(t for t in DATA if _exists(engine, t))} CASCADE")
    for t in CATALOGS:
        if _exists(engine, t):
            run(engine, f"DELETE FROM {t}")
    for t in ('device_weekly_schedule', 'aetitle_modality_map'):
        run(engine, f"DELETE FROM {t} WHERE aetitle IN :d".replace(':d', str(DEVICES)))
    # a rollup's last success makes the next one incremental; each test starts without one
    run(engine, "DELETE FROM etl_job_log WHERE job_name IN ('STORAGE_CUMULATIVE_SYNC', 'SITE_ROUTE_SJH')")
    run(engine, "DELETE FROM summary_storage_daily")


def _exists(engine, table):
    return rows(engine, "SELECT to_regclass(:t) IS NOT NULL", t=f'public.{table}')[0][0]


@pytest.fixture
def dbs(app):
    from db import db
    from utils import site_db
    with app.app_context():
        main, sjh = db.engine, site_db.engine_for(site_db.each_site_id()[1])
        assert rows(main, "SELECT id FROM sites WHERE code = 'SJH'")[0][0] == SJH
        for e in (main, sjh):
            wipe(e)
        yield main, sjh
        for e in (main, sjh):
            wipe(e)


def two_hospitals(main):
    """RH study 101, SJH studies 201 and 202, study 301 with no site; patient 10 only at
    SJH, patient 11 at both; an RH and an SJH order; ORU reports; PPS."""
    run(main, "INSERT INTO go_live_config (go_live_date) SELECT DATE '2019-01-01' "
              "WHERE NOT EXISTS (SELECT 1 FROM go_live_config)")
    run(main, """INSERT INTO aetitle_modality_map (aetitle, modality, site_id)
                 VALUES ('CT_RH_T', 'CT', 1), ('CT_SJ_T', 'CT', 2)""")
    run(main, "INSERT INTO device_weekly_schedule (aetitle, day_of_week, std_opening_minutes) VALUES ('CT_SJ_T', 0, 600)")
    run(main, "INSERT INTO std_resources_ris (resource_id_key, first_name, last_name) VALUES (61, 'Rad', 'One')")
    run(main, "INSERT INTO etl_patient_view (patient_db_uid, fallback_id) VALUES (10, 'P10'), (11, 'P11')")
    run(main, """INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, site_id, accession_number, storing_ae,
                                               study_instance_uid, study_date, report_status)
                 VALUES (101, 11, 1, 'A101', 'CT_RH_T', '1.2.101', :d, 'APPROVED'),
                        (201, 10, 2, 'A201', 'CT_SJ_T', '1.2.201', :d, 'WETREAD'),
                        (202, 11, 2, 'A202', 'CT_SJ_T', '1.2.202', :d, NULL),
                        (301, 11, NULL, 'A301', 'UNKNOWN', '1.2.301', :d, NULL)""", d=T.date())
    run(main, """INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid) VALUES (1011, 101), (2011, 201)""")
    run(main, """INSERT INTO etl_didb_raw_images (raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid,
                                                  study_instance_uid, series_instance_uid)
                 VALUES (90201, 10, 201, 2011, '1.2.201', '1.2.201.1'), (90101, 11, 101, 1011, '1.2.101', '1.2.101.1')""")
    run(main, "INSERT INTO etl_image_locations (raw_image_db_uid, image_size_kb) VALUES (90201, 512), (90101, 256)")
    run(main, """INSERT INTO etl_sticky_notes (data_db_uid, study_db_uid, user_name, date_created)
                 VALUES (9001, 201, 'res.one@ad', :t)""", t=T)
    run(main, """INSERT INTO etl_orders (order_dbid, order_control, accession_number, study_db_uid, visit_dbid, patient_dbid)
                 VALUES (5101, 'SAP_PROD', 'A101', 101, 7101, '8101'),
                        (5201, 'SAP_SJH', 'A201', 201, 7001, '8001')""")
    run(main, """INSERT INTO std_worklist_exam_done (site_worklist_key, exam_done_at) VALUES (5101, :t), (5201, :t)""", t=T)
    run(main, """INSERT INTO std_reports (report_key, version, reported_acc_number, verified1_date)
                 VALUES (61, 1, 'A201', :t), (62, 1, 'A101', :t)""", t=T)
    run(main, "INSERT INTO std_visits (visit_key) VALUES (7001), (7101)")
    run(main, "INSERT INTO std_patients_ris (patient_person_key) VALUES (8001), (8101)")
    run(main, "INSERT INTO std_patient_ids (patient_id_list_key, patient_person_key, patient_id) VALUES (80011, 8001, 'S1')")
    run(main, """INSERT INTO std_pps (pps_key, study_db_uid, performing_ae_title)
                 VALUES (6201, 201, 'CT_SJ_T'), (6202, 101, 'CT_SJ_T'), (6101, 101, 'CT_RH_T')""")
    run(main, """INSERT INTO hl7_oru_reports (accession_number, report_text, site_id)
                 VALUES ('A101', 'rh report', 1), ('A201', 'sjh report', 2), ('A999', 'no site', NULL)""")
    run(main, """INSERT INTO hl7_oru_analysis (report_id, is_critical)
                 SELECT id, accession_number = 'A201' FROM hl7_oru_reports WHERE accession_number IN ('A101', 'A201')""")
    run(main, """INSERT INTO ai_nlp_cache (source_id, classification)
                 SELECT id, 'normal' FROM hl7_oru_reports WHERE accession_number = 'A201'""")


def move(main, sjh, **kw):
    return route.route_site(main, sjh, SJH, **{'delete': True, 'images': True, 'first_time': True, **kw})


def test_the_move_puts_sjh_in_its_database_and_leaves_rh_alone(dbs):
    main, sjh = dbs
    two_hospitals(main)

    report = move(main, sjh)

    assert report['verified'] and report['deleted'] and not report['blocked']
    assert keys(sjh, "SELECT study_db_uid FROM etl_didb_studies") == [201, 202]
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 301]
    assert keys(sjh, "SELECT series_db_uid FROM etl_didb_serieses") == [2011]
    assert keys(main, "SELECT series_db_uid FROM etl_didb_serieses") == [1011]
    assert keys(sjh, "SELECT raw_image_db_uid FROM etl_image_locations") == [90201]
    assert keys(main, "SELECT raw_image_db_uid FROM etl_didb_raw_images") == [90101]
    assert keys(sjh, "SELECT data_db_uid FROM etl_sticky_notes") == [9001]
    assert keys(main, "SELECT COUNT(*) FROM etl_sticky_notes") == [0]
    assert keys(sjh, "SELECT order_dbid FROM etl_orders") == [5201]
    assert keys(main, "SELECT order_dbid FROM etl_orders") == [5101]
    assert keys(sjh, "SELECT site_worklist_key FROM std_worklist_exam_done") == [5201]
    assert keys(main, "SELECT site_worklist_key FROM std_worklist_exam_done") == [5101]
    assert keys(sjh, "SELECT report_key FROM std_reports") == [61]
    assert keys(main, "SELECT report_key FROM std_reports") == [62]
    assert keys(sjh, "SELECT accession_number FROM hl7_oru_reports") == ['A201']
    assert keys(main, "SELECT accession_number FROM hl7_oru_reports") == ['A101', 'A999']


def his_order(main, message_id, pv1_3, unit=''):
    orc = '|'.join(['ORC', 'NW', f'{message_id}^HIS'] + [''] * 14 + [f'{unit}^^'])   # unit = ORC-17
    raw = (f"MSH|^~\\&|SAP_P||RAYD||20261119120000||ORM^O01|{message_id}|P|2.3\r"
           f"PV1|1|O|{pv1_3}\r{orc}\r")
    run(main, """INSERT INTO hl7_orders (message_id, placer_order_number, patient_location, raw_message, received_at)
                 VALUES (:m, :m, :loc, :raw, :t)""", m=message_id, loc=pv1_3.split('^')[0], raw=raw, t=T)


def test_the_move_takes_sjhs_his_orders(dbs):
    """Item 13d: the HIS's orders of SJH (PV1-3.7 = 2000, else an SJH location or unit ending in
    -J) go to SJH's database; RH's (1000) and those naming no hospital stay."""
    main, sjh = dbs
    two_hospitals(main)
    his_order(main, 'H-RH', 'WARD9^^^^^^1000')
    his_order(main, 'H-SJ', 'EM-J^^^^^^2000')
    his_order(main, 'H-SJ-UNIT', '', unit='ENDOS-J')
    his_order(main, 'H-NONE', 'NEWWARD')

    report = move(main, sjh)

    assert report['verified']
    assert keys(sjh, "SELECT message_id FROM hl7_orders") == ['H-SJ', 'H-SJ-UNIT']
    assert keys(main, "SELECT message_id FROM hl7_orders") == ['H-NONE', 'H-RH']


def test_oru_children_find_their_report_by_accession(dbs):
    main, sjh = dbs
    two_hospitals(main)
    run(sjh, "INSERT INTO hl7_oru_reports (accession_number, report_text) VALUES ('X1', 'own row')")  # ids differ

    move(main, sjh)

    sjh_id = rows(sjh, "SELECT id FROM hl7_oru_reports WHERE accession_number = 'A201'")[0][0]
    assert rows(sjh, "SELECT report_id, is_critical FROM hl7_oru_analysis") == [(sjh_id, True)]
    assert rows(sjh, "SELECT source_id FROM ai_nlp_cache") == [(sjh_id,)]
    assert rows(main, "SELECT COUNT(*) FROM hl7_oru_analysis") == [(1,)]       # A101's stays


def test_patients_and_visits_stay_on_rh_while_rh_still_uses_them(dbs):
    main, sjh = dbs
    two_hospitals(main)

    move(main, sjh)

    assert keys(sjh, "SELECT patient_db_uid FROM etl_patient_view") == [10, 11]
    assert keys(main, "SELECT patient_db_uid FROM etl_patient_view") == [11]          # 11 also has RH studies
    assert keys(sjh, "SELECT visit_key FROM std_visits") == [7001]
    assert keys(main, "SELECT visit_key FROM std_visits") == [7101]
    assert keys(sjh, "SELECT patient_person_key FROM std_patients_ris") == [8001]
    assert keys(main, "SELECT patient_person_key FROM std_patients_ris") == [8001, 8101]  # RIS patients stay
    assert keys(sjh, "SELECT patient_id_list_key FROM std_patient_ids") == [80011]


def test_devices_and_catalogs_are_copied_and_kept(dbs):
    main, sjh = dbs
    two_hospitals(main)

    move(main, sjh)

    assert rows(sjh, "SELECT aetitle, site_id FROM aetitle_modality_map WHERE aetitle IN ('CT_RH_T', 'CT_SJ_T')") \
        == [('CT_SJ_T', SJH)]
    assert rows(main, "SELECT COUNT(*) FROM aetitle_modality_map WHERE aetitle = 'CT_SJ_T'") == [(1,)]
    assert rows(sjh, "SELECT std_opening_minutes FROM device_weekly_schedule WHERE aetitle = 'CT_SJ_T'") == [(600,)]
    assert rows(sjh, "SELECT last_name FROM std_resources_ris") == [('One',)]


def test_a_pps_on_an_sjh_device_for_an_rh_study_loses_its_link(dbs):
    main, sjh = dbs
    two_hospitals(main)

    move(main, sjh)

    assert rows(sjh, "SELECT pps_key, study_db_uid FROM std_pps ORDER BY pps_key") == [(6201, 201), (6202, None)]
    assert rows(main, "SELECT pps_key, study_db_uid FROM std_pps") == [(6101, 101)]


def test_rows_with_no_site_stay_on_rh_and_are_reported(dbs):
    main, sjh = dbs
    two_hospitals(main)

    report = move(main, sjh)

    assert report['no_site'] == {'studies': 1, 'oru_reports': 1}
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies WHERE site_id IS NULL") == [301]


def test_copy_only_deletes_nothing(dbs):
    main, sjh = dbs
    two_hospitals(main)

    report = route.route_site(main, sjh, SJH)

    assert report['verified'] and not report['deleted']
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 201, 202, 301]
    assert keys(sjh, "SELECT study_db_uid FROM etl_didb_studies") == [201, 202]


def test_a_second_pass_rewrites_only_what_changed(dbs):
    """The move after a copy run (20 Nov evening: no separate copy step, --move copies again):
    rows already there and unchanged are not rewritten (millions of series at LAUMC), a changed
    one is, and the move still verifies every table and deletes from RH."""
    main, sjh = dbs
    two_hospitals(main)
    first = route.route_site(main, sjh, SJH, first_time=True)
    assert first['verified'] and not first['deleted']
    copied = lambda report, table: next(r['copied'] for r in report['tables'] if r['table'] == table)
    assert copied(first, 'etl_didb_studies') == 2
    xmin = lambda: dict(rows(sjh, "SELECT study_db_uid, xmin::text FROM etl_didb_studies"))
    before = xmin()
    run(main, "UPDATE etl_didb_studies SET report_status = 'APPROVED' WHERE study_db_uid = 202")

    second = move(main, sjh)

    assert second['verified'] and second['deleted']
    assert copied(second, 'etl_didb_studies') == 1 and copied(second, 'etl_didb_serieses') == 0
    after = xmin()
    assert after[201] == before[201] and after[202] != before[202]      # 201 untouched, 202 rewritten
    assert rows(sjh, "SELECT report_status FROM etl_didb_studies WHERE study_db_uid = 202") == [('APPROVED',)]
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 301]


def test_the_move_script_has_no_dry_run_and_no_copy_before_the_move():
    """20 Nov evening: copy = copy only; move = one --move run (it verifies before deleting)."""
    import os
    import re
    src = open(os.path.join(os.path.dirname(__file__), '..', 'scripts', 'sjh_move.sh'), encoding='utf-8').read()
    calls = re.findall(r'\$ROUTE (--[a-z-]+)', src)
    assert calls == ['--copy', '--move']
    assert src.index('$ROUTE --copy') < src.index('exit 0', src.index('$ROUTE --copy')) < src.index('$ROUTE --move')


def test_the_next_night_updates_what_the_etl_read_again(dbs):
    """The studies ETL re-reads the last 10 days, so a moved study comes back to RH with
    its new values and a new series: SJH's copy is updated, not duplicated."""
    main, sjh = dbs
    two_hospitals(main)
    move(main, sjh)
    run(main, """INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid, site_id, accession_number, storing_ae,
                                               study_instance_uid, study_date, report_status)
                 VALUES (201, 10, 2, 'A201', 'CT_SJ_T', '1.2.201', :d, 'APPROVED')""", d=T.date())
    run(main, "INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid) VALUES (2012, 201)")
    run(main, "INSERT INTO hl7_oru_reports (accession_number, report_text, site_id) VALUES ('A201', 'final text', 2)")

    report = route.route_site(main, sjh, SJH, delete=True, images=True, lock=False)

    assert report['verified'] and report['deleted']
    assert rows(sjh, "SELECT report_status FROM etl_didb_studies WHERE study_db_uid = 201") == [('APPROVED',)]
    assert keys(sjh, "SELECT series_db_uid FROM etl_didb_serieses") == [2011, 2012]
    assert rows(sjh, "SELECT report_text FROM hl7_oru_reports WHERE accession_number = 'A201'") == [('final text',)]
    assert keys(sjh, "SELECT data_db_uid FROM etl_sticky_notes") == [9001]      # untouched by the update
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 301]


def test_nothing_moves_while_the_etl_runs(dbs):
    main, sjh = dbs
    two_hospitals(main)
    with main.connect() as etl:
        etl.execute(text("SELECT pg_advisory_lock(:k)"), {'k': route._ETL_SYNC_LOCK_ID})
        try:
            with pytest.raises(RuntimeError, match='ETL is running'):
                move(main, sjh)
        finally:
            etl.execute(text("SELECT pg_advisory_unlock(:k)"), {'k': route._ETL_SYNC_LOCK_ID})
    assert keys(sjh, "SELECT COUNT(*) FROM etl_didb_studies") == [0]


def test_a_crn_row_keeps_its_oru_report_on_rh(dbs):
    main, sjh = dbs
    two_hospitals(main)
    run(main, """INSERT INTO crn_notifications (ref_code, report_id, marker, report_fingerprint)
                 SELECT 'R1', id, 'CRN', 'f' FROM hl7_oru_reports WHERE accession_number = 'A201'""")

    report = move(main, sjh)

    assert report['deleted'] and report['blocked']
    assert keys(main, "SELECT accession_number FROM hl7_oru_reports") == ['A101', 'A201', 'A999']
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 301]      # the rest moved


def test_the_nightly_step_waits_for_the_one_time_move(dbs):
    """Before the history is moved, a nightly run would copy every image: it is skipped,
    and runs once the move has recorded itself."""
    from ETL_JOBS import etl_runner
    main, sjh = dbs
    two_hospitals(main)
    run(main, "DELETE FROM settings WHERE key = 'site_route_moved_SJH'")
    failures = []

    etl_runner._route_sites(main, failures)
    assert failures == [] and keys(sjh, "SELECT COUNT(*) FROM etl_didb_studies") == [0]

    route.record_moved(main, 'SJH', datetime.now())
    etl_runner._route_sites(main, failures)
    assert failures == [] and keys(sjh, "SELECT study_db_uid FROM etl_didb_studies") == [201, 202]
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 301]
    assert rows(main, "SELECT status FROM etl_job_log WHERE job_name = 'SITE_ROUTE_SJH' ORDER BY id DESC LIMIT 1") \
        == [('SUCCESS',)]
    run(main, "DELETE FROM settings WHERE key = 'site_route_moved_SJH'")


def test_the_move_takes_the_storage_history_and_only_the_recent_images(dbs):
    """The image history is not copied (operator, 2026-11-17): the site's daily storage is
    computed once from it, with the rollup's formula, and only the last days' images go."""
    main, sjh = dbs
    two_hospitals(main)
    run(main, "UPDATE etl_didb_studies SET study_date = :old WHERE study_db_uid = 202", old=T.date().replace(month=6))
    run(main, "UPDATE etl_didb_studies SET study_modality = 'CT', procedure_code = 'CT01' WHERE study_db_uid IN (201, 202)")
    run(main, "INSERT INTO etl_didb_serieses (series_db_uid, study_db_uid) VALUES (2021, 202)")
    run(main, """INSERT INTO etl_didb_raw_images (raw_image_db_uid, patient_db_uid, study_db_uid, series_db_uid,
                                                  study_instance_uid, series_instance_uid)
                 VALUES (90202, 11, 202, 2021, '1.2.202', '1.2.202.1')""")
    run(main, "INSERT INTO etl_image_locations (raw_image_db_uid, image_size_kb) VALUES (90202, 1073741824)")  # 1 GB

    report = move(main, sjh, images=False, images_since=T.date().replace(day=1))

    assert keys(sjh, "SELECT raw_image_db_uid FROM etl_didb_raw_images") == [90201]       # October only
    assert keys(main, "SELECT raw_image_db_uid FROM etl_didb_raw_images") == [90101]      # June's went
    history = rows(sjh, "SELECT study_date, storing_ae, study_count, total_gb FROM summary_storage_daily ORDER BY 1")
    assert [(d.month, ae, n, float(gb)) for d, ae, n, gb in history] == [(6, 'CT_SJ_T', 1, 1.0), (10, 'CT_SJ_T', 1, 0.0)]
    assert next(r for r in report['tables'] if r['table'] == 'summary_storage_daily (history)')['missing'] == 0
    assert rows(sjh, "SELECT COUNT(*) FROM go_live_config") == rows(main, "SELECT COUNT(*) FROM go_live_config")


def test_the_storage_rollup_counts_the_site_its_database_serves(dbs):
    from ETL_JOBS.etl_analytics_refresh import refresh_storage_summary
    from utils import site_db
    main, sjh = dbs
    two_hospitals(main)
    run(main, "UPDATE etl_didb_studies SET study_modality = 'CT', procedure_code = 'CT01'")
    move(main, sjh)
    run(sjh, "DELETE FROM summary_storage_daily")

    with site_db.use_site(SJH):
        refresh_storage_summary()

    assert rows(sjh, "SELECT storing_ae, study_count FROM summary_storage_daily") == [('CT_SJ_T', 1)]


def test_a_dry_run_counts_and_writes_nothing(dbs):
    main, sjh = dbs
    two_hospitals(main)

    report = route.route_site(main, sjh, SJH, dry_run=True, images=True)

    counts = {r['table']: r['selected'] for r in report['tables']}
    assert counts['etl_didb_studies'] == 2 and counts['etl_orders'] == 1 and counts['hl7_oru_reports'] == 1
    assert counts['std_pps'] == 2 and counts['etl_didb_raw_images'] == 1
    assert keys(sjh, "SELECT COUNT(*) FROM etl_didb_studies") == [0]
    assert keys(main, "SELECT COUNT(*) FROM etl_didb_studies") == [4]


# ── speed and resilience at production size (2026-11-20) ──────────────────────

def test_the_verification_matches_with_equals_where_a_key_cannot_be_null():
    """IS NOT DISTINCT FROM uses neither an index nor a hash: on every key it made the check
    quadratic (two copies of SJH ran for hours on production). Only nullable keys keep it."""
    t = next(t for t in route.TABLES if t.name == 'std_worklist_scheduled')
    assert route._verify_match(t, {'site_worklist_key'}) == \
        'x."site_worklist_key" = s."site_worklist_key" AND x."scheduled_at" IS NOT DISTINCT FROM s."scheduled_at"'
    studies = next(t for t in route.TABLES if t.name == 'etl_didb_studies')
    assert route._verify_match(studies, {'study_db_uid'}) == 'x."study_db_uid" = s."study_db_uid"'
    oru_child = next(t for t in route.TABLES if t.name == 'hl7_oru_analysis')
    assert 'hl7_oru_reports r WHERE r.accession_number = s._parent_acc' in route._verify_match(oru_child, set())


def test_the_check_runs_as_a_hash_anti_join_on_a_large_table(dbs):
    """60,000 staged rows against 60,000 copied: the plan the copy now gets."""
    _main, sjh = dbs
    with sjh.connect() as c:
        c.execute(text("CREATE TEMP TABLE _stage AS SELECT g AS study_db_uid FROM generate_series(1, 60000) g"))
        c.execute(text("INSERT INTO etl_didb_studies (study_db_uid, patient_db_uid) SELECT study_db_uid, study_db_uid FROM _stage"))
        c.execute(text("ANALYZE _stage"))
        c.execute(text("ANALYZE etl_didb_studies"))
        t = next(t for t in route.TABLES if t.name == 'etl_didb_studies')
        match = route._verify_match(t, {'study_db_uid'})
        plan = '\n'.join(r[0] for r in c.execute(text(
            f"EXPLAIN SELECT COUNT(*) FROM _stage s WHERE NOT EXISTS (SELECT 1 FROM public.etl_didb_studies x WHERE {match})")))
        assert 'Anti Join' in plan and 'Nested Loop' not in plan, plan
        c.rollback()


def test_a_nullable_key_is_checked_with_a_hash_and_null_keys_still_match(dbs):
    """2026-11-21: std_worklist_arrivals (both key columns nullable) took 817 s to check at
    LAUMC. Rows whose key has a value in every column now go through a hash anti-join; a row
    with a NULL key part is still matched (IS NOT DISTINCT FROM) and still counted if absent."""
    _main, sjh = dbs
    t = next(t for t in route.TABLES if t.name == 'std_worklist_arrivals')
    with sjh.connect() as c:
        c.execute(text("DROP TABLE IF EXISTS _stage"))
        c.execute(text("""CREATE TEMP TABLE _stage AS
                          SELECT g::bigint AS site_worklist_key, TIMESTAMP '2026-10-01' + g * INTERVAL '1 minute' AS arrived_at
                          FROM generate_series(1, 60000) g"""))
        c.execute(text("INSERT INTO std_worklist_arrivals (site_worklist_key, arrived_at) SELECT * FROM _stage"))
        c.execute(text("ANALYZE _stage"))
        c.execute(text("ANALYZE std_worklist_arrivals"))
        sql = route._missing_sql(t, set())
        plan = '\n'.join(r[0] for r in c.execute(text('EXPLAIN ' + sql)))
        assert re.search(r'(Hash|Merge) Anti Join', plan), plan      # the large part: no nested loop
        assert c.execute(text(sql)).scalar() == 0
        # a NULL key part: found when the site holds the same row, counted when it does not
        c.execute(text("INSERT INTO std_worklist_arrivals (site_worklist_key, arrived_at) VALUES (70001, NULL)"))
        c.execute(text("INSERT INTO _stage VALUES (70001, NULL), (70002, NULL), (70003, TIMESTAMP '2026-12-01')"))
        assert c.execute(text(sql)).scalar() == 2
        c.rollback()


def test_the_oru_key_set_is_a_hash_join_not_a_subplan(dbs):
    """2026-11-21: "accession IN (...) OR site_id = ..." took 6,027 s at LAUMC (no hash across
    an OR: every report compared with every accession). Now a join and a UNION."""
    main, _sjh = dbs
    sql = dict(route.KEY_SETS)['_r_oru'].replace('%(site)s', ':site')
    with main.connect() as c:
        c.execute(text("DROP TABLE IF EXISTS _r_acc"))        # a pooled session may hold the route's
        c.execute(text("CREATE TEMP TABLE _r_acc AS SELECT 'ACC' || g AS acc FROM generate_series(1, 60000) g"))
        c.execute(text("""INSERT INTO hl7_oru_reports (accession_number, report_text, site_id)
                          SELECT 'ACC' || g, 'x', CASE WHEN g % 3 = 0 THEN 2 END FROM generate_series(1, 90000, 2) g"""))
        c.execute(text("INSERT INTO hl7_oru_reports (accession_number, report_text, site_id) VALUES ('ONLY-SITE', 'x', 2)"))
        c.execute(text("ANALYZE _r_acc"))
        c.execute(text("ANALYZE hl7_oru_reports"))
        plan = '\n'.join(r[0] for r in c.execute(text('EXPLAIN ' + sql), {'site': SJH}))
        assert 'SubPlan' not in plan, plan
        got = c.execute(text(f"SELECT COUNT(*) FROM ({sql}) k"), {'site': SJH}).scalar()
        # every odd accession up to 60,000 is in _r_acc (30,000), plus the SJH reports beyond it
        # (odd g in 60,001..89,999 divisible by 3: 5,000), plus the one with only the site
        assert got == 30000 + 5000 + 1
        c.rollback()


def test_a_copy_cut_short_keeps_the_tables_it_finished(dbs, monkeypatch):
    """One transaction per table on the site database: a cut loses the table under way only,
    and the site database's tables are not held for the whole copy."""
    main, sjh = dbs
    two_hospitals(main)
    real = route._copy

    def cut(cur, dcur, t):
        if t.name == 'etl_orders':
            raise RuntimeError('deploy restarted the app')
        return real(cur, dcur, t)
    monkeypatch.setattr(route, '_copy', cut)
    with pytest.raises(RuntimeError):
        route.route_site(main, sjh, SJH, first_time=True)
    assert keys(sjh, "SELECT study_db_uid FROM etl_didb_studies") == [201, 202]      # finished before the cut
    assert keys(sjh, "SELECT order_dbid FROM etl_orders") == []                      # the table under way
    assert keys(main, "SELECT study_db_uid FROM etl_didb_studies") == [101, 201, 202, 301]   # RH untouched
    monkeypatch.setattr(route, '_copy', real)
    report = route.route_site(main, sjh, SJH, first_time=True)                       # the rerun completes it
    assert report['verified'] and keys(sjh, "SELECT order_dbid FROM etl_orders") == [5201]
