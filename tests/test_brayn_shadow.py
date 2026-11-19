"""
BRAYN on LAUMC, observing only (utils/brayn_shadow.py, migration 0133).

The promise to the operator was "without breaking anything", so the first tests
here are about LAUMC, not BRAYN: what the listener writes and answers is the same
with BRAYN on or off, and BRAYN failing cannot reach it. The rest check that BRAYN,
once on, sees what it sees on the HL7 branch — in its own tables only.
"""
import os
import time
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

import hl7kit as k

LAUMC_TABLES = ['hl7_orders', 'hl7_oru_reports', 'hl7_scn_studies', 'order_status_log']
BRAYN_TABLES = ['hl7_message_archive', 'brayn_findings', 'hl7_study_events',
                'brayn_study_state', 'hl7_patients', 'brayn_interface_stats',
                'brayn_unknown_values']
REPORT_TABLES = ['etl_didb_studies', 'etl_patient_view', 'etl_orders']


def _rows(pipe, table):
    """Every row, minus columns the database fills itself (serial ids, NOW())."""
    cols = [r['column_name'] for r in pipe.q("""
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = :t
           AND COALESCE(column_default, '') NOT ILIKE :now
           AND COALESCE(column_default, '') NOT ILIKE :seq
         ORDER BY ordinal_position""", t=table, now='%now()%', seq='%nextval%')]
    rows = pipe.q(f"SELECT {', '.join(cols)} FROM {table}")
    return sorted(rows, key=repr)


def _traffic():
    """A LAUMC afternoon in miniature, built once so both runs send the same bytes."""
    return [
        k.order('PL-10', when=k.ts(120)),                               # HIS order
        k.status('AR', 'ACC10', k.ts(90), placer='PL-10'),              # RIS status
        k.status('CM', 'ACC10', k.ts(60), app='PACS', control='SC'),    # PACS completion
        k.report('ACC10', k.ts(30)),                                    # report
        k.adt(),                                                        # ADT
        k.report('ACC11', k.ts(20), text='No order behind this one.'),
        k.status('ZZ', 'ACC12', k.ts(10)),                              # unmapped status
        k.message(k.msh('PACS', None, 'ORU^R01', k.ts(5)), k.pid(),     # no identifier
                  k.obr(), k.obx('Free text, nothing to attach it to.')),
    ]


# ── LAUMC is unchanged ────────────────────────────────────────────────────────

def test_off_until_switched_on(pipe):
    assert pipe.scalar("SELECT value FROM settings WHERE key = 'brayn_enabled'") == 'false'
    assert pipe.send(k.order('PL-20', when=k.ts(30))) == 'AA'
    assert pipe.screen() == 0
    assert {t: pipe.count(t) for t in BRAYN_TABLES} == {t: 0 for t in BRAYN_TABLES}
    assert pipe.count('hl7_orders') == 1


def test_laumc_writes_and_answers_the_same_with_brayn_on_or_off(pipe):
    traffic = _traffic()

    def run():
        acks = [pipe.send(raw) for raw in traffic]
        out = {t: _rows(pipe, t) for t in LAUMC_TABLES}
        for t in reversed(LAUMC_TABLES):
            pipe.exec(f'DELETE FROM {t}')
        return acks, out

    off = run()
    pipe.brayn()
    on = run()
    assert pipe.screen() == len(traffic)

    assert on == off
    assert off[0] == ['AA'] * len(traffic)
    assert any(off[1].values())                      # LAUMC did store things
    assert pipe.count('hl7_message_archive') == len(traffic)
    assert {t: pipe.count(t) for t in REPORT_TABLES} == {t: 0 for t in REPORT_TABLES}


def test_brayn_failing_cannot_touch_the_ack_or_laumc_data(pipe, monkeypatch):
    pipe.brayn()
    from utils import hl7_ingest

    def broken(msg):
        raise RuntimeError('archive unavailable')

    monkeypatch.setattr(hl7_ingest, 'archive', broken)
    assert pipe.send(k.order('PL-60', when=k.ts(30))) == 'AA'
    assert pipe.count('hl7_orders') == 1
    assert pipe.count('hl7_message_archive') == 0

    monkeypatch.undo()
    assert pipe.send(k.order('PL-61', when=k.ts(20))) == 'AA'
    assert pipe.count('hl7_orders') == 2
    assert pipe.count('hl7_message_archive') == 1


def test_a_message_that_fails_screening_is_set_aside_not_retried(pipe, monkeypatch):
    pipe.brayn()
    pipe.send(k.order('PL-70', when=k.ts(30)))
    from utils import brayn

    def broken(msg, archive_id=None):
        raise RuntimeError('rule bug')

    monkeypatch.setattr(brayn, 'screen', broken)
    assert pipe.screen() == 0
    assert pipe.one("SELECT brayn_status, parse_status FROM hl7_message_archive") == \
        {'brayn_status': 'flagged', 'parse_status': 'error'}
    monkeypatch.undo()
    assert pipe.screen() == 0
    assert pipe.count('hl7_orders') == 1


# ── what BRAYN does once on ───────────────────────────────────────────────────

def test_the_listener_only_archives_and_screening_comes_after(pipe):
    pipe.brayn()
    pipe.send(k.status('AR', 'ACC20', k.ts(30)))
    assert pipe.one("SELECT brayn_status, parse_status FROM hl7_message_archive") == \
        {'brayn_status': 'pending', 'parse_status': 'ok'}
    assert pipe.count('hl7_study_events') == 0 and pipe.count('brayn_study_state') == 0

    assert pipe.screen() == 1
    assert pipe.one("SELECT brayn_status, projected_at FROM hl7_message_archive") == \
        {'brayn_status': 'accepted', 'projected_at': None}
    assert pipe.scalar("SELECT arrived_at IS NOT NULL FROM brayn_study_state "
                       "WHERE accession_number = 'ACC20'") is True
    assert pipe.count('hl7_study_events', "accession_number = 'ACC20'") == 1


def test_a_whole_study_builds_brayn_state_and_no_report_data(pipe):
    pipe.brayn()
    pipe.send(k.order('PL-30', acc='ACC30', when=k.ts(200)))
    for _, raw in k.lifecycle('ACC30', start_minutes_ago=150, placer='PL-30'):
        pipe.send(raw)
    pipe.send(k.report('ACC30', k.ts(10)))
    pipe.screen()

    s = pipe.one("""SELECT ordered_at IS NOT NULL AS ordered, arrived_at IS NOT NULL AS arrived,
                           completed_at IS NOT NULL AS completed,
                           signed_final_at IS NOT NULL AS signed
                      FROM brayn_study_state WHERE accession_number = 'ACC30'""")
    assert s == {'ordered': True, 'arrived': True, 'completed': True, 'signed': True}
    assert pipe.count('hl7_message_archive', "brayn_status <> 'accepted'") == 0
    assert {t: pipe.count(t) for t in REPORT_TABLES} == {t: 0 for t in REPORT_TABLES}


def test_a_message_brayn_cannot_classify_is_marked_and_laumc_still_has_it(pipe):
    pipe.brayn()
    assert pipe.send(k.status('ZZ', 'ACC40', k.ts(10))) == 'AA'
    pipe.screen()
    assert pipe.scalar("SELECT brayn_status FROM hl7_message_archive") == 'quarantined'
    assert pipe.scalar("SELECT rule_code FROM brayn_findings") == 'UNKNOWN_STATUS_CODE'
    assert pipe.count('brayn_unknown_values', "kind = 'status_code'") == 1
    assert pipe.count('brayn_study_state') == 0
    # a RIS status event is BRAYN's to read; LAUMC never stores it as an order (2026-11-18)
    assert pipe.count('hl7_orders', "accession_number = 'ACC40'") == 0


def test_a_message_with_no_identifier_is_marked_not_identified(pipe):
    pipe.brayn()
    pipe.send(k.message(k.msh('PACS', None, 'ORU^R01', k.ts(5)), k.pid(), k.obr(),
                        k.obx('Free text.')))
    pipe.screen()
    assert pipe.scalar("SELECT brayn_status FROM hl7_message_archive") == 'quarantined'
    assert pipe.scalar("SELECT rule_code FROM brayn_findings") == 'MISSING_IDENTIFIER'


def test_an_exact_redelivery_is_counted_not_stored_twice(pipe):
    pipe.brayn()
    raw = k.order('PL-50', when=k.ts(30), ctrl='DUP-1')
    assert pipe.send(raw) == 'AA' and pipe.send(raw) == 'AA'
    assert pipe.count('hl7_message_archive') == 1
    assert pipe.scalar("SELECT SUM(n) FROM brayn_interface_stats "
                       "WHERE metric = 'EXACT_REDELIVERY'") == 1
    assert pipe.count('hl7_orders') == 1


def test_the_worker_screens_in_the_background(pipe, brayn_worker):
    pipe.brayn()
    pipe.send(k.status('AR', 'ACC80', k.ts(30)))
    for _ in range(100):
        if pipe.scalar("SELECT brayn_status FROM hl7_message_archive") != 'pending':
            break
        time.sleep(0.1)
    assert pipe.scalar("SELECT brayn_status FROM hl7_message_archive") == 'accepted'


# ── the hospital clock ────────────────────────────────────────────────────────

def test_brayn_transactions_run_on_hospital_time_and_laumc_ones_do_not(app):
    from db import db
    from utils import brayn_shadow
    zone = brayn_shadow.hospital_zone()
    show = lambda c=None: (c or db.session).execute(text('SHOW timezone')).scalar()
    with app.app_context():
        server = show()
        db.session.rollback()
        if server == zone:
            pytest.skip('the test Postgres already runs on the hospital zone')

        db.session.execute(text('SELECT 1'))            # a transaction already open
        with brayn_shadow.clock():
            assert show() == zone
            db.session.commit()
            assert show() == zone                       # the next transaction too
            db.session.rollback()
            with db.engine.begin() as conn:             # the counters' own connection
                assert show(conn) == zone
        assert show() == server
        db.session.rollback()
        with db.engine.connect() as conn:               # nothing left on the pool
            assert show(conn) == server


def test_the_sweep_runs_only_while_brayn_is_on(app, pipe, monkeypatch):
    from db import db
    from utils import brayn_shadow, brayn_sweep
    calls = []
    monkeypatch.setattr(brayn_sweep, 'run_sweep', lambda: calls.append(
        db.session.execute(text('SHOW timezone')).scalar()) or {})
    assert brayn_shadow.sweep(app) == {} and calls == []
    pipe.brayn()
    brayn_shadow.sweep(app)
    assert calls == [brayn_shadow.hospital_zone()]


def test_a_study_left_unreported_is_found_on_hospital_time(app, pipe):
    """Completed 150 minutes ago (hospital time), threshold 120: overdue. On the
    server's UTC clock it would look 30 minutes old in winter and not be raised."""
    from utils import brayn_shadow
    pipe.brayn()
    pipe.exec("UPDATE brayn_rules SET threshold_minutes = 120 WHERE rule_code = 'UNREPORTED'")
    pipe.send(k.order('PL-90', acc='ACC90', when=k.ts(240)))
    for _, raw in k.lifecycle('ACC90', start_minutes_ago=210, step=20, placer='PL-90'):
        pipe.send(raw)
    pipe.screen()
    brayn_shadow.sweep(app)
    assert pipe.count('brayn_findings',
                      "rule_code = 'UNREPORTED' AND accession_number = 'ACC90' "
                      "AND resolved_at IS NULL") == 1


# ── the LAUMC feeds' own codes (migration 0170, 2026-11-19) ───────────────────

def test_the_ris_arrival_and_the_his_sap_codes_are_classified(pipe):
    """First day of the live feeds: RIS "SC/A" (arrived) and the HIS's RQ / CA with SAP codes in
    ORC-5 (^E0001 ...) were "not identified"; PACS FAP / AAP were unknown result statuses."""
    pipe.brayn()
    pipe.send(k.order('PL-80', when=k.ts(200), app='SAP_P'))
    pipe.send(k.status('^E0003', None, k.ts(190), placer='PL-80', app='SAP_P', control='RQ'))
    pipe.send(k.status('^E0006', None, k.ts(185), placer='PL-80', app='SAP_P', control='RQ'))   # a code never seen
    pipe.send(k.status('SC', 'ACC80', k.ts(180), placer='PL-80'))
    pipe.send(k.status('A', 'ACC80', k.ts(120), placer='PL-80'))
    pipe.send(k.status('IP', 'ACC80', k.ts(110), placer='PL-80'))
    pipe.send(k.status('CM', 'ACC80', k.ts(90), placer='PL-80'))
    pipe.send(k.report('ACC80', k.ts(10), result_status='FAP', app='CARESTREAM'))
    pipe.send(k.status('c^E0001', None, k.ts(5), placer='PL-81', app='SAP_P', control='CA'))
    pipe.screen()

    assert pipe.count('hl7_message_archive', "brayn_status = 'quarantined'") == 0
    assert pipe.count('brayn_findings', "rule_code = 'UNKNOWN_STATUS_CODE'") == 0
    assert pipe.count('brayn_unknown_values', "kind IN ('status_code', 'result_status')") == 0
    s = pipe.one("""SELECT arrived_at IS NOT NULL AS arrived, started_at IS NOT NULL AS started,
                           signed_final_at IS NOT NULL AS signed
                      FROM brayn_study_state WHERE accession_number = 'ACC80'""")
    assert s == {'arrived': True, 'started': True, 'signed': True}


def test_an_his_rq_with_orc5_empty_is_the_order_changing(pipe):
    """The wildcard is any status, none included; a RIS SC with ORC-5 empty has no wildcard and
    stays "not identified"."""
    pipe.brayn()
    pipe.send(k.order('PL-71', when=k.ts(30), app='SAP_P'))
    pipe.send(k.status(None, None, k.ts(20), placer='PL-71', app='SAP_P', control='RQ'))
    pipe.send(k.status(None, 'ACC72', k.ts(10)))
    pipe.screen()
    held = pipe.q("SELECT sending_app FROM hl7_message_archive WHERE brayn_status = 'quarantined'")
    assert [r['sending_app'] for r in held] == ['RIS']


# ── the HIS's time and the RIS study's report (migration 0175, 2026-11-20) ────

def test_the_his_times_are_its_message_time_not_its_planned_orc9(pipe):
    """SAP's ORC-9 is a planned time (production: later than MSH-7 on all 147 "future" HIS
    messages), and it made the order read as ordered after the RIS scheduled it."""
    pipe.brayn()
    planned = k.hl7(k.now() + timedelta(days=3))
    pipe.send(k.order('PL-70', when=planned, msh_when=k.ts(60), app='SAP_P'))
    pipe.send(k.status('^E0003', None, planned, placer='PL-70', app='SAP_P', control='RQ',
                       msh_when=k.ts(50)))
    pipe.send(k.status('SC', 'ACC70', k.ts(40), placer='PL-70'))
    pipe.send(k.status('c^E0001', None, planned, placer='PL-79', app='SAP_P', control='CA',
                       msh_when=k.ts(30)))
    pipe.screen()
    assert pipe.count('brayn_findings', "rule_code IN ('FUTURE_EVENT', 'TIME_CONTRADICTION')") == 0
    s = pipe.one("SELECT ordered_at, scheduled_at FROM brayn_study_state WHERE accession_number = 'ACC70'")
    assert abs((s['ordered_at'] - k.ago(60)).total_seconds()) < 120
    assert s['ordered_at'] < s['scheduled_at']
    times = [r['event_time'] for r in pipe.q(
        "SELECT event_time FROM hl7_study_events WHERE placer_order_number IN ('PL-70', 'PL-79')")]
    assert times and max(times) < datetime.now()


def test_the_pacs_report_reaches_the_ris_study(app, pipe):
    """BRAYN keys a RIS study by the RIS number and the report by the PACS accession, which the
    RIS's completion names in OBR-20: all 148 UNREPORTED in production were such studies."""
    from utils import brayn_shadow
    pipe.brayn()
    pipe.exec("UPDATE brayn_rules SET threshold_minutes = 120 WHERE rule_code = 'UNREPORTED'")
    for acc, pacs in (('1003000000071', 'PACS71'), ('1003000000073', 'PACS73')):
        for code, mins in (('SC', 300), ('IP', 260), ('CM', 240)):
            pipe.send(k.status(code, acc, k.ts(mins), obr20=pacs if code == 'CM' else None))
    pipe.send(k.report('PACS71', k.ts(30), result_status='FAP', app='CARESTREAM'))
    # the report first, the RIS's completion after it
    pipe.send(k.report('PACS72', k.ts(200), result_status='FAP', app='CARESTREAM'))
    for code, mins in (('SC', 300), ('CM', 240)):
        pipe.send(k.status(code, '1003000000072', k.ts(mins), obr20='PACS72' if code == 'CM' else None))
    pipe.screen()
    brayn_shadow.sweep(app)
    rows = {r['accession_number']: r for r in pipe.q(
        "SELECT accession_number, pacs_accession, reported_at FROM brayn_study_state "
        "WHERE accession_number LIKE '1003%'")}
    assert {a: r['pacs_accession'] for a, r in rows.items()} == {
        '1003000000071': 'PACS71', '1003000000072': 'PACS72', '1003000000073': 'PACS73'}
    assert abs((rows['1003000000071']['reported_at'] - k.ago(30)).total_seconds()) < 120
    assert abs((rows['1003000000072']['reported_at'] - k.ago(200)).total_seconds()) < 120
    assert rows['1003000000073']['reported_at'] is None
    open_ = pipe.q("SELECT accession_number FROM brayn_findings WHERE rule_code = 'UNREPORTED' AND resolved_at IS NULL")
    assert [r['accession_number'] for r in open_] == ['1003000000073']


def test_migration_0175_corrects_the_studies_already_in_brayn(pipe):
    raw_his = k.order('PL81', when='20261121083000', msh_when='20261118100000', app='SAP_P', ctrl='H81')
    raw_prov = k.order('PL82', when='20261125090000', msh_when='20261118120000', app='SAP_P', ctrl='H82')
    raw_ris = k.status('CM', '1003000000083', '20261119080000', obr20='PACS83', ctrl='R83')
    ids = {}
    for ctrl, app_, raw, at in (('H81', 'SAP_P', raw_his, '2026-11-18 10:00'),
                                ('H82', 'SAP_P', raw_prov, '2026-11-18 12:00'),
                                ('R83', 'RIS', raw_ris, '2026-11-19 08:00')):
        ids[ctrl] = pipe.scalar("""INSERT INTO hl7_message_archive (message_control_id, sending_app, raw_message,
                                                                    content_hash, message_datetime)
                                   VALUES (:c, :a, :r, :c, :t) RETURNING id""", c=ctrl, a=app_, r=raw, t=at)
    pipe.exec("""INSERT INTO hl7_study_events (message_archive_id, accession_number, placer_order_number,
                                               canonical_state, ladder_rank, event_time) VALUES
                 (:h81, '1003000000081', 'PL81', 'ordered', 20, '2026-11-21 08:30'),
                 (NULL, '1003000000081', 'PL81', 'scheduled', 40, '2026-11-18 11:00'),
                 (:h82, NULL, 'PL82', 'ordered', 20, '2026-11-25 09:00')""", h81=ids['H81'], h82=ids['H82'])
    pipe.exec("""INSERT INTO brayn_study_state (accession_number, placer_order_number, ordered_at, scheduled_at,
                                                completed_at, reported_at, signed_final_at, last_event_at,
                                                current_rank, is_provisional) VALUES
                 ('1003000000081', 'PL81', '2026-11-21 08:30', '2026-11-18 11:00', NULL, NULL, NULL,
                  '2026-11-21 08:30', 40, FALSE),
                 ('~ORD:PL82', 'PL82', '2026-11-25 09:00', NULL, NULL, NULL, NULL, '2026-11-25 09:00', 20, TRUE),
                 ('1003000000083', NULL, NULL, '2026-11-19 07:00', '2026-11-19 08:00', NULL, NULL,
                  '2026-11-19 08:00', 100, FALSE),
                 ('PACS83', NULL, NULL, NULL, NULL, '2026-11-19 09:00', '2026-11-19 09:00',
                  '2026-11-19 09:00', 140, FALSE)""")
    pipe.exec("""INSERT INTO brayn_findings (message_archive_id, rule_code, severity, accession_number, sending_app, detail)
                 VALUES (NULL, 'TIME_CONTRADICTION', 'warning', '1003000000081', NULL,
                         '{"pairs": [{"earlier_rung": "ordered", "later_rung": "scheduled"}]}'),
                        (:h81, 'FUTURE_EVENT', 'warning', '1003000000081', 'SAP_P', '{}')""", h81=ids['H81'])

    sql = open(os.path.join(os.path.dirname(__file__), '..', 'migrations', '0175_brayn_his_time_report_link.sql'),
               encoding='utf-8').read()
    pipe.exec(sql)

    s = {r['accession_number']: r for r in pipe.q(
        "SELECT accession_number, ordered_at, last_event_at, pacs_accession, reported_at FROM brayn_study_state")}
    assert s['1003000000081']['ordered_at'] == datetime(2026, 11, 18, 10, 0)
    assert s['1003000000081']['last_event_at'] == datetime(2026, 11, 18, 11, 0)
    assert s['~ORD:PL82']['ordered_at'] == datetime(2026, 11, 18, 12, 0)
    assert s['1003000000083']['pacs_accession'] == 'PACS83'
    assert s['1003000000083']['reported_at'] == datetime(2026, 11, 19, 9, 0)
    assert s['PACS83']['pacs_accession'] is None
    assert pipe.count('brayn_findings', "resolved_at IS NULL") == 0


def test_an_exact_status_row_beats_the_wildcard(app, pipe):
    from utils import brayn
    pipe.exec("""INSERT INTO hl7_status_map (sending_app, order_control, order_status, canonical_state, ladder_rank)
                 VALUES ('SAP_P', 'RQ', '^E0009', 'scheduled', 40)""")
    brayn._cache.pop('status_map', None)
    try:
        with app.app_context():
            assert brayn.resolve_status('SAP_P', 'RQ', '^E0009') == ('scheduled', 40)
            assert brayn.resolve_status('SAP_P', 'RQ', '^E0004') == ('ordered', 20)
            assert brayn.resolve_status('OTHER', 'RQ', '^E0004') == (None, None)    # the wildcard is SAP_P's
    finally:
        pipe.exec("DELETE FROM hl7_status_map WHERE order_status = '^E0009'")
        brayn._cache.pop('status_map', None)
