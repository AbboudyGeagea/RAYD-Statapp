"""
BRAYN on LAUMC, observing only (utils/brayn_shadow.py, migration 0133).

The promise to the operator was "without breaking anything", so the first tests
here are about LAUMC, not BRAYN: what the listener writes and answers is the same
with BRAYN on or off, and BRAYN failing cannot reach it. The rest check that BRAYN,
once on, sees what it sees on the HL7 branch — in its own tables only.
"""
import time

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
    assert pipe.count('hl7_orders', "accession_number = 'ACC40'") == 1


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
