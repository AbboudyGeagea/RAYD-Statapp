"""
One study end to end: ADT, HIS order, the four RIS events, the report — and every
table that should hold a trace of it afterwards.

The happy path is split into one assertion per test on purpose. A single test with
forty asserts reports the first broken column and hides the other thirty-nine.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(minutes_ago):
    return BASE - timedelta(minutes=minutes_ago)


ACC, PLACER, PAT = 'ACC-HP-1', 'PL-HP-1', 'P0001'


def send_happy(pipe, with_report=True, map_ae=True):
    if map_ae:
        pipe.exec("INSERT INTO aetitle_modality_map (aetitle, modality) VALUES ('CT64_RH', 'CT') "
                  "ON CONFLICT DO NOTHING")
    msgs = [
        k.adt('A08', when=k.hl7(at(330))),
        k.order(PLACER, when=k.hl7(at(320))),
        k.status('SC', ACC, k.hl7(at(300)), placer=PLACER),
        k.status('AR', ACC, k.hl7(at(280))),
        k.status('IP', ACC, k.hl7(at(260))),
        k.status('CM', ACC, k.hl7(at(240))),
    ]
    if with_report:
        msgs.append(k.report(ACC, k.hl7(at(200))))
    assert all(pipe.send_all(msgs))
    return msgs


@pytest.fixture
def happy(pipe):
    send_happy(pipe)
    return pipe


# ── the archive and RAY7's verdicts ───────────────────────────────────────────

def test_every_message_is_archived(happy):
    assert happy.count('hl7_message_archive') == 7


def test_every_message_is_accepted(happy):
    assert {r['ray7_status'] for r in happy.archive()} == {'accepted'}


def test_every_message_is_parsed_and_projected(happy):
    rows = happy.archive()
    assert all(r['parse_status'] == 'ok' and r['parsed_at'] and r['projected_at'] for r in rows)


def test_a_clean_study_raises_no_findings(happy):
    assert happy.codes() == []


# ── the lifecycle log and the per-study state ─────────────────────────────────

def test_events_carry_every_rung_under_the_accession(happy):
    """Including the order: the ORM NW was the provisional key's event, and the rekey
    has to bring it along or the study looks like it was never ordered."""
    assert [e['canonical_state'] for e in happy.events(ACC)] == [
        'ordered', 'scheduled', 'arrived', 'started', 'completed', 'signed_final']


def test_no_event_is_left_without_an_accession(happy):
    assert happy.count('hl7_study_events', 'accession_number IS NULL') == 0


@pytest.mark.parametrize('column,minutes', [
    ('ordered_at', 320), ('scheduled_at', 300), ('arrived_at', 280), ('started_at', 260),
    ('completed_at', 240), ('signed_final_at', 200), ('reported_at', 200),
])
def test_state_rung_times(happy, column, minutes):
    assert happy.state(ACC)[column] == at(minutes)


@pytest.mark.parametrize('column,want', [
    ('placer_order_number', PLACER), ('patient_id', PAT), ('modality', 'CT'),
    ('aetitle', 'CT64_RH'), ('room_name', 'CT-ROOM-2'), ('procedure_code', 'CTABD'),
    ('procedure_text', 'CT ABDOMEN AND PELVIS'), ('patient_class', 'O'),
    ('patient_location', 'RAD'), ('current_rank', 140), ('is_closed', True),
    ('is_provisional', False), ('pacs_completed_at', None), ('cancelled_at', None),
])
def test_state_columns(happy, column, want):
    assert happy.state(ACC)[column] == want


def test_one_state_row_and_no_provisional_left(happy):
    assert happy.count('ray7_study_state') == 1
    assert happy.count('ray7_study_state', "accession_number LIKE '~ORD:%'") == 0


def test_no_provisional_key_leaks_anywhere(happy):
    for table, col in (('hl7_study_events', 'accession_number'),
                       ('ray7_findings', 'accession_number'),
                       ('etl_orders', 'accession_number'),
                       ('etl_didb_studies', 'accession_number')):
        assert happy.count(table, f"{col} LIKE '~ORD:%'") == 0, table


# ── etl_didb_studies: what every report reads ─────────────────────────────────

def _age(on, born):
    return round((on - born).days / 365.25, 2)


@pytest.mark.parametrize('column,want', [
    ('study_date', at(240).date()),
    ('study_time', at(240).strftime('%H:%M:%S')),
    ('insert_time', at(240)),                         # the TAT anchor = exam completion
    ('storing_ae', 'CT64_RH'),
    ('study_modality', 'CT'),
    ('study_description', 'CT ABDOMEN AND PELVIS'),
    ('procedure_code', 'CTABD'),
    ('patient_class', 'O'),
    ('patient_location', 'RAD'),
    ('order_status', 'CM'),
    ('study_status', 'closed'),
    ('study_has_report', True),
    ('rep_final_timestamp', at(200)),
    ('rep_final_signed_by', 'rad@sjh.com'),
    ('reading_physician_id', 'rad@sjh.com'),
    ('referring_physician_first_name', 'GERARD'),
    ('referring_physician_last_name', 'WAKIM'),
    ('is_linked_study', False),
])
def test_projected_study_column(happy, column, want):
    assert happy.study(ACC)[column] == want


def test_projected_age_at_exam(happy):
    from datetime import date
    assert float(happy.study(ACC)['age_at_exam']) == _age(at(240).date(), date(1985, 3, 12))


def test_study_and_patient_surrogates_join(happy):
    s = happy.study(ACC)
    assert s['study_db_uid'] == happy.scalar("SELECT hl7_surrogate_id('study', :a)", a=ACC)
    assert s['patient_db_uid'] == happy.patient(PAT)['patient_db_uid']


def test_exactly_one_projected_study(happy):
    assert happy.count('etl_didb_studies') == 1


# ── etl_orders and etl_patient_view ───────────────────────────────────────────

@pytest.mark.parametrize('column,want', [
    ('accession_number', ACC), ('patient_dbid', PAT), ('proc_id', 'CTABD'),
    ('proc_text', 'CT ABDOMEN AND PELVIS'), ('order_status', 'CM'), ('modality', 'CT'),
    ('has_study', True), ('scheduled_datetime', at(300)),
])
def test_projected_order_column(happy, column, want):
    assert happy.order(PLACER)[column] == want


def test_the_order_links_to_the_study(happy):
    assert happy.order(PLACER)['study_db_uid'] == happy.study(ACC)['study_db_uid']


def test_exactly_one_projected_order(happy):
    assert happy.count('etl_orders') == 1


@pytest.mark.parametrize('column,want', [
    ('id', PAT), ('fallback_id', PAT), ('sex', 'F'), ('gender', 'F'),
    ('number_of_patient_studies', 1),
])
def test_projected_patient_column(happy, column, want):
    assert happy.patient(PAT)[column] == want


def test_projected_patient_age_group_is_whole_years_as_text(happy):
    ag = happy.patient(PAT)['age_group']
    assert ag.isdigit() and 30 <= int(ag) <= 50


# ── the legacy tables and the RIS worklist shape ──────────────────────────────

def test_one_order_row_not_one_per_lifecycle_message(happy):
    assert happy.count('hl7_orders') == 1


def test_report_row(happy):
    r = happy.one("SELECT * FROM hl7_oru_reports WHERE accession_number = :a", a=ACC)
    assert 'No acute abnormality.' in r['report_text']
    assert r['impression_text'] == 'Normal study.'
    assert r['physician_id'] == 'rad@sjh.com'
    assert r['result_datetime'] == at(200)


@pytest.mark.parametrize('table,column,minutes', [
    ('std_worklist_scheduled', 'scheduled_at', 300),
    ('std_worklist_arrivals', 'arrived_at', 280),
    ('std_worklist_exam_done', 'exam_done_at', 240),
])
def test_worklist_projection(happy, table, column, minutes):
    rows = happy.q(f"SELECT {column} AS t FROM {table}")
    assert [r['t'] for r in rows] == [at(minutes)]


def test_pps_projection(happy):
    r = happy.one("SELECT * FROM std_pps")
    assert (r['start_datetime'], r['end_datetime'], r['performing_ae_title']) == \
           (at(260), at(240), 'CT64_RH')
    assert r['study_db_uid'] == happy.study(ACC)['study_db_uid']


def test_reprojection_is_idempotent(happy):
    """The projector runs after every event; running it again must change nothing."""
    before = happy.snapshot()
    from utils.hl7_project import project_study, project_worklist
    with happy.app.app_context():
        from db import db
        project_study(ACC)
        project_worklist(ACC)
        db.session.commit()
    assert happy.snapshot() == before
    assert happy.count('std_worklist_arrivals') == 1


# ── what an unscheduled order is, and is not ──────────────────────────────────

def test_an_order_alone_is_an_order_not_a_study(pipe):
    pipe.send(k.order(PLACER, when=k.ts(30)))
    st = pipe.state(f'~ORD:{PLACER}')
    assert st['is_provisional'] and st['ordered_at'] is not None
    assert pipe.count('etl_didb_studies') == 0
    o = pipe.order(PLACER)
    assert (o['order_status'], o['has_study'], o['accession_number'], o['study_db_uid']) == \
           ('NW', False, None, None)


def test_an_order_that_already_carries_an_accession_is_still_not_a_study(pipe):
    """Live traffic on 2026-09-24: 300 NW orders with OBR-3 filled became 300 study
    rows with no study_date and no insert_time."""
    pipe.send(k.order(PLACER, acc=ACC, when=k.ts(30)))
    assert pipe.count('etl_didb_studies') == 0
    assert pipe.order(PLACER)['order_status'] == 'NW'


def test_scheduling_rekeys_the_provisional_lifecycle(pipe):
    pipe.send(k.order(PLACER, when=k.hl7(at(60))))
    pipe.send(k.status('SC', ACC, k.hl7(at(50)), placer=PLACER))
    st = pipe.state(ACC)
    assert st['ordered_at'] == at(60) and st['scheduled_at'] == at(50)
    assert not st['is_provisional']
    assert pipe.count('ray7_study_state') == 1
    assert pipe.count('etl_orders') == 1


def test_an_order_delivered_after_its_scheduling_joins_the_same_study(pipe):
    """SC first, then the NW it came from. The NW carries only the placer number; it
    must fold into the study SC created rather than open a second, provisional one."""
    pipe.send(k.status('SC', ACC, k.hl7(at(50)), placer=PLACER))
    pipe.send(k.order(PLACER, when=k.hl7(at(60))))
    assert pipe.count('ray7_study_state') == 1
    assert pipe.state(ACC)['ordered_at'] == at(60)
    assert pipe.count('etl_orders') == 1


def test_scheduling_twice_does_not_split_the_study(pipe):
    pipe.send(k.order(PLACER, when=k.hl7(at(60))))
    pipe.send(k.status('SC', ACC, k.hl7(at(50)), placer=PLACER))
    pipe.send(k.status('SC', ACC, k.hl7(at(49)), placer=PLACER))
    assert pipe.count('ray7_study_state') == 1
    assert pipe.state(ACC)['scheduled_at'] == at(50)


def test_one_order_two_accessions(pipe):
    """A single HIS order the RIS splits into two exams."""
    pipe.send(k.order(PLACER, when=k.hl7(at(60))))
    pipe.send(k.status('SC', 'ACC-A', k.hl7(at(50)), placer=PLACER))
    pipe.send(k.status('SC', 'ACC-B', k.hl7(at(49)), placer=PLACER, proc='CTCHEST'))
    assert {r['accession_number'] for r in pipe.q("SELECT accession_number FROM ray7_study_state")} \
        == {'ACC-A', 'ACC-B'}
    assert pipe.count('etl_didb_studies') == 2


# ── walk-ins, partial ladders, cancellation ───────────────────────────────────

def test_walk_in_without_scheduling(pipe):
    for code, m in (('AR', 90), ('IP', 80), ('CM', 70)):
        pipe.send(k.status(code, ACC, k.hl7(at(m))))
    s = pipe.study(ACC)
    assert s['insert_time'] == at(70) and s['order_status'] == 'CM'
    assert 'SKIPPED_RUNG' not in pipe.codes()


@pytest.mark.parametrize('codes,anchor,status', [
    (('SC',), 300, 'SC'), (('SC', 'AR'), 280, 'AR'), (('SC', 'AR', 'IP'), 260, 'IP'),
    (('SC', 'AR', 'IP', 'CM'), 240, 'CM'),
])
def test_insert_time_falls_back_down_the_ladder(pipe, codes, anchor, status):
    times = dict(zip(('SC', 'AR', 'IP', 'CM'), (300, 280, 260, 240)))
    for c in codes:
        pipe.send(k.status(c, ACC, k.hl7(at(times[c]))))
    s = pipe.study(ACC)
    assert (s['insert_time'], s['order_status']) == (at(anchor), status)


def test_study_status_open_until_final_signature(pipe):
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, ACC, k.hl7(at(m))))
    assert pipe.study(ACC)['study_status'] == 'open'
    assert pipe.state(ACC)['is_closed'] is False


@pytest.mark.parametrize('code', ['CA', 'DC', 'ER'])
def test_cancellation(pipe, code):
    pipe.send(k.status('SC', ACC, k.hl7(at(100))))
    pipe.send(k.status(code, ACC, k.hl7(at(90))))
    st, s = pipe.state(ACC), pipe.study(ACC)
    assert st['cancelled_at'] == at(90) and st['is_closed']
    assert (s['order_status'], s['study_status']) == ('CA', 'cancelled')


def test_cancellation_does_not_walk_the_ladder_back(pipe):
    pipe.send(k.status('SC', ACC, k.hl7(at(100))))
    pipe.send(k.status('CA', ACC, k.hl7(at(90))))
    assert pipe.state(ACC)['current_rank'] == 40


def test_late_lower_rung_never_overwrites_a_higher_one(pipe):
    pipe.send(k.status('CM', ACC, k.hl7(at(240))))
    pipe.send(k.status('AR', ACC, k.hl7(at(280))))
    st = pipe.state(ACC)
    assert (st['current_rank'], st['completed_at'], st['arrived_at']) == (100, at(240), at(280))


def test_repeated_rung_keeps_the_first_time(pipe):
    pipe.send(k.status('AR', ACC, k.hl7(at(280))))
    pipe.send(k.status('AR', ACC, k.hl7(at(100))))
    assert pipe.state(ACC)['arrived_at'] == at(280)


# ── isolation ─────────────────────────────────────────────────────────────────

def test_interleaved_studies_do_not_bleed(pipe):
    a = k.lifecycle('ACC-A', 100, pat='PA', ae='CT64_RH', mod='CT')
    b = k.lifecycle('ACC-B', 95, pat='PB', ae='MR1', mod='MR', proc='MRBRAIN', desc='MRI BRAIN')
    for (_, x), (_, y) in zip(a, b):
        pipe.send(x)
        pipe.send(y)
    sa, sb = pipe.study('ACC-A'), pipe.study('ACC-B')
    assert (sa['study_modality'], sa['storing_ae']) == ('CT', 'CT64_RH')
    assert (sb['study_modality'], sb['storing_ae'], sb['procedure_code']) == ('MR', 'MR1', 'MRBRAIN')
    assert sa['patient_db_uid'] != sb['patient_db_uid']


def test_two_studies_one_patient(pipe):
    for acc in ('ACC-A', 'ACC-B'):
        for _, raw in k.lifecycle(acc, 100):
            pipe.send(raw)
    assert pipe.patient(PAT)['number_of_patient_studies'] == 2
    assert len({r['patient_db_uid'] for r in pipe.q("SELECT patient_db_uid FROM etl_didb_studies")}) == 1


def test_study_without_a_patient_gets_its_own_synthetic_patient(pipe):
    for acc in ('ACC-A', 'ACC-B'):
        pipe.send(k.status('CM', acc, k.ts(10), pat=None))
    uids = [r['patient_db_uid'] for r in pipe.q("SELECT patient_db_uid FROM etl_didb_studies")]
    assert len(uids) == 2 and len(set(uids)) == 2 and None not in uids


def test_long_patient_location_is_truncated_not_fatal(pipe):
    pipe.send(k.status('CM', ACC, k.ts(10), loc='EMERGENCY^BAY-4^^RH'))
    assert pipe.study(ACC)['patient_location'] == 'EME'


@pytest.mark.parametrize('n', [1, 5, 25])
def test_many_studies(pipe, n):
    for i in range(n):
        for _, raw in k.lifecycle(f'ACC-{i:04d}', 200, pat=f'P{i:04d}'):
            pipe.send(raw)
    assert pipe.count('etl_didb_studies') == n
    assert pipe.count('etl_didb_studies', "order_status = 'CM'") == n
    assert pipe.count('hl7_study_events') == 4 * n
