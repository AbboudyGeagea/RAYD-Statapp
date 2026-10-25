"""
What the pipeline does with input nobody designed for, under concurrency, and when
its own parts fail. The invariant everywhere: the message is archived and the
sender gets AA; whatever else goes wrong is recoverable by replay.
"""
import random
import threading
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


def no_dead_letters(pipe):
    return pipe.count('hl7_message_archive', "parse_status = 'error'") == 0


# ── hostile and oversized input ───────────────────────────────────────────────

@pytest.mark.parametrize('acc', [
    "A1'; DROP TABLE ray7_rules; --", 'A1"quoted"', 'A1\\escaped\\', 'A1%_like',
    '~ORD:spoofed', 'A' * 300, '  padded  ', 'ACC/2026/1', 'ÄCC-ü',
])
def test_strange_accessions_are_stored_literally(pipe, acc):
    assert pipe.send(k.status('SC', acc, k.ts(5)))
    assert pipe.count('hl7_message_archive') == 1
    assert pipe.count('ray7_rules') > 20
    assert no_dead_letters(pipe)


def test_a_spoofed_provisional_key_is_not_a_provisional_study(pipe):
    """'~ORD:' is RAYD's own marker. An accession that happens to start with it must
    not be mistaken for an unscheduled order and hidden from the reports."""
    pipe.send(k.status('CM', '~ORD:X', k.ts(5)))
    assert pipe.count('ray7_study_state', 'is_provisional') == 0


def test_nul_byte_is_archived(pipe):
    assert pipe.send(k.report('A1', k.ts(5), text='a\x00b'))
    assert pipe.count('hl7_message_archive') == 1


def test_five_megabyte_message(pipe):
    assert pipe.send(k.report('A1', k.ts(5), text='y' * 5_000_000))
    assert pipe.count('hl7_message_archive') == 1


def test_thousands_of_obx_segments(pipe):
    raw = k.message(k.msh('PACS', None, 'ORU^R01', k.ts(5)), k.pid(),
                    k.obr(acc='A1', result_dt=k.ts(5)),
                    *[k.obx(f'line {i}', 'F', set_id=i) for i in range(3000)])
    assert pipe.send(raw)
    assert 'line 2999' in pipe.one("SELECT report_text FROM hl7_oru_reports")['report_text']


@pytest.mark.parametrize('seed', range(40))
def test_fuzzed_messages_are_always_archived(pipe, seed):
    rnd = random.Random(1000 + seed)
    base = rnd.choice([k.status('AR', 'F1', k.ts(5)), k.report('F1', k.ts(5)),
                       k.order('PLF', when=k.ts(5)), k.adt('A08')])
    chars = list(base)
    for _ in range(rnd.randint(1, 25)):
        i = rnd.randrange(len(chars))
        op = rnd.random()
        if op < 0.4:
            chars[i] = rnd.choice('|^~\\&\r\nXZ09 \x0b\x1c\xe9')
        elif op < 0.7:
            del chars[i]
        else:
            chars.insert(i, rnd.choice('|^\r'))
    raw = ''.join(chars)
    assert pipe.send(raw)
    assert pipe.count('hl7_message_archive') == 1


@pytest.mark.xfail(strict=True, reason=(
    "KNOWN GAP: the pipeline reads one ORC/OBR group per message (parse_message, "
    "parse_orm_o01, the archive-keyed hl7_orders row and the (message, rule) finding "
    "key all assume it), so a second procedure in the same ORM is archived but never "
    "ingested. Needs per-group ingestion — a contract change, not a patch."))
def test_multiple_procedures_in_one_order(pipe):
    """One ORM, two ORC/OBR groups: two exams ordered together. Each is an order."""
    raw = k.message(k.msh('SAP_HIS', None, 'ORM^O01', k.ts(30)), k.pid(), k.pv1(),
                    k.orc('NW', placer='PL-1', when=k.ts(30)),
                    k.obr(placer='PL-1', code='CTCHEST', desc='CT CHEST'),
                    k.orc('NW', placer='PL-2', when=k.ts(30)),
                    k.obr(placer='PL-2', code='CTABD', desc='CT ABDOMEN'))
    pipe.send(raw)
    assert pipe.count('etl_orders') == 2


def test_screening_stays_inside_its_budget_under_volume(pipe):
    """RAY7 runs inside the sender's ACK window with a 250 ms budget. 400 messages
    in a row must never blow it, and the whole path must stay far from a timeout."""
    import time
    t0 = time.time()
    for i in range(100):
        for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
            assert pipe.send(k.status(code, f'V{i:04d}', k.hl7(at(m)), pat=f'PV{i:04d}'))
    per_message = (time.time() - t0) / 400
    assert pipe.stat('RAY7_BUDGET_EXCEEDED') == 0
    assert pipe.stat('RAY7_DEGRADED') == 0
    assert pipe.stat('MESSAGES') == 400
    assert per_message < 0.5, f'{per_message * 1000:.0f} ms per message'


# ── internal failure ──────────────────────────────────────────────────────────

def test_processing_failure_dead_letters_but_keeps_the_message(pipe, monkeypatch):
    from utils import hl7_ingest

    def boom(msg, archive_id):
        raise RuntimeError('persist bug')
    monkeypatch.setattr(hl7_ingest, 'persist_parsed', boom)
    assert pipe.send(k.status('SC', 'A1', k.ts(5))) is True
    a = pipe.archive()
    assert len(a) == 1 and a[0]['parse_status'] == 'error'
    assert 'persist bug' in a[0]['parse_error']


def test_dead_letter_is_recoverable_by_replay(pipe, monkeypatch):
    from utils import hl7_ingest
    real = hl7_ingest.persist_parsed
    monkeypatch.setattr(hl7_ingest, 'persist_parsed',
                        lambda m, a: (_ for _ in ()).throw(RuntimeError('x')))
    pipe.send(k.status('SC', 'A1', k.hl7(at(5))))
    monkeypatch.setattr(hl7_ingest, 'persist_parsed', real)
    pipe.replay()
    assert pipe.state('A1')['scheduled_at'] == at(5)


def test_a_failed_projection_costs_only_the_projection(pipe, monkeypatch):
    from utils import hl7_project
    monkeypatch.setattr(hl7_project, '_STUDY_SQL', 'SELECT no_such_function()')
    assert pipe.send(k.status('SC', 'A1', k.ts(5)))
    assert pipe.state('A1') is not None
    assert no_dead_letters(pipe)


def test_a_failed_finding_write_costs_only_the_finding(pipe, monkeypatch):
    from utils import ray7
    monkeypatch.setattr(ray7, '_json', lambda obj: 'not json')
    assert pipe.send(k.adt('A01', dob='99991111'))          # raises PLACEHOLDER_DATA
    assert pipe.findings() == []
    assert pipe.count('hl7_patients') == 1


# ── concurrency inside the pipeline ───────────────────────────────────────────

def _parallel(pipe, raws, workers=None):
    errors = []

    def run(raw):
        try:
            assert pipe.send(raw)
        except Exception as exc:          # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(r,)) for r in raws]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    return errors


@pytest.mark.parametrize('rep', range(8))
def test_one_study_four_events_at_once(pipe, rep):
    raws = [k.status(c, 'A1', k.hl7(at(m))) for c, m in
            (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240))]
    assert _parallel(pipe, raws) == []
    st = pipe.state('A1')
    assert (st['scheduled_at'], st['arrived_at'], st['started_at'], st['completed_at']) == \
           (at(300), at(280), at(260), at(240))
    assert (st['current_rank'], st['event_count']) == (100, 4)
    assert pipe.count('etl_didb_studies') == 1
    assert no_dead_letters(pipe)


@pytest.mark.parametrize('rep', range(8))
def test_order_and_scheduling_race(pipe, rep):
    raws = [k.order('PL1', when=k.hl7(at(320))),
            k.status('SC', 'A1', k.hl7(at(300)), placer='PL1')]
    assert _parallel(pipe, raws) == []
    assert pipe.count('ray7_study_state') == 1
    assert pipe.state('A1')['ordered_at'] == at(320)


def test_identical_message_from_five_connections(pipe):
    raw = k.status('SC', 'A1', k.ts(5))
    assert _parallel(pipe, [raw] * 5) == []
    assert pipe.count('hl7_message_archive') == 1
    assert pipe.stat('EXACT_REDELIVERY') == 4


def test_forty_studies_interleaved_across_threads(pipe):
    raws = []
    for i in range(40):
        raws += [k.status(c, f'S{i:03d}', k.hl7(at(m)), pat=f'P{i:03d}') for c, m in
                 (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240))]
    random.Random(7).shuffle(raws)
    batches = [raws[i::8] for i in range(8)]
    errors = []

    def run(batch):
        for r in batch:
            try:
                pipe.send(r)
            except Exception as exc:      # pragma: no cover
                errors.append(exc)

    threads = [threading.Thread(target=run, args=(b,)) for b in batches]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert errors == []
    assert pipe.count('etl_didb_studies', "order_status = 'CM'") == 40
    assert pipe.count('hl7_study_events') == 160
    assert no_dead_letters(pipe)
