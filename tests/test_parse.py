"""
utils/hl7_parse.py — primitives, the dispatcher, and the ParsedMessage contract.

The dispatcher resolves status codes through hl7_status_map, so the message-level
tests run inside an app context against the real seeded map.
"""
import random
from datetime import datetime, date, timedelta

import pytest

import hl7kit as k
from utils import hl7_parse as hp


@pytest.fixture
def parse(app):
    def _parse(raw):
        with app.app_context():
            return hp.parse_message(raw, source_ip='10.0.0.7')
    return _parse


# ── segment primitives ────────────────────────────────────────────────────────

@pytest.mark.parametrize('raw', [
    'MSH|a\rPID|b\rOBR|c', 'MSH|a\nPID|b\nOBR|c', 'MSH|a\r\nPID|b\r\nOBR|c',
    'MSH|a\r\n\rPID|b\n\nOBR|c\r', '  MSH|a \r PID|b\rOBR|c  \r\r',
])
def test_split_segments_normalises_every_line_ending(raw):
    assert [s.split('|')[0] for s in hp.split_segments(raw)] == ['MSH', 'PID', 'OBR']


@pytest.mark.parametrize('raw', [None, '', '\r\r', '\n \n'])
def test_split_segments_of_nothing(raw):
    assert hp.split_segments(raw) == []


def test_seg_takes_the_first_and_never_a_prefix_match():
    segs = ['OBRX|wrong', 'OBR|1|first', 'OBR|2|second']
    assert hp.seg(segs, 'OBR')[2] == 'first'
    assert hp.seg(segs, 'ORC') == []


def test_all_segs_keeps_repeats_in_order():
    segs = ['OBX|1|a', 'NTE|x', 'OBX|2|b']
    assert [s[2] for s in hp.all_segs(segs, 'OBX')] == ['a', 'b']


@pytest.mark.parametrize('segment,index,default,want', [
    (['PID', '', '', 'X'], 3, None, 'X'),
    (['PID', '', '', '  X  '], 3, None, 'X'),
    (['PID', '', '', '   '], 3, 'd', 'd'),
    (['PID'], 9, 'd', 'd'),
    ([], 1, None, None),
])
def test_field(segment, index, default, want):
    assert hp.field(segment, index, default) == want


@pytest.mark.parametrize('value,index,want', [
    ('A^B^C', 0, 'A'), ('A^B^C', 2, 'C'), ('A^B', 5, None), ('A^ ^C', 1, None),
    ('', 0, None), (None, 0, None),
])
def test_component(value, index, want):
    assert hp.component(value, index) == want


# ── timestamps ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('raw,want', [
    ('20261025103015', datetime(2026, 10, 25, 10, 30, 15)),
    ('202610251030', datetime(2026, 10, 25, 10, 30)),
    ('20261025', datetime(2026, 10, 25)),
    ('20261025103015.1234', datetime(2026, 10, 25, 10, 30, 15)),
    ('20261025103015+0300', datetime(2026, 10, 25, 10, 30, 15)),
    ('20261025103015-0500', datetime(2026, 10, 25, 10, 30, 15)),
    ('20261025103015.12+0200', datetime(2026, 10, 25, 10, 30, 15)),
    ('  20261025103015  ', datetime(2026, 10, 25, 10, 30, 15)),
    # DTM allows hour precision (YYYYMMDDHH). Truncating it to midnight moves the
    # event by up to 23 hours.
    ('2026102510', datetime(2026, 10, 25, 10)),
])
def test_parse_hl7_datetime(raw, want):
    assert hp.parse_hl7_datetime(raw) == want


@pytest.mark.parametrize('raw', [None, '', 'garbage', '2026', '20261340', '20260230',
                                 'ABCDEFGHIJKLMN', '99999999999999'])
def test_parse_hl7_datetime_rejects(raw):
    assert hp.parse_hl7_datetime(raw) is None


def test_parse_hl7_datetime_is_always_naive():
    assert hp.parse_hl7_datetime('20261025103015+0300').tzinfo is None


# ── names ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('raw,want', [
    ('DOE^JANE^M', 'JANE M DOE'), ('DOE^JANE', 'JANE DOE'), ('DOE', 'DOE'),
    ('GP42^^Khaled Hadla', 'Khaled Hadla'), ('', None), (None, None), ('^^', None),
])
def test_format_name(raw, want):
    assert hp.format_name(raw) == want


@pytest.mark.parametrize('raw,want', [
    ('20000191^WAKIM^GERARD', 'GERARD WAKIM'), ('20000191^WAKIM', 'WAKIM'),
    ('20000191^^', None), ('', None), (None, None),
])
def test_xcn_name_never_folds_the_id_in(raw, want):
    assert hp.xcn_name(raw) == want


def test_signing_physician_matches_the_report_time():
    obr = ['OBR', 'a@x.com&Ann&One^^20261025090000', 'b@x.com&Bob&Two^^20261025100000']
    assert hp.extract_signing_physician(obr, '20261025100000') == ('b@x.com', 'Bob Two')


def test_signing_physician_falls_back_to_the_first_stamp():
    obr = ['OBR', 'a@x.com&Ann&One^^20261025090000', 'b@x.com&Bob&Two^^20261025100000']
    assert hp.extract_signing_physician(obr, '20200101000000') == ('a@x.com', 'Ann One')


def test_signing_physician_absent():
    assert hp.extract_signing_physician(['OBR', 'nothing', ''], '2026') == (None, None)


# ── birth dates ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize('raw', ['99991111', '00000000', '18000101', '999911110000'])
def test_birth_date_placeholders_are_nulled_and_reported(raw):
    notes = []
    assert hp.parse_birth_date(raw, notes) is None
    assert notes and 'placeholder' in notes[0]


@pytest.mark.parametrize('raw,note', [('18790101', 'before 1880'),
                                      ((date.today() + timedelta(days=3)).strftime('%Y%m%d'),
                                       'in the future')])
def test_birth_date_implausible(raw, note):
    notes = []
    assert hp.parse_birth_date(raw, notes) is None
    assert note in notes[0]


@pytest.mark.parametrize('raw,want', [('19850312', date(1985, 3, 12)),
                                      ('19850312083000', date(1985, 3, 12)),
                                      ('', None), ('junk', None)])
def test_birth_date_valid(raw, want):
    notes = []
    assert hp.parse_birth_date(raw, notes) == want
    assert notes == []


# ── envelope ──────────────────────────────────────────────────────────────────

def test_envelope_reads_msh_with_the_off_by_one(parse):
    m = parse(k.message(k.msh('RIS', 'CTRL-1', 'ORM^O01', '20261025100000', 'FAC', '2.3'),
                        k.orc('SC', acc='A1', status='AR', when='20261025100000'),
                        k.obr(acc='A1')))
    assert (m.sending_app, m.sending_facility, m.message_type, m.control_id, m.hl7_version) == \
           ('RIS', 'FAC', 'ORM^O01', 'CTRL-1', '2.3')
    assert m.message_datetime == datetime(2026, 10, 25, 10)
    assert m.source_ip == '10.0.0.7'


def test_blank_control_id_falls_back_to_the_content_hash(parse):
    raw = k.message(k.seg('MSH', {2: r'^~\&', 3: 'RIS', 9: 'ORM^O01'}),
                    k.orc('SC', acc='A1', status='AR'))
    m = parse(raw)
    assert m.control_id == m.content_hash and len(m.content_hash) == 64


# ── dispatch: what kind of thing is this ──────────────────────────────────────

@pytest.mark.parametrize('event', ['A01', 'A04', 'A08', 'A28', 'A31', 'A03'])
def test_adt_events_are_adt(parse, event):
    m = parse(k.adt(event))
    assert m.kind == 'adt'
    assert (m.patient_id, m.patient_name, m.birth_date, m.sex) == \
           ('P0001', 'JANE M DOE', date(1985, 3, 12), 'F')
    assert (m.patient_class, m.patient_location) == ('O', 'EM')


def test_adt_placeholder_is_recorded(parse):
    m = parse(k.adt('A01', dob='99991111', sex='U'))
    assert m.birth_date is None
    assert len(m.placeholders) == 2


@pytest.mark.parametrize('mtype', ['ORM^O01', 'OMG^O19', 'OMI^O23', 'ORR^O02'])
def test_order_family_new_order(parse, mtype):
    raw = k.message(k.msh('SAP_HIS', None, mtype, k.ts(10)), k.pid(),
                    k.orc('NW', placer='PL1', when=k.ts(10)), k.obr(placer='PL1'))
    m = parse(raw)
    assert (m.kind, m.canonical_state, m.ladder_rank) == ('order', 'ordered', 20)
    assert m.placer_order_number == 'PL1' and m.accession_number is None


def test_new_order_is_recognised_in_lower_case(parse):
    raw = k.message(k.msh('SAP_HIS'), k.orc('nw', placer='PL1'), k.obr(placer='PL1'))
    assert parse(raw).kind == 'order'


@pytest.mark.parametrize('when_field', ['orc9', 'obr7', 'msh7'])
def test_order_event_time_fallbacks(parse, when_field):
    t = datetime(2026, 10, 25, 8, 15)
    orc9 = k.hl7(t) if when_field == 'orc9' else None
    obr7 = k.hl7(t) if when_field == 'obr7' else None
    mshw = k.hl7(t) if when_field == 'msh7' else '20261025230000'
    if when_field != 'msh7':
        mshw = '20261025230000'
    raw = k.message(k.msh('SAP_HIS', None, 'ORM^O01', mshw), k.orc('NW', placer='PL', when=orc9),
                    k.obr(placer='PL', obr7=obr7))
    assert parse(raw).event_time == t


def test_order_carries_the_referring_doctor(parse):
    m = parse(k.order('PL1'))
    assert (m.performed_by_id, m.performed_by_name, m.performed_by_role) == \
           ('20000191', 'GERARD WAKIM', 'referring')


@pytest.mark.parametrize('code,state,rank', [
    ('SC', 'scheduled', 40), ('AR', 'arrived', 60), ('IP', 'started', 70),
    ('CM', 'completed', 100), ('CA', 'cancelled', -1), ('DC', 'cancelled', -1),
    ('ER', 'cancelled', -1), ('ip', 'started', 70),
])
def test_status_codes_resolve(parse, code, state, rank):
    m = parse(k.status(code, 'A1', k.ts(5)))
    assert (m.kind, m.canonical_state, m.ladder_rank) == ('status', state, rank)


def test_status_carries_attribution_device_and_room(parse):
    m = parse(k.status('IP', 'A1', k.ts(5)))
    assert (m.performed_by_id, m.performed_by_name) == ('TECH01', 'RIMA HADDAD')
    assert (m.aetitle, m.room_name, m.modality) == ('CT64_RH', 'CT-ROOM-2', 'CT')
    assert (m.procedure_code, m.procedure_text) == ('CTABD', 'CT ABDOMEN AND PELVIS')
    assert (m.patient_class, m.patient_location, m.patient_id) == ('O', 'RAD', 'P0001')


def test_status_event_time_prefers_orc9_over_msh7(parse):
    m = parse(k.status('AR', 'A1', '20261025090000', msh_when='20261025093000'))
    assert m.event_time == datetime(2026, 10, 25, 9)


def test_status_event_time_falls_back_to_msh7(parse):
    m = parse(k.status('AR', 'A1', '20261025090000', msh_when='20261025093000', orc9=False))
    assert m.event_time == datetime(2026, 10, 25, 9, 30)


def test_accession_from_obr3_when_orc3_is_empty(parse):
    raw = k.message(k.msh('RIS'), k.orc('SC', status='AR'), k.obr(acc='FROM-OBR'))
    assert parse(raw).accession_number == 'FROM-OBR'


def test_orc3_wins_over_obr3(parse):
    raw = k.message(k.msh('RIS'), k.orc('SC', acc='FROM-ORC', status='AR'), k.obr(acc='FROM-OBR'))
    assert parse(raw).accession_number == 'FROM-ORC'


def test_visit_number_from_pv1_19(parse):
    assert parse(k.status('AR', 'A1', visit='V77')).visit_number == 'V77'


@pytest.mark.parametrize('obr24,want', [('CT', 'CT'), ('US^US', 'US'),
                                        ('MR^Magnetic Resonance^DCM', 'MR')])
def test_modality_is_the_code_not_the_whole_coded_element(parse, obr24, want):
    """Live traffic on 2026-09-24 projected every study with modality 'CT^CT'."""
    assert parse(k.status('AR', 'A1', mod=obr24)).modality == want


def test_unmapped_status_keeps_the_raw_values(parse):
    m = parse(k.status('ZZ', 'A1'))
    assert (m.kind, m.canonical_state, m.ladder_rank) == ('status', None, None)
    assert (m.raw_order_control, m.raw_order_status) == ('SC', 'ZZ')


@pytest.mark.parametrize('control', ['CA', 'OC', 'DC'])
def test_cancellation_carried_in_orc1_alone(parse, control):
    """Order control CA / OC / DC with ORC-5 empty is how many senders cancel."""
    raw = k.message(k.msh('RIS'), k.pid(), k.orc(control, acc='A1', when=k.ts(5)),
                    k.obr(acc='A1'))
    m = parse(raw)
    assert (m.kind, m.canonical_state) == ('status', 'cancelled')


# ── results ───────────────────────────────────────────────────────────────────

def test_result_basics(parse):
    m = parse(k.report('A1', '20261025110000'))
    assert (m.kind, m.accession_number, m.event_time) == \
           ('result', 'A1', datetime(2026, 10, 25, 11))
    assert (m.canonical_state, m.ladder_rank, m.raw_result_status) == ('signed_final', 140, 'F')
    assert (m.performed_by_id, m.performed_by_name, m.performed_by_role) == \
           ('rad@sjh.com', 'Dany Abou Chedid', 'radiologist')


@pytest.mark.parametrize('statuses,state', [
    (['W'], 'signed_prelim'), (['P'], 'signed_prelim'), (['F'], 'signed_final'),
    (['C'], 'signed_final'), (['W', 'F'], 'signed_final'), (['F', 'W'], 'signed_final'),
    (['W', 'W', 'F'], 'signed_final'), (['X', 'W'], 'signed_prelim'), (['W', 'X'], 'signed_prelim'),
])
def test_the_most_final_obx_wins(parse, statuses, state):
    assert parse(k.report('A1', obx_statuses=statuses)).canonical_state == state


def test_unmapped_result_status_is_remembered(parse):
    m = parse(k.report('A1', obx_statuses=['X', 'Y']))
    assert (m.canonical_state, m.ladder_rank, m.raw_result_status) == (None, None, 'X')


def test_obr25_is_the_fallback_signature(parse):
    m = parse(k.report('A1', obx_statuses=['', ''], obr25='W'))
    assert (m.canonical_state, m.raw_result_status) == ('signed_prelim', 'W')


def test_result_time_falls_back_to_obr7(parse):
    raw = k.message(k.msh('PACS', None, 'ORU^R01'), k.pid(),
                    k.obr(acc='A1', obr7='20261025070000'), k.obx('text'))
    assert parse(raw).event_time == datetime(2026, 10, 25, 7)


# ── anything else, and anything broken ────────────────────────────────────────

@pytest.mark.parametrize('mtype', ['MDM^T02', 'ACK', 'SIU^S12', 'QRY^A19', '', 'DFT^P03'])
def test_unhandled_types_are_other(parse, mtype):
    assert parse(k.message(k.msh('X', None, mtype), k.pid())).kind == 'other'


@pytest.mark.parametrize('raw', [
    '', '\r', 'not hl7 at all', 'PID|||123', 'MSH|', 'MSH|^~\\&', 'ZZZ|\x01\x02',
    'MSH|^~\\&|RIS||||||ORM^O01|X|P|2.4\rORC|SC', 'MSH#^~\\&#RIS#####ORM^O01#X',
])
def test_malformed_input_never_raises_and_is_other_or_status(parse, raw):
    m = parse(raw)
    assert m.kind in ('other', 'status')
    assert m.control_id and m.content_hash


@pytest.mark.parametrize('seed', range(30))
def test_fuzz_parse_never_raises(parse, seed):
    rnd = random.Random(seed)
    alphabet = 'MSHPIDORCOBRX|^~\\&\r\n 0123456789ABCDEFNWSCARIPCM\x00\x0b\x1c\xe9'
    body = ''.join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 600)))
    if rnd.random() < 0.6:
        body = 'MSH|^~\\&|RIS|||||' + rnd.choice(['ORM^O01', 'ORU^R01', 'ADT^A08']) + '|' + body
    m = parse(body)
    assert m.kind in ('other', 'status', 'order', 'result', 'adt')
