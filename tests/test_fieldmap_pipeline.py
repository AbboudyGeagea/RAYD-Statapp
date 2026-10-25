"""
Operator field mappings (hl7_field_mappings) in the live pipeline: the overlay onto
the parse, and the direct writes into etl_*.

A mapping is how an implementation engineer adapts RAYD to a site's real messages
without a code change. If a mapped field is read but its consequences are not —
a mapped status code that never reclassifies the message, say — the screen shows a
saved mapping that does nothing.
"""
import pytest

import hl7kit as k


def mapping(pipe, target_field, segment, field_index, component_index=None, kind='',
            sending_app='', target_kind='parsed', priority=1, transform='text', active=True,
            repeat_index=None):
    pipe.exec("""
        INSERT INTO hl7_field_mappings (sending_app, message_kind, target_kind, target_field,
               segment, field_index, component_index, repeat_index, priority, transform, active)
        VALUES (:app, :kind, :tk, :tf, :seg, :fi, :ci, :ri, :pr, :tr, :ac)
    """, app=sending_app, kind=kind, tk=target_kind, tf=target_field, seg=segment,
              fi=field_index, ci=component_index, ri=repeat_index, pr=priority, tr=transform,
              ac=active)
    from conftest import _reset_caches
    _reset_caches()


def with_z(raw, *zsegs):
    return raw.rstrip('\r') + '\r' + '\r'.join(zsegs) + '\r'


def test_accession_from_a_z_segment(pipe):
    mapping(pipe, 'accession_number', 'ZDS', 1, 1, kind='status')
    pipe.send(with_z(k.status('SC', 'ORC-ACC', k.ts(5)), 'ZDS|Z-ACC^RIS'))
    assert pipe.state('Z-ACC') is not None and pipe.state('ORC-ACC') is None


def test_empty_mapped_field_keeps_the_built_in_value(pipe):
    mapping(pipe, 'accession_number', 'ZDS', 1, 1, kind='status')
    pipe.send(k.status('SC', 'ORC-ACC', k.ts(5)))
    assert pipe.state('ORC-ACC') is not None


def test_priority_chain(pipe):
    mapping(pipe, 'aetitle', 'ZXA', 1, priority=1, kind='status')
    mapping(pipe, 'aetitle', 'ZXB', 1, priority=2, kind='status')
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5)), 'ZXB|SECOND'))
    pipe.send(with_z(k.status('IP', 'A2', k.ts(5)), 'ZXA|FIRST', 'ZXB|SECOND'))
    assert pipe.state('A1')['aetitle'] == 'SECOND'
    assert pipe.state('A2')['aetitle'] == 'FIRST'


def test_sender_scoped_mapping(pipe):
    mapping(pipe, 'room_name', 'ZRM', 1, sending_app='RIS2', kind='status')
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5), app='RIS2'), 'ZRM|SCOPED'))
    pipe.send(with_z(k.status('IP', 'A2', k.ts(5), app='RIS'), 'ZRM|SCOPED'))
    assert pipe.state('A1')['room_name'] == 'SCOPED'
    assert pipe.state('A2')['room_name'] == 'CT-ROOM-2'


def test_kind_scoped_mapping(pipe):
    mapping(pipe, 'room_name', 'ZRM', 1, kind='result')
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5)), 'ZRM|NOPE'))
    assert pipe.state('A1')['room_name'] == 'CT-ROOM-2'


def test_inactive_mapping_is_ignored(pipe):
    mapping(pipe, 'room_name', 'ZRM', 1, kind='status', active=False)
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5)), 'ZRM|NOPE'))
    assert pipe.state('A1')['room_name'] == 'CT-ROOM-2'


def test_msh_field_numbers_are_human_numbers(pipe):
    mapping(pipe, 'room_name', 'MSH', 4, kind='status')       # MSH-4 sending facility
    pipe.send(k.status('IP', 'A1', k.ts(5)))
    assert pipe.state('A1')['room_name'] == 'SITE'


def test_repeat_index_selects_the_second_segment(pipe):
    mapping(pipe, 'room_name', 'ZRM', 1, kind='status', repeat_index=2)
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5)), 'ZRM|ONE', 'ZRM|TWO'))
    assert pipe.state('A1')['room_name'] == 'TWO'


def test_upper_transform(pipe):
    mapping(pipe, 'aetitle', 'ZXA', 1, kind='status', transform='upper')
    pipe.send(with_z(k.status('IP', 'A1', k.ts(5)), 'ZXA|ct64_rh'))
    assert pipe.state('A1')['aetitle'] == 'CT64_RH'


def test_datetime_transform_moves_the_event(pipe):
    mapping(pipe, 'event_time', 'ZTS', 1, kind='status', transform='datetime')
    pipe.send(with_z(k.status('AR', 'A1', k.ts(5)), 'ZTS|20261020101500'))
    from datetime import datetime
    assert pipe.state('A1')['arrived_at'] == datetime(2026, 10, 20, 10, 15)


def test_unparseable_mapped_datetime_keeps_the_built_in(pipe):
    mapping(pipe, 'event_time', 'ZTS', 1, kind='status', transform='datetime')
    pipe.send(with_z(k.status('AR', 'A1', '20261025090000'), 'ZTS|not-a-date'))
    from datetime import datetime
    assert pipe.state('A1')['arrived_at'] == datetime(2026, 10, 25, 9)


def test_mapped_status_code_reclassifies_the_message(pipe):
    """A site carrying the real status in a Z-segment maps raw_order_status there.
    The mapped code has to drive the lifecycle state, not just sit in a column."""
    mapping(pipe, 'raw_order_status', 'ZST', 1, kind='status')
    pipe.send(with_z(k.status('SC', 'A1', k.ts(30)), 'ZST|SC'))
    pipe.send(with_z(k.status('XX', 'A1', k.ts(5)), 'ZST|AR'))
    st = pipe.state('A1')
    assert st['arrived_at'] is not None
    assert 'UNKNOWN_STATUS_CODE' not in pipe.codes()


# ── direct writes into etl_* ──────────────────────────────────────────────────

def test_direct_override_lands_in_the_study_row(pipe):
    mapping(pipe, 'storing_ae', 'ZDV', 1, target_kind='study', kind='status')
    pipe.send(with_z(k.status('CM', 'A1', k.ts(5)), 'ZDV|OVERRIDDEN'))
    assert pipe.study('A1')['storing_ae'] == 'OVERRIDDEN'


def test_direct_override_wins_over_the_projection(pipe):
    mapping(pipe, 'study_description', 'ZDV', 1, target_kind='study', kind='status')
    pipe.send(with_z(k.status('CM', 'A1', k.ts(5)), 'ZDV|SITE TEXT'))
    pipe.send(with_z(k.status('CM', 'A1', k.ts(4)), 'ZDV|SITE TEXT'))
    assert pipe.study('A1')['study_description'] == 'SITE TEXT'


def test_override_onto_an_unknown_column_is_refused_quietly(pipe):
    mapping(pipe, 'no_such_column', 'ZDV', 1, target_kind='study', kind='status')
    assert pipe.send(with_z(k.status('CM', 'A1', k.ts(5)), 'ZDV|X'))
    assert pipe.study('A1') is not None


def test_override_onto_a_key_column_is_refused(pipe):
    mapping(pipe, 'study_db_uid', 'ZDV', 1, target_kind='study', kind='status',
            transform='number')
    pipe.send(with_z(k.status('CM', 'A1', k.ts(5)), 'ZDV|1'))
    assert pipe.study('A1')['study_db_uid'] != 1


def test_patient_override(pipe):
    mapping(pipe, 'sex', 'ZPS', 1, target_kind='patient', kind='adt')
    pipe.send(with_z(k.adt('A08'), 'ZPS|X'))
    assert pipe.patient('P0001')['sex'] == 'X'


def test_seeded_birth_date_mapping_keeps_placeholders_out(pipe):
    pipe.send(k.adt('A01', dob='99991111'))
    assert pipe.one("SELECT birth_date FROM hl7_patients")['birth_date'] is None
