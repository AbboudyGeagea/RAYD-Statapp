"""
Patients: ADT demographics, the patient row every study needs whether or not ADT
exists, and last-write-wins by the sender's clock.
"""
from datetime import date, timedelta

import pytest

import hl7kit as k

BASE = k.now()


def at(m):
    return BASE - timedelta(minutes=m)


def hp(pipe, pid='P0001'):
    return pipe.one("SELECT * FROM hl7_patients WHERE patient_id = :p", p=pid)


def test_adt_creates_the_patient(pipe):
    pipe.send(k.adt('A01', when=k.hl7(at(100))))
    p = hp(pipe)
    assert (p['patient_name'], p['birth_date'], p['sex'], p['last_adt_event']) == \
           ('JANE M DOE', date(1985, 3, 12), 'F', 'ADT^A01')
    assert pipe.patient('P0001')['birth_date'] == date(1985, 3, 12)


def test_a08_corrects_demographics(pipe):
    pipe.send(k.adt('A01', name='MISPELT^NAME', dob='19700101', sex='M', when=k.hl7(at(100))))
    pipe.send(k.adt('A08', name='CORRECTED^NAME', dob='19700202', sex='F', when=k.hl7(at(90))))
    p = hp(pipe)
    assert (p['patient_name'], p['birth_date'], p['sex']) == \
           ('NAME CORRECTED', date(1970, 2, 2), 'F')


def test_an_older_adt_delivered_late_does_not_win(pipe):
    pipe.send(k.adt('A08', name='NEW^NAME', when=k.hl7(at(50))))
    pipe.send(k.adt('A08', name='OLD^NAME', when=k.hl7(at(90))))
    assert hp(pipe)['patient_name'] == 'NAME NEW'


def test_adt_that_omits_a_field_keeps_the_stored_value(pipe):
    pipe.send(k.adt('A01', dob='19850312', when=k.hl7(at(100))))
    pipe.send(k.adt('A08', dob='', sex='', when=k.hl7(at(90))))
    p = hp(pipe)
    assert (p['birth_date'], p['sex']) == (date(1985, 3, 12), 'F')


def test_order_traffic_does_not_block_a_later_correction(pipe):
    """The RIS clock running ahead of the HIS must not make an ORM look 'newer' than
    the ADT correction that follows it: an ORM carries no demographics to protect."""
    pipe.send(k.adt('A01', name='MISPELT^NAME', when=k.hl7(at(100))))
    pipe.send(k.status('SC', 'A1', k.hl7(at(60)), msh_when=k.hl7(at(10))))
    pipe.send(k.adt('A08', name='CORRECTED^NAME', when=k.hl7(at(50))))
    assert hp(pipe)['patient_name'] == 'NAME CORRECTED'


def test_a_study_without_any_adt_still_gets_its_patient(pipe):
    for _, raw in k.lifecycle('A1', 100):
        pipe.send(raw)
    assert hp(pipe) is not None
    s = pipe.study('A1')
    assert s['patient_db_uid'] == pipe.patient('P0001')['patient_db_uid']


def test_adt_after_the_study_reprojects_age_at_exam(pipe):
    for _, raw in k.lifecycle('A1', 100):
        pipe.send(raw)
    assert pipe.study('A1')['age_at_exam'] is None
    pipe.send(k.adt('A08', dob='19850312'))
    assert pipe.study('A1')['age_at_exam'] is not None


def test_adt_placeholder_dob_is_never_stored(pipe):
    """The seeded PID-7 field mapping once put 99991111 back after the parser had
    nulled it, and the CHECK on hl7_patients then rejected the whole row (0129)."""
    pipe.send(k.adt('A01', dob='99991111', sex='U'))
    p = hp(pipe)
    assert p is not None and p['birth_date'] is None


@pytest.mark.parametrize('dob', ['18700101', '21000101'])
def test_out_of_range_dob_does_not_lose_the_patient(pipe, dob):
    pipe.send(k.adt('A01', dob=dob))
    assert hp(pipe) is not None


def test_patient_class_and_location_follow_the_latest_visit(pipe):
    pipe.send(k.adt('A01', cls='E', loc='ER^BAY1^^RH', when=k.hl7(at(100))))
    pipe.send(k.adt('A02', cls='I', loc='W3^301^^RH', when=k.hl7(at(50))))
    p = hp(pipe)
    assert (p['patient_class'], p['patient_location']) == ('I', 'W3')


def test_patient_without_pv1_uses_pid18_for_class(pipe):
    raw = k.message(k.msh('SAP_HIS', None, 'ADT^A08'), k.pid(pid18='E'))
    pipe.send(raw)
    assert hp(pipe)['patient_class'] == 'E'


def test_adt_without_a_patient_id_is_archived_and_harmless(pipe):
    raw = k.message(k.msh('SAP_HIS', None, 'ADT^A08'), k.pid(pat=None))
    assert pipe.send(raw)
    assert pipe.count('hl7_patients') == 0
    assert pipe.count('hl7_message_archive') == 1


def test_number_of_patient_studies(pipe):
    for acc in ('A1', 'A2', 'A3'):
        pipe.send(k.status('CM', acc, k.ts(10)))
    assert pipe.patient('P0001')['number_of_patient_studies'] == 3


def test_the_study_patient_id_is_never_rewritten_by_a_later_message(pipe):
    pipe.send(k.status('SC', 'A1', k.ts(30), pat='P-RIGHT'))
    pipe.send(k.status('CM', 'A1', k.ts(10), pat='P-WRONG'))
    assert pipe.state('A1')['patient_id'] == 'P-RIGHT'
