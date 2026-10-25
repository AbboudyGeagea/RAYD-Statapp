"""
The two completions: a RIS CM means the exam finished, a PACS CM means the images
arrived. They are byte-identical and only the sender tells them apart
(settings.hl7_pacs_sending_app). Mixing them up moves every turnaround figure.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()
ACC = 'ACC-P1'


def at(m):
    return BASE - timedelta(minutes=m)


@pytest.fixture
def pacs(pipe):
    pipe.setting('hl7_pacs_sending_app', 'PACS')
    return pipe


def test_unconfigured_every_completion_is_the_exam(pipe):
    pipe.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    st = pipe.state(ACC)
    assert (st['completed_at'], st['pacs_completed_at']) == (at(100), None)


def test_pacs_completion_lands_in_its_own_column(pacs):
    pacs.send(k.status('IP', ACC, k.hl7(at(120))))
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    st = pacs.state(ACC)
    assert (st['completed_at'], st['pacs_completed_at']) == (None, at(100))


def test_both_completions_kept_apart(pacs):
    pacs.send(k.status('CM', ACC, k.hl7(at(110)), app='RIS'))
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    st = pacs.state(ACC)
    assert (st['completed_at'], st['pacs_completed_at']) == (at(110), at(100))
    assert pacs.study(ACC)['insert_time'] == at(110)


def test_pacs_completion_never_overwrites_the_exam_time(pacs):
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    pacs.send(k.status('CM', ACC, k.hl7(at(110)), app='RIS'))
    assert pacs.state(ACC)['completed_at'] == at(110)


@pytest.mark.parametrize('configured', ['pacs', 'Pacs', ' PACS '])
def test_sender_match_ignores_case_and_padding(pipe, configured):
    pipe.setting('hl7_pacs_sending_app', configured)
    pipe.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    assert pipe.state(ACC)['pacs_completed_at'] == at(100)


def test_pacs_completion_writes_hl7_scn_studies(pacs):
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    assert pacs.count('hl7_scn_studies', 'accession_number = :a', a=ACC) == 1


def test_pacs_completion_marks_the_order_done(pacs):
    pacs.send(k.order('PL1', acc=ACC, when=k.hl7(at(200))))
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    o = pacs.one("SELECT * FROM hl7_orders WHERE accession_number = :a", a=ACC)
    assert (o['order_status'], o['done_by']) == ('CM', 'PACS')


def test_pacs_only_site_still_gets_a_dated_study(pacs):
    """Variant A: PACS completion and report, no RIS lifecycle. Every report filters
    on study_date, so a study with no RIS rung must still carry a date — and the
    image-arrival time is the anchor the Oracle branch always used."""
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    s = pacs.study(ACC)
    assert s is not None
    assert (s['study_date'], s['insert_time']) == (at(100).date(), at(100))


def test_pacs_split_survives_a_replay(pacs):
    pacs.send(k.status('CM', ACC, k.hl7(at(110)), app='RIS'))
    pacs.send(k.status('CM', ACC, k.hl7(at(100)), app='PACS'))
    pacs.replay()
    st = pacs.state(ACC)
    assert (st['completed_at'], st['pacs_completed_at']) == (at(110), at(100))
