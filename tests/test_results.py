"""
ORU: the report row, the signature rungs, and what the reports see of them.

On this branch the live ORU is the ONLY source of report text — there is no RIS
batch behind it to be authoritative — so a preliminary report must give way to the
final one, and a correction to whatever it corrects.
"""
from datetime import timedelta

import pytest

import hl7kit as k

BASE = k.now()
ACC = 'ACC-R1'


def at(m):
    return BASE - timedelta(minutes=m)


def exam(pipe, acc=ACC):
    for code, m in (('SC', 300), ('AR', 280), ('IP', 260), ('CM', 240)):
        pipe.send(k.status(code, acc, k.hl7(at(m))))


def oru(pipe, minutes, status='F', text=None, acc=ACC, **kw):
    pipe.send(k.report(acc, k.hl7(at(minutes)), result_status=status,
                       text=text or f'Report signed {status} at -{minutes}m.', **kw))


def report_row(pipe, acc=ACC):
    return pipe.one("SELECT * FROM hl7_oru_reports WHERE accession_number = :a", a=acc)


# ── one final report ──────────────────────────────────────────────────────────

def test_final_report_closes_the_study(pipe):
    exam(pipe)
    oru(pipe, 200)
    st = pipe.state(ACC)
    assert (st['signed_final_at'], st['reported_at'], st['is_closed']) == (at(200), at(200), True)
    assert pipe.study(ACC)['study_status'] == 'closed'


def test_one_report_row_per_accession(pipe):
    exam(pipe)
    oru(pipe, 200)
    oru(pipe, 150, 'C')
    assert pipe.count('hl7_oru_reports', 'accession_number = :a', a=ACC) == 1


def test_report_text_and_impression(pipe):
    exam(pipe)
    oru(pipe, 200, text='Line one.\\.br\\Line two.')
    r = report_row(pipe)
    assert r['report_text'].startswith('Line one. Line two.')
    assert r['impression_text'] == 'Normal study.'


def test_signer_is_extracted(pipe):
    exam(pipe)
    oru(pipe, 200)
    assert report_row(pipe)['physician_id'] == 'rad@sjh.com'
    assert pipe.study(ACC)['rep_final_signed_by'] == 'rad@sjh.com'


# ── preliminary, then final ───────────────────────────────────────────────────

def test_preliminary_does_not_close(pipe):
    exam(pipe)
    oru(pipe, 200, 'W')
    st = pipe.state(ACC)
    assert (st['signed_prelim_at'], st['signed_final_at'], st['is_closed']) == (at(200), None, False)
    assert pipe.study(ACC)['study_status'] == 'open'


def test_preliminary_is_not_a_final_signature_in_the_reports(pipe):
    """rep_final_timestamp is the end of every report-TAT figure. A W is not final."""
    exam(pipe)
    oru(pipe, 200, 'W')
    s = pipe.study(ACC)
    assert s['study_has_report'] is True
    assert s['rep_final_timestamp'] is None


def test_final_after_preliminary_sets_both_rungs(pipe):
    exam(pipe)
    oru(pipe, 200, 'W')
    oru(pipe, 120, 'F')
    st = pipe.state(ACC)
    assert (st['signed_prelim_at'], st['signed_final_at'], st['is_closed']) == \
           (at(200), at(120), True)


def test_final_signature_time_is_the_tat_end(pipe):
    exam(pipe)
    oru(pipe, 200, 'W')
    oru(pipe, 120, 'F')
    assert pipe.study(ACC)['rep_final_timestamp'] == at(120)


def test_final_text_replaces_the_preliminary_text(pipe):
    exam(pipe)
    oru(pipe, 200, 'W', text='Preliminary read.')
    oru(pipe, 120, 'F', text='Final read.')
    assert report_row(pipe)['report_text'].startswith('Final read.')


def test_final_delivered_before_preliminary(pipe):
    exam(pipe)
    oru(pipe, 120, 'F', text='Final read.')
    oru(pipe, 200, 'W', text='Preliminary read.')
    st = pipe.state(ACC)
    assert (st['signed_prelim_at'], st['signed_final_at'], st['is_closed']) == \
           (at(200), at(120), True)
    assert report_row(pipe)['report_text'].startswith('Final read.')
    assert pipe.study(ACC)['rep_final_timestamp'] == at(120)


def test_correction_replaces_the_text_but_not_the_first_final_time(pipe):
    exam(pipe)
    oru(pipe, 200, 'F', text='Original.')
    oru(pipe, 60, 'C', text='Corrected.')
    assert report_row(pipe)['report_text'].startswith('Corrected.')
    assert pipe.state(ACC)['signed_final_at'] == at(200)


def test_mixed_obx_statuses_file_by_the_most_final(pipe):
    exam(pipe)
    pipe.send(k.report(ACC, k.hl7(at(200)), obx_statuses=['W', 'F']))
    assert pipe.state(ACC)['signed_final_at'] == at(200)


# ── sites that do not use the signature ladder ────────────────────────────────

def test_report_without_any_result_status(pipe):
    exam(pipe)
    pipe.send(k.report(ACC, k.hl7(at(200)), obx_statuses=['', '']))
    st, s = pipe.state(ACC), pipe.study(ACC)
    assert st['reported_at'] == at(200)
    assert (s['study_has_report'], s['rep_final_timestamp']) == (True, at(200))


def test_report_status_from_obr25(pipe):
    exam(pipe)
    pipe.send(k.report(ACC, k.hl7(at(200)), obx_statuses=['', ''], obr25='F'))
    assert pipe.state(ACC)['signed_final_at'] == at(200)


# ── a report with no lifecycle at all (PACS-only, or the RIS feed is down) ────

def test_report_for_an_unknown_study_still_makes_a_dated_study(pipe):
    oru(pipe, 200)
    s = pipe.study(ACC)
    assert s is not None
    assert s['study_date'] == at(200).date()
    assert s['study_has_report'] is True


def test_report_only_study_has_no_turnaround_anchor(pipe):
    """With no exam event there is nothing to measure TAT from; anchoring on the
    report itself would record a zero-minute turnaround."""
    oru(pipe, 200)
    assert pipe.study(ACC)['insert_time'] is None


def test_report_before_the_exam_events(pipe):
    oru(pipe, 200)
    exam(pipe)
    s = pipe.study(ACC)
    assert (s['insert_time'], s['rep_final_timestamp'], s['study_status']) == \
           (at(240), at(200), 'closed')


# ── referring doctor from the report when no order reached us ─────────────────

def test_referring_from_the_reports_pv1(pipe):
    exam(pipe)
    pipe.send(k.report(ACC, k.hl7(at(200)), pv1_seg=k.pv1(ref='555^HADDAD^SAMIR')))
    s = pipe.study(ACC)
    assert (s['referring_physician_first_name'], s['referring_physician_last_name']) == \
           ('SAMIR', 'HADDAD')


def test_order_referrer_wins_over_the_report(pipe):
    pipe.send(k.order('PL1', acc=ACC, when=k.hl7(at(320))))
    exam(pipe)
    pipe.send(k.report(ACC, k.hl7(at(200)), pv1_seg=k.pv1(ref='555^HADDAD^SAMIR')))
    assert pipe.study(ACC)['referring_physician_last_name'] == 'WAKIM'


# ── reports nobody can read ───────────────────────────────────────────────────

def test_report_with_only_numeric_obx_still_records_the_signature(pipe):
    exam(pipe)
    raw = k.message(k.msh('PACS', None, 'ORU^R01', k.hl7(at(200))), k.pid(),
                    k.obr(acc=ACC, result_dt=k.hl7(at(200)), mod='CT'),
                    k.obx('42', 'F', vtype='NM'))
    pipe.send(raw)
    assert pipe.state(ACC)['signed_final_at'] == at(200)


def test_report_without_an_accession_is_quarantined(pipe):
    raw = k.message(k.msh('PACS', None, 'ORU^R01'), k.pid(), k.obr(result_dt=k.ts(5)),
                    k.obx('text'))
    pipe.send(raw)
    assert 'MISSING_IDENTIFIER' in pipe.codes()
