"""
utils/brayn_quality.py
────────────────────────────────────────────────────────────────
BRAYN layer ② — data-quality notes on a STUDY.

    accept path ─▶ project ─▶ evaluate_study(accession) ─▶ open / close notes

JUDGED ON THE STUDY, NOT ON THE MESSAGE
The first BRAYN judged each message against the state as it stood when the message
arrived. A completion delivered ahead of its arrival therefore raised SKIPPED_RUNG
truthfully, and then the arrival turned up and the finding was simply wrong; a
special auto-resolve in the sweep existed only to clean up after the engine's own
premature judgements. Here the question is asked of the whole lifecycle after every
change: is this study, as we now know it, inconsistent? A note opens when the answer
becomes yes and closes itself when it becomes no — so a reordered delivery opens and
closes a note within seconds, and a genuinely broken study keeps one.

NOTES NEVER WITHHOLD. The study projects exactly as it would without them. A note
is something for a person to look at, not a gate.

A HUMAN'S "IGNORED" STICKS: a note an operator has marked ignored is not reopened by
re-evaluation, or ignoring a known-harmless pattern would be impossible.

ONE INDEXED READ per evaluation (the state row, by primary key), a handful of
statements at most to change notes, each inside a savepoint, fail-open.
"""
import json
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("BRAYN_QUALITY")

# The ladder as columns, in rank order. Signature rungs are compared with the exam
# rungs (a report signed before the exam was done is a contradiction) but not with
# each other: a preliminary signed after the final is how an addendum looks.
_LADDER = (
    (20, 'ordered', 'ordered_at'), (40, 'scheduled', 'scheduled_at'),
    (60, 'arrived', 'arrived_at'), (70, 'started', 'started_at'),
    (100, 'completed', 'completed_at'),
    (120, 'signed_prelim', 'signed_prelim_at'), (140, 'signed_final', 'signed_final_at'),
)
_SIGNATURE = {'signed_prelim', 'signed_final'}

QUALITY_RULES = ('TIME_CONTRADICTION', 'SKIPPED_RUNG', 'ORPHAN_EVENT')


def conditions(state, expected):
    """
    {rule_code: detail} for every quality condition true of this lifecycle. Pure:
    `state` is a brayn_study_state row as a dict, `expected(rank)` the ladder profile.
    """
    out = {}
    if not state or state.get('cancelled_at'):
        return out

    present = [(rank, name, state.get(col)) for rank, name, col in _LADDER if state.get(col)]

    contradictions = []
    for i, (r_lo, n_lo, t_lo) in enumerate(present):
        for r_hi, n_hi, t_hi in present[i + 1:]:
            if n_lo in _SIGNATURE and n_hi in _SIGNATURE:
                continue
            if t_hi < t_lo:
                contradictions.append({'earlier_rung': n_lo, 'earlier_rung_time': t_lo.isoformat(),
                                       'later_rung': n_hi, 'later_rung_time': t_hi.isoformat()})
    if contradictions:
        out['TIME_CONTRADICTION'] = {'pairs': contradictions,
                                     'effect': 'negative turnaround times downstream'}

    if state.get('completed_at'):
        missing = [name for rank, name, col in _LADDER
                   if name in ('arrived', 'started') and expected(rank) and not state.get(col)]
        if missing:
            out['SKIPPED_RUNG'] = {'missing_rungs': missing,
                                   'completed_at': state['completed_at'].isoformat()}

    introduced = state.get('ordered_at') or state.get('scheduled_at')
    exam = state.get('arrived_at') or state.get('started_at') or state.get('completed_at')
    if exam and not introduced and (expected(20) or expected(40)):
        out['ORPHAN_EVENT'] = {'note': 'exam events, but no HIS order and no scheduling message'}

    return out


_OPEN_SQL = """
INSERT INTO brayn_findings
    (message_archive_id, rule_code, severity, accession_number, patient_id, detail)
SELECT NULL, :code, :severity, :acc, :pid, CAST(:detail AS jsonb)
 WHERE NOT EXISTS (
        SELECT 1 FROM brayn_findings f
         WHERE f.rule_code = :code AND f.accession_number = :acc
           AND f.message_archive_id IS NULL AND f.resolution = 'ignored')
ON CONFLICT (rule_code, accession_number)
    WHERE message_archive_id IS NULL AND resolved_at IS NULL
DO UPDATE SET detail = EXCLUDED.detail, severity = EXCLUDED.severity
"""

_CLOSE_SQL = """
UPDATE brayn_findings
   SET resolved_at = NOW(), resolution = 'cleared',
       resolution_note = 'auto: no longer true of the study'
 WHERE rule_code = :code AND accession_number = :acc
   AND message_archive_id IS NULL AND resolved_at IS NULL
"""


def evaluate_study(accession_number):
    """Open or close this study's quality notes. Returns log labels; never raises."""
    if not accession_number:
        return []
    from utils.brayn import is_expected, _rule_config
    try:
        with db.session.begin_nested():
            row = db.session.execute(text(
                "SELECT * FROM brayn_study_state WHERE accession_number = :a"),
                {'a': accession_number}).mappings().first()
    except Exception:
        logger.exception("BRAYN quality: state read failed | acc=%s", accession_number)
        return []
    if not row or row['is_provisional']:
        return []

    state = dict(row)
    try:
        true_now = conditions(state, is_expected)
    except Exception:
        # Fail-open, like every BRAYN judgement: a bug in a condition must cost the
        # notes, never the message being ingested.
        logger.exception("BRAYN quality: conditions failed | acc=%s", accession_number)
        return []
    done = []
    for code in QUALITY_RULES:
        cfg = _rule_config(code, state.get('modality'))
        try:
            with db.session.begin_nested():
                if code in true_now and cfg.get('enabled', True):
                    db.session.execute(text(_OPEN_SQL), {
                        'code': code, 'severity': cfg.get('severity') or 'warning',
                        'acc': accession_number, 'pid': state.get('patient_id'),
                        'detail': json.dumps(true_now[code], default=str),
                    })
                    done.append('note:' + code)
                else:
                    if db.session.execute(text(_CLOSE_SQL),
                                          {'code': code, 'acc': accession_number}).rowcount:
                        done.append('closed:' + code)
        except Exception:
            logger.exception("BRAYN quality: %s failed | acc=%s", code, accession_number)
    return done
