"""
utils/ray7_sweep.py
────────────────────────────────────────────────────────────────
RAY7's absence rules — the findings that can only be produced by waiting.

Every other RAY7 rule inspects a message. These inspect the ABSENCE of one, which
no message-time check can ever do: the whole content of STALLED_ARRIVED is that
the next message never came. So they run periodically, over accumulated state,
rather than inline on the ACK path.

    ray7_study_state ──(partial stall indexes)──▶ sweep ──▶ ray7_findings

WHY THIS READS ray7_study_state AND NOT hl7_study_events
Each rule matches studies sitting at one rung with the next rung empty, which is
precisely the shape of the partial indexes migration 0119 created
(idx_ray7_state_stalled_*). The sweep therefore reads only the stalled set. The
alternative — aggregating the event log every run — gets slower every week the
install runs, and this job runs forever.

ONE OPEN FINDING PER STUDY PER RULE, enforced by the partial unique index
ux_ray7_findings_open_absence rather than by logic here. Without it a sweep every
fifteen minutes would raise the same stall four times an hour and bury the queue
within a day. Resolved findings drop out of that index, so a study that stalls
again later can legitimately raise a fresh one.

THRESHOLDS ARE OPERATOR CONFIGURATION, per (rule_code, modality) in ray7_rules. An
MR that has been "started" for two hours is unremarkable; a plain film in the same
state is not, and a single global number either floods the queue with MR noise or
never catches the X-ray. The seeded values are opening guesses and are meant to be
tuned against real traffic.
"""
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("RAY7_SWEEP")


# Each rule: the column whose timestamp starts the clock, and the SQL predicate
# identifying studies stuck at that rung. The predicates deliberately mirror the
# partial indexes in 0119 so the planner can use them.
_LEGACY_STALL_RULES = (
    {
        'code': 'STALLED_SCHEDULED',
        'since': 'scheduled_at',
        'where': "s.scheduled_at IS NOT NULL AND s.arrived_at IS NULL",
        'note': 'scheduled, and no arrival since — a no-show nobody cancelled, or '
                'the arrival feed has stopped',
    },
    {
        'code': 'STALLED_ARRIVED',
        'since': 'arrived_at',
        'where': "s.arrived_at IS NOT NULL AND s.started_at IS NULL",
        'note': 'patient arrived and the exam never started — a real wait, or a '
                'missing start event',
    },
    {
        'code': 'STALLED_STARTED',
        'since': 'started_at',
        'where': "s.started_at IS NOT NULL AND s.completed_at IS NULL",
        'note': 'an exam left open on the device, or a completion that never '
                'arrived — inflates every duration metric it touches',
    },
    {
        'code': 'UNREPORTED',
        'since': 'completed_at',
        'where': "s.completed_at IS NOT NULL AND s.reported_at IS NULL",
        'note': 'the exam completed and no report followed',
    },
)

# The rung a stall is named after is the one the study is SITTING at, not the one it
# is waiting for — which is why disabling a rung renames nothing and only moves the
# target. The awaited column is computed from the profile at run time.
_RUNG_COLUMN = {
    'ordered':       'ordered_at',
    'scheduled':     'scheduled_at',
    'arrived':       'arrived_at',
    'started':       'started_at',
    'completed':     'completed_at',
    'signed_prelim': 'signed_prelim_at',
    'signed_final':  'signed_final_at',
}

_STALL_RULE_FOR_RUNG = {
    'scheduled':     ('STALLED_SCHEDULED',
                      'scheduled, and no arrival since — a no-show nobody cancelled, '
                      'or the arrival feed has stopped'),
    'arrived':       ('STALLED_ARRIVED',
                      'patient arrived and the exam never started — a real wait, or '
                      'a missing start event'),
    'started':       ('STALLED_STARTED',
                      'an exam left open on the device, or a completion that never '
                      'arrived — inflates every duration metric it touches'),
    'completed':     ('UNREPORTED',
                      'the exam completed and no report followed'),
    'signed_prelim': ('STALLED_UNSIGNED',
                      'a first signature arrived and the final one never followed — '
                      'the study reads as complete while the report is provisional'),
}


def _stall_rules():
    """
    Build the stall rules from the ladder profile, bridging over disabled rungs.

    THE BRIDGE IS THE POINT. Turning a rung off must not blind the segment it sat in.
    With started disabled, the static rules above would ask two useless questions:
    arrived → started fires on every study forever, because the start is never
    coming, and started → completed can never fire at all, because started_at is
    permanently NULL. Between them a patient could arrive and never be scanned and
    nothing would notice.

    Pairing each expected rung with the NEXT expected rung instead turns that into
    one meaningful question — arrived → completed — which is what a site without a
    start feed actually wants to know.

    An UNCONFIGURED profile (nothing expected — the state migration 0130 ships) falls
    back to the original four rules, so this changes nothing at an install that has
    not opted in. Silently disabling every absence rule on merge would be a far worse
    outcome than the ones this whole design is guarding against.
    """
    try:
        from utils.ray7 import ladder_profile
        profile = ladder_profile()
    except Exception:
        logger.exception("RAY7 sweep: could not read ladder profile; using static rules")
        return _LEGACY_STALL_RULES

    expected = sorted(
        (p['ladder_rank'], rung)
        for rung, p in (profile or {}).items() if p.get('expected')
    )
    if not expected:
        return _LEGACY_STALL_RULES

    rules = []
    for i, (_, rung) in enumerate(expected):
        entry = _STALL_RULE_FOR_RUNG.get(rung)
        if not entry:
            continue                        # 'ordered' is ORPHAN_ORDER's job
        code, note = entry
        since = _RUNG_COLUMN[rung]

        awaited = _RUNG_COLUMN[expected[i + 1][1]] if i + 1 < len(expected) else None
        if awaited is None:
            # Nothing expected above this rung. Only completion has a meaningful
            # answer here — "reported at all", regardless of signature level — which
            # is also what preserves UNREPORTED at a site with no signature feed.
            if rung != 'completed':
                continue
            awaited = 'reported_at'

        rules.append({
            'code': code,
            'since': since,
            'where': f"s.{since} IS NOT NULL AND s.{awaited} IS NULL",
            'note': note,
        })
    return rules

# Shared shape. The ON CONFLICT predicate must match ux_ray7_findings_open_absence
# exactly or Postgres cannot infer the index and the statement fails outright.
_STALL_SQL = """
INSERT INTO ray7_findings
    (message_archive_id, rule_code, severity, accession_number, patient_id, detail)
SELECT
    NULL, :code, :severity, s.accession_number, s.patient_id,
    jsonb_build_object(
        'since',            s.{since},
        'stalled_minutes',  ROUND(EXTRACT(EPOCH FROM (NOW() - s.{since})) / 60.0),
        'threshold_minutes', :threshold,
        'modality',         s.modality,
        'aetitle',          s.aetitle,
        'note',             :note
    )
FROM ray7_study_state s
WHERE {where}
  AND s.cancelled_at IS NULL
  AND NOT s.is_closed
  AND s.{since} < NOW() - make_interval(mins => :threshold)
  AND (:modality = '' OR s.modality = :modality)
ON CONFLICT (rule_code, accession_number)
    WHERE message_archive_id IS NULL AND resolved_at IS NULL
DO NOTHING
"""

# ORPHAN_ORDER looks at the other side: a HIS order whose accession the RIS never
# minted, so it can never be matched to a study. Sourced from hl7_orders because
# by definition no ray7_study_state row was ever created for it.
_ORPHAN_ORDER_SQL = """
INSERT INTO ray7_findings
    (message_archive_id, rule_code, severity, accession_number, patient_id, detail)
SELECT
    NULL, 'ORPHAN_ORDER', :severity, o.accession_number, o.patient_id,
    jsonb_build_object(
        'ordered_at',        o.message_datetime,
        'stalled_minutes',   ROUND(EXTRACT(EPOCH FROM (NOW() - o.message_datetime)) / 60.0),
        'threshold_minutes', :threshold,
        'placer_order_number', o.placer_order_number,
        'note', 'a HIS order the RIS never scheduled, so it can never match a study'
    )
FROM hl7_orders o
WHERE o.accession_number IS NOT NULL
  AND o.message_datetime < NOW() - make_interval(mins => :threshold)
  AND NOT EXISTS (
        SELECT 1 FROM ray7_study_state s
         WHERE s.accession_number = o.accession_number
           AND s.scheduled_at IS NOT NULL
  )
ON CONFLICT (rule_code, accession_number)
    WHERE message_archive_id IS NULL AND resolved_at IS NULL
DO NOTHING
"""

# Closing the loop the other way: a stall that has since resolved should not sit in
# the queue as though it were still happening. An operator's own resolution is left
# alone — only findings RAY7 raised and RAY7 can see are now satisfied get closed,
# and they are marked distinctly so nobody reads them as human triage.
_AUTO_RESOLVE_SQL = """
UPDATE ray7_findings f
   SET resolved_at = NOW(),
       resolution  = 'cleared',
       resolution_note = 'auto: the awaited event arrived'
  FROM ray7_study_state s
 WHERE f.resolved_at IS NULL
   AND f.accession_number = s.accession_number
   AND (
        -- Absence findings: the rung we were waiting for turned up.
        (f.message_archive_id IS NULL AND (
            (f.rule_code = 'STALLED_SCHEDULED' AND s.arrived_at   IS NOT NULL) OR
            (f.rule_code = 'STALLED_ARRIVED'   AND s.started_at   IS NOT NULL) OR
            (f.rule_code = 'STALLED_STARTED'   AND s.completed_at IS NOT NULL) OR
            (f.rule_code = 'UNREPORTED'        AND s.reported_at  IS NOT NULL) OR
            (f.rule_code = 'STALLED_UNSIGNED'  AND s.signed_final_at IS NOT NULL) OR
            (f.rule_code = 'ORPHAN_ORDER'      AND s.scheduled_at IS NOT NULL) OR
            s.cancelled_at IS NOT NULL
        ))
        OR
        -- PREMATURE message findings: true when raised, overtaken since.
        --
        -- SKIPPED_RUNG and ORPHAN_EVENT are judged against the state as it stood
        -- when the message arrived. If a study's completion is delivered FIRST
        -- and its earlier rungs follow moments later — ordinary out-of-order
        -- delivery on this interface — both fire truthfully and then become
        -- wrong, because the rungs were not missing, merely late.
        --
        -- At the moment of judgement that case is indistinguishable from a study
        -- that completes and genuinely never reports an arrival. The engine
        -- cannot know which it is looking at, so it says what it sees and this
        -- closes the finding once the rest arrives. Raising nothing instead would
        -- mean never reporting the genuine case; leaving it open forever would
        -- fill the queue with studies that turned out fine.
        --
        -- Caught by the out_of_order scenario, which is documented as having to
        -- produce no findings and produced two.
        -- OUT_OF_SEQUENCE_DELIVERY IS DELIBERATELY ABSENT FROM THIS LIST, and must
        -- stay absent. The two below were judged against incomplete information and
        -- became untrue once the rest of the lifecycle turned up. That one was true
        -- when it was raised and stays true forever: the messages DID arrive out of
        -- order, and no later message can change what already happened on the wire.
        -- It is cleared by a human acknowledging it in the RAY7 console, which is
        -- also what releases the quarantined study. Auto-resolving it would silently
        -- re-admit data nobody had looked at.
        (f.message_archive_id IS NOT NULL AND (
            (f.rule_code = 'SKIPPED_RUNG'
                 AND s.arrived_at IS NOT NULL AND s.started_at IS NOT NULL) OR
            (f.rule_code = 'ORPHAN_EVENT'
                 AND (s.scheduled_at IS NOT NULL OR s.ordered_at IS NOT NULL))
        ))
   )
"""


def _rule_settings(code):
    """
    Every (modality, severity, threshold) this rule is configured for.

    Returns the per-modality rows plus the '' catch-all, so one pass covers a site
    that has tuned CT separately from everything else.
    """
    try:
        rows = db.session.execute(text("""
            SELECT modality, COALESCE(severity, default_severity) AS severity,
                   threshold_minutes
              FROM ray7_rules
             WHERE rule_code = :code AND enabled AND threshold_minutes IS NOT NULL
        """), {'code': code}).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        logger.exception("RAY7 sweep: could not read settings for %s", code)
        return []


def run_sweep():
    """
    One pass of the absence rules. Returns {rule_code: findings_raised}.

    Safe to run on any schedule and safe to run concurrently with ingestion: every
    statement is an INSERT ... ON CONFLICT DO NOTHING or an UPDATE of already-open
    findings, so a sweep overlapping another sweep, or a message arriving mid-pass,
    changes nothing about the outcome.
    """
    raised = {}

    for rule in _stall_rules():
        sql = _STALL_SQL.replace('{since}', rule['since']).replace('{where}', rule['where'])
        for cfg in _rule_settings(rule['code']):
            try:
                with db.session.begin_nested():
                    result = db.session.execute(text(sql), {
                        'code':      rule['code'],
                        'severity':  cfg['severity'],
                        'threshold': cfg['threshold_minutes'],
                        'modality':  cfg['modality'] or '',
                        'note':      rule['note'],
                    })
                if result.rowcount:
                    raised[rule['code']] = raised.get(rule['code'], 0) + result.rowcount
            except Exception:
                logger.exception("RAY7 sweep: %s failed (modality=%r)",
                                 rule['code'], cfg['modality'])

    for cfg in _rule_settings('ORPHAN_ORDER'):
        if cfg['modality']:
            continue        # hl7_orders is not scoped by our modality mapping
        try:
            with db.session.begin_nested():
                result = db.session.execute(text(_ORPHAN_ORDER_SQL), {
                    'severity':  cfg['severity'],
                    'threshold': cfg['threshold_minutes'],
                })
            if result.rowcount:
                raised['ORPHAN_ORDER'] = result.rowcount
        except Exception:
            logger.exception("RAY7 sweep: ORPHAN_ORDER failed")

    resolved = 0
    try:
        with db.session.begin_nested():
            resolved = db.session.execute(text(_AUTO_RESOLVE_SQL)).rowcount
    except Exception:
        logger.exception("RAY7 sweep: auto-resolve failed")

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("RAY7 sweep: commit failed")
        return {}

    if raised or resolved:
        logger.info("RAY7 sweep: raised %s | auto-resolved %d",
                    raised or '{}', resolved)
    return raised
