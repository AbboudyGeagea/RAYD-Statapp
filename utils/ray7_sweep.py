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


# What "moved on" means for each stall: any of these columns set. ONE list, read by
# both the rule that raises the stall and the auto-resolve that closes it.
#
# They used to be written separately and disagreed. Raising looked only at the next
# rung (arrived, started still NULL) while closing looked at any later rung, so a
# study that skipped a rung — arrived, never started, completed — was raised and
# closed again in the same pass, every pass: 20 closed STALLED_ARRIVED notes on one
# study in five hours, a new one every 15 minutes for as long as it existed.
_LATER = {
    'STALLED_SCHEDULED': ('arrived_at', 'started_at', 'completed_at', 'pacs_completed_at'),
    'STALLED_ARRIVED':   ('started_at', 'completed_at', 'pacs_completed_at'),
    'STALLED_STARTED':   ('completed_at', 'pacs_completed_at'),
    'UNREPORTED':        ('reported_at', 'signed_prelim_at', 'signed_final_at'),
    'STALLED_UNSIGNED':  ('signed_final_at',),
}


def _stuck_where(code, since, awaited):
    """
    Studies sitting at `since` with nothing after it. Written as a conjunction of
    IS NULL tests rather than COALESCE(...) IS NULL, so it still implies the
    partial indexes in 0119 (arrived_at IS NOT NULL AND started_at IS NULL, ...)
    and the planner can keep using them.
    """
    cols = [awaited] + [c for c in _LATER[code] if c != awaited]
    return ' AND '.join([f's.{since} IS NOT NULL'] + [f's.{c} IS NULL' for c in cols])


# Each rule: the column whose timestamp starts the clock, and the SQL predicate
# identifying studies stuck at that rung.
_LEGACY_STALL_RULES = (
    {
        'code': 'STALLED_SCHEDULED',
        'since': 'scheduled_at',
        'where': _stuck_where('STALLED_SCHEDULED', 'scheduled_at', 'arrived_at'),
        'note': 'scheduled, and no arrival since — a no-show nobody cancelled, or '
                'the arrival feed has stopped',
    },
    {
        'code': 'STALLED_ARRIVED',
        'since': 'arrived_at',
        'where': _stuck_where('STALLED_ARRIVED', 'arrived_at', 'started_at'),
        'note': 'patient arrived and the exam never started — a real wait, or a '
                'missing start event',
    },
    {
        'code': 'STALLED_STARTED',
        'since': 'started_at',
        'where': _stuck_where('STALLED_STARTED', 'started_at', 'completed_at'),
        'note': 'an exam left open on the device, or a completion that never '
                'arrived — inflates every duration metric it touches',
    },
    {
        'code': 'UNREPORTED',
        'since': 'completed_at',
        'where': _stuck_where('UNREPORTED', 'completed_at', 'reported_at'),
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
            'where': _stuck_where(code, since, awaited),
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

# ORPHAN_ORDER looks at the other side: a HIS order the RIS never scheduled, so it
# can never be matched to a study.
#
# Read from ray7_study_state, where every ORM NW has opened a lifecycle since 0130 —
# under its accession if it carried one, under the provisional ~ORD: key if not.
# It used to read hl7_orders and require an accession, which is exactly what a HIS
# order does NOT have (the RIS mints it at scheduling), so the normal shape of an
# orphan was the one shape it could never see. The provisional key is a stable
# accession_number for the open-finding index, and the rekey carries the finding
# over to the real accession once scheduling turns up, where auto-resolve closes it.
#
# "Never progressed" rather than "never scheduled": a walk-in order that went
# straight to Arrived is not an orphan.
_ORPHAN_ORDER_SQL = """
INSERT INTO ray7_findings
    (message_archive_id, rule_code, severity, accession_number, patient_id, detail)
SELECT
    NULL, 'ORPHAN_ORDER', :severity, s.accession_number, s.patient_id,
    jsonb_build_object(
        'ordered_at',          s.ordered_at,
        'stalled_minutes',     ROUND(EXTRACT(EPOCH FROM (NOW() - s.ordered_at)) / 60.0),
        'threshold_minutes',   :threshold,
        'placer_order_number', s.placer_order_number,
        'provisional',         s.is_provisional,
        'note', 'a HIS order the RIS never scheduled, so it can never match a study'
    )
FROM ray7_study_state s
WHERE s.ordered_at IS NOT NULL
  AND s.scheduled_at IS NULL AND s.arrived_at IS NULL AND s.started_at IS NULL
  AND s.completed_at IS NULL AND s.pacs_completed_at IS NULL
  AND s.cancelled_at IS NULL
  AND NOT s.is_closed
  AND s.ordered_at < NOW() - make_interval(mins => :threshold)
  AND (:modality = '' OR s.modality = :modality)
ON CONFLICT (rule_code, accession_number)
    WHERE message_archive_id IS NULL AND resolved_at IS NULL
DO NOTHING
"""

# Closing the loop the other way: a stall that has since resolved should not sit in
# the queue as though it were still happening. An operator's own resolution is left
# alone — only findings RAY7 raised and RAY7 can see are now satisfied get closed,
# and they are marked distinctly so nobody reads them as human triage.
_MOVED_ON = ' OR\n            '.join(
    f"(f.rule_code = '{code}' AND COALESCE({', '.join('s.' + c for c in cols)}) IS NOT NULL)"
    for code, cols in _LATER.items())

_AUTO_RESOLVE_SQL = f"""
UPDATE ray7_findings f
   SET resolved_at = NOW(),
       resolution  = 'cleared',
       resolution_note = 'auto: the awaited event arrived'
  FROM ray7_study_state s
 WHERE f.resolved_at IS NULL
   AND f.accession_number = s.accession_number
   AND (
        -- Absence findings: the study moved past the rung it was stuck at.
        --
        -- ANY later rung, not only the next one. With a rung disabled the stall
        -- rules bridge over it (arrived waits on completed when started is off), and
        -- the old "next rung only" test then waited forever for an event the site
        -- does not send — the finding could be raised but never cleared. A later
        -- rung arriving means the study is not stuck where the finding says.
        -- Same _LATER list the raise uses, so the two cannot disagree again.
        (f.message_archive_id IS NULL AND (
            {_MOVED_ON} OR
            (f.rule_code = 'ORPHAN_ORDER'      AND (s.scheduled_at IS NOT NULL OR
                                                    s.arrived_at   IS NOT NULL OR
                                                    s.started_at   IS NOT NULL OR
                                                    s.completed_at IS NOT NULL)) OR
            s.cancelled_at IS NOT NULL
        ))
   )
"""
# There used to be a second half here, closing SKIPPED_RUNG and ORPHAN_EVENT raised
# per MESSAGE against a half-delivered lifecycle once the rest turned up. Since the
# 0135 rebuild those are study notes judged on the whole lifecycle after every change
# (utils/ray7_quality.py), which close themselves; the engine no longer makes the
# premature judgements this existed to clean up after. The cancellation clause above
# still closes every open study note, quality ones included.


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
        try:
            with db.session.begin_nested():
                result = db.session.execute(text(_ORPHAN_ORDER_SQL), {
                    'severity':  cfg['severity'],
                    'threshold': cfg['threshold_minutes'],
                    'modality':  cfg['modality'] or '',
                })
            if result.rowcount:
                raised['ORPHAN_ORDER'] = raised.get('ORPHAN_ORDER', 0) + result.rowcount
        except Exception:
            logger.exception("RAY7 sweep: ORPHAN_ORDER failed")

    resolved = 0
    try:
        with db.session.begin_nested():
            resolved = db.session.execute(text(_AUTO_RESOLVE_SQL)).rowcount
    except Exception:
        logger.exception("RAY7 sweep: auto-resolve failed")

    # Layer ③: per-sender rates against their thresholds. Here because the sweep is
    # already the periodic pass; the counters themselves are written by the listener.
    try:
        from utils.ray7_health import evaluate_alerts
        alerts = evaluate_alerts()
        if alerts.get('raised'):
            raised['INTERFACE_ALERTS'] = alerts['raised']
        resolved += alerts.get('cleared', 0)
    except Exception:
        logger.exception("RAY7 sweep: interface alerts failed")

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
