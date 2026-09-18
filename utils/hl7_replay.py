"""
utils/hl7_replay.py
────────────────────────────────────────────────────────────────
Rebuild everything downstream of the archive, from the archive.

    hl7_message_archive ──▶ re-parse ──▶ re-screen ──▶ re-persist ──▶ re-project

This is the payoff for writing the raw bytes before anything else. On the Oracle
branches a parser mistake was recoverable because the source rows stayed in the
PACS; here the message arrived once over a socket and is gone. Replay is what
makes a parser fix retroactive instead of only applying to future traffic — and
given how much of the status-message layout is still an educated guess (see
_STATUS_FIELDS in utils/hl7_parse.py), that is not a theoretical convenience. It
is the reason we could ship those guesses at all.

WHAT IT REBUILDS, AND WHAT IT LEAVES ALONE
Rebuilt, because all of it is derived: hl7_study_events, ray7_findings,
ray7_study_state, and the projected etl_* rows.
Never touched: hl7_message_archive itself. Replay reads it and must never be able
to damage it, or a bad replay would destroy the only copy of the source data.

STATE IS REBUILT BY AGGREGATION, NOT BY REPLAYING INCREMENTS
ray7_study_state could be rebuilt by letting each replayed event upsert it the way
live ingestion does. It is not, because event_count would double and any
non-commutative field would depend on replay order. Instead the events are
re-derived first and the state is then computed from them in one aggregate pass,
which is deterministic and gives the same answer no matter what order the archive
is walked in.

ORDER OF REPLAY STILL MATTERS FOR FINDINGS, though. RAY7 screens each message
against the state as it stood beforehand, so a full replay CLEARS ray7_study_state
first and then walks the archive in id order — true arrival order — letting the
state build up again as it goes. Judging every message against the final state
would make most of the sequence rules vacuous.

WHERE REPLAY LEGITIMATELY DIFFERS FROM THE LIVE RUN
Two differences are inherent, not bugs, and worth knowing before comparing counts:

  CONTROL_ID_REUSE flags BOTH sides on replay, one side live. Live, the first
  message is screened before its twin exists, so only the second sees a conflict.
  On replay the archive is already complete and both see each other. Arguably the
  replayed answer is the better one — both messages are implicated in the reuse —
  but the counts will not match.

  EXACT_REDELIVERY is never regenerated. A redelivery has no archive row by
  definition, so there is nothing to walk. Its existing findings are therefore
  preserved rather than deleted and lost.
"""
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("HL7_REPLAY")


_REBUILD_STATE_SQL = """
INSERT INTO ray7_study_state (
    accession_number, placer_order_number, patient_id, modality, aetitle,
    room_name, procedure_code, procedure_text, patient_class, patient_location,
    scheduled_at, arrived_at, started_at, completed_at, cancelled_at,
    current_rank, event_count, is_closed, first_seen_at, last_event_at, updated_at
)
SELECT
    e.accession_number,
    -- Earliest non-null wins for identity-ish fields, latest for the ones a later
    -- event legitimately refines (device, room, class, location).
    (array_agg(e.placer_order_number) FILTER (WHERE e.placer_order_number IS NOT NULL))[1],
    (array_agg(e.patient_id)          FILTER (WHERE e.patient_id IS NOT NULL))[1],
    (array_agg(e.modality)            FILTER (WHERE e.modality IS NOT NULL))[1],
    (array_agg(e.aetitle    ORDER BY e.ladder_rank DESC) FILTER (WHERE e.aetitle IS NOT NULL))[1],
    (array_agg(e.room_name  ORDER BY e.ladder_rank DESC) FILTER (WHERE e.room_name IS NOT NULL))[1],
    (array_agg(e.procedure_code) FILTER (WHERE e.procedure_code IS NOT NULL))[1],
    (array_agg(e.procedure_text) FILTER (WHERE e.procedure_text IS NOT NULL))[1],
    (array_agg(e.patient_class    ORDER BY e.ladder_rank DESC) FILTER (WHERE e.patient_class IS NOT NULL))[1],
    (array_agg(e.patient_location ORDER BY e.ladder_rank DESC) FILTER (WHERE e.patient_location IS NOT NULL))[1],
    -- FIRST timestamp per rung, matching live ingestion: a repeated Arrived is a
    -- real occurrence here and the meaningful moment is the first one.
    MIN(e.event_time) FILTER (WHERE e.canonical_state = 'scheduled'),
    MIN(e.event_time) FILTER (WHERE e.canonical_state = 'arrived'),
    MIN(e.event_time) FILTER (WHERE e.canonical_state = 'started'),
    MIN(e.event_time) FILTER (WHERE e.canonical_state = 'completed'),
    MIN(e.event_time) FILTER (WHERE e.canonical_state = 'cancelled'),
    MAX(e.ladder_rank),
    COUNT(*),
    (MAX(e.ladder_rank) >= 100 OR bool_or(e.canonical_state = 'cancelled')),
    MIN(e.received_at),
    MAX(e.event_time),
    NOW()
FROM hl7_study_events e
WHERE e.accession_number IS NOT NULL
GROUP BY e.accession_number
ON CONFLICT (accession_number) DO UPDATE SET
    placer_order_number = EXCLUDED.placer_order_number,
    patient_id          = EXCLUDED.patient_id,
    modality            = EXCLUDED.modality,
    aetitle             = EXCLUDED.aetitle,
    room_name           = EXCLUDED.room_name,
    procedure_code      = EXCLUDED.procedure_code,
    procedure_text      = EXCLUDED.procedure_text,
    patient_class       = EXCLUDED.patient_class,
    patient_location    = EXCLUDED.patient_location,
    scheduled_at        = EXCLUDED.scheduled_at,
    arrived_at          = EXCLUDED.arrived_at,
    started_at          = EXCLUDED.started_at,
    completed_at        = EXCLUDED.completed_at,
    cancelled_at        = EXCLUDED.cancelled_at,
    current_rank        = EXCLUDED.current_rank,
    event_count         = EXCLUDED.event_count,
    is_closed           = EXCLUDED.is_closed,
    first_seen_at       = EXCLUDED.first_seen_at,
    last_event_at       = EXCLUDED.last_event_at,
    updated_at          = NOW()
"""

# reported_at is not in the event log — a report lives in hl7_oru_reports — so it
# is restored separately after the aggregate rebuild.
_RESTORE_REPORTED_SQL = """
UPDATE ray7_study_state s
   SET reported_at = r.result_datetime,
       is_closed   = TRUE
  FROM (
        SELECT accession_number, MIN(result_datetime) AS result_datetime
          FROM hl7_oru_reports
         WHERE accession_number IS NOT NULL AND result_datetime IS NOT NULL
         GROUP BY accession_number
  ) r
 WHERE s.accession_number = r.accession_number
"""


def replay(app, limit=None, since_id=0, rescreen=True, dry_run=False):
    """
    Walk the archive in arrival order and rebuild everything derived from it.

    limit / since_id scope the run — useful for replaying only what arrived after a
    parser fix rather than the entire history.

    Returns a dict of counters.
    """
    from utils.hl7_parse import parse_message
    from utils.hl7_ingest import persist_parsed
    from utils.hl7_project import project_message
    from utils import ray7

    stats = {'read': 0, 'reparsed': 0, 'skipped': 0, 'failed': 0,
             'quarantined': 0, 'projected': 0}

    with app.app_context():
        rows = db.session.execute(text("""
            SELECT id, raw_message, source_ip
              FROM hl7_message_archive
             WHERE id > :since
             ORDER BY id
             {limit}
        """.replace('{limit}', 'LIMIT :lim' if limit else '')),
            {'since': since_id, 'lim': limit} if limit else {'since': since_id}
        ).fetchall()

        stats['read'] = len(rows)
        logger.info("Replay: %d archived message(s) to process", len(rows))
        if dry_run:
            return stats

        # Clear ONLY the derived rows for the messages being replayed. The archive
        # itself is never touched: a bad replay must not be able to destroy the one
        # copy of the source data.
        ids = [r[0] for r in rows]
        full_replay = (since_id == 0 and not limit)

        if ids:
            try:
                db.session.execute(
                    text("DELETE FROM hl7_study_events WHERE message_archive_id = ANY(:ids)"),
                    {'ids': ids})

                if rescreen:
                    # EXACT_REDELIVERY is deliberately preserved. A redelivery has no
                    # archive row of its own — that is the whole point, it collided —
                    # so replay walks only the originals and can never recreate it.
                    # Deleting it would silently erase the record that redeliveries
                    # happened at all, and that rate is a real signal about the
                    # interface. Verified the hard way: the first replay dropped it.
                    db.session.execute(text("""
                        DELETE FROM ray7_findings
                         WHERE message_archive_id = ANY(:ids)
                           AND rule_code <> 'EXACT_REDELIVERY'
                    """), {'ids': ids})

                if full_replay:
                    # THE STATE MUST GO TOO, and this was missed the first time.
                    #
                    # RAY7 screens each message against the state as it stood
                    # BEFORE that message. Leaving ray7_study_state populated means
                    # every replayed event is judged against a state that already
                    # contains itself, so every rung looks like a duplicate of
                    # itself — the first replay produced five spurious
                    # LOGICAL_DUPLICATE findings for exactly this reason.
                    #
                    # Only safe on a FULL replay. A partial one cannot legitimately
                    # clear state that earlier, unreplayed messages built.
                    db.session.execute(text("DELETE FROM ray7_study_state"))
                elif rescreen:
                    logger.warning(
                        "Replay: partial range with re-screening. ray7_study_state "
                        "still holds events from outside the range, including ones "
                        "these messages produced, so the findings will not exactly "
                        "reproduce the live run. Use a full replay, or --no-rescreen."
                    )

                db.session.commit()
            except Exception:
                db.session.rollback()
                logger.exception("Replay: could not clear derived rows; aborting")
                return stats

        for archive_id, raw_message, source_ip in rows:
            try:
                msg = parse_message(raw_message, source_ip=source_ip)

                if rescreen:
                    verdict = ray7.screen(msg, archive_id)
                    ray7.persist(verdict, msg, archive_id)
                    if not verdict.may_project:
                        stats['quarantined'] += 1
                        db.session.commit()
                        continue

                persist_parsed(msg, archive_id)
                project_message(msg)
                db.session.commit()

                stats['reparsed'] += 1
                if msg.accession_number:
                    stats['projected'] += 1

            except Exception:
                db.session.rollback()
                stats['failed'] += 1
                logger.exception("Replay: failed on archive_id=%s", archive_id)

        # Recompute state by aggregation rather than trusting the increments above,
        # so the result is identical regardless of how the walk went.
        try:
            db.session.execute(text(_REBUILD_STATE_SQL))
            db.session.execute(text(_RESTORE_REPORTED_SQL))
            db.session.commit()
            logger.info("Replay: ray7_study_state rebuilt from the event log")
        except Exception:
            db.session.rollback()
            logger.exception("Replay: state rebuild failed")

        # Reproject every touched study off the rebuilt state, so etl_* reflects
        # the aggregate rather than whatever the last incremental pass left.
        try:
            from utils.hl7_project import project_study, project_patient
            accs = db.session.execute(text(
                "SELECT accession_number FROM ray7_study_state")).fetchall()
            for (acc,) in accs:
                project_study(acc)
            pats = db.session.execute(text(
                "SELECT patient_id FROM hl7_patients")).fetchall()
            for (pid,) in pats:
                project_patient(pid)
            db.session.commit()
        except Exception:
            db.session.rollback()
            logger.exception("Replay: reprojection failed")

    logger.info("Replay complete: %s", stats)
    return stats
