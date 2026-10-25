"""
utils/hl7_replay.py
────────────────────────────────────────────────────────────────
Rebuild everything downstream of the archive, from the archive.

    hl7_message_archive ──▶ re-parse ──▶ re-screen ──▶ the live accept path

This is the payoff for writing the raw bytes before anything else. On the Oracle
branches a parser mistake was recoverable because the source rows stayed in the
PACS; here the message arrived once over a socket and is gone. Replay is what
makes a parser fix retroactive instead of only applying to future traffic — and
given how much of the status-message layout is still an educated guess (see
_STATUS_FIELDS in utils/hl7_parse.py), that is not a theoretical convenience. It
is the reason we could ship those guesses at all.

WHAT IT REBUILDS, AND WHAT IT LEAVES ALONE
Rebuilt, because all of it is derived: hl7_study_events, ray7_findings,
ray7_study_state, and the projected etl_* rows, plus the legacy hl7_orders /
hl7_oru_reports rows of every message that is (now) accepted.
Never touched: hl7_message_archive itself, beyond its RAY7 and projection stamps.
Replay reads it and must never be able to damage it, or a bad replay would destroy
the only copy of the source data.

REPLAY IS THE LIVE PATH, WALKED AGAIN
Each archived message goes through hl7_listener.apply_accepted() — the same
function live ingestion uses — in archive (arrival) order. Two consequences:

  It reproduces the live result exactly, because every column of ray7_study_state
  is order-independent (rung times LEAST, rank GREATEST, refined attributes from
  the highest-ranked event; see hl7_ingest.update_study_state). There used to be a
  separate aggregate rebuild here instead, and it had drifted: it re-closed every
  completed study (resurrecting the closure-at-completion bug that made UNREPORTED
  dead code), dropped ordered_at and both signature rungs, and ran over the WHOLE
  table — so acknowledging one quarantined study in the console rewrote the
  lifecycle of every other study in the install.

  A released message gets everything an accepted one would have had — including
  the quality notes. The old walk skipped the legacy writes, so a released report
  never reached hl7_oru_reports and a released order never reached hl7_orders.

  RAY7's interface counters are NOT touched: they describe the live wire, and a
  replay is not wire traffic (ray7.record_observations is the listener's alone).

ORDER OF REPLAY STILL MATTERS FOR FINDINGS. RAY7 screens each message against the
state as it stood beforehand, so a full replay CLEARS ray7_study_state first and
then walks the archive in id order — true arrival order — letting the state build
up again as it goes. Judging every message against the final state would make most
of the sequence rules vacuous.

WHERE REPLAY LEGITIMATELY DIFFERS FROM THE LIVE RUN
  etl_* rows of a study that no longer projects at all (every one of its messages
  now quarantined, say) are left as they were: replay rewrites rows, it does not
  hunt for ones that should no longer exist.
"""
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("HL7_REPLAY")


# event_count is the one counter in ray7_study_state. Re-walking messages whose
# events were deleted and re-inserted (a partial replay) would count them twice, so
# it is recomputed from the lifecycle log for every study the replay touched.
_RECOUNT_SQL = """
UPDATE ray7_study_state s
   SET event_count = x.n
  FROM (SELECT accession_number, COUNT(*) AS n
          FROM hl7_study_events
         WHERE accession_number = ANY(:accs)
         GROUP BY accession_number) x
 WHERE s.accession_number = x.accession_number
"""


def replay(app, limit=None, since_id=0, rescreen=True, dry_run=False, archive_ids=None):
    """
    Walk the archive in arrival order and rebuild everything derived from it.

    limit / since_id scope the run — useful for replaying only what arrived after a
    parser fix rather than the entire history.

    archive_ids scopes it to an explicit set of messages, which is how the RAY7
    console releases HELD messages: the operator fixes the configuration (maps the
    status code, points the accession mapping at the right field) and "Replay held
    messages" re-screens exactly those, with rescreen=True — since the 0135 rebuild a
    message is held only when it cannot be identified or classified, so re-screening
    is how the fix is confirmed. (rescreen=False remains for `app.py -m
    --no-rescreen`, which reprojects without judging.)

    Returns a dict of counters.
    """
    from hl7_listener import apply_accepted, _split
    from utils.hl7_parse import parse_message
    from utils.hl7_ingest import mark_parsed, mark_quarantined
    from utils import ray7

    stats = {'read': 0, 'reparsed': 0, 'skipped': 0, 'failed': 0,
             'quarantined': 0, 'projected': 0}

    with app.app_context():
        if archive_ids:
            rows = db.session.execute(text("""
                SELECT id, raw_message, source_ip
                  FROM hl7_message_archive
                 WHERE id = ANY(:ids)
                 ORDER BY id
            """), {'ids': list(archive_ids)}).fetchall()
        else:
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
        # `not archive_ids` is load-bearing: a targeted release passes no since_id and
        # no limit, so without it every single-study release would satisfy this test
        # and DELETE the whole of ray7_study_state on its way past — destroying every
        # other study's lifecycle to re-admit one.
        full_replay = (since_id == 0 and not limit and not archive_ids)

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
                    # THE STATE MUST GO TOO. RAY7 screens each message against the
                    # state as it stood BEFORE that message; leaving it populated means
                    # every replayed event is judged against a state that already
                    # contains itself, so every rung looks like a duplicate of itself.
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

        touched = set()
        for archive_id, raw_message, source_ip in rows:
            try:
                msg = parse_message(raw_message, source_ip=source_ip)

                if rescreen:
                    verdict = ray7.screen(msg, archive_id)
                    ray7.persist(verdict, msg, archive_id)
                    if not verdict.may_project:
                        stats['quarantined'] += 1
                        mark_quarantined(archive_id)
                        db.session.commit()
                        continue

                apply_accepted(raw_message, _split(raw_message), msg, archive_id, app)
                mark_parsed(archive_id, projected=True)
                db.session.commit()

                stats['reparsed'] += 1
                if msg.accession_number:
                    stats['projected'] += 1
                    touched.add(msg.accession_number)

            except Exception:
                db.session.rollback()
                stats['failed'] += 1
                logger.exception("Replay: failed on archive_id=%s", archive_id)

        if touched:
            try:
                db.session.execute(text(_RECOUNT_SQL), {'accs': sorted(touched)})
                db.session.commit()
            except Exception:
                db.session.rollback()
                logger.exception("Replay: event_count recount failed")

    logger.info("Replay complete: %s", stats)
    return stats
