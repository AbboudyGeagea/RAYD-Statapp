"""
utils/brayn_health.py
────────────────────────────────────────────────────────────────
BRAYN layer ③ — interface health: how each SENDER is behaving, as rates.

    listener ─▶ record(sender, metrics, unknowns) ─▶ brayn_interface_stats (15-min buckets)
                                                └─▶ brayn_unknown_values
    sweep    ─▶ evaluate_alerts() ─▶ one open alert per (rule, sender) past threshold_pct

WHY RATES AND NOT FINDINGS
Redelivery, control-ID reuse, out-of-order delivery, duplicates through two routes:
none of these is wrong with a STUDY. Each says something about the interface, and
what it says is in the trend — "this RIS started delivering 30% of its events out of
order an hour ago" — not in any single message. As per-study findings (and, for two
of them, quarantines) they buried the queue and held valid data; as counters they
are cheap, they cannot hold anything, and they answer the question an operator
actually asks.

WHY ITS OWN TRANSACTION
The counters are hot rows: every message one sender delivers in a quarter-hour adds
to the same MESSAGES row. Incremented inside the ingest transaction, that row's lock
would be held until the message committed, serialising every connection from that
sender behind every other. record() writes on its own short-lived connection after
the message has committed, and a failure here costs a count, never a message.
"""
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("BRAYN_HEALTH")

BUCKET_MINUTES = 15
WINDOW_MINUTES = 60

# Below this many messages in the window a rate is noise: two out-of-order messages
# out of three is 67% and means nothing.
MIN_VOLUME = 20

# The interface rules that are rates. Anything else in brayn_rules is not counted here.
INTERFACE_RULES = (
    'EXACT_REDELIVERY', 'CONTROL_ID_REUSE', 'OUT_OF_SEQUENCE_DELIVERY',
    'LOGICAL_DUPLICATE', 'REPEATED_TRANSITION', 'UNEXPECTED_RUNG', 'UNKNOWN_AETITLE',
    'UNKNOWN_RESULT_STATUS', 'BRAYN_DEGRADED', 'BRAYN_BUDGET_EXCEEDED',
)

_BUCKET = (f"date_trunc('hour', LOCALTIMESTAMP) + "
           f"floor(extract(minute FROM LOCALTIMESTAMP) / {BUCKET_MINUTES}) "
           f"* interval '{BUCKET_MINUTES} minutes'")

_STATS_SQL = f"""
INSERT INTO brayn_interface_stats (bucket_start, sending_app, metric, n)
SELECT {_BUCKET}, :app, m, 1 FROM unnest(CAST(:metrics AS text[])) AS m
ON CONFLICT (bucket_start, sending_app, metric) DO UPDATE
   SET n = brayn_interface_stats.n + 1
"""

_UNKNOWN_SQL = """
INSERT INTO brayn_unknown_values (kind, sending_app, value, n, sample_archive_id)
VALUES (:kind, :app, :value, 1, :aid)
ON CONFLICT (kind, sending_app, value) DO UPDATE
   SET n = brayn_unknown_values.n + 1,
       last_seen = NOW(),
       sample_archive_id = COALESCE(EXCLUDED.sample_archive_id,
                                    brayn_unknown_values.sample_archive_id)
"""


def record(sending_app, metrics, unknowns):
    """
    Add one to each metric for this sender's current bucket, and register each
    unknown (kind, value, archive_id). Own connection, own commit, never raises.

    Metrics are sorted before the insert so two connections always take the row
    locks in the same order — the difference between contention and a deadlock.
    """
    app = sending_app or ''
    try:
        with db.engine.begin() as conn:
            conn.execute(text(_STATS_SQL), {'app': app, 'metrics': sorted(set(metrics))})
            for kind, value, archive_id in sorted(set(unknowns), key=lambda u: (u[0], str(u[1]))):
                if value in (None, ''):
                    continue
                conn.execute(text(_UNKNOWN_SQL), {'kind': kind, 'app': app,
                                                  'value': str(value)[:200], 'aid': archive_id})
    except Exception:
        logger.exception("BRAYN health: could not record %s for sender %r", metrics, app)


def rates(window_minutes=WINDOW_MINUTES):
    """
    {sender: {'MESSAGES': n, 'HELD': n, <rule>: n, ...}} over the window, for the
    console and for evaluate_alerts(). The window is whole buckets, so "the last
    hour" is the current quarter-hour plus the three before it.
    """
    rows = db.session.execute(text(f"""
        SELECT sending_app, metric, SUM(n) AS n
          FROM brayn_interface_stats
         WHERE bucket_start > {_BUCKET} - make_interval(mins => :w)
         GROUP BY sending_app, metric
    """), {'w': window_minutes}).mappings().all()
    out = {}
    for r in rows:
        out.setdefault(r['sending_app'], {})[r['metric']] = int(r['n'])
    return out


_ALERT_SQL = """
INSERT INTO brayn_findings
    (message_archive_id, rule_code, severity, accession_number, sending_app, detail)
VALUES (NULL, :code, :severity, NULL, :app, CAST(:detail AS jsonb))
ON CONFLICT (rule_code, sending_app)
    WHERE message_archive_id IS NULL AND accession_number IS NULL AND resolved_at IS NULL
DO UPDATE SET detail = EXCLUDED.detail, severity = EXCLUDED.severity
"""

_CLEAR_SQL = """
UPDATE brayn_findings
   SET resolved_at = NOW(), resolution = 'cleared',
       resolution_note = 'auto: the rate is back under its threshold'
 WHERE rule_code = :code AND sending_app = :app
   AND message_archive_id IS NULL AND accession_number IS NULL AND resolved_at IS NULL
"""


def evaluate_alerts():
    """
    Raise or clear one alert per (interface rule, sender) from the last hour's rate.

    An alert stays open while the sender is quiet: no traffic is not evidence that
    the interface has recovered, only that nothing is being measured. It clears the
    first time the rate is measured under threshold on enough volume.

    Returns {'raised': n, 'cleared': n}. Called by the sweep, which commits.
    """
    import json
    out = {'raised': 0, 'cleared': 0}
    try:
        cfg = db.session.execute(text("""
            SELECT rule_code, COALESCE(severity, default_severity) AS severity, threshold_pct
              FROM brayn_rules
             WHERE modality = '' AND enabled AND threshold_pct IS NOT NULL
               AND rule_code = ANY(:codes)
        """), {'codes': list(INTERFACE_RULES)}).mappings().all()
        if not cfg:
            return out
        by_sender = rates()
    except Exception:
        logger.exception("BRAYN health: could not read rates or rules")
        return out

    for app, counts in by_sender.items():
        volume = counts.get('MESSAGES', 0)
        if volume < MIN_VOLUME:
            continue
        for rule in cfg:
            n = counts.get(rule['rule_code'], 0)
            pct = round(100.0 * n / volume, 2)
            try:
                with db.session.begin_nested():
                    if pct >= float(rule['threshold_pct']):
                        db.session.execute(text(_ALERT_SQL), {
                            'code': rule['rule_code'], 'severity': rule['severity'], 'app': app,
                            'detail': json.dumps({
                                'rate_pct': pct, 'count': n, 'messages': volume,
                                'threshold_pct': float(rule['threshold_pct']),
                                'window_minutes': WINDOW_MINUTES,
                            }),
                        })
                        out['raised'] += 1
                    else:
                        out['cleared'] += db.session.execute(
                            text(_CLEAR_SQL), {'code': rule['rule_code'], 'app': app}).rowcount
            except Exception:
                logger.exception("BRAYN health: alert %s for %r failed", rule['rule_code'], app)
    return out
