-- Migration 0135: RAY7 rebuilt in four layers (operator decision, 2026-10-25).
--
-- WHY. The end-to-end suite (tests/) showed RAY7's quarantine mostly withheld GOOD
-- data: on the 2026-09-24 load 360 of 660 messages were held for control-ID reuse
-- although every one of them was valid, and with the lifecycle state now proven
-- order-independent (every permutation, tests/test_permutations.py) holding an
-- out-of-order message protects nothing. A held message also left its study
-- half-projected with nothing in the reports saying so.
--
-- THE FOUR LAYERS
--   intake     inline, before the ACK. Can the message be attached to a study and
--              classified? If not it is HELD: stored, nothing derived, released by
--              fixing the configuration and replaying. The only layer that holds.
--   quality    notes on a STUDY, judged on its current lifecycle after every
--              change, so they open and close themselves. Never withhold.
--   interface  per-sender rates (redelivery, control-ID reuse, out-of-order ...)
--              counted in ray7_interface_stats; an alert is raised only when a rate
--              crosses its threshold_pct. Never per-study rows.
--   workflow   the absence sweep, unchanged: stalled, unreported, unsigned, orphan.
--
-- Nothing is truncated: every rule row is kept and re-filed. Two retire outright
-- (LADDER_REGRESSION folds into the state-based TIME_CONTRADICTION; a future
-- timestamp stays a sticky study note under FUTURE_EVENT).


-- 1. The category vocabulary becomes the layer.
ALTER TABLE ray7_rules DROP CONSTRAINT IF EXISTS ck_ray7_rules_category;

UPDATE ray7_rules SET category = CASE
    WHEN rule_code IN ('MISSING_IDENTIFIER', 'UNKNOWN_STATUS_CODE') THEN 'intake'
    WHEN rule_code IN ('TIME_CONTRADICTION', 'SKIPPED_RUNG', 'ORPHAN_EVENT', 'FUTURE_EVENT',
                       'PLACEHOLDER_DATA', 'LADDER_REGRESSION') THEN 'quality'
    WHEN rule_code IN ('STALLED_SCHEDULED', 'STALLED_ARRIVED', 'STALLED_STARTED',
                       'UNREPORTED', 'STALLED_UNSIGNED', 'ORPHAN_ORDER') THEN 'workflow'
    ELSE 'interface' END,
    updated_at = NOW();

ALTER TABLE ray7_rules ADD CONSTRAINT ck_ray7_rules_category CHECK (category IN
    ('intake', 'quality', 'interface', 'workflow'));


-- 2. Interface rules alert on a RATE. threshold_pct = percent of a sender's messages
--    in the last hour; NULL = measured and shown, never alerted.
ALTER TABLE ray7_rules ADD COLUMN IF NOT EXISTS threshold_pct NUMERIC(6,2);

UPDATE ray7_rules SET
    default_severity = 'warning',
    threshold_pct = CASE rule_code
        WHEN 'CONTROL_ID_REUSE'         THEN 1
        WHEN 'OUT_OF_SEQUENCE_DELIVERY' THEN 10
        WHEN 'LOGICAL_DUPLICATE'        THEN 5
        WHEN 'RAY7_DEGRADED'            THEN 1
        WHEN 'RAY7_BUDGET_EXCEEDED'     THEN 1
        ELSE threshold_pct END,
    updated_at = NOW()
 WHERE category = 'interface' AND modality = '';

UPDATE ray7_rules SET
    title = 'Messages delivered out of order',
    description = 'Share of a sender''s lifecycle messages that arrived after a later rung of the same study. The data is unaffected — the lifecycle is order-independent — but a rising rate means the sender''s queue is disturbed. Measured on the rungs marked "measure order" in the ladder profile (all rungs when none are marked).'
 WHERE rule_code = 'OUT_OF_SEQUENCE_DELIVERY';

UPDATE ray7_rules SET
    title = 'Control ID reused with different content',
    description = 'Share of a sender''s messages carrying an MSH-10 it already used for different content. Both messages are stored and both are ingested; nothing is lost. A non-zero rate is still a sender bug worth raising with its vendor.'
 WHERE rule_code = 'CONTROL_ID_REUSE';

UPDATE ray7_rules SET
    description = 'Share of a sender''s messages that were byte-identical redeliveries. Expected at a Mirth hub; measured, not alerted, unless a threshold is set.'
 WHERE rule_code = 'EXACT_REDELIVERY';

UPDATE ray7_rules SET
    title = 'Timestamps contradict the lifecycle',
    description = 'A later rung of this study is stamped earlier than an earlier one — completed before arrived, signed before the exam was done. Judged on the whole lifecycle after every change; cancellation closes it, otherwise a person does. Produces negative turnaround times downstream.'
 WHERE rule_code = 'TIME_CONTRADICTION';

UPDATE ray7_rules SET
    description = 'The study is completed but an expected arrival or start is missing. Judged on the whole lifecycle: a late-delivered rung closes it.'
 WHERE rule_code = 'SKIPPED_RUNG';

UPDATE ray7_rules SET
    title = 'No order or scheduling received',
    description = 'The study has exam events but no HIS order and no scheduling message. Closes itself when either arrives; open for good means the order feed missed it.'
 WHERE rule_code = 'ORPHAN_EVENT';

UPDATE ray7_rules SET
    enabled = FALSE,
    description = 'Retired in 0135: the state-based TIME_CONTRADICTION covers an event stamped after a higher rung.'
 WHERE rule_code = 'LADDER_REGRESSION';


-- 3. Per-sender counters, in 15-minute buckets. One row per (bucket, sender,
--    metric); metric is the interface rule code, or MESSAGES / HELD.
CREATE TABLE IF NOT EXISTS ray7_interface_stats (
    bucket_start TIMESTAMP NOT NULL,
    sending_app  TEXT      NOT NULL DEFAULT '',
    metric       TEXT      NOT NULL,
    n            INTEGER   NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, sending_app, metric)
);

CREATE INDEX IF NOT EXISTS idx_ray7_interface_stats_recent
    ON ray7_interface_stats (bucket_start DESC, sending_app);


-- 4. Values this install does not know yet — the implementation engineer's to-do
--    list: AE titles to map, status codes to map, result statuses, rungs.
CREATE TABLE IF NOT EXISTS ray7_unknown_values (
    kind              TEXT      NOT NULL,      -- aetitle | status_code | result_status | rung
    sending_app       TEXT      NOT NULL DEFAULT '',
    value             TEXT      NOT NULL,
    first_seen        TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen         TIMESTAMP NOT NULL DEFAULT NOW(),
    n                 INTEGER   NOT NULL DEFAULT 0,
    sample_archive_id BIGINT,
    PRIMARY KEY (kind, sending_app, value)
);


-- 5. Interface alerts are per SENDER, so findings learn which sender they are about,
--    and get their own one-open-alert-per-sender-per-rule guarantee.
ALTER TABLE ray7_findings ADD COLUMN IF NOT EXISTS sending_app TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_ray7_findings_open_interface
    ON ray7_findings (rule_code, sending_app)
    WHERE message_archive_id IS NULL AND accession_number IS NULL AND resolved_at IS NULL;


-- 6. Close what the old engine raised per message and the new one never will. The
--    held messages behind them are NOT released here — a migration cannot replay.
--    Use "Replay held messages" in the RAY7 console (or python app.py -m): re-screened
--    under the new rules, only unidentifiable / unclassifiable messages stay held.
UPDATE ray7_findings
   SET resolved_at = NOW(), resolution = 'cleared',
       resolution_note = 'retired in 0135: now measured as an interface rate, or judged on the study'
 WHERE resolved_at IS NULL
   AND message_archive_id IS NOT NULL
   AND rule_code IN ('CONTROL_ID_REUSE', 'OUT_OF_SEQUENCE_DELIVERY', 'LOGICAL_DUPLICATE',
                     'REPEATED_TRANSITION', 'LADDER_REGRESSION', 'TIME_CONTRADICTION',
                     'SKIPPED_RUNG', 'ORPHAN_EVENT', 'UNKNOWN_AETITLE', 'UNEXPECTED_RUNG',
                     'UNKNOWN_RESULT_STATUS', 'EXACT_REDELIVERY', 'RAY7_DEGRADED',
                     'RAY7_BUDGET_EXCEEDED', 'FUTURE_EVENT');


-- 7. The ladder profile's second switch now MEASURES order instead of quarantining.
UPDATE ray7_ladder_profile
   SET notes = COALESCE(notes, '') ||
               CASE WHEN COALESCE(notes, '') LIKE '%0135%' THEN ''
                    ELSE ' [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]' END,
       updated_at = NOW();
