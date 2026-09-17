-- Migration 0119: RAY7 — the screening engine between the HL7 listener and the DB.
--
-- Every inbound message passes through RAY7 before anything is written to the
-- reporting tables. It inspects, classifies and routes: clean messages project
-- normally, suspect ones project with a finding attached, and dangerous ones are held
-- back until a human clears them.
--
-- THE GOVERNING CONSTRAINT: RAY7 NEVER DROPS A MESSAGE.
--
-- This is what separates it from an ordinary validation layer, and it follows from
-- the archive: on the Oracle branches rejecting bad input was safe because the source
-- row stayed in the PACS and you could re-extract it. Here the message arrives once
-- over a socket and hl7_message_archive is the only copy that will ever exist.
-- Rejecting is deleting. So RAY7 has no reject verdict at all — only accepted,
-- flagged and quarantined, and all three keep the message.
--
-- For the same reason the listener still answers AA to everything, including messages
-- RAY7 quarantines. The finding is RAYD's internal business; NAK-ing would only make
-- a sender we do not control retry a message we have already safely stored.
--
--
-- DISPOSITION IS DRIVEN BY SEVERITY (operator decision, 2026-09-17)
-- ─────────────────────────────────────────────────────────────────
--   info      recorded, projects normally. Something to know, not to act on.
--   warning   projects normally, appears in the findings queue.
--   critical  QUARANTINED — parsed and stored, but withheld from the etl_* tables
--             until someone resolves it.
--
-- Only three levels, deliberately. A four- or five-level scale invites arguing about
-- the middle and produces a queue nobody reads, and an unread queue is worse than no
-- engine at all — it converts a visible problem into a false sense of safety. The
-- three levels map exactly onto the only three things the system can actually do.
--
-- Severity is per-rule and operator-editable (ray7_rules.severity overrides
-- default_severity), because which anomalies matter is a site question, not ours.
--
--
-- WHY ray7_study_state EXISTS
-- ───────────────────────────
-- The sequence rules need to know, cheaply and synchronously, what is already known
-- about an accession — RAY7 runs inline before the ACK, so no rule may scan
-- hl7_study_events. One indexed row per accession keeps every inline rule to a
-- primary-key lookup, and the absence sweep to a partial-index scan over only the
-- studies that are actually stalled.
--
-- It doubles as the projector's working set: it is already "the current state of this
-- study's lifecycle", which is precisely what has to be written into
-- etl_didb_studies.

-- ── Archive: content hash + RAY7 verdict ────────────────────────────────────────
--
-- content_hash separates the two duplicate cases that matter. Same sender, same
-- MSH-10, same hash is the known Mirth redelivery and is boring. Same sender, same
-- MSH-10, DIFFERENT hash means the control ID has been reused to carry different
-- content, which is the one duplicate case rated critical.
--
-- THIS ALSO FIXES THE DEDUPE KEY FROM 0115, which was wrong.
--
-- 0115 made (sending_app, message_control_id) unique. That looked right, but it
-- quietly contradicts the rule this whole engine is built on: with that key, a
-- control-ID reuse carrying NEW clinical content cannot be inserted at all. The
-- listener's only options would be to discard the message or to mangle its ID to get
-- it past the index — and discarding is exactly what must never happen, because the
-- archive is the only copy that will ever exist.
--
-- Adding content_hash to the key resolves it without a special case. An identical
-- redelivery still collides and is deduped, which is what we want. A reused ID
-- carrying different content no longer collides, so the message is stored safely —
-- and RAY7 detects the reuse afterwards by finding two archive rows that share a
-- sender and control ID but differ in hash. The message is kept AND flagged, rather
-- than one or the other.
--
-- NOT NULL DEFAULT '' because a NULL would defeat the unique index entirely (NULLs
-- never compare equal in Postgres, so every message with an unhashed body would be
-- treated as distinct and dedupe would silently stop working). Safe to add
-- unconditionally: no install has ingested a message yet.
ALTER TABLE hl7_message_archive
    ADD COLUMN IF NOT EXISTS content_hash     TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS ray7_status      TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS ray7_severity    TEXT,
    ADD COLUMN IF NOT EXISTS ray7_screened_at TIMESTAMP;

DROP INDEX IF EXISTS ux_hl7_archive_sender_control;

CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_archive_identity
    ON hl7_message_archive (sending_app, message_control_id, content_hash);

-- ADD CONSTRAINT has no IF NOT EXISTS, so it is guarded explicitly. Migration 0089
-- failed permanently on exactly this and had to be repaired; no internal COMMIT here
-- either, which is what broke 0073.
DO $ck$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_hl7_archive_ray7_status') THEN
        ALTER TABLE hl7_message_archive
            ADD CONSTRAINT ck_hl7_archive_ray7_status
            CHECK (ray7_status IN ('pending', 'accepted', 'flagged', 'quarantined'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_hl7_archive_ray7_severity') THEN
        ALTER TABLE hl7_message_archive
            ADD CONSTRAINT ck_hl7_archive_ray7_severity
            CHECK (ray7_severity IS NULL OR ray7_severity IN ('info', 'warning', 'critical'));
    END IF;
END
$ck$;

-- No separate index on content_hash. CONTROL_ID_REUSE asks "does another row share
-- this sender and control ID?", and (sending_app, message_control_id) is the leading
-- prefix of ux_hl7_archive_identity above, so that probe is already an index scan.
-- A second index would only serve a lookup by hash alone, which no rule performs —
-- and this is the hottest write path in the application, where an index that earns
-- nothing still costs something on every single message.

-- The quarantine queue: small by design, so a partial index keeps it nearly free.
CREATE INDEX IF NOT EXISTS idx_hl7_archive_ray7_open
    ON hl7_message_archive (ray7_status, received_at DESC)
    WHERE ray7_status IN ('flagged', 'quarantined');


-- ── ray7_rules — the catalogue, operator-editable ───────────────────────────────
--
-- Keyed on (rule_code, modality) rather than rule_code alone so a site can hold one
-- modality to a different standard than the rest. An MR that has been "started" for
-- two hours is unremarkable; a plain film in the same state is not, and a single
-- global threshold would either flood the queue with MR noise or never catch the
-- X-ray. modality = '' is the default that applies where no specific row exists.
CREATE TABLE IF NOT EXISTS ray7_rules (
    rule_code         TEXT      NOT NULL,
    modality          TEXT      NOT NULL DEFAULT '',   -- '' = applies to all
    category          TEXT      NOT NULL,              -- duplicate|sequence|referential|absence
    title             TEXT      NOT NULL,
    description       TEXT,

    default_severity  TEXT      NOT NULL,
    severity          TEXT,                            -- operator override; NULL = use default
    enabled           BOOLEAN   NOT NULL DEFAULT TRUE,

    -- Absence rules only: how long a study may sit at a rung before it is stalled.
    threshold_minutes INTEGER,

    updated_at        TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (rule_code, modality),
    CONSTRAINT ck_ray7_rules_category CHECK (category IN
        ('duplicate', 'sequence', 'referential', 'absence')),
    CONSTRAINT ck_ray7_rules_default_severity CHECK (default_severity IN
        ('info', 'warning', 'critical')),
    CONSTRAINT ck_ray7_rules_severity CHECK (severity IS NULL OR severity IN
        ('info', 'warning', 'critical'))
);


-- ── ray7_findings — what RAY7 found ─────────────────────────────────────────────
--
-- message_archive_id is nullable because absence findings have no message by
-- definition: the whole point of STALLED_ARRIVED is that the next message never came.
-- Those findings hang off an accession instead.
CREATE TABLE IF NOT EXISTS ray7_findings (
    id                 BIGSERIAL PRIMARY KEY,
    message_archive_id BIGINT REFERENCES hl7_message_archive(id) ON DELETE CASCADE,

    rule_code          TEXT      NOT NULL,
    severity           TEXT      NOT NULL,

    accession_number   TEXT,
    patient_id         TEXT,

    -- Rule-specific evidence: the conflicting timestamps, the other message's id, the
    -- unmapped code. JSONB so a new rule needs no migration to explain itself.
    detail             JSONB     NOT NULL DEFAULT '{}'::jsonb,

    created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at        TIMESTAMP,
    resolved_by        INTEGER,                        -- users.id
    resolution         TEXT,                           -- cleared|ignored|fixed_upstream
    resolution_note    TEXT,

    CONSTRAINT ck_ray7_findings_severity CHECK (severity IN ('info', 'warning', 'critical')),
    CONSTRAINT ck_ray7_findings_resolution CHECK (resolution IS NULL OR resolution IN
        ('cleared', 'ignored', 'fixed_upstream'))
);

-- A message is judged once per rule. Re-screening the same message (a replay) updates
-- rather than piling up duplicates of its own findings.
CREATE UNIQUE INDEX IF NOT EXISTS ux_ray7_findings_message_rule
    ON ray7_findings (message_archive_id, rule_code)
    WHERE message_archive_id IS NOT NULL;

-- Absence findings: at most one OPEN finding per accession per rule. Without this the
-- sweep would re-raise STALLED_ARRIVED on the same study every time it ran and bury
-- the queue within a day. Resolved rows drop out of the index, so a study that stalls
-- again later can legitimately raise a fresh finding.
CREATE UNIQUE INDEX IF NOT EXISTS ux_ray7_findings_open_absence
    ON ray7_findings (rule_code, accession_number)
    WHERE message_archive_id IS NULL AND resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_ray7_findings_open
    ON ray7_findings (severity, created_at DESC)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_ray7_findings_accession
    ON ray7_findings (accession_number)
    WHERE accession_number IS NOT NULL;


-- ── ray7_study_state — one row per accession, the lifecycle so far ──────────────
CREATE TABLE IF NOT EXISTS ray7_study_state (
    accession_number    TEXT      PRIMARY KEY,
    placer_order_number TEXT,
    patient_id          TEXT,

    modality            TEXT,
    aetitle             TEXT,
    room_name           TEXT,
    procedure_code      TEXT,
    patient_class       TEXT,

    -- The ladder. Each column is set once, by the first event that reports that rung,
    -- and carries the event's own timestamp rather than its arrival time.
    ordered_at          TIMESTAMP,   -- HIS ORM NW
    scheduled_at        TIMESTAMP,   -- rank 40
    arrived_at          TIMESTAMP,   -- rank 60
    started_at          TIMESTAMP,   -- rank 70
    completed_at        TIMESTAMP,   -- rank 100 — the cutover's new TAT anchor
    reported_at         TIMESTAMP,   -- ORU final
    cancelled_at        TIMESTAMP,

    current_rank        INTEGER   NOT NULL DEFAULT 0,
    is_closed           BOOLEAN   NOT NULL DEFAULT FALSE,
    event_count         INTEGER   NOT NULL DEFAULT 0,
    open_findings       INTEGER   NOT NULL DEFAULT 0,

    first_seen_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    last_event_at       TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ray7_state_placer
    ON ray7_study_state (placer_order_number)
    WHERE placer_order_number IS NOT NULL;

-- One partial index per stall condition. Each matches only studies actually sitting
-- at that rung, so the sweep reads the stalled set directly instead of scanning every
-- study ever received — the difference between a cheap periodic job and one that gets
-- slower every week the install runs.
CREATE INDEX IF NOT EXISTS idx_ray7_state_stalled_scheduled
    ON ray7_study_state (scheduled_at)
    WHERE arrived_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;

CREATE INDEX IF NOT EXISTS idx_ray7_state_stalled_arrived
    ON ray7_study_state (arrived_at)
    WHERE started_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;

CREATE INDEX IF NOT EXISTS idx_ray7_state_stalled_started
    ON ray7_study_state (started_at)
    WHERE completed_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;

CREATE INDEX IF NOT EXISTS idx_ray7_state_unreported
    ON ray7_study_state (completed_at)
    WHERE reported_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;


-- ── Seed the rule catalogue ─────────────────────────────────────────────────────
--
-- DO NOTHING on conflict: once a site has tuned a severity or a threshold through the
-- studio, re-applying this file must never reset it.
--
-- On the referential rules being info rather than critical: until MFN master data
-- lands, UNKNOWN_PROCEDURE and UNKNOWN_AETITLE will fire on virtually every message,
-- because RAYD has no procedure or device catalogue yet. Rating them critical would
-- quarantine the entire feed on day one and teach everyone to ignore the queue. They
-- are recorded so the gaps are measurable, and can be raised once MFN is flowing.
INSERT INTO ray7_rules (rule_code, category, title, description, default_severity, threshold_minutes)
VALUES
    -- Duplicates
    ('EXACT_REDELIVERY',   'duplicate', 'Exact redelivery',
     'Same sender and MSH-10, identical content. Expected: the SAP Mirth hub redelivers and there is no fix coming. Deduped silently.', 'info', NULL),
    ('CONTROL_ID_REUSE',   'duplicate', 'Control ID reused with different content',
     'Same sender and MSH-10 but the content differs. Dedupe keys on that ID, so the new message would be swallowed and a real event lost.', 'critical', NULL),
    ('LOGICAL_DUPLICATE',  'duplicate', 'Logical duplicate',
     'Different MSH-10, but same accession, state and event time. Usually the same event reaching us by two routes.', 'warning', NULL),
    ('REPEATED_TRANSITION','duplicate', 'Repeated transition',
     'The same rung reported again at a different time. Legitimate in this RIS — a worklist entry can genuinely arrive more than once — so informational, suppressed within the threshold window.', 'info', 10),

    -- Sequence
    ('LADDER_REGRESSION',  'sequence', 'Lifecycle went backwards',
     'An event ranked below the rung this study already reached. Distinct from mere out-of-order delivery, which is normal and not flagged.', 'warning', NULL),
    ('SKIPPED_RUNG',       'sequence', 'Rung skipped',
     'A study reached completion without a recorded arrival or start. The exam happened; the events did not.', 'warning', NULL),
    ('TIME_CONTRADICTION', 'sequence', 'Timestamps contradict the lifecycle',
     'Completed before arrived, started before scheduled, and so on. Produces negative turnaround times downstream.', 'warning', NULL),
    ('FUTURE_EVENT',       'sequence', 'Event timestamped in the future',
     'The sender clock is ahead of ours, or a date was mistyped at the source.', 'warning', NULL),
    ('ORPHAN_EVENT',       'sequence', 'Event for an unknown accession',
     'A lifecycle event for an accession no order or scheduling message ever introduced.', 'warning', NULL),

    -- Referential
    ('UNKNOWN_STATUS_CODE','referential', 'Status code not mapped',
     'The ORC status value has no row in hl7_status_map, so the transition cannot be classified at all. Projecting it would silently lose the event.', 'critical', NULL),
    ('MISSING_IDENTIFIER', 'referential', 'No usable identifier',
     'Neither an accession number nor a placer order number, so the message cannot be attached to any study.', 'critical', NULL),
    ('UNKNOWN_PROCEDURE',  'referential', 'Procedure code not in the catalogue',
     'Expected to fire constantly until MFN master data is loaded. Kept as info so it measures the gap instead of blocking the feed.', 'info', NULL),
    ('UNKNOWN_AETITLE',    'referential', 'Device AE title not mapped',
     'The device on a Started event has no row in aetitle_modality_map, so its utilisation will not be attributed.', 'info', NULL),
    ('UNKNOWN_PERFORMER',  'referential', 'Performer not recognised',
     'The identifier in the "by whom" field resolves to nobody known. Pending MFN staff master data.', 'info', NULL),
    ('PLACEHOLDER_DATA',   'referential', 'Placeholder or test data',
     'ER quick-registration placeholders (birth date 9999-11-11, sex U), known test patients, service accounts.', 'info', NULL),

    -- Absence — thresholds are opening guesses, meant to be tuned per modality
    ('STALLED_SCHEDULED',  'absence', 'Scheduled but never arrived',
     'Scheduled, and no arrival event since. Either a no-show nobody cancelled, or the arrival feed has stopped.', 'warning', 2880),
    ('STALLED_ARRIVED',    'absence', 'Arrived but never started',
     'Patient arrived and the exam never started. A real wait, or a missing start event.', 'warning', 240),
    ('STALLED_STARTED',    'absence', 'Started but never completed',
     'An exam left open on the device, or a completion message that never arrived. Inflates every duration metric it touches.', 'warning', 180),
    ('UNREPORTED',         'absence', 'Completed but never reported',
     'The exam completed and no ORU followed.', 'warning', 1440),
    ('ORPHAN_ORDER',       'absence', 'Order never scheduled',
     'A HIS order whose accession was never minted by the RIS, so it can never be matched to a study.', 'warning', 1440)
ON CONFLICT (rule_code, modality) DO NOTHING;
