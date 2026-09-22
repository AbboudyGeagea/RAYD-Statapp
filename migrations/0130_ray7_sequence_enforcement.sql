-- Migration 0130: RAY7 sequence enforcement — the 7-rung ladder, the per-site
-- profile that governs it, and the identity that makes a pre-accession rung possible.
--
-- WHAT THIS IS FOR
-- ────────────────
-- RAY7 today tolerates out-of-order delivery on purpose. utils/ray7.py's
-- LADDER_REGRESSION flags a lower rung only when its TIMESTAMPS contradict a higher
-- one already recorded; a message that merely arrived late, with timestamps that are
-- perfectly consistent, raises nothing. scripts/hl7_scenarios.py's out_of_order
-- scenario asserts exactly that — "Must produce NO findings".
--
-- That was the right default for a projector built to be order-independent. It is the
-- wrong default for the interface itself: a sender whose queue is disturbed is telling
-- us something about the integration, and it stays invisible until the day it also
-- loses a message. This migration is the schema half of making it visible.
--
-- The operator decisions this encodes (2026-09-22), so a later reader does not have to
-- reconstruct them from the code:
--
--   * Out-of-order delivery is CRITICAL — quarantined, withheld from etl_* until a
--     human acknowledges it. Not merely flagged.
--   * Ordering is judged PER STUDY, never per patient. A patient with three
--     concurrent studies interleaves their messages as a matter of course, and
--     judging those together would quarantine most of the emergency workload.
--   * Everything ships TURNED OFF. See ray7_ladder_profile below.
--
--
-- THE SEVEN RUNGS
-- ───────────────
--     20  ordered        ORC-1 = NW, from the HIS
--     40  scheduled      ORC-5 = SC — the RIS mints the accession here
--     60  arrived        ORC-5 = AR
--     70  started        ORC-5 = IP
--    100  completed      ORC-5 = CM
--    120  signed_prelim  OBX-11 = W  ("signed 1")
--    140  signed_final   OBX-11 = F  ("signed 3")
--
-- Ranks keep the existing spacing, which mirrors the RIS STATUS_KEY numbering the site
-- already uses, and leave room between rungs for a site that turns out to have an
-- intermediate state. Cancellation stays at -1: it is not a point on the ladder but a
-- terminal state reachable from anywhere.
--
-- 'ordered' is deliberately NOT seeded into hl7_status_map. That table is keyed on
-- ORC-5, and "new order" is an ORC-1 value — there is no ORC-5 code meaning ordered.
-- The rung is derived from ORC-1 = NW in the parser, and its RANK is owned by
-- ray7_ladder_profile below, so the two halves each live in exactly one place.


-- ── Widen the canonical-state vocabulary ────────────────────────────────────────
--
-- Both CHECKs currently permit five values. DROP-then-ADD rather than a guarded ADD:
-- the constraint already exists under this name, so an IF NOT EXISTS guard would find
-- it present and silently leave the OLD five-value definition in place — which is the
-- failure mode where the migration reports success and the new rungs are rejected at
-- INSERT time. Dropping first makes re-application idempotent AND correct.
--
-- No internal COMMIT anywhere in this file; that is what broke 0073.

ALTER TABLE hl7_status_map DROP CONSTRAINT IF EXISTS ck_hl7_status_canonical;
ALTER TABLE hl7_status_map
    ADD CONSTRAINT ck_hl7_status_canonical CHECK (canonical_state IN (
        'ordered', 'scheduled', 'arrived', 'started', 'completed',
        'signed_prelim', 'signed_final', 'cancelled'
    ));

ALTER TABLE hl7_study_events DROP CONSTRAINT IF EXISTS ck_hl7_event_canonical;
ALTER TABLE hl7_study_events
    ADD CONSTRAINT ck_hl7_event_canonical CHECK (canonical_state IN (
        'ordered', 'scheduled', 'arrived', 'started', 'completed',
        'signed_prelim', 'signed_final', 'cancelled'
    ));


-- ── ray7_ladder_profile — which rungs this install actually has ─────────────────
--
-- THE POINT OF THIS TABLE. A RIS does not have to send all seven states, and a
-- PACS-only install cannot send most of them. Without a per-site statement of what to
-- expect, every absent rung reads as a fault: SKIPPED_RUNG fires on every completed
-- study, and STALLED_ARRIVED fires on every study forever, because the start it is
-- waiting for is never coming. That is the flood that teaches people to stop reading
-- the queue — the exact failure 0119 was shaped to avoid.
--
-- TWO SWITCHES, deliberately separate:
--
--   expected       This site sends this status at all. Off ⇒ the absence rules stop
--                  asking for it, and SKIPPED_RUNG stops counting it as missing.
--   enforce_order  This rung must arrive in ladder position. Off ⇒ the rung is still
--                  recorded and still projects, it simply does not participate in the
--                  sequence judgement.
--
-- They are separate because the useful middle case exists: a site that genuinely
-- receives AR but over an unreliable path may want it recorded and time-tracked
-- without having its lateness quarantine anything.
--
-- BRIDGING. Turning a rung off must not blind the segment it sat in. With started
-- disabled the stall check becomes arrived → completed directly, rather than
-- arrived → started (which would fire on everything) plus started → completed (which
-- could never fire at all). The sweep computes the bridge from this table at run time;
-- nothing here needs to encode the pairs.
--
-- SEEDED ENTIRELY OFF. Every row ships expected = FALSE, enforce_order = FALSE, so
-- this migration is inert on arrival at every existing install. Merging it into CHN,
-- Bekaa, Mazloum or LAUMC changes no behaviour until that site's implementation
-- engineer turns rungs on deliberately. Given the enforcement verdict is CRITICAL —
-- quarantine, i.e. studies withheld from the reports — an opt-in default is the only
-- responsible one.
--
-- ladder_rank is structure rather than policy and is not meant to be edited; it is a
-- column so the studio can order the screen by it and so the sweep can compute
-- bridges without a hardcoded ladder. UNIQUE stops two rungs claiming one rank, which
-- would make "the rung above this one" ambiguous.

CREATE TABLE IF NOT EXISTS ray7_ladder_profile (
    rung          TEXT      PRIMARY KEY,
    ladder_rank   INTEGER   NOT NULL UNIQUE,
    label         TEXT      NOT NULL,

    expected      BOOLEAN   NOT NULL DEFAULT FALSE,
    enforce_order BOOLEAN   NOT NULL DEFAULT FALSE,

    notes         TEXT,
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_ray7_ladder_rung CHECK (rung IN (
        'ordered', 'scheduled', 'arrived', 'started', 'completed',
        'signed_prelim', 'signed_final'
    ))
);

INSERT INTO ray7_ladder_profile (rung, ladder_rank, label, notes)
VALUES
    ('ordered',        20, 'Ordered (NW)',
     'HIS order, ORC-1 = NW. The accession does not exist yet at this rung — the RIS mints it at scheduling — so these are matched on order number.'),
    ('scheduled',      40, 'Scheduled (SC)',
     'The accession is minted at this point. Walk-ins and emergencies legitimately never reach this rung.'),
    ('arrived',        60, 'Arrived (AR)',
     'Patient arrived. Vendor-specific; not a standard HL7 order status.'),
    ('started',        70, 'Started (IP)',
     'Exam started on the device.'),
    ('completed',     100, 'Exam done (CM)',
     'Clinical completion, the TAT anchor. Distinct from a PACS CM, which means images stored and lands in pacs_completed_at.'),
    ('signed_prelim', 120, 'Signed 1 (W)',
     'First signature, from OBX-11. Mapped through hl7_result_status_map, not hardcoded.'),
    ('signed_final',  140, 'Signed 3 (F)',
     'Final signature, from OBX-11. Reaching this rung is what closes a study.')
ON CONFLICT (rung) DO NOTHING;


-- ── hl7_result_status_map — OBX-11 → signature rung ────────────────────────────
--
-- Same shape and same reasoning as hl7_status_map (0117): which code carries which
-- signature level is a site fact, not a product fact, so it is configuration from the
-- first day rather than a constant someone has to come back and extract later. This
-- site reports W for the first signature and F for the final one; those are not the
-- HL7 standard meanings of those values, which is precisely why they must be mapped
-- rather than assumed.
--
-- P and C are seeded alongside because they are what a DIFFERENT sender is most likely
-- to use for the same two ideas, and an unmapped result status is a finding rather
-- than a silent no-op. A site that never sees them can deactivate the rows; a site
-- that does gets sensible behaviour without an emergency migration.
--
-- OBR-25 carries the same concept at report level and is used as the fallback when no
-- OBX-11 is present. One map serves both, since the value space is shared.

CREATE TABLE IF NOT EXISTS hl7_result_status_map (
    id              SERIAL    PRIMARY KEY,
    sending_app     TEXT      NOT NULL DEFAULT '',   -- MSH-3; '' = any sender
    result_status   TEXT      NOT NULL,              -- OBX-11, or OBR-25 as fallback
    canonical_state TEXT      NOT NULL,
    ladder_rank     INTEGER   NOT NULL,
    active          BOOLEAN   NOT NULL DEFAULT TRUE,
    notes           TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_hl7_result_canonical CHECK (canonical_state IN
        ('signed_prelim', 'signed_final'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_result_status_map
    ON hl7_result_status_map (sending_app, result_status);

INSERT INTO hl7_result_status_map (sending_app, result_status, canonical_state, ladder_rank, notes)
VALUES
    ('', 'W', 'signed_prelim', 120, 'Signed 1 at this site. Not the standard HL7 meaning of W — mapped, not assumed.'),
    ('', 'F', 'signed_final',  140, 'Signed 3 at this site — the final signature.'),
    ('', 'P', 'signed_prelim', 120, 'Standard HL7 preliminary. Seeded for senders that use it instead of W.'),
    ('', 'C', 'signed_final',  140, 'Correction to a final result. Treated as final; the corrected report replaces the original.')
ON CONFLICT (sending_app, result_status) DO NOTHING;


-- ── ray7_study_state — the two signature rungs, visit, and provisional identity ─
--
-- PROVISIONAL IDENTITY, and why the primary key does not change.
--
-- The ordered rung has no accession: the RIS mints one at scheduling, so an ORM NW
-- carries only a placer order number. Keying the lifecycle solely on accession means
-- rung 20 can never have a state row, which is why ordered_at has sat unwritten by the
-- ingest path since 0119 — only the absence sweep ever filled it, from hl7_orders.
--
-- The alternative designs were a second pre-accession table, or re-keying this table
-- onto a surrogate. Both were rejected for the same reason: every consumer —
-- ray7_findings, hl7_study_events, the projector, the sweep, the auto-resolve SQL —
-- addresses a study by accession_number as TEXT, and none of them is a foreign key.
-- Splitting the lifecycle across two tables would double every one of those reads.
--
-- So the key stays one column and is allowed to hold a PROVISIONAL value, of the form
--     ~ORD:<order number>
-- until the scheduling message supplies the real accession, at which point the row is
-- REKEYED in place and is_provisional clears. The tilde prefix cannot occur in an
-- accession issued by any of these systems, so a provisional key is recognisable on
-- sight and can never collide with a real one.
--
-- is_provisional exists so the projector can refuse these rows rather than having to
-- pattern-match the key. A provisional study must never reach etl_didb_studies: an
-- order nobody has scheduled is not a study, it belongs to etl_orders, and letting the
-- synthetic key through would put a fabricated accession in front of a radiologist.

ALTER TABLE ray7_study_state
    ADD COLUMN IF NOT EXISTS signed_prelim_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS signed_final_at  TIMESTAMP,
    ADD COLUMN IF NOT EXISTS visit_number     TEXT,
    ADD COLUMN IF NOT EXISTS is_provisional   BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE hl7_study_events
    ADD COLUMN IF NOT EXISTS visit_number TEXT;

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS visit_number TEXT;

-- Identity resolution runs inline, inside the sender's ACK window, so every step of
-- the precedence chain has to be an index lookup. Accession is the primary key and
-- placer already has idx_ray7_state_placer (0119); these cover the rest.
--
-- The visit index is partial and carries procedure_code because visit alone is not an
-- identity — it is the TIEBREAKER used when one order number maps to more than one
-- open lifecycle. Indexing it alone would invite a future reader to match on it.
CREATE INDEX IF NOT EXISTS idx_ray7_state_visit
    ON ray7_study_state (visit_number, procedure_code)
    WHERE visit_number IS NOT NULL;

-- The provisional set is small and drains as scheduling messages arrive, so a partial
-- index keeps the rekey lookup and the "orders never scheduled" sweep nearly free.
CREATE INDEX IF NOT EXISTS idx_ray7_state_provisional
    ON ray7_study_state (placer_order_number)
    WHERE is_provisional;

CREATE INDEX IF NOT EXISTS idx_hl7_events_visit
    ON hl7_study_events (visit_number)
    WHERE visit_number IS NOT NULL;

-- Stall detection for the two new rungs. Mirrors the partial-index pattern 0119
-- established for the rungs below, so the sweep reads only the studies actually
-- sitting at each rung rather than scanning every study ever received.
CREATE INDEX IF NOT EXISTS idx_ray7_state_unsigned_final
    ON ray7_study_state (signed_prelim_at)
    WHERE signed_final_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;


-- ── Fix is_closed: closure is the final signature, not completion ──────────────
--
-- THIS IS A BUG FIX, not a consequence of the new rungs, and it is worth stating
-- plainly because the symptom is silence rather than an error.
--
-- update_study_state() sets is_closed whenever an event of rank >= 100 lands. The
-- UNREPORTED sweep rule looks for `completed_at IS NOT NULL AND reported_at IS NULL
-- AND NOT is_closed`. But completed_at can only be set by a rank-100 event, which
-- always sets is_closed — so the predicate is unsatisfiable and the rule can never
-- fire. Measured on this install before the change: 7 of 7 completed studies closed,
-- 0 eligible. idx_ray7_state_unreported has been indexing an empty set since 0119.
--
-- With signature rungs above completion the old definition is also simply wrong:
-- a study whose images are done but whose report is unsigned is the study the queue
-- most needs to show, not one to stop tracking.
--
-- Closed now means the final signature arrived, or the study was cancelled.
--
-- THE BACKFILL TREATS reported_at AS EQUIVALENT TO A FINAL SIGNATURE for rows that
-- already exist. Those rows predate the signature rungs, so signed_final_at is NULL
-- for all of them; recomputing without this clause would reopen every finished study
-- at once and raise an UNREPORTED finding on each — the queue flood this whole design
-- is built to avoid, delivered by the migration meant to prevent it.

UPDATE ray7_study_state
   SET is_closed = (
           signed_final_at IS NOT NULL
        OR reported_at     IS NOT NULL
        OR cancelled_at    IS NOT NULL
       ),
       updated_at = NOW()
 WHERE is_closed <> (
           signed_final_at IS NOT NULL
        OR reported_at     IS NOT NULL
        OR cancelled_at    IS NOT NULL
       );


-- ── The rules ──────────────────────────────────────────────────────────────────
--
-- OUT_OF_SEQUENCE_DELIVERY is the new enforcement rule and the reason for this file.
--
-- It is the third of three cases that all present as "CM before AR", and keeping them
-- distinct is the whole design:
--
--   TIME_CONTRADICTION        the timestamps disagree — the source is wrong
--   SKIPPED_RUNG              the rung is genuinely absent — the event never existed
--   OUT_OF_SEQUENCE_DELIVERY  the timestamps agree, the ARRIVAL ORDER did not
--
-- CRITICAL by operator decision, so a study whose lifecycle arrived scrambled is held
-- out of the reports until a human has looked at it. It fires only for rungs whose
-- profile row has enforce_order set, which is why shipping the profile off makes this
-- inert.
--
-- It does NOT auto-resolve. The sweep closes SKIPPED_RUNG and ORPHAN_EVENT once the
-- missing rungs turn up, because those were judged against incomplete information and
-- became untrue. This one was true when raised and stays true: the messages did arrive
-- out of order, and no later message changes that. It clears when a human acknowledges
-- it, which also releases the study.
--
-- UNEXPECTED_RUNG is its counterpart on the configuration side: a status arriving for
-- a rung the site declared it does not send. Info, not a fault — the site's profile is
-- probably just out of date, and the useful response is to update it rather than to
-- hold traffic. Without this a wrong profile is invisible, and a profile nobody
-- notices is wrong is worse than no profile.
--
-- UNKNOWN_RESULT_STATUS mirrors UNKNOWN_STATUS_CODE for the signature rungs. Warning
-- rather than critical, unlike its ORC counterpart: an unmapped OBX-11 costs the
-- signature rung, but the report itself still lands in hl7_oru_reports and still
-- reaches the radiologist, so quarantining the message would remove more information
-- than it protects.

INSERT INTO ray7_rules (rule_code, category, title, description, default_severity, threshold_minutes)
VALUES
    ('OUT_OF_SEQUENCE_DELIVERY', 'sequence', 'Messages arrived out of order',
     'A lifecycle message was delivered after one that ranks above it, with timestamps that do not contradict each other — the events are sound but the sender''s queue was disturbed. Quarantined until acknowledged. Applies only to rungs with enforce_order set in the ladder profile.',
     'critical', NULL),

    ('UNEXPECTED_RUNG', 'referential', 'Status received for a rung this site does not send',
     'The ladder profile marks this rung as not expected at this install, but a message carrying it arrived. Usually means the profile needs updating rather than that anything is wrong with the message.',
     'info', NULL),

    ('UNKNOWN_RESULT_STATUS', 'referential', 'Result status not mapped',
     'The OBX-11 (or OBR-25) value has no row in hl7_result_status_map, so the report cannot be placed on the signature ladder. The report itself still lands; only the signature rung is lost.',
     'warning', NULL),

    ('STALLED_UNSIGNED', 'absence', 'Preliminary signed but never finalised',
     'A first signature arrived and the final one never followed. The study reads as complete to everyone downstream while the report is still provisional.',
     'warning', 2880)
ON CONFLICT (rule_code, modality) DO NOTHING;
