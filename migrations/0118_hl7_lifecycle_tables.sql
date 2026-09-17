-- Migration 0118: hl7_study_events and hl7_patients — the parsed layer.
--
-- Shape of the pipeline this completes:
--
--     MLLP  ──▶  hl7_message_archive        raw bytes, deduped, never rewritten
--                        │
--                     parse
--                        ▼
--                hl7_study_events           one row per lifecycle transition
--                hl7_patients               current demographics per patient
--                        │
--                    project
--                        ▼
--                etl_didb_studies / etl_patient_view / etl_orders
--                        │
--                        ▼
--                124 report queries, unchanged
--
-- The middle layer is what makes the projection order-independent. A study's row in
-- etl_didb_studies is not built incrementally as messages land — it is computed from
-- the full set of events known for that accession at the time. Messages may arrive
-- out of order, be redelivered, or be replayed months later after a parser fix, and
-- the projected row comes out the same either way. Writing straight from the parser
-- into etl_didb_studies would make the result depend on arrival order, which is the
-- one thing nobody controls.
--
--
-- hl7_study_events
-- ────────────────
-- One row per transition, never updated. The lifecycle is an append-only log and this
-- table is the log; correcting a mistake means reprojecting from it, not editing it.
--
-- Every event carries its own attribution because this feed provides it per
-- transition — who scheduled, who received the patient, who ran the scan and on which
-- device and in which room, who completed the read. That is strictly more than the
-- Oracle path ever gave: the RIS worklist table has no arrived or started date column
-- at all (which is why status history had to be mined), and the obvious technologist
-- column on the PPS table was confirmed 100% NULL at this install. Room name has no
-- Oracle counterpart whatsoever.
--
-- ladder_rank is denormalised onto the event rather than joined from hl7_status_map
-- at read time, deliberately. The map is operator-editable; if someone reclassifies a
-- code next year, events already recorded must keep the meaning they were ingested
-- with. A projection replayed after a map edit should reproduce history, not silently
-- reinterpret it. The raw ORC-1 and ORC-5 values are kept alongside so a genuine
-- re-interpretation is still possible when it is actually wanted.
--
-- performed_by_role is nullable on purpose. At LAUMC the identifier is an LDAP
-- account, and whether the role travels on the message or has to be resolved against
-- master data is still open with the integration specialist — MFN is the expected
-- source. Recording the ID now and resolving the role later costs nothing; the
-- reverse is not true.
--
--
-- hl7_patients
-- ────────────
-- ADT is the only source of demographics on this branch, and until now ADT was
-- acknowledged and dropped on the floor by the listener. Without this table there is
-- no birth date and no sex, so every age band and sex split in every report is empty.
--
-- Current state per patient, not a log: ADT A08 exists precisely to correct earlier
-- demographics, so last write wins is the correct semantics here, unlike the event
-- table above.
--
-- PLACEHOLDER BIRTH DATES. ER quick-registration at this site emits a literal
-- 9999-11-11 with sex U for a patient admitted before they are identified. Stored
-- naively that produces a negative age and poisons every age-banded report. The
-- parser is responsible for normalising an implausible or future birth date to NULL
-- before it reaches this table; the constraint below is the backstop that turns a
-- parser regression into a loud failure instead of a wrong chart.
--
-- patient_name is carried here because the ER and live screens need it and because
-- hl7_orders already stores it, so this introduces no exposure that did not exist.
-- Note that it is deliberately NOT projected into etl_patient_view, which has no name
-- column and should not gain one.

CREATE TABLE IF NOT EXISTS hl7_study_events (
    id                  BIGSERIAL PRIMARY KEY,
    message_archive_id  BIGINT REFERENCES hl7_message_archive(id) ON DELETE CASCADE,

    -- Identity. accession_number is the join key to PACS and to the ORU; before
    -- scheduling mints one, placer_order_number (ORC-2) is all the HIS order has.
    accession_number    TEXT,
    placer_order_number TEXT,
    patient_id          TEXT,

    canonical_state     TEXT      NOT NULL,
    ladder_rank         INTEGER   NOT NULL,
    event_time          TIMESTAMP NOT NULL,

    -- Attribution, per transition
    performed_by_id     TEXT,                  -- LDAP account at LAUMC
    performed_by_name   TEXT,
    performed_by_role   TEXT,                  -- tech | resident | radiologist, once resolvable

    -- Where. Both arrive on the Started event; neither has an Oracle equivalent.
    aetitle             TEXT,
    room_name           TEXT,

    modality            TEXT,
    procedure_code      TEXT,
    procedure_text      TEXT,
    patient_class       TEXT,
    patient_location    TEXT,

    -- What the wire actually said, kept for re-interpretation and for diagnosing a
    -- mapping that turns out to be wrong.
    raw_order_control   TEXT,
    raw_order_status    TEXT,

    received_at         TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_hl7_event_canonical CHECK (canonical_state IN (
        'scheduled', 'arrived', 'started', 'completed', 'cancelled'
    )),
    -- An event about nothing identifiable cannot be projected onto any study.
    CONSTRAINT ck_hl7_event_identified CHECK (
        accession_number IS NOT NULL OR placer_order_number IS NOT NULL
    )
);

-- The projector's main access path: every event for one accession, in ladder order.
CREATE INDEX IF NOT EXISTS idx_hl7_events_accession
    ON hl7_study_events (accession_number, ladder_rank)
    WHERE accession_number IS NOT NULL;

-- Stitching a HIS order to the accession the RIS mints for it later.
CREATE INDEX IF NOT EXISTS idx_hl7_events_placer
    ON hl7_study_events (placer_order_number)
    WHERE placer_order_number IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_hl7_events_time
    ON hl7_study_events (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_hl7_events_state_time
    ON hl7_study_events (canonical_state, event_time DESC);

-- Replay deletes and re-derives a message's events by archive row.
CREATE INDEX IF NOT EXISTS idx_hl7_events_archive
    ON hl7_study_events (message_archive_id);

-- Device and room utilisation, once the Started events are flowing.
CREATE INDEX IF NOT EXISTS idx_hl7_events_aetitle
    ON hl7_study_events (aetitle, event_time DESC)
    WHERE aetitle IS NOT NULL;


CREATE TABLE IF NOT EXISTS hl7_patients (
    patient_id         TEXT      PRIMARY KEY,
    patient_name       TEXT,
    birth_date         DATE,
    sex                TEXT,
    patient_class      TEXT,
    patient_location   TEXT,

    last_adt_event     TEXT,                   -- A01 / A02 / A08 …
    last_message_at    TIMESTAMP,              -- sender's clock, for last-write-wins
    message_archive_id BIGINT REFERENCES hl7_message_archive(id) ON DELETE SET NULL,
    updated_at         TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Backstop for the 9999-11-11 quick-registration placeholder. The parser should
    -- already have nulled it; if one reaches here the insert fails loudly rather than
    -- producing a negative age in a report nobody re-checks.
    --
    -- Bounded by fixed literals rather than CURRENT_DATE. Postgres accepts a
    -- non-immutable CHECK but never re-validates stored rows against it, so the
    -- constraint would quietly mean different things at different times — and a dump
    -- reloaded years later would be re-checked against a date that has moved. A wide
    -- literal window catches the placeholder, which is the actual failure mode, and
    -- means exactly the same thing forever.
    CONSTRAINT ck_hl7_patient_birth_date CHECK (
        birth_date IS NULL
        OR (birth_date > DATE '1880-01-01' AND birth_date < DATE '2100-01-01')
    )
);

CREATE INDEX IF NOT EXISTS idx_hl7_patients_updated
    ON hl7_patients (updated_at DESC);
