-- Migration 0115: hl7_message_archive — every inbound MLLP message, stored verbatim.
--
-- This is the foundation the rest of the HL7 branch rests on, and it exists for one
-- reason: on the Oracle branches, a parsing mistake was recoverable. The source data
-- still sat in the PACS database, so you fixed the extractor and re-ran the phase.
-- Here the message arrives once, over a socket, and is gone. Without an archive the
-- only way to correct a parser bug is to ask the hospital to re-send — which for
-- historical traffic generally means "no".
--
-- So: the listener writes the raw bytes here FIRST, acknowledges, and only then
-- parses. Everything downstream (hl7_study_events, hl7_patients, and the etl_*
-- projection) is derived and can be rebuilt from this table alone.
--
-- IDEMPOTENCY. Duplicate delivery is a permanent fact of this integration, not a
-- transient fault — the SAP Mirth hub re-sends and there is no fix coming — so
-- MSH-10 (message control ID) dedupe is mandatory rather than defensive.
--
-- The unique key is (sending_app, message_control_id), NOT message_control_id alone.
-- Three separate systems feed this install (HIS, RIS, PACS) and each mints its own
-- control IDs, commonly from a plain numeric sequence. Keying on the ID alone would
-- eventually treat one system's message #12345 as a duplicate of another's and drop
-- it silently. Over-deduping loses clinical data; under-deduping only costs a
-- conflict that the projector ignores. The asymmetry decides it.
--
-- MSH-3 is the discriminator because it is the field that actually differs per
-- source (per the LAUMC capture, MSH-4 is 'SAP_P' for both sites). Where MSH-10 is
-- absent or blank the listener substitutes a content hash of the raw message, so the
-- NOT NULL below never forces a message to be discarded — a message with no control
-- ID still dedupes correctly against an identical redelivery of itself.
--
-- parse_status is the dead-letter mechanism: a message that fails to parse is still
-- archived, marked 'error' with the reason, and stays queryable. A parse failure must
-- never lose the message, because the message is the only copy.

CREATE TABLE IF NOT EXISTS hl7_message_archive (
    id                 BIGSERIAL PRIMARY KEY,

    -- MSH-10, or a content hash when the sender leaves it blank
    message_control_id TEXT      NOT NULL,
    -- MSH-3; '' rather than NULL so it participates in the unique index
    sending_app        TEXT      NOT NULL DEFAULT '',
    sending_facility   TEXT,                      -- MSH-4
    message_type       TEXT,                      -- MSH-9, e.g. ORM^O01 / ORU^R01 / ADT^A08
    message_datetime   TIMESTAMP,                 -- MSH-7, the sender's own clock
    hl7_version        TEXT,                      -- MSH-12; this feed carries 2.3, 2.3.1 and 2.4

    raw_message        TEXT      NOT NULL,
    source_ip          TEXT,
    received_at        TIMESTAMP NOT NULL DEFAULT NOW(),

    -- pending | ok | error | ignored  ('ignored' = a type we deliberately do not store)
    parse_status       TEXT      NOT NULL DEFAULT 'pending',
    parse_error        TEXT,
    parsed_at          TIMESTAMP,
    projected_at       TIMESTAMP,

    CONSTRAINT ck_hl7_archive_parse_status
        CHECK (parse_status IN ('pending', 'ok', 'error', 'ignored'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_archive_sender_control
    ON hl7_message_archive (sending_app, message_control_id);

-- Operational views: "what arrived recently", "what is broken", "what type mix".
CREATE INDEX IF NOT EXISTS idx_hl7_archive_received
    ON hl7_message_archive (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_hl7_archive_type
    ON hl7_message_archive (message_type);

-- Partial: the healthy rows are the overwhelming majority and nobody queries for them
-- by status, so indexing only the exceptions keeps this small and the writes cheap.
CREATE INDEX IF NOT EXISTS idx_hl7_archive_unhealthy
    ON hl7_message_archive (parse_status, received_at DESC)
    WHERE parse_status <> 'ok';

-- Replay driver: find everything not yet projected, oldest first.
CREATE INDEX IF NOT EXISTS idx_hl7_archive_unprojected
    ON hl7_message_archive (id)
    WHERE projected_at IS NULL;
