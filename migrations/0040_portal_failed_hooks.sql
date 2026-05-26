-- Migration 0040: dead-letter table for failed portal ORM hooks
-- When process_orm_for_portal raises, the failure is recorded here so it
-- can be retried or investigated without losing the raw HL7 message.

CREATE TABLE IF NOT EXISTS portal_failed_hooks (
    id               SERIAL PRIMARY KEY,
    mrn              TEXT,
    accession_number TEXT,
    raw_message      TEXT,
    error_message    TEXT,
    retry_count      INT          NOT NULL DEFAULT 0,
    last_attempt     TIMESTAMP    NOT NULL DEFAULT NOW(),
    resolved         BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_portal_failed_hooks_resolved
    ON portal_failed_hooks (resolved)
    WHERE resolved = FALSE;
