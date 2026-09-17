-- Migration 0117: hl7_status_map — configurable ORC status code → canonical state.
--
-- The RIS reports the exam lifecycle as a stream of status messages: Scheduled,
-- Arrived, Started, Completed. What RAYD cannot do is hardcode the codes carrying
-- those states, for two separate reasons.
--
-- First, they vary by hospital. The values seen so far are ORC-5 SC / AR / IP / CM,
-- but that is "usually", not "always", and this branch is meant to install at sites
-- whose integration nobody here has read yet. A per-install mapping row is the
-- difference between a configuration change and a code change.
--
-- Second, even at one site the code space is larger than the four states. The RIS
-- status ladder decoded for LAUMC carries roughly thirty custom sub-statuses —
-- Porter, Oral STR, Preparation, Contrast, Pre Exam, DNA, Call, Changed and so on —
-- which all roll up to one of the core states through a base-status pointer. On the
-- Oracle branches that pointer was read live from the RIS database on every event.
-- There is no database to read here, so the rollup has to live as data in RAYD, and
-- this table is where it lives. The standing rule from that work still applies: map
-- every code through configuration, never hardcode the ladder in a query.
--
-- This table is the backing store for the status-mapping screen in the customization
-- studio. It is seeded below with the defaults so an install works out of the box;
-- the screen edits rows, it does not create the concept.
--
-- MATCHING. Lookup is most-specific-first: an exact (sending_app, order_control,
-- order_status) match beats a per-sender wildcard, which beats the global default.
-- '' means "any" in both sending_app and order_control — stored as empty string
-- rather than NULL so the unique index actually constrains it, since NULLs do not
-- compare equal in Postgres and would let duplicate mappings accumulate silently.
--
-- LADDER_RANK orders forward progress and mirrors the RIS STATUS_KEY numbering the
-- site already uses (40 Scheduled, 60 Arrived, 70 Started, 100 Exam Done), so the two
-- systems can be reasoned about side by side. The projector uses it to stay
-- order-independent: messages can arrive out of sequence or be replayed in any order,
-- and a lower-ranked event must never overwrite the timestamp of a higher-ranked one
-- already recorded. Cancellation is not a point on that ladder — it is a terminal
-- state that can arrive from anywhere — so it carries -1 and is handled explicitly
-- rather than by comparison.

CREATE TABLE IF NOT EXISTS hl7_status_map (
    id              SERIAL PRIMARY KEY,

    sending_app     TEXT      NOT NULL DEFAULT '',   -- MSH-3; '' = any sender
    order_control   TEXT      NOT NULL DEFAULT '',   -- ORC-1; '' = any
    order_status    TEXT      NOT NULL,              -- ORC-5, the code as it arrives

    canonical_state TEXT      NOT NULL,
    ladder_rank     INTEGER   NOT NULL,
    active          BOOLEAN   NOT NULL DEFAULT TRUE,
    notes           TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_hl7_status_canonical CHECK (canonical_state IN (
        'scheduled', 'arrived', 'started', 'completed', 'cancelled'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_status_map_lookup
    ON hl7_status_map (sending_app, order_control, order_status);

-- Seed: the codes confirmed for this integration, as global defaults.
--
-- ON CONFLICT DO NOTHING so re-running never stomps an operator's edit — once a site
-- has tuned a row through the studio, this migration must not reset it. (It is also
-- what makes the file safe to re-apply, per the idempotency rule these migrations
-- follow.)
INSERT INTO hl7_status_map
    (sending_app, order_control, order_status, canonical_state, ladder_rank, notes)
VALUES
    ('', '', 'SC', 'scheduled',  40, 'Scheduled — accession is minted at this point'),
    ('', '', 'AR', 'arrived',    60, 'Patient arrived; vendor-specific, not a standard HL7 order status'),
    ('', '', 'IP', 'started',    70, 'In progress — exam started on the device'),
    ('', '', 'CM', 'completed', 100, 'Completed — the TAT anchor the cutover moves to'),
    ('', '', 'CA', 'cancelled',  -1, 'Cancelled'),
    ('', '', 'DC', 'cancelled',  -1, 'Discontinued'),
    ('', '', 'ER', 'cancelled',  -1, 'Error — sender retracted the order')
ON CONFLICT (sending_app, order_control, order_status) DO NOTHING;
