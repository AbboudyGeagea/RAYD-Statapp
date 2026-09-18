-- Migration 0123: hl7_field_mappings — per-site HL7 field mapping, owned by
-- implementation engineers rather than R&D.
--
-- WHY THIS EXISTS
-- Everything the HL7 pipeline parses is currently hardcoded: 51 segment/field
-- references in utils/hl7_parse.py, chosen against one hospital's integration.
-- That is correct for LAUMC and wrong for a distribution branch, because which
-- field carries the AE title, the performing technologist or the room number is
-- a per-hospital fact, not a product fact. Without this table, every new site
-- needs a developer and a release.
--
-- The legacy `settings.hl7_field_map` JSON blob covered only ORM -> hl7_orders
-- and had no UI at all. This replaces it for the new pipeline: a real table, with
-- scoping, fallback chains, transforms, and an audit trail.
--
--
-- THIS DELIBERATELY ALLOWS MORE THAN IS SAFE, BY OPERATOR DECISION (2026-09-18)
--
-- Mappings can target RAYD's parsed fields OR write straight into an etl_* column.
-- The second is genuinely dangerous and the danger is not hypothetical: mapping an
-- arrival timestamp onto completed_at silently inverts turnaround time on every
-- report that uses it, produces no error, and looks entirely plausible in the
-- data. The standing rule recorded for the customization studio was that source
-- remapping is never delegated to a non-developer for exactly this reason.
--
-- The operator has overridden that for HL7, knowingly, because HL7 layouts differ
-- per hospital in a way Oracle schemas do not, and waiting on R&D per site is the
-- larger cost. So the guardrails here are VISIBILITY, not restriction:
--
--   * target_kind + target_field are validated against a catalogue, so a typo
--     fails loudly instead of silently mapping nothing
--   * is_dangerous marks targets that can corrupt derived metrics without error,
--     so the UI can warn at the moment of editing rather than after go-live
--   * updated_by / updated_at record who changed what
--   * every mapping change is retroactively correctable by replaying the archive,
--     which is the real safety net and the reason this is defensible at all
--
--
-- OVERLAY SEMANTICS: AN EMPTY TABLE MEANS TODAY'S BEHAVIOUR
-- The parser tries mappings first, in priority order, and falls back to its
-- built-in logic when none produces a value. So this migration changes nothing on
-- its own — it opens a door rather than moving anything through it. The seeded
-- rows below reproduce the current hardcoded positions so an engineer can SEE
-- what the product assumes before deciding to change it.

CREATE TABLE IF NOT EXISTS hl7_field_mappings (
    id              SERIAL PRIMARY KEY,

    -- ── Scope. '' means "any", and more specific rows win. ──────────────────
    sending_app     TEXT    NOT NULL DEFAULT '',   -- MSH-3
    message_kind    TEXT    NOT NULL DEFAULT '',   -- adt|order|status|result

    -- ── Target: where the value goes ────────────────────────────────────────
    -- parsed  -> a field on the in-flight message, feeding the whole pipeline
    -- study / patient / order -> written straight to that etl_* table's column
    target_kind     TEXT    NOT NULL DEFAULT 'parsed',
    target_field    TEXT    NOT NULL,

    -- ── Source: where it comes from. Field and component are HUMAN numbers, ──
    -- i.e. OBR-24 is segment='OBR', field_index=24. The MSH off-by-one is
    -- handled in code so nobody configuring this has to know about it.
    segment         TEXT    NOT NULL,
    field_index     INTEGER NOT NULL,
    component_index INTEGER,                        -- NULL = the whole field
    repeat_index    INTEGER,                        -- NULL = first repetition

    -- ── Behaviour ───────────────────────────────────────────────────────────
    -- Lower priority is tried first, which is what makes fallback chains work:
    -- "AE title is in OBR-21, or ORC-19 if that is empty".
    priority        INTEGER NOT NULL DEFAULT 100,
    transform       TEXT    NOT NULL DEFAULT 'text',
    active          BOOLEAN NOT NULL DEFAULT TRUE,

    notes           TEXT,
    updated_by      INTEGER,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_hl7_fm_target_kind CHECK (target_kind IN
        ('parsed', 'study', 'patient', 'order')),
    CONSTRAINT ck_hl7_fm_transform CHECK (transform IN
        ('text', 'upper', 'datetime', 'date', 'name_xpn', 'name_xcn', 'number')),
    CONSTRAINT ck_hl7_fm_field_index CHECK (field_index > 0),
    CONSTRAINT ck_hl7_fm_component CHECK (component_index IS NULL OR component_index > 0)
);

-- One mapping per (scope, target, priority): the same target may legitimately
-- have several rows forming a fallback chain, distinguished by priority.
CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_fm_slot
    ON hl7_field_mappings (sending_app, message_kind, target_kind, target_field, priority);

-- The parser's read path: everything applicable to one message, in try order.
CREATE INDEX IF NOT EXISTS idx_hl7_fm_lookup
    ON hl7_field_mappings (target_kind, message_kind, sending_app, priority)
    WHERE active;


-- ── The catalogue of legal targets ──────────────────────────────────────────
--
-- Exists so the editor offers a dropdown rather than a free-text box. A typo in a
-- target name maps a value into nothing at all, and nothing would ever report
-- that — so the constraint here is about catching mistakes, not about limiting
-- what an engineer is permitted to do.
--
-- is_dangerous flags the targets whose misuse corrupts a derived metric silently.
-- The UI warns on these; it does not prevent them.
CREATE TABLE IF NOT EXISTS hl7_field_targets (
    target_kind  TEXT    NOT NULL,
    target_field TEXT    NOT NULL,
    data_type    TEXT    NOT NULL,          -- text | datetime | date | number
    label        TEXT    NOT NULL,
    description  TEXT,
    is_dangerous BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order   INTEGER NOT NULL DEFAULT 100,
    PRIMARY KEY (target_kind, target_field)
);

INSERT INTO hl7_field_targets
    (target_kind, target_field, data_type, label, description, is_dangerous, sort_order)
VALUES
    -- Identity. Getting these wrong breaks matching outright, which at least
    -- fails visibly rather than quietly.
    ('parsed','accession_number','text','Accession number',
     'The join key to everything. Usually ORC-3 or OBR-3.', TRUE, 10),
    ('parsed','placer_order_number','text','Placer order number',
     'The HIS order identity, before the RIS mints an accession. Usually ORC-2.', FALSE, 20),
    ('parsed','patient_id','text','Patient ID','Usually PID-3 component 1.', TRUE, 30),

    -- Lifecycle timing.
    ('parsed','event_time','datetime','Event timestamp',
     'When the transition happened. Feeds arrived/started/completed and therefore every turnaround figure.', TRUE, 40),
    ('parsed','raw_order_control','text','ORC-1 order control','Raw value, used for status lookup.', FALSE, 50),
    ('parsed','raw_order_status','text','ORC-5 order status','Raw value, mapped to a lifecycle state.', TRUE, 60),

    -- Attribution.
    ('parsed','performed_by_id','text','Performed by (ID)','Who did it. LDAP account at LAUMC.', FALSE, 70),
    ('parsed','performed_by_name','text','Performed by (name)', NULL, FALSE, 80),
    ('parsed','performed_by_role','text','Performed by (role)','tech / resident / radiologist.', FALSE, 90),

    -- Where.
    ('parsed','aetitle','text','Device AE title',
     'The performing device. No standard HL7 field carries this; the default guess is OBR-21.', FALSE, 100),
    ('parsed','room_name','text','Room name','Default guess is PV1-3 component 2.', FALSE, 110),

    -- Clinical.
    ('parsed','modality','text','Modality','CT, MR, US. Default OBR-24.', FALSE, 120),
    ('parsed','procedure_code','text','Procedure code', NULL, FALSE, 130),
    ('parsed','procedure_text','text','Procedure description', NULL, FALSE, 140),
    ('parsed','patient_class','text','Patient class','I / O / E.', FALSE, 150),
    ('parsed','patient_location','text','Patient location',
     'ER detection keys on this prefix, so a wrong value hides emergency studies.', TRUE, 160),

    -- Demographics, from ADT.
    ('parsed','patient_name','text','Patient name', NULL, FALSE, 170),
    ('parsed','birth_date','date','Birth date','Drives age at exam. Placeholders are nulled automatically.', TRUE, 180),
    ('parsed','sex','text','Sex', NULL, FALSE, 190),

    -- Direct etl_* overrides. Every one of these is dangerous by definition:
    -- it bypasses the lifecycle and writes a reporting column straight from the
    -- wire, so nothing downstream can tell it apart from a value the pipeline
    -- derived properly.
    ('study','storing_ae','text','etl_didb_studies.storing_ae',
     'Direct override. Bypasses the lifecycle.', TRUE, 200),
    ('study','study_modality','text','etl_didb_studies.study_modality',
     'Direct override. The SR exclusion filter reads this column.', TRUE, 210),
    ('study','study_description','text','etl_didb_studies.study_description',
     'Direct override.', TRUE, 220),
    ('study','patient_class','text','etl_didb_studies.patient_class','Direct override.', TRUE, 230),
    ('study','patient_location','text','etl_didb_studies.patient_location',
     'Direct override. Truncated to 3 characters by the schema.', TRUE, 240),
    ('study','study_date','date','etl_didb_studies.study_date',
     'Direct override. Every date-ranged report filters on this.', TRUE, 250),
    ('study','insert_time','datetime','etl_didb_studies.insert_time',
     'Direct override. This is the TAT ANCHOR — on this branch it holds exam-completion time, not ingest time. Overriding it changes every turnaround figure in the product.', TRUE, 260),
    ('patient','sex','text','etl_patient_view.sex','Direct override.', TRUE, 300),
    ('patient','birth_date','date','etl_patient_view.birth_date','Direct override.', TRUE, 310),
    ('order','proc_id','text','etl_orders.proc_id','Direct override.', TRUE, 400),
    ('order','order_status','text','etl_orders.order_status','Direct override.', TRUE, 410)
ON CONFLICT (target_kind, target_field) DO NOTHING;


-- ── Seed the current hardcoded positions ────────────────────────────────────
--
-- These reproduce exactly what utils/hl7_parse.py does today, so the editor opens
-- showing the product's real assumptions rather than an empty table. Behaviour is
-- unchanged: the parser falls back to the same built-in logic when a mapping
-- yields nothing, so seeding is documentation that happens to be executable.
--
-- The three marked provisional in the parser are seeded too, precisely because
-- they are the ones most likely to need changing at a new site.
INSERT INTO hl7_field_mappings
    (sending_app, message_kind, target_kind, target_field, segment, field_index,
     component_index, priority, transform, notes)
VALUES
    ('', 'status', 'parsed', 'accession_number',    'ORC',  3, 1, 10,  'text',     'filler order number'),
    ('', 'status', 'parsed', 'accession_number',    'OBR',  3, 1, 20,  'text',     'fallback when ORC-3 is empty'),
    ('', 'status', 'parsed', 'event_time',          'ORC',  9, NULL, 10, 'datetime', 'transaction time'),
    ('', 'status', 'parsed', 'performed_by_id',     'ORC', 19, 1, 10,  'text',     'PROVISIONAL — ORC-19 Action By, unconfirmed'),
    ('', 'status', 'parsed', 'performed_by_name',   'ORC', 19, NULL, 10, 'name_xcn', 'PROVISIONAL — unconfirmed'),
    ('', 'status', 'parsed', 'aetitle',             'OBR', 21, 1, 10,  'text',     'PROVISIONAL — no standard field carries an AE title'),
    ('', 'status', 'parsed', 'room_name',           'PV1',  3, 2, 10,  'text',     'PROVISIONAL — PV1-3 component 2'),
    ('', 'status', 'parsed', 'modality',            'OBR', 24, NULL, 10, 'text',    'diagnostic service section id'),
    ('', '',       'parsed', 'patient_id',          'PID',  3, 1, 10,  'text',     NULL),
    ('', '',       'parsed', 'procedure_code',      'OBR',  4, 1, 10,  'text',     NULL),
    ('', '',       'parsed', 'procedure_text',      'OBR',  4, 2, 10,  'text',     NULL),
    ('', '',       'parsed', 'patient_class',       'PV1',  2, NULL, 10, 'text',    NULL),
    ('', '',       'parsed', 'patient_location',    'PV1',  3, 1, 10,  'text',     'ER detection reads this'),
    ('', 'adt',    'parsed', 'patient_name',        'PID',  5, NULL, 10, 'name_xpn', NULL),
    ('', 'adt',    'parsed', 'birth_date',          'PID',  7, NULL, 10, 'date',    NULL),
    ('', 'adt',    'parsed', 'sex',                 'PID',  8, NULL, 10, 'text',    NULL),
    ('', 'order',  'parsed', 'placer_order_number', 'ORC',  2, 1, 10,  'text',     NULL),
    ('', 'result', 'parsed', 'accession_number',    'OBR',  3, 1, 10,  'text',     'ORU joins on OBR-3'),
    ('', 'result', 'parsed', 'event_time',          'OBR', 22, NULL, 10, 'datetime', 'results report status change')
ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority) DO NOTHING;
