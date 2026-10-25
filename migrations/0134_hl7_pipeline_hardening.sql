-- Migration 0134: pipeline hardening found by the end-to-end test suite (tests/).
--
-- Each block below backs a code change made at the same time; the comment on each
-- says which, and what went wrong without it. All of it is idempotent.


-- 1. hl7_orders is keyed on the ARCHIVE ROW, not on MSH-10.
--
-- UNIQUE (message_id) made two systems that both number their messages from 1
-- overwrite each other's orders, and made a reused control ID (CONTROL_ID_REUSE)
-- cost the first order the moment the second was released from quarantine. The
-- archive de-duplicates real redeliveries before they reach this table, so the
-- upsert only has to absorb a replay of the same archived message — which is
-- exactly what message_archive_id identifies. message_id stays, indexed, because
-- the live feed looks orders up by it.
ALTER TABLE hl7_orders ADD COLUMN IF NOT EXISTS message_archive_id BIGINT;

UPDATE hl7_orders o
   SET message_archive_id = a.id
  FROM hl7_message_archive a
 WHERE o.message_archive_id IS NULL
   AND a.raw_message = o.raw_message;

ALTER TABLE hl7_orders DROP CONSTRAINT IF EXISTS hl7_orders_message_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_orders_archive
    ON hl7_orders (message_archive_id)
    WHERE message_archive_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_hl7_orders_message_id
    ON hl7_orders (message_id);

-- The projector now finds a study's order (and so its referring doctor) by placer
-- number as well as by accession: an ORM NW normally carries no accession.
CREATE INDEX IF NOT EXISTS idx_hl7_orders_placer
    ON hl7_orders (placer_order_number)
    WHERE placer_order_number IS NOT NULL;


-- 2. The signature rung of the stored report version.
--
-- hl7_oru_reports was fill-only (inherited from LAUMC, where a RIS batch held the
-- authoritative text). This branch has no RIS batch, so the preliminary text stayed
-- for good once the final arrived. A report now replaces the stored one when it is
-- at least as final and no older, which needs the stored version's rung.
ALTER TABLE hl7_oru_reports ADD COLUMN IF NOT EXISTS signature_rank INTEGER;


-- 3. Cancellation by order-control code alone.
--
-- Many senders cancel with ORC-1 = CA / OC / CR / DC / OD / DR and ORC-5 empty;
-- those messages were silently ignored. The parser now classifies them through
-- this map, so each code needs a row. CA and DC are already seeded (0117).
INSERT INTO hl7_status_map
    (sending_app, order_control, order_status, canonical_state, ladder_rank, notes)
VALUES
    ('', '', 'OC', 'cancelled', -1, 'Order cancelled (ORC-1, with ORC-5 empty)'),
    ('', '', 'CR', 'cancelled', -1, 'Cancelled as requested (ORC-1, with ORC-5 empty)'),
    ('', '', 'OD', 'cancelled', -1, 'Order discontinued (ORC-1, with ORC-5 empty)'),
    ('', '', 'DR', 'cancelled', -1, 'Discontinued as requested (ORC-1, with ORC-5 empty)')
ON CONFLICT (sending_app, order_control, order_status) DO NOTHING;


-- 4. ORPHAN_ORDER reads ray7_study_state now.
--
-- It read hl7_orders and required an accession — which a HIS order normally does
-- not have — so an unscheduled placer-only order could never be reported. Partial
-- index on the shape the sweep asks for.
CREATE INDEX IF NOT EXISTS idx_ray7_state_unscheduled
    ON ray7_study_state (ordered_at)
    WHERE scheduled_at IS NULL AND cancelled_at IS NULL AND NOT is_closed;


-- 4b. Patient lookups on the per-message path.
--
-- project_patient() counts a patient's studies and ADT reprojects every study of
-- the patient, both by ray7_study_state.patient_id — which had no index, so every
-- message carrying a patient scanned the whole table, a cost that grows with every
-- study the install has ever seen and is paid inside the sender's ACK window.
CREATE INDEX IF NOT EXISTS idx_ray7_state_patient
    ON ray7_study_state (patient_id)
    WHERE patient_id IS NOT NULL;


-- 5. Studies with no date are not studies.
--
-- An ORM NW that already carried an accession projected a study row with NULL
-- study_date and NULL insert_time (300 of them in the local test database on
-- 2026-09-24). The projector no longer creates them; these are the ones it did.
-- Only HL7-minted rows, and only ones nothing else hangs off.
DELETE FROM etl_didb_studies s
 WHERE s.study_date IS NULL
   AND s.insert_time IS NULL
   AND s.study_db_uid IN (SELECT surrogate_id FROM hl7_surrogate_keys WHERE entity = 'study')
   AND NOT EXISTS (SELECT 1 FROM std_pps p WHERE p.study_db_uid = s.study_db_uid)
   AND NOT EXISTS (SELECT 1 FROM etl_didb_serieses x WHERE x.study_db_uid = s.study_db_uid);


-- 6. Coded modalities.
--
-- OBR-24 sent as a coded element ("US^US", "MR^Magnetic Resonance^DCM") was stored
-- whole, so no modality filter, colour or SR exclusion recognised it. The parsers
-- now keep component 1 — and so must the seeded OBR-24 mapping from 0123, which
-- reads the WHOLE field and overlays it after the parse, putting the coded value
-- straight back (the same trap 0129 fixed for PID-7). Only the untouched seed row
-- is changed; a site that configured its own component keeps it.
UPDATE hl7_field_mappings
   SET component_index = 1, updated_at = NOW()
 WHERE sending_app = '' AND message_kind = 'status' AND target_kind = 'parsed'
   AND target_field = 'modality' AND segment = 'OBR' AND field_index = 24
   AND component_index IS NULL AND priority = 10;

-- And what the parsers already wrote.
UPDATE ray7_study_state SET modality = split_part(modality, '^', 1)
 WHERE modality LIKE '%^%';
UPDATE hl7_study_events SET modality = split_part(modality, '^', 1)
 WHERE modality LIKE '%^%';
UPDATE etl_didb_studies SET study_modality = split_part(study_modality, '^', 1)
 WHERE study_modality LIKE '%^%';
UPDATE etl_orders SET modality = split_part(modality, '^', 1)
 WHERE modality LIKE '%^%';
UPDATE hl7_orders SET modality = split_part(modality, '^', 1)
 WHERE modality LIKE '%^%';
UPDATE hl7_oru_reports SET modality = split_part(modality, '^', 1)
 WHERE modality LIKE '%^%';
