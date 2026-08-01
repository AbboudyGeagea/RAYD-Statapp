-- Migration 0097: create std_pps_person_reference (RIS PPS_PERSON_REFERENCE, LAUMC) —
-- the working replacement for std_pps.primary_tech_person_key on Report 35 (Technician
-- TAT).
--
-- Why this exists: std_pps.primary_tech_person_key (migration 0065, sourced from
-- PPS.PRIMARY_TECH_PERSON_KEY) was confirmed 100% NULL against production (live query,
-- 2026-08-01) — a dead column, not usable for resolving which technologist performed an
-- exam. PPS_PERSON_REFERENCE, joined by PPS_KEY, is the working alternative: it gives a
-- RESOURCE_ID_KEY that resolves to a real person via std_resources_ris, confirmed ~99%
-- populated (12,209/12,301 recent PPS rows).
--
-- Grain: one row per PPS per referenced person — a single PPS can carry SEVERAL
-- PPS_PERSON_REFERENCE rows (technologist, but also receptionist/nurse/radiologist/etc.,
-- all under the same broad PERSON_REFERENCE_TYPE_KEY catch-all column). This table is
-- NOT technologist-only; it is the raw per-PPS reference list.
--
-- *** HOW TO GET THE TECHNOLOGIST — read before querying ***
-- Do NOT filter on person_reference_type_key. Cross-referencing RESOURCE_ID_KEY against
-- std_resources_ris.role_code confirmed several different person_reference_type_key
-- values all resolve to genuine technologists (role_code = 'TEC') — there is no single
-- "this is the tech row" type-key value. Worse, the single largest/most common
-- person_reference_type_key is itself a MIXED bucket containing technologists alongside
-- other roles. The only reliable filter is a role-level join at query time:
--
--   SELECT ...
--   FROM std_pps_person_reference ppr
--   JOIN std_resources_ris r ON r.resource_id_key = ppr.resource_id_key
--   WHERE r.role_code = 'TEC'   -- resolved via etl_ris_resources.py's vendor role map
--
-- No timestamp/date column of any kind on the Oracle source table (confirmed via
-- all_tab_columns) — no created_date, no last_updated, nothing to watermark on except
-- the key itself. See ETL_JOBS/etl_ris_pps_person_reference.py for the resulting
-- MAX(pps_person_reference_key) watermark / full-pull-on-fresh-load pattern.
--
-- pps_person_reference_key is used as the primary key directly (unlike migration 0089's
-- worklist_status_history_key) — confirmed real, populated, and presumably unique via
-- all_tab_columns, no evidence of the same NULL-PK problem.
--
-- resource_id_key is NOT given an FK to std_resources_ris(resource_id_key): RIS load
-- order between this table and Phase 13's RIS Resources isn't guaranteed, same
-- reasoning as std_pps's own unresolved *_person_key columns — resolve at query time.

CREATE TABLE IF NOT EXISTS std_pps_person_reference (
    pps_person_reference_key   BIGINT PRIMARY KEY,
    pps_key                    BIGINT,
    person_reference_type_key  BIGINT,   -- broad catch-all; do NOT filter on this alone — see note above
    sequence_id                BIGINT,
    resource_id_key            BIGINT,   -- resolve via std_resources_ris.resource_id_key; filter role_code = 'TEC' for technologist
    display_sort_order         BIGINT,
    last_update                TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pps_person_reference_pps_key ON std_pps_person_reference (pps_key);
CREATE INDEX IF NOT EXISTS idx_pps_person_reference_resource_id_key ON std_pps_person_reference (resource_id_key);
