-- Migration 0085: resolve RIS resource_role_key to a human-readable code/description.
--
-- resource_role_key was previously unresolved (migration 0063's own comment: "no role
-- lookup provided yet"). Operator pulled the real RIS RESOURCE_ROLE lookup table
-- (2026-07-31) -- a small, vendor-provided, rarely-changing set (16 rows), same shape
-- as the existing GENDER_KEY map in etl_ris_patients.py. Resolved in Python at ETL
-- time (see ETL_JOBS/etl_ris_resources.py) rather than a separate DB table, matching
-- that precedent -- these two columns just hold the resolved result.
--
-- A person can carry MULTIPLE resource_role_key rows (one per role/site/assignment),
-- so this is a per-row resolution, not a single "this person's role" flag -- e.g. to
-- check whether someone is ever tagged as a Resident, look for ANY of their rows with
-- role_code = 'RES'.

ALTER TABLE std_resources_ris
    ADD COLUMN IF NOT EXISTS role_code        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS role_description TEXT;
