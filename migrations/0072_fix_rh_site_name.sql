-- Migration 0072: fix RH site name — it's Rizk Hospital, not Rafic Hariri.
--
-- migration 0046 seeded sites.name as 'LAUMC - Rafic Hariri (Main)' for code='RH'.
-- Wrong hospital name. Operator correction, 2026-07-27: RH = Rizk Hospital. 0046
-- already ran on live installs, so its own corrected seed text (also fixed in this
-- pass) only takes effect on a genuinely fresh install — this migration fixes the
-- name on every install that already has the RH row.
--
-- code/pacs_site_id/ris_issuer/hl7_building/ris_org_struct are untouched — those
-- were never wrong, only the display name was.

UPDATE sites
SET name = 'LAUMC - Rizk Hospital (Main)', updated_at = NOW()
WHERE code = 'RH' AND name = 'LAUMC - Rafic Hariri (Main)';
