-- Migration 0123: operator-entered reader-role override for PACS logins.
--
-- Report 36's "Resident vs. Radiologist TAT" split and report 25's RES/RAD badges on
-- the Radiologist Workload Matrix both classify a signer purely by PACS security-group
-- membership (std_pacs_user_groups, migration 0087).
--
-- Measured on LAUMC 2026-09-21, that group data is internally clean and the matching
-- code was doing the right thing with it:
--   * group_name is always a bare 'radiologists' / 'residents' -- the site suffix lives
--     in the separate group_domain column ('ad' / 'SJ'), so the reports' exact-match
--     IN ('radiologists','residents') list drops nobody and there is no '@domain' to
--     strip off the group name;
--   * no signer holds both roles (the only multi-row logins are the same role across
--     two domains, e.g. radiologists@ad + radiologists@SJ, which the lookup's DISTINCT
--     already collapses), so there was no join-duplication bug either.
--
-- The actual gap: PACS itself has several residents sitting in the 'radiologists'
-- group, presumably because that group is what grants reading permission. No query
-- over std_pacs_user_groups can recover their real job function. The RIS alternative
-- (std_resources_ris.role_code) is not usable as a fallback -- it was over-granted to
-- every user as a workaround for an installation-time RIS permissions bug, and its
-- known failure mode is exactly the wrong one here: it has already misclassified a
-- real radiologist as Resident (see ETL_JOBS/etl_ris_resources.py's docstring). So the
-- correction has to be operator-entered, which is what this table is.
--
-- Precedence, implemented in utils/pacs_roles.py and shared by both reports: an
-- override row wins outright; failing that, 'residents' beats 'radiologists' for
-- anyone who somehow ends up in both groups later.
--
-- login_id is stored UPPER and WITHOUT the '@domain' suffix, matching how both reports
-- key the lookup -- SPLIT_PART(UPPER(TRIM(<signed_by>)), '@', 1). The CHECK enforces
-- that, so a row entered as 'rola.chalfoun@ad' fails loudly instead of silently never
-- matching anything.
--
-- No ETL job writes this table; it survives every PACS re-sync.

CREATE TABLE IF NOT EXISTS pacs_user_role_override (
    login_id   TEXT PRIMARY KEY
               CHECK (login_id = UPPER(BTRIM(login_id)) AND login_id NOT LIKE '%@%'),
    role       TEXT NOT NULL CHECK (role IN ('radiologists', 'residents')),
    note       TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- The shared lookup matches on LOWER(BTRIM(group_name)) rather than a bare equality,
-- so idx_pacs_user_groups_group (plain btree on group_name, migration 0087) stops
-- being usable for it -- confirmed by EXPLAIN, which fell back to a seq scan. That
-- table is mostly web_patients/web_guests rows (~69k on LAUMC, vs ~70 actual readers)
-- and the lookup runs on every report 25 and report 36 render, so give the normalized
-- expression its own index.
CREATE INDEX IF NOT EXISTS idx_pacs_user_groups_group_norm
    ON std_pacs_user_groups (LOWER(BTRIM(group_name)));

-- Seed: TAMINA.RIZK signs reports but her only PACS group is 'mammography', so she
-- resolved to no role at all -- she was the entire "unclassified" count on report 36,
-- excluded from both TAT panels. Confirmed a radiologist by the operator 2026-09-21.
-- Further corrections go through Config -> Reader Roles in the app, not this file.
INSERT INTO pacs_user_role_override (login_id, role, note) VALUES
    ('TAMINA.RIZK', 'radiologists', 'PACS group is mammography only; confirmed radiologist by operator 2026-09-21')
ON CONFLICT (login_id) DO NOTHING;

COMMENT ON TABLE pacs_user_role_override IS
    'Manual reader-role correction for logins whose PACS security group does not '
    'reflect their real job function (e.g. residents placed in the radiologists '
    'group for reading permission). Overrides std_pacs_user_groups. Never ETL-written.';
