-- Migration 0052: org-structure -> site rollup map + per-user site grants
-- *** LAUMC BRANCH ONLY ***
--
-- (a) site_org_map — replaces the single-value sites.ris_org_struct approach.
--     The RIS ORG_STRUCTURE is a hierarchy: sub-departments (ER=5521, VASC=5120)
--     carry their own org keys that must ROLL UP to a hospital site. Any
--     org_structure_key seen on ORDERS / SITE_WORKLIST / MODALITY resolves to a
--     canonical site through this table. New sub-departments = one new row.
--
--     RIS hierarchy (vendor, 2026-07-07):
--       1     LAUMC (root)            -> no site (pre-scheduling rows carry 1 = unassigned)
--       3926  RH Radiology            -> RH
--       5521  RH ER Department        -> RH   (parent 3926)
--       5320  SJH Radiology           -> SJH
--       5120  Vascular LAB-RH         -> RH   (parents to root, but physically RH —
--                                              business rule, pending final confirm Qog1)
--
-- (b) user_sites — per-user site grants for RLS scoping. A user's effective scope =
--     (their granted sites) ∩ (their site-picker selection). Users with role 'admin'
--     and no rows here are treated as all-sites by the app layer.

CREATE TABLE IF NOT EXISTS site_org_map (
    org_structure_key  VARCHAR(32) PRIMARY KEY,   -- RIS ORG_STRUCTURE_KEY (stored as text)
    site_id            INTEGER NOT NULL REFERENCES sites(id),
    description        TEXT,
    created_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO site_org_map (org_structure_key, site_id, description)
SELECT v.org_key, s.id, v.descr
FROM (VALUES
        ('3926', 'RH',  'LAUMCRH Radiology Department'),
        ('5521', 'RH',  'RH ER Department (child of 3926)'),
        ('5120', 'RH',  'Vascular LAB-RH (business rule: counts as RH)'),
        ('5320', 'SJH', 'LAUMCSJH Radiology Department')
     ) AS v(org_key, site_code, descr)
JOIN sites s ON s.code = v.site_code
ON CONFLICT (org_structure_key) DO NOTHING;
-- NOTE: root org '1' (LAUMC) is deliberately NOT mapped — rows carrying it (orders not yet
-- scheduled) resolve to NULL site = "unassigned", visible only to all-sites users. Fail-closed.

-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_sites (
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    site_id     INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    granted_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    granted_by  INTEGER REFERENCES users(id),
    PRIMARY KEY (user_id, site_id)
);

CREATE INDEX IF NOT EXISTS idx_user_sites_site ON user_sites (site_id);
