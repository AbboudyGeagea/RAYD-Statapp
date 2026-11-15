-- Migration 0152: RAYD's new roles, and access granted site by site.
-- *** LAUMC BRANCH ONLY ***
--
-- Meeting of 2026-11-13, item 13f: one set of RAYD logins for both sites. The old roles
-- (admin, viewer, viewer2, tec, finance, secretary) are replaced by
--     superuser      Super user     R&D, everything
--     cht            CHT            implementation: mapping, ETL, BRAYN console
--     administrator  Administrator  users and settings of the sites granted
--     user           User           the pages granted
-- Each account is granted sites, then pages per site. Existing admins become
-- Administrators; viewer, viewer2, tec, finance and secretary become Users keeping their
-- pages. What each role may do is decided in one place, utils/roles.py.
--
-- Nothing anyone can open changes:
--   1. every page grant and report grant gets a site, RH (the default site), the only data
--      this database serves until the SJH database exists (item 13a);
--   2. each User's pages are written out as they were in effect: an explicit tick or block
--      as it was, otherwise the old role's default, plus what the old role gave by itself
--      (viewer and viewer2: the Mapping page and the CRN board; viewer: AI Reports). The new
--      code has no role defaults, so the pages must be explicit;
--   3. each User gets a row for every report it could reach: the Viewer Dashboard used to
--      grant every report to every non-admin on each visit (tec never reached it), and no
--      longer does;
--   4. every account that is not a pending registration is granted RH;
--   5. the roles are renamed. The built-in "admin" account (init-db/schema.sql) becomes the
--      Super user, so the ETL Control Room, Mapping and the BRAYN console stay reachable;
--      other admins become Administrators. A Super user can change any of this in User
--      Management afterwards.
-- The old page keys ('sticky_notes', 'scheduling') stay in place, unread.

-- The default site, for the column defaults below (a DEFAULT cannot hold a subquery).
CREATE OR REPLACE FUNCTION rayd_default_site_id() RETURNS INTEGER
LANGUAGE sql STABLE AS $$
    SELECT id FROM sites WHERE is_default ORDER BY id LIMIT 1
$$;

DO $$
BEGIN
    IF rayd_default_site_id() IS NULL THEN
        RAISE EXCEPTION 'migration 0152: no default site in table sites';
    END IF;
END $$;

-- ── 1. a site on every page grant and report grant ───────────────────────────
ALTER TABLE user_page_permissions
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE;
UPDATE user_page_permissions SET site_id = rayd_default_site_id() WHERE site_id IS NULL;
ALTER TABLE user_page_permissions ALTER COLUMN site_id SET DEFAULT rayd_default_site_id();
ALTER TABLE user_page_permissions ALTER COLUMN site_id SET NOT NULL;

-- one row per account, site and page: keep the newest where an old bug wrote two
DELETE FROM user_page_permissions a
USING user_page_permissions b
WHERE a.user_id = b.user_id AND a.site_id = b.site_id AND a.page_key = b.page_key
  AND a.id < b.id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_user_page_permissions
    ON user_page_permissions (user_id, site_id, page_key);

ALTER TABLE report_access_control
    ADD COLUMN IF NOT EXISTS site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE;
UPDATE report_access_control SET site_id = rayd_default_site_id() WHERE site_id IS NULL;
ALTER TABLE report_access_control ALTER COLUMN site_id SET DEFAULT rayd_default_site_id();
ALTER TABLE report_access_control ALTER COLUMN site_id SET NOT NULL;
ALTER TABLE report_access_control DROP CONSTRAINT IF EXISTS uq_report_access_control;
ALTER TABLE report_access_control
    ADD CONSTRAINT uq_report_access_control UNIQUE (user_id, site_id, report_template_id);

-- ── 2. each User's pages, written out as they were in effect ─────────────────
-- Old role defaults (db.ROLE_PAGE_DEFAULTS before item 13f): viewer and viewer2 every page
-- of the old list; tec live_feed and hl7_orders; finance financial; secretary live_feed.
-- By role alone, whatever the tick said: viewer and viewer2 opened Mapping (view) and the
-- CRN board, viewer opened AI Reports.
INSERT INTO user_page_permissions (user_id, site_id, page_key, is_enabled)
SELECT u.id, rayd_default_site_id(), k.page_key,
       CASE
           WHEN k.page_key = 'mapping'   AND u.role IN ('viewer', 'viewer2') THEN TRUE
           WHEN k.page_key = 'report_ai' AND u.role = 'viewer'               THEN TRUE
           WHEN k.page_key = 'crn' THEN u.role IN ('viewer', 'viewer2')
           ELSE COALESCE(p.is_enabled, CASE u.role
                   WHEN 'viewer'    THEN k.page_key <> 'crn'
                   WHEN 'viewer2'   THEN k.page_key <> 'crn'
                   WHEN 'tec'       THEN k.page_key IN ('live_feed', 'hl7_orders')
                   WHEN 'finance'   THEN k.page_key = 'financial'
                   WHEN 'secretary' THEN k.page_key = 'live_feed'
                   ELSE FALSE
               END)
       END
FROM users u
CROSS JOIN (VALUES ('live_feed'), ('hl7_orders'), ('report_ai'), ('oru'), ('mapping'),
                   ('financial'), ('cd_print'), ('referring_intel'), ('custom_reports'),
                   ('crn')) AS k(page_key)
LEFT JOIN user_page_permissions p
       ON p.user_id = u.id AND p.site_id = rayd_default_site_id() AND p.page_key = k.page_key
WHERE u.role IN ('viewer', 'viewer2', 'tec', 'finance', 'secretary')
  AND COALESCE(u.status, 'active') <> 'pending'
ON CONFLICT (user_id, site_id, page_key) DO UPDATE SET is_enabled = EXCLUDED.is_enabled;

-- ── 3. each User's reports: what the Viewer Dashboard would have granted ─────
-- An explicit block stays (DO NOTHING). tec never reached the dashboard, so gets nothing new.
INSERT INTO report_access_control (user_id, site_id, report_template_id, is_enabled)
SELECT u.id, rayd_default_site_id(), t.report_id, TRUE
FROM users u
CROSS JOIN report_template t
WHERE t.is_base
  AND u.role IN ('viewer', 'viewer2', 'finance', 'secretary')
  AND COALESCE(u.status, 'active') <> 'pending'
ON CONFLICT (user_id, site_id, report_template_id) DO NOTHING;

-- ── 4. every account granted RH ──────────────────────────────────────────────
-- granted_by now names who granted a site; deleting that account keeps the grant (0052 had
-- no ON DELETE, so an administrator who had granted a site could not be deleted)
ALTER TABLE user_sites DROP CONSTRAINT IF EXISTS user_sites_granted_by_fkey;
ALTER TABLE user_sites ADD CONSTRAINT user_sites_granted_by_fkey
    FOREIGN KEY (granted_by) REFERENCES users(id) ON DELETE SET NULL;

INSERT INTO user_sites (user_id, site_id)
SELECT u.id, rayd_default_site_id()
FROM users u
WHERE COALESCE(u.status, 'active') <> 'pending'
ON CONFLICT (user_id, site_id) DO NOTHING;

-- ── 5. the roles ─────────────────────────────────────────────────────────────
UPDATE users SET role = 'superuser' WHERE username = 'admin' AND role = 'admin';
UPDATE users SET role = 'administrator' WHERE role = 'admin';
UPDATE users SET role = 'user'
WHERE role IS NULL OR role NOT IN ('superuser', 'cht', 'administrator', 'user');
UPDATE active_sessions s SET role = u.role FROM users u WHERE u.id = s.user_id;

ALTER TABLE users ALTER COLUMN role SET DEFAULT 'user';
ALTER TABLE users ALTER COLUMN role SET NOT NULL;
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check
    CHECK (role IN ('superuser', 'cht', 'administrator', 'user'));
