-- Migration 0087: PACS security-group membership -> std_pacs_user_groups (LAUMC).
--
-- Source: MEDILINK.SECM_USERS / SECM_GROUPS / SECM_USER_IN_GROUP -- the real Oracle
-- tables behind the PACS "Users / Groups / Profiles" admin screen (operator
-- screenshot, 2026-07-31). These are PACS reading-permission groups (e.g.
-- "radiologists", "radiologists@SJ", "residents") -- confirmed by the operator to be
-- the reliable role source, unlike RIS's resource_role_key, which was over-granted to
-- every user as a workaround for an installation-time RIS permissions bug and is not
-- trustworthy for role classification (see ETL_JOBS/etl_ris_resources.py).
--
-- A user can belong to multiple groups, so this mirrors the bridge table's own grain
-- (one row per membership) rather than collapsing a user to a single group.

CREATE TABLE IF NOT EXISTS std_pacs_user_groups (
    membership_dbid BIGINT PRIMARY KEY,   -- SECM_USER_IN_GROUP.DBID
    user_dbid        BIGINT,               -- SECM_USERS.DBID
    login_id         TEXT,                 -- SECM_USERS.LOGIN_ID
    email            TEXT,                 -- SECM_USERS.EMAIL
    group_dbid       BIGINT,               -- SECM_GROUPS.DBID
    group_name       TEXT,                 -- SECM_GROUPS.GROUP_NAME (e.g. "radiologists")
    group_domain     TEXT,                 -- SECM_GROUPS.DOMAIN (site distinction, e.g. SJ)
    last_update      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pacs_user_groups_login ON std_pacs_user_groups (LOWER(login_id));
CREATE INDEX IF NOT EXISTS idx_pacs_user_groups_group ON std_pacs_user_groups (group_name);
