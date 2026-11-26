-- Migration 0183: the Super Report's chosen blocks (to-do item 43, 2026-11-26).
-- *** LAUMC BRANCH ONLY ***
--
-- The customer asked to remove and add charts on the Super Report; the operator chose "remove and
-- reorder" first. Each user may keep their own list of blocks (ticked, in their order), and an
-- account with the settings capability may save one for everyone, used by whoever has none.
-- A saved preset carries its own list in saved_reports.filter_json ("blocks"), no column needed.
-- The block keys are utils/super_report_blocks.KEYS.
--
-- On the main database only (utils/site_db.MAIN_TABLES, like logins and access): one list per
-- person whichever hospital is picked. Every database gets the table (every migration runs on
-- every database); the site databases' copies stay empty.
--
-- scope_key: 'user:<users.id>' or 'everyone'.

CREATE TABLE IF NOT EXISTS public.super_report_layouts (
    scope_key   VARCHAR(40) PRIMARY KEY,
    blocks      JSONB       NOT NULL,
    updated_by  INTEGER,
    updated_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    CHECK (scope_key = 'everyone' OR scope_key ~ '^user:[0-9]+$'),
    CHECK (jsonb_typeof(blocks) = 'array')
);
