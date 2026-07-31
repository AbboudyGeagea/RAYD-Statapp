-- Migration 0089: worklist_status_history_key confirmed always NULL in real Oracle
-- data (operator sample, 2026-07-31) -- not a usable identifier despite the name.
-- The one load attempt skipped all 627,491 matching rows because of this exact
-- issue (every row's "primary key" came back null). Table was created this session
-- with 0 real rows ever loaded, safe to restructure directly rather than migrate
-- data.
--
-- Switch to a synthetic serial PK + a real uniqueness constraint on
-- (site_worklist_key, arrived_at) -- a worklist entry can genuinely have multiple
-- "Arrived" transitions (confirmed in the same sample), so the pair, not either
-- column alone, is what's actually unique.

ALTER TABLE std_worklist_arrivals DROP CONSTRAINT IF EXISTS std_worklist_arrivals_pkey;
ALTER TABLE std_worklist_arrivals DROP COLUMN IF EXISTS worklist_status_history_key;
ALTER TABLE std_worklist_arrivals ADD COLUMN IF NOT EXISTS id BIGSERIAL PRIMARY KEY;

ALTER TABLE std_worklist_arrivals
    ADD CONSTRAINT uq_worklist_arrivals_site_time UNIQUE (site_worklist_key, arrived_at);
