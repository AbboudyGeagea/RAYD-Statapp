-- Add columns that exist in the ORM model but were never added via migration.
-- All three use IF NOT EXISTS so re-running is safe.

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS room_name       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS description     TEXT,
    ADD COLUMN IF NOT EXISTS display_aetitle VARCHAR(100);
