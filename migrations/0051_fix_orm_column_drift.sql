-- Migration 0051: fix ORM-model / database drift on aetitle_modality_map and
-- procedure_duration_map.
--
-- db.py's SQLAlchemy models declare columns that no Mazloum migration ever created:
--   aetitle_modality_map.room_name        (String(100))
--   aetitle_modality_map.display_aetitle  (String(100))  -- routes/mapping_controller.py's
--                                                            CSV export and edit endpoint
--   procedure_duration_map.modality       (String(20))   -- same export route
--
-- routes/mapping_controller.py's /mapping page 500s with "column ... does not exist"
-- the moment it's hit, since the ORM SELECT always includes every declared column
-- regardless of what's actually in the table. Purely additive (IF NOT EXISTS) so it's
-- a no-op if a column somehow already exists.

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS room_name       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS display_aetitle VARCHAR(100);

ALTER TABLE procedure_duration_map
    ADD COLUMN IF NOT EXISTS modality        VARCHAR(20);
