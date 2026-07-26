-- Migration 0058: fix ORM-model / database drift on aetitle_modality_map and
-- procedure_duration_map, discovered while building the RIS modality/procedure loader.
--
-- db.py's SQLAlchemy models declare columns that no migration ever created:
--   aetitle_modality_map.room_name        (String(100))  -- read/written by
--   aetitle_modality_map.display_aetitle  (String(100))  -- routes/mapping_controller.py's
--                                                            CSV export (r.room_name, r.display_aetitle)
--   procedure_duration_map.modality       (String(20))   -- same export route (r.modality)
--
-- Those routes would 500 with "column ... does not exist" the moment they're hit. This
-- migration is purely additive (IF NOT EXISTS) so it's a no-op if a column somehow
-- already exists, and safe on every site regardless of how it got here.

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS room_name       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS display_aetitle VARCHAR(100);

ALTER TABLE procedure_duration_map
    ADD COLUMN IF NOT EXISTS modality        VARCHAR(20);
