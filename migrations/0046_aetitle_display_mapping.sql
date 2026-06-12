-- Optional display alias for an AE title.
-- When set, reports show this value instead of the raw aetitle,
-- allowing multiple physical AE titles to appear as one in reports.
ALTER TABLE aetitle_modality_map ADD COLUMN IF NOT EXISTS display_aetitle VARCHAR(100);
