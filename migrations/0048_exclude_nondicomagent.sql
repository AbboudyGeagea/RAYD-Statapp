-- NonDICOMAAgent is the PACS itself when importing non-DICOM documents
-- (PDFs, scanned reports, etc.). It produces studies with mixed modalities
-- and must be excluded from all statistics.
--
-- Adds exclude_from_stats flag to aetitle_modality_map so the exclusion is
-- data-driven rather than hardcoded, and marks NonDICOMAAgent as excluded.

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS exclude_from_stats BOOLEAN DEFAULT FALSE;

INSERT INTO aetitle_modality_map (aetitle, modality, exclude_from_stats)
VALUES ('NonDICOMAAgent', 'OT', TRUE)
ON CONFLICT (aetitle) DO UPDATE SET exclude_from_stats = TRUE;
