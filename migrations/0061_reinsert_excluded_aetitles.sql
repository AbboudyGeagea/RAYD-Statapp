-- Migration 0061: re-insert NonDICOMAgent and SVSM as permanently excluded AEs.
-- Migration 0060 deleted them entirely, which broke exclusion — a LEFT JOIN to
-- aetitle_modality_map on a missing row returns NULL for exclude_from_stats,
-- and COALESCE(NULL, FALSE) = FALSE passes the filter, counting noise studies.
-- These rows must exist with exclude_from_stats = TRUE so the JOIN catches them.
-- ETL Phase 8 already filters them from its INSERT, so they will never be reset.

INSERT INTO aetitle_modality_map (aetitle, modality, exclude_from_stats)
VALUES
    ('NonDICOMAgent', 'OT', TRUE),
    ('SVSM',          'OT', TRUE)
ON CONFLICT (aetitle) DO UPDATE SET exclude_from_stats = TRUE;
