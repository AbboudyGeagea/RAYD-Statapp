-- Migration 0117: correct four AE modalities that contradict the PACS data.
--
-- Measured from medistore.didb_studies.STUDY_MODALITY over 2025-01-01..2026-09-18.
-- STUDY_MODALITY is backslash-delimited ('\DX\SR\'); SR/PR/KO/DOC are DICOM artifacts
-- (structured report, presentation state, key object), so the share below is of the
-- dominant ACQUISITION tag.
--
--     AE               map had   PACS says          map's own room_name
--     DEFINIUM1        RF        DX  99.8%          "X-Ray Room 1 - Definium (GE)"
--     DEFINIUM2        RF        DX  99.9%          "XRAY-3 Emergency Department"
--     FCR-CSL          RF        CR  99.9%          "CR X-Ray - Fuji"
--     OEC9900VASC      RF        XA  95.3%          "CARM3-VX OEC9900 (GE)"
--
-- In each case the room_name already in aetitle_modality_map agrees with PACS and
-- disagrees with the modality column, so this is correcting the map against two
-- independent sources rather than one.
--
-- DEFINIUM1 and DEFINIUM2 matter most: they were 78 of the 250 studies on the yesterday
-- tab for 2026-09-17, the two busiest devices at RH, both classified as fluoroscopy when
-- they are plain digital radiography. Every modality chart, the modality filter, and any
-- RF-vs-DX split has been wrong by that volume.
--
-- NOT CHANGED, deliberately:
--   AZURION7FLEXARM -- map says XA, PACS says HYBRID (99.4%). Both defensible: HYBRID is
--     what the device actually sends, XA is the useful reporting bucket, and the room is
--     literally "Hybrid OR (Philips)". Needs an operator decision on whether hybrid-OR
--     work should aggregate with angiography, so it is left alone rather than churned.
--   PHCARDIO, ECHOPAC-PC -- map says US, PACS says CARD. Both are in
--     etl_didb_studies.py's _EXCLUDED_AE_SQL, so no study on them is ever loaded and the
--     modality value is inert.
--   GELUNAR -- map says OT and PACS agrees (99.9%); DEXA really is tagged that way here.
--     It stays OT. Note this means report 23's NOT IN ('SR','OT') filter keeps hiding
--     bone densitometry -- that is a separate operator decision about the filter, NOT a
--     mapping error, and correcting the mapping does not fix it.
--
-- Guarded on the current value so re-running is a no-op, and so a later manual
-- correction is never silently reverted by a re-apply.

UPDATE aetitle_modality_map SET modality = 'DX'
WHERE UPPER(TRIM(aetitle)) = 'DEFINIUM1'   AND UPPER(TRIM(modality)) = 'RF';

UPDATE aetitle_modality_map SET modality = 'DX'
WHERE UPPER(TRIM(aetitle)) = 'DEFINIUM2'   AND UPPER(TRIM(modality)) = 'RF';

UPDATE aetitle_modality_map SET modality = 'CR'
WHERE UPPER(TRIM(aetitle)) = 'FCR-CSL'     AND UPPER(TRIM(modality)) = 'RF';

UPDATE aetitle_modality_map SET modality = 'XA'
WHERE UPPER(TRIM(aetitle)) = 'OEC9900VASC' AND UPPER(TRIM(modality)) = 'RF';
