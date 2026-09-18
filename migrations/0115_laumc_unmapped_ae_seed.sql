-- Migration 0115: seed the 21 LAUMC AE titles that had no aetitle_modality_map row.
--
-- Found while fixing Report 23's site filter (commit f0a12cb0). That filter used
-- `m.site_id = :rh_site_id` against a LEFT JOIN, so every study on an unmapped AE was
-- silently dropped -- 34,224 studies, 83% of the RH total. The filter is fixed, so these
-- studies now COUNT, but without a map row their modality resolves to NULL and they stay
-- missing from every modality chart and from the Modality filter.
--
-- MODALITY IS MEASURED, NOT GUESSED. Each value below is the dominant
-- medistore.didb_studies.STUDY_MODALITY for that AE over 2025-01-01..2026-09-18,
-- with its share. STUDY_MODALITY is backslash-delimited (e.g. '\DX\SR\'); SR/PR/KO/DOC
-- are DICOM artifacts (structured report, presentation state, key object) rather than
-- acquisitions, so the dominant ACQUISITION tag is what is recorded here.
--
-- Two values contradict what the AE name suggests, which is why they were measured:
--   AZURION7FLEXARM -> HYBRID (99.4%), not XA. Exactly ONE study carries XA.
--   OEC9900GSP      -> RF (98.4%), not XA. Only 3 studies carry XA.
-- If the operator would rather group either with angiography for reporting, remap them;
-- the point is that the source data says HYBRID/RF today.
--
-- NOT excluded as cardiology kit. OEC9900VASC / OEC9900GSP / AZURION7FLEXARM were
-- suspected of being vascular-lab archival equipment of the same class as the 12 AEs in
-- etl_didb_studies.py's _EXCLUDED_AE_SQL. Two checks say otherwise: all three
-- self-reference in PACS ORIGINAL_STORING_AE (real acquisition devices, not gateway
-- relays), and none of their series carry the CARD modality, while every genuinely
-- excluded control (AWVASC, TERRA2, ECHOPAC-PC, PHCARDIO) does. They are interventional
-- and angio rooms.
--
-- NEAR-MISS ROWS IN MIGRATION 0083 are left in place rather than deleted. The seed
-- carries GELUNAR11, SYMBIANET, AWCTHD1, SENO2 and SENOIRIS; production actually stores
-- under GELUNAR, SYMBIA, CTHD and SENO1. The old rows match no studies, so they are
-- inert -- deleting live config is a bigger risk than leaving a few unused rows, and
-- AWCTHD1 is additionally in _EXCLUDED_AE_SQL so it can never load again anyway.
--
-- site_id = 1 (RH) for every row, including the three AEs that also carry SJH traffic
-- (SENO1 1,415 SJH / 102 RH; CM_CT_CMW_V1 57/10; KIZUNA 3/1). aetitle is UNIQUE here, so
-- one row cannot express two sites. Setting RH is the safe choice: since commit f0a12cb0
-- Report 23 separates sites on the study's own etl_didb_studies.pacs_site_id_raw, not on
-- this column, so site visibility there is already correct. m.site_id still matters to
-- report_25/report_31, where marking these SJH would wrongly hide their RH studies.
--
-- Fill-only, matching migration 0083: ON CONFLICT (aetitle) DO NOTHING never overwrites
-- a manually-corrected row and re-running is a no-op.

INSERT INTO aetitle_modality_map (aetitle, modality, site_id, description) VALUES
    -- Digital radiography
    ('DEFINIUM1',       'DX',     1, 'GE Definium DR room 1 (DX 99.8%)'),
    ('DEFINIUM2',       'DX',     1, 'GE Definium DR room 2 (DX 99.9%)'),
    ('OPTIMAXR',        'DX',     1, 'GE Optima digital radiography (DX 100%)'),
    ('AMX2',            'DX',     1, 'GE AMX portable radiography (DX 100%)'),
    -- Computed radiography
    ('FCR-CSL',         'CR',     1, 'Fuji FCR computed radiography (CR 99.9%)'),
    -- CT
    ('CT99',            'CT',     1, 'CT scanner (CT 99.9%)'),
    ('CTHD',            'CT',     1, 'CT HD scanner (CT 99.8%) — production AE for the unused AWCTHD1 seed row'),
    ('CM_CT_CMW_V1',    'CT',     1, 'CT workstation (CT 100%) — also carries SJH traffic'),
    -- Ultrasound
    ('LOGIQTOTUS',      'US',     1, 'GE Logiq ultrasound (US 100%)'),
    ('LOGIQS8',         'US',     1, 'GE Logiq S8 ultrasound (US 99.9%)'),
    ('LOGIQP9',         'US',     1, 'GE Logiq P9 ultrasound (US 100%)'),
    ('LOGIQE9',         'US',     1, 'GE Logiq E9 ultrasound (US 99.9%)'),
    -- Mammography
    ('SENO1',           'MG',     1, 'Mammography (MG 99.7%) — majority of its volume is SJH; see note above'),
    -- Nuclear medicine
    ('SYMBIA',          'NM',     1, 'Siemens Symbia SPECT/CT (NM 100%) — production AE for the unused SYMBIANET seed row'),
    -- Fluoroscopy / angiography / interventional
    ('FLUOROSTAR',      'RF',     1, 'Fluoroscopy (RF 100%)'),
    ('PRECISION',       'RF',     1, 'Fluoroscopy (RF 98%)'),
    ('OEC9900GSP',      'RF',     1, 'GE OEC 9900 C-arm (RF 98.4% — only 3 studies tagged XA)'),
    ('OEC9900VASC',     'XA',     1, 'GE OEC 9900 vascular C-arm (XA 95.3%)'),
    ('AZURION7FLEXARM', 'HYBRID', 1, 'Philips Azurion 7 FlexArm, hybrid OR (HYBRID 99.4% — only 1 study tagged XA)'),
    -- MR
    ('KIZUNA',          'MR',     1, 'MR (MR 100%, 4 studies only) — also carries SJH traffic'),
    -- Bone densitometry
    -- NOTE: DEXA really is tagged OT in this PACS (99.9%), and Report 23 excludes
    -- 'OT'. Mapping it correctly therefore does NOT make it visible there — that
    -- needs the separate operator decision on whether 'OT' should stay excluded.
    ('GELUNAR',         'OT',     1, 'GE Lunar DEXA / bone densitometry (OT 99.9%) — production AE for the unused GELUNAR11 seed row')
ON CONFLICT (aetitle) DO NOTHING;
