-- LAUMC AE title -> modality/site seed, sourced from:
--   1. Real production "unmapped storing_ae" diagnostic (etl_didb_studies vs aetitle_modality_map)
--   2. Oracle medistore.ae_modality_mapping (operator-provided ground truth for device type)
--   3. RIS MODALITY export (org_structure_key -> site) for the RH/SJH split
--
-- Two groups:
--   - Real imaging devices (including secondary/workstation AEs for the same physical
--     scanner) get their true DICOM-ish modality code.
--   - AE titles with no corresponding RIS-registered device (workflow-manager/gateway/
--     import/SCU nodes) get modality = 'PACS' instead of being forced into a fake device
--     type, per operator instruction: "map them to something called PACS Modalities, or
--     without RIS modalities."
--
-- SVSM and 'LAUMC' are intentionally NOT seeded here — ETL_JOBS/etl_didb_studies.py
-- already excludes them at the source query (they are duplicate rows of a real device's
-- AE, not real devices); any rows still showing up in production predate that exclusion
-- and need a data cleanup, not a mapping entry.

INSERT INTO aetitle_modality_map (aetitle, modality, site_id, description) VALUES
    ('MR750W',          'MR',   1, 'MRI 3T - Discovery 750W (RIS-registered AE: GEHCGEHC)'),
    ('MR450W',          'MR',   1, 'MRI 1.5T - Optima 450W (RIS-registered AE: MR45W)'),
    ('AWMR450W',        'MR',   1, 'MR Advantage Workstation - 1.5T'),
    ('AWMR750W',        'MR',   1, 'MR Advantage Workstation - 3T'),
    ('ECHOPAC-PC',      'US',   1, 'Echocardiography analysis workstation (Vascular Lab)'),
    ('PHCARDIO',        'US',   1, 'Cardiac echo workstation (Vascular Lab)'),
    ('LOGIQ7-1',        'US',   1, 'Ultrasound Room 2 - Logiq'),
    ('LOGIQ7-2',        'US',   1, 'Ultrasound Room 2 - Logiq'),
    ('LOGIQV-000000',   'US',   1, 'Portable ultrasound (LOGIQ V variant AE)'),
    ('AWPETCT',         'PT',   1, 'PET-CT Advantage Workstation'),
    ('AE_DBT',          'MG',   1, 'Digital breast tomosynthesis'),
    ('SENO2',           'MG',   1, 'Mammography'),
    ('SENOIRIS',        'MG',   1, 'Mammography'),
    ('AWSENO1',         'MG',   1, 'Mammography workstation'),
    ('GELUNAR11',       'OT',   1, 'Bone densitometry / DEXA (secondary AE)'),
    ('BAY85CT',         'CT',   1, 'PET-CT - CT component/workstation (RIS-registered AE: bay87ct)'),
    ('AWS',             'CT',   1, 'CT Advantage Workstation'),
    ('AWCTHD1',         'CT',   1, 'CT64 Advantage Workstation'),
    ('RAPID',           'CT',   1, 'iSchemaView RAPID stroke-imaging AI'),
    ('TERRA1',          'XA',   1, 'IR-1 Innova 3131 (acquisition chain 1)'),
    ('TERRA2',          'XA',   1, 'IR-1 Innova 3131 (acquisition chain 2)'),
    ('ARTIS121403',     'XA',   1, 'IR2-NX Artis Q Biplane (Siemens)'),
    ('LEO22529',        'XA',   1, 'Angiography / interventional suite'),
    ('ADW_8',           'XA',   1, '3D vascular / multi-modality workstation'),
    ('AWVASC',          'XA',   1, 'Vascular Advantage Workstation'),
    ('SYMBIANET',       'NM',   1, 'SPECT-CT network node'),
    ('LAUOCT',          'OCT',  1, 'Ophthalmic OCT'),
    ('LAUMCWFM1FIR',    'PACS', 1, 'Workflow manager / integration gateway - not a physical modality'),
    ('LAUMCWFM2FIR',    'PACS', 1, 'Workflow manager / integration gateway - not a physical modality'),
    ('LAUMCWFM1AR',     'PACS', 1, 'Legacy specialty router - not a physical modality'),
    ('LAUMCPACSFIR',    'PACS', 1, 'Legacy specialty router - not a physical modality'),
    ('IIP_STORE_SCU',   'PACS', 1, 'Storage SCU / import gateway'),
    ('VDICOM',          'PACS', 1, 'DICOM import/conversion gateway'),
    ('VDICOM_STR_SCU',  'PACS', 1, 'DICOM import/conversion gateway'),
    ('XVIMPORT',        'PACS', 1, 'Import gateway'),
    ('MASSI1FIR',       'PACS', 1, 'Integration gateway'),
    ('AETITLE',         'PACS', 1, 'Literal placeholder AE title - likely misconfigured source, needs device-side investigation'),
    ('NONDICOMAGENT',   'PACS', 1, 'Non-DICOM ingestion agent'),
    ('SJHCSAPWFMFIR',   'PACS', 2, 'SJH workflow manager gateway')
ON CONFLICT (aetitle) DO UPDATE SET
    modality    = EXCLUDED.modality,
    site_id     = EXCLUDED.site_id,
    description = EXCLUDED.description;
