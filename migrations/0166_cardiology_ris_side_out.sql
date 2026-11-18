-- Migration 0166: the cath lab's RIS side out, as its PACS studies are (item 13b).
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 2026-11-17: "leave cardiology out, filter them from ETL level". Migration 0159 took
-- the cardiology devices' PACS studies out (EPIQCVX, the cardiac echo; AZURION, SJH's cath lab,
-- which now sends to the CVIS). Their RIS side stayed (production, 18 Nov): 164 performed steps
-- on AZURION (17 in the last 90 days), its device row and its 7 weekly schedule rows, so the cath
-- lab still showed as an SJH device with capacity and almost no work.
--
-- The nightly RIS copies now skip both devices (ETL_JOBS/etl_ris_pps.py, etl_ris_modality.py,
-- etl_ris_modality_availability.py, by etl_didb_studies.CARDIOLOGY_AES). This removes what is
-- loaded: the steps with their person references and site extension, the device row (its weekly
-- schedule and exceptions follow by cascade) and the RIS rows that name it. The weekly
-- availability is rebuilt from the device rows every night, so it follows. BRAYN's own tables
-- are not touched. Every site database runs this too.

DELETE FROM public.std_pps_person_reference
 WHERE pps_key IN (SELECT pps_key FROM public.std_pps
                   WHERE UPPER(TRIM(performing_ae_title)) IN ('EPIQCVX', 'AZURION'));

DELETE FROM public.std_site_pps_ext
 WHERE pps_key IN (SELECT pps_key FROM public.std_pps
                   WHERE UPPER(TRIM(performing_ae_title)) IN ('EPIQCVX', 'AZURION'));

DELETE FROM public.std_pps WHERE UPPER(TRIM(performing_ae_title)) IN ('EPIQCVX', 'AZURION');

DELETE FROM public.std_modality_exceptions WHERE UPPER(TRIM(aetitle)) IN ('EPIQCVX', 'AZURION');
DELETE FROM public.std_device_weekly_availability WHERE UPPER(TRIM(aetitle)) IN ('EPIQCVX', 'AZURION');
DELETE FROM public.std_device_weekly_windows WHERE UPPER(TRIM(aetitle)) IN ('EPIQCVX', 'AZURION');
DELETE FROM public.std_schedule_template_items WHERE UPPER(TRIM(aetitle)) IN ('EPIQCVX', 'AZURION');

-- device_weekly_schedule and device_exceptions reference it ON DELETE CASCADE
DELETE FROM public.aetitle_modality_map WHERE UPPER(TRIM(aetitle)) IN ('EPIQCVX', 'AZURION');
