-- Migration 0164: a gateway study's device is the scanner behind the gateway (item 13, device rule).
-- *** LAUMC BRANCH ONLY ***
--
-- SJH's studies all reach PACS through its middleware SJHCSAPWFMFIR (and some through
-- LAUMCWFM2FIR), so PACS's storing AE is the gateway and every report read every SJH study as
-- modality "PACS" on one device; Report 25 leaves modality PACS out, so SJH showed nothing there.
-- PACS keeps the scanner in ORIGINAL_STORING_AE, copied since migration 0159.
--
-- About 70 queries join the device mapping on etl_didb_studies.storing_ae, so the scanner goes
-- into storing_ae and the gateway into the new gateway_ae: every report then reads the scanner's
-- modality, room and site, with no query changed. The nightly copy does the same for each study
-- it reads (ETL_JOBS/etl_didb_studies.py: GATEWAY_AES, device_of), and after every load.
-- Only when the original AE is a device the mapping knows (a modality other than PACS): an
-- import agent (XVIMPORT, 231 SJH studies), a workstation or an unmapped AE keeps the gateway,
-- as before, since reports keep only the site's mapped devices. LAUMCWFM1AR is not a gateway
-- here (it names itself as the original AE). To undo:
--   UPDATE etl_didb_studies SET storing_ae = gateway_ae, gateway_ae = NULL WHERE gateway_ae IS NOT NULL;
--
-- Scanners behind SJH's middleware that the mapping did not know (production, 2026-11-18), with
-- the RIS device each one is where the RIS books it under another name (0161's links: room,
-- code and site come from the RIS device):
--   KIZUNA_GOC   623 MR, Apr - Dec 2021, then the same scanner as KIZUNA     -> RIS KIZUNA
--   MEDISON      149 US, May 2024 - now, after MEDISON-1 (Sep 2021 - Mar 2024) -> RIS MEDISON-1
--   AFFINITI50     4 US, May - Jun 2026, no RIS device: SJH, ultrasound
--   PHILIPSCARM    5 RF, Aug 2025 - Jan 2026: SJH, fluoroscopy (C-arm), not linked yet
-- Left on the gateway: VIMS_AE (38), AW (2), CBOGT02 (1), SVSM (2), XVIMPORT, no AE (288).

ALTER TABLE public.etl_didb_studies ADD COLUMN IF NOT EXISTS gateway_ae VARCHAR(100);

INSERT INTO public.aetitle_modality_map (aetitle, modality, site_id, ris_aetitle, description)
SELECT v.ae, v.modality, s.id, v.ris, v.note
FROM (VALUES ('KIZUNA_GOC', 'MR', 'KIZUNA', 'SJH MRI, its earlier AE title (migration 0164)'),
             ('MEDISON', 'US', 'MEDISON-1', 'SJH portable ultrasound, its AE title since 2024 (migration 0164)'),
             ('AFFINITI50', 'US', NULL, 'SJH ultrasound (migration 0164)'),
             ('PHILIPSCARM', 'RF', NULL, 'SJH C-arm (migration 0164)')) AS v(ae, modality, ris, note)
JOIN public.sites s ON s.code = 'SJH'
WHERE NOT EXISTS (SELECT 1 FROM public.aetitle_modality_map m WHERE UPPER(TRIM(m.aetitle)) = v.ae);

SELECT public.rayd_apply_room_links();

DO $$
DECLARE
    swapped bigint;
BEGIN
    UPDATE public.etl_didb_studies s
       SET gateway_ae = UPPER(TRIM(s.storing_ae)), storing_ae = UPPER(TRIM(s.original_storing_ae))
     WHERE UPPER(TRIM(s.storing_ae)) IN ('SJHCSAPWFMFIR', 'LAUMCWFM2FIR')
       AND UPPER(TRIM(s.original_storing_ae)) <> UPPER(TRIM(s.storing_ae))
       AND EXISTS (SELECT 1 FROM public.aetitle_modality_map m
                   WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.original_storing_ae))
                     AND NULLIF(TRIM(m.modality), '') IS NOT NULL AND UPPER(TRIM(m.modality)) <> 'PACS');
    GET DIAGNOSTICS swapped = ROW_COUNT;
    RAISE NOTICE 'gateway studies given their scanner as device: %', swapped;
END
$$;

-- The site follows the device now (a mapped scanner gives its site; an unmapped one, the PACS
-- label, as before): re-resolved here, then by the ETL after every load (migration 0145).
SELECT rh_resolve_study_sites();

-- rh.etl_didb_studies lists the table's columns: rebuilt so the new one reaches it.
SELECT rh_refresh_views();
