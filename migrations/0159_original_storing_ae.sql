-- Migration 0159: the device behind a gateway, and cardiology left out (LAUMC to-do items 4, 13).
-- *** LAUMC BRANCH ONLY ***
--
-- SJH's studies all reach PACS through its middleware (storing AE SJHCSAPWFMFIR); PACS keeps the
-- real device in MEDISTORE.DIDB_STUDIES.ORIGINAL_STORING_AE (operator, 2026-11-17). On the last
-- ~60,000 studies it names every SJH device (DR300_StoreSCU, CM_CT_CMW_V1, kizuna, AFFINITI,
-- AFFINITI1, CANON_CXDI, MAMMOPRISTINA, MEDISON) and two cardiology ones, EPIQCVX (cardiac echo)
-- and AZURION (the cath lab, now sending to the CVIS), which the operator wants out of RAYD.
--
-- The nightly study copy now reads the column and leaves the cardiology devices out; the gateway
-- studies already loaded get it once, then their cardiology studies are deleted
-- (ETL_JOBS/etl_didb_studies.py: fill_original_ae, purge_cardiology). Nothing reads the column
-- for the device yet: that is the next step of item 4.

ALTER TABLE public.etl_didb_studies ADD COLUMN IF NOT EXISTS original_storing_ae VARCHAR(100);

-- rh.etl_didb_studies lists the table's columns: rebuilt so the new one reaches it (0157's
-- function; the column is added at the end, as CREATE OR REPLACE VIEW requires).
SELECT rh_refresh_views();
