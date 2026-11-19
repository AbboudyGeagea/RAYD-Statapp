-- Migration 0171: ARTIS121403 shows the RIS room TERRA (LAUMC to-do item 4). *** LAUMC BRANCH ONLY ***
--
-- ARTIS121403 is the Siemens Artis Q of IR2-NX by its name, but the RIS books its exams on TERRA,
-- IR-1 "Interventional Radiology - Innova 3131 (GE)" (production, 90 days, 19 Nov: 18 exams on
-- TERRA, 2 on INTERAET, 11 on no room). Operator, 19 Nov: link it by its bookings, to TERRA.
-- LEO22529 (likely the Siemens Leonardo workstation; 25 TERRA, 7 INTERAET, 12 none) stays
-- unlinked: it serves both rooms (operator, 19 Nov).
--
-- TERRA is on the ETL's excluded list as a PACS storing AE (etl_didb_studies.EXCLUDED_AES), so it
-- stores nothing in RAYD: the link only copies its room name, code and site onto ARTIS121403
-- (rayd_apply_room_links, migration 0161), whose studies stay in. Every site database runs this;
-- SJH's holds no TERRA row, so nothing is copied there.

UPDATE public.aetitle_modality_map
   SET ris_aetitle = 'TERRA'
 WHERE UPPER(TRIM(aetitle)) = 'ARTIS121403'
   AND ris_aetitle IS NULL;

SELECT public.rayd_apply_room_links();
