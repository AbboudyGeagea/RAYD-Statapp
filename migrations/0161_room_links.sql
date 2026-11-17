-- Migration 0161: a PACS AE title that the RIS does not know shows the room of the RIS device it
-- really is (LAUMC to-do item 4). *** LAUMC BRANCH ONLY ***
--
-- Some devices store to PACS under another AE title than the one the RIS books them under. Each
-- link below is proven from the exams themselves: the PACS study linked to its RIS exam, and the
-- RIS room that performed it (production, 90 days, 17 Nov):
--   MR750W      -> GEHCGEHC   MRI 3T       902 of 961 exams (94 %; the rest are booked on the
--                                          other MRI and the room is not changed, operator)
--   MR450W      -> MR45W      MRI 1.5T     766 of 810 (95 %)
--   AWPETCT     -> BAY87CT    PET-CT       308 of 308 (the AW workstation sends the PET-CTs)
--   BAY85CT     -> BAY87CT    PET-CT       130 of 132 (the PET-CT's CT)
--   AWS         -> CTHD       CT64         43 of 43 (the AW server)
--   SYMBIANET   -> SYMBIA     SPECT-CT     6 of 6
-- Not linked yet: LEO22529 and ARTIS121403 (no RIS exam at all; likely IR2-NX, to confirm).
-- SJH's devices behind its middleware come from the original storing AE (0159), item 13.
--
-- rayd_apply_room_links() copies the RIS device's room name, code and site onto the PACS AE
-- (never its modality: BAY85CT stays CT). Where the room has another PACS AE as well (two links
-- to one RIS device, or the RIS AE stores to PACS itself), the label adds the AE title, so a
-- device chart never shows two bars with one name
-- ("PET-CT Discovery ES 16 (GE) · AWPETCT"). Run here and after every nightly RIS copy
-- (ETL_JOBS/etl_ris_modality.py), so a room renamed on the RIS reaches its linked AEs too.

ALTER TABLE public.aetitle_modality_map ADD COLUMN IF NOT EXISTS ris_aetitle VARCHAR(100);

UPDATE public.aetitle_modality_map m SET ris_aetitle = l.ris
FROM (VALUES ('MR750W', 'GEHCGEHC'), ('MR450W', 'MR45W'), ('AWPETCT', 'BAY87CT'),
             ('BAY85CT', 'BAY87CT'), ('AWS', 'CTHD'), ('SYMBIANET', 'SYMBIA')) AS l(pacs, ris)
WHERE UPPER(TRIM(m.aetitle)) = l.pacs
  AND m.ris_aetitle IS NULL;

CREATE OR REPLACE FUNCTION public.rayd_apply_room_links() RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
    changed integer;
BEGIN
    WITH links AS (
        SELECT p.id, p.aetitle, r.station_name, r.room_code, r.site_id,
               (COUNT(*) OVER (PARTITION BY UPPER(TRIM(p.ris_aetitle))) > 1
                OR EXISTS (SELECT 1 FROM public.etl_didb_studies s
                           WHERE UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(r.aetitle))
                             AND s.study_date >= current_date - 90)) AS shared
        FROM public.aetitle_modality_map p
        JOIN public.aetitle_modality_map r ON UPPER(TRIM(r.aetitle)) = UPPER(TRIM(p.ris_aetitle))
        WHERE NULLIF(BTRIM(p.ris_aetitle), '') IS NOT NULL
          AND NULLIF(BTRIM(r.station_name), '') IS NOT NULL
          AND r.id <> p.id
    )
    UPDATE public.aetitle_modality_map m
    SET station_name = CASE WHEN l.shared THEN BTRIM(l.station_name) || ' · ' || BTRIM(m.aetitle)
                            ELSE BTRIM(l.station_name) END,
        room_name    = CASE WHEN l.shared THEN BTRIM(l.station_name) || ' · ' || BTRIM(m.aetitle)
                            ELSE BTRIM(l.station_name) END,
        room_code    = COALESCE(l.room_code, m.room_code),
        site_id      = COALESCE(l.site_id, m.site_id)
    FROM links l
    WHERE m.id = l.id
      AND (m.station_name IS DISTINCT FROM CASE WHEN l.shared THEN BTRIM(l.station_name) || ' · ' || BTRIM(m.aetitle)
                                                ELSE BTRIM(l.station_name) END
           OR m.room_code IS DISTINCT FROM COALESCE(l.room_code, m.room_code)
           OR m.site_id IS DISTINCT FROM COALESCE(l.site_id, m.site_id));
    GET DIAGNOSTICS changed = ROW_COUNT;
    RETURN changed;
END
$fn$;

SELECT public.rayd_apply_room_links();
