-- Migration 0148: Report Composer names a device by its RIS room name
-- *** LAUMC BRANCH ONLY ***
--
-- Meeting of 2026-11-13, item 4: the room name from the RIS instead of the AE title,
-- everywhere, Report Composer included, and the label typed in RAYD (display_aetitle)
-- dropped so that the RIS name wins. rpt.exams and rpt.studies (migration 0136) read the
-- device through one LATERAL on aetitle_modality_map, taking room_name as `room` and
-- display_aetitle as `device_name`. Both now take the RIS room name, as utils/ae_display.py
-- resolves it for every other screen: station_name (written from the RIS every night by
-- ETL_JOBS/etl_ris_modality.py), else room_name (a device the RIS does not know). `room`
-- feeds Composer's Room filter; `device_name` stays a column, so saved reports keep working.
--
-- The views are rewritten in place from their stored definition, as migration 0145 does,
-- so they keep pointing at the rh views. Only the LATERAL's select list changes. A view
-- whose definition does not hold the expected text is left as it is, with a WARNING
-- (tests/test_worklists.py fails in that case, on the same Postgres 15 as production).

DO $mig$
DECLARE
    v      record;
    def    text;
    label  constant text :=
        'COALESCE(NULLIF(btrim(mm.station_name), ''''::text), '
        'NULLIF(btrim((mm.room_name)::text), ''''::text))';
BEGIN
    FOR v IN
        SELECT c.oid, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'rpt' AND c.relkind = 'v' AND c.relname IN ('exams', 'studies')
    LOOP
        def := pg_get_viewdef(v.oid, false);
        IF def !~ '\mmm\.room_name,\s+mm\.display_aetitle\s+FROM aetitle_modality_map mm\M' THEN
            RAISE WARNING 'rpt.%: device LATERAL not found, view left unchanged', v.relname;
            CONTINUE;
        END IF;
        def := regexp_replace(def,
            '\mmm\.room_name,(\s+)mm\.display_aetitle(\s+)FROM aetitle_modality_map mm\M',
            label || ' AS room_name,\1' || label || ' AS display_aetitle\2FROM aetitle_modality_map mm');
        EXECUTE format('CREATE OR REPLACE VIEW rpt.%I AS %s', v.relname, def);
    END LOOP;
END
$mig$;
