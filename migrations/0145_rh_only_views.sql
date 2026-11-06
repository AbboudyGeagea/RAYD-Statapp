-- Migration 0145: RH only, through filtered views. Nothing is deleted.
-- *** LAUMC BRANCH ONLY ***
--
-- Operator rule, 2026-11-05: SJH appears in nothing, and SJH rows stay in the database.
--
-- About 40 files read this data, so a filter in each query could only be as complete as an
-- audit of them all. Schema rh instead holds a filtered view per table, under the table's
-- own name. Web requests run with search_path rh, public (utils/rh_scope.py), so every
-- unqualified table name in every query finds the view, with no query changed. The ETL, the
-- HL7 listener, the NLP worker and CRN keep reading the real tables.
--
-- To turn it off: set RAYD_RH_ONLY=0 and restart. Nothing here has to be undone.
--
-- A study's site: a physical device gives it. The PACS label only decides for gateways and
-- unmapped devices, because double reading stamps RH mammography with site '1'. Same rule as
-- utils/report_filters.study_site_sql. It is stored in etl_didb_studies.site_id, a column that
-- was added in 0049 and never filled, so the views need no join. LAUMCWFM1AR's label '2' is
-- not a site, so it stays NULL and stays visible for the reports that count it aside.
--
-- Filling site_id updates every study once, here at deploy: minutes, not hours, because no
-- image rows are touched. The ETL keeps it current after each load.
--
-- Not filtered, on purpose: aetitle_modality_map (the Mapping screen, and the rule above read
-- it; device lists use rh_device_sql), series and image detail (always read through a study),
-- etl_patient_view (reports join it from a study), hl7_orders (Report 22 alters it during a
-- request, so it must stay a table, and it is not populated on this install).

CREATE SCHEMA IF NOT EXISTS rh;

CREATE OR REPLACE FUNCTION rh_resolve_study_sites() RETURNS bigint
LANGUAGE plpgsql AS $fn$
DECLARE
    changed bigint;
BEGIN
    UPDATE public.etl_didb_studies s
    SET site_id = x.site
    FROM (
        SELECT s2.study_db_uid,
               CASE WHEN d.site_id IS NULL OR d.modality = 'PACS'
                    THEN (SELECT si.id FROM public.sites si
                          WHERE si.pacs_site_id = s2.pacs_site_id_raw LIMIT 1)
                    ELSE d.site_id END AS site
        FROM public.etl_didb_studies s2
        LEFT JOIN LATERAL (
            SELECT m.modality, m.site_id
            FROM public.aetitle_modality_map m
            WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s2.storing_ae))
            ORDER BY m.id
            LIMIT 1
        ) d ON TRUE
    ) x
    WHERE x.study_db_uid = s.study_db_uid
      AND s.site_id IS DISTINCT FROM x.site;
    GET DIAGNOSTICS changed = ROW_COUNT;
    RETURN changed;
END
$fn$;

-- Also run after every batch of migrations (db_migrations.py): a view lists its table's columns
-- as they were when it was made, so a column added later would be missing from it. Search path
-- is pinned so pg_get_viewdef prints every table with its schema.
CREATE OR REPLACE FUNCTION rh_refresh_views() RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $fn$
DECLARE
    t text;
    v record;
    rh_id  text := '(SELECT id FROM public.sites WHERE is_default LIMIT 1)';
    others text := '(SELECT ris_issuer FROM public.sites WHERE NOT is_default AND ris_issuer IS NOT NULL)';
BEGIN
    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.etl_didb_studies AS
         SELECT s.* FROM public.etl_didb_studies s
         WHERE COALESCE(s.site_id = %s, TRUE)', rh_id);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.etl_orders AS
         SELECT o.* FROM public.etl_orders o
         WHERE o.order_control IS NULL OR o.order_control NOT IN %s', others);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.hl7_oru_reports AS
         SELECT r.* FROM public.hl7_oru_reports r
         WHERE NOT EXISTS (SELECT 1 FROM public.etl_orders o
                           WHERE o.accession_number = r.accession_number
                             AND o.order_control IN %s)
           AND NOT EXISTS (SELECT 1 FROM public.etl_didb_studies s
                           WHERE s.accession_number = r.accession_number
                             AND s.site_id <> %s)', others, rh_id);

    EXECUTE 'CREATE OR REPLACE VIEW rh.hl7_oru_analysis AS
             SELECT a.* FROM public.hl7_oru_analysis a
             WHERE a.report_id IN (SELECT id FROM rh.hl7_oru_reports)';

    EXECUTE 'CREATE OR REPLACE VIEW rh.ai_nlp_cache AS
             SELECT c.* FROM public.ai_nlp_cache c
             WHERE c.source_id IN (SELECT id FROM rh.hl7_oru_reports)';

    FOREACH t IN ARRAY ARRAY['std_worklist_arrivals', 'std_worklist_exam_done', 'std_worklist_scheduled',
                             'std_worklist_cancellations', 'worklist_status_history'] LOOP
        EXECUTE format(
            'CREATE OR REPLACE VIEW rh.%I AS
             SELECT d.* FROM public.%I d
             WHERE NOT EXISTS (SELECT 1 FROM public.etl_orders o
                               WHERE o.order_dbid = d.site_worklist_key
                                 AND o.order_control IN %s)', t, t, others);
    END LOOP;

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_reports AS
         SELECT r.* FROM public.std_reports r
         WHERE NOT EXISTS (SELECT 1 FROM public.etl_orders o
                           WHERE o.accession_number = r.reported_acc_number
                             AND o.order_control IN %s)', others);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_pps AS
         SELECT p.* FROM public.std_pps p
         WHERE NOT EXISTS (SELECT 1 FROM public.aetitle_modality_map m
                           WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(p.performing_ae_title))
                             AND COALESCE(m.modality, %L) <> %L
                             AND m.site_id <> %s)
           AND NOT EXISTS (SELECT 1 FROM public.etl_didb_studies s
                           WHERE s.study_db_uid = p.study_db_uid
                             AND s.site_id <> %s)', '', 'PACS', rh_id, rh_id);

    -- Report Composer logs in as its own role and reads only rpt.*, so search_path cannot reach
    -- it. Its views are pointed at rh instead, in place, so a later change to one stays covered.
    FOR v IN
        SELECT c.oid, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'rpt' AND c.relkind = 'v'
    LOOP
        EXECUTE format('CREATE OR REPLACE VIEW rpt.%I AS %s', v.relname,
            regexp_replace(pg_get_viewdef(v.oid, false),
                '\mpublic\.(etl_didb_studies|etl_orders|hl7_oru_reports|hl7_oru_analysis|ai_nlp_cache|std_worklist_arrivals|std_worklist_exam_done|std_worklist_scheduled|std_worklist_cancellations|worklist_status_history|std_reports|std_pps)\M',
                'rh.\1', 'g'));
    END LOOP;
END
$fn$;

SELECT rh_resolve_study_sites();
SELECT rh_refresh_views();
