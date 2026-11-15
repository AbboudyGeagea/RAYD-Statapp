-- Migration 0151: placeholder records, kept out through BRAYN's list (LAUMC to-do item 19).
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 2026-11-13: "let brayn filter them". Three kinds of record are loaded faithfully from
-- the RIS and PACS, but are not what they claim to be:
--
--   test patients          every study, order, step and report of theirs is left out of what
--                          people read
--   placeholder referrers  "Referring, Generic" (RIS 4101031379966 and 4101031379968): the exam is
--                          real, so it stays in every count; only its referring doctor reads as
--                          blank, so it leads no referrer list. CRN texts the fallback contact
--                          instead (utils/crn_dispatch.py)
--   service accounts       the CSH interface account (9271030205716, SERVICE-PROD): left out of
--                          staff lists
--
-- The list is BRAYN's (Administration > BRAYN > Settings), and BRAYN names a listed patient,
-- referrer or account on any HL7 message (PLACEHOLDER_DATA). BRAYN never sees what the Oracle ETL
-- loads, so the list is applied where every page reads: the rh views of migration 0145. Nothing is
-- deleted. The ETL, the listener, the NLP worker and CRN keep reading the real tables.
--
-- A patient is listed by patient ID, as the RIS (any of its patient IDs) or PACS shows it. Never a
-- name: no patient name is kept in RAYD (operator, 2026-07-27), hence the second CHECK.
--
-- PACS studies carry the referring doctor's name only, so a referrer is matched by code on HL7 and
-- RIS rows and by "Last, First" on PACS studies.

CREATE TABLE IF NOT EXISTS public.brayn_placeholders (
    id          SERIAL PRIMARY KEY,
    kind        TEXT NOT NULL CHECK (kind IN ('patient', 'referring', 'staff')),
    code        TEXT,
    name        TEXT,
    label       TEXT NOT NULL DEFAULT '',
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    updated_by  INTEGER,
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT brayn_placeholders_code_or_name
        CHECK (NULLIF(BTRIM(code), '') IS NOT NULL OR NULLIF(BTRIM(name), '') IS NOT NULL),
    CONSTRAINT brayn_placeholders_no_patient_name
        CHECK (kind <> 'patient' OR name IS NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_brayn_placeholders
    ON public.brayn_placeholders (kind, UPPER(COALESCE(code, '')), UPPER(COALESCE(name, '')));

INSERT INTO public.brayn_placeholders (kind, code, name, label) VALUES
    ('referring', '4101031379966', 'Referring, Generic', 'Placeholder referring doctor (RIS)'),
    ('referring', '4101031379968', 'Referring, Generic', 'Placeholder referring doctor (RIS)'),
    ('staff',     '9271030205716', 'SERVICE-PROD, CSH',  'CSH interface service account')
ON CONFLICT DO NOTHING;

-- A test patient's PACS studies are found by PACS patient ID.
CREATE INDEX IF NOT EXISTS idx_etl_patient_view_id ON public.etl_patient_view (id);

-- Migration 0145's function, with the list applied. Each set below is small and is worked out once
-- per query (a hashed subplan), so no study is looked up one by one.
CREATE OR REPLACE FUNCTION rh_refresh_views() RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $fn$
DECLARE
    t text;
    v record;
    cols text;
    rh_id  text := '(SELECT id FROM public.sites WHERE is_default LIMIT 1)';
    others text := '(SELECT ris_issuer FROM public.sites WHERE NOT is_default AND ris_issuer IS NOT NULL)';
    -- BRAYN's list (0151). None of these sets can hold a NULL, which would empty a NOT IN.
    test_ids text := format(
        '(SELECT BTRIM(x.code) FROM public.brayn_placeholders x
          WHERE x.kind = %L AND x.active AND BTRIM(x.code) <> %L)', 'patient', '');
    test_keys text;        -- RIS patient_person_key
    test_keys_text text;   -- the same, as etl_orders.patient_dbid holds it
    test_pacs text;        -- PACS patient_db_uid
    test_acc text;         -- accession numbers of a test patient's RIS orders
    ref_names text := format(
        '(SELECT UPPER(BTRIM(x.name)) FROM public.brayn_placeholders x
          WHERE x.kind = %L AND x.active AND BTRIM(x.name) <> %L)', 'referring', '');
BEGIN
    test_keys := format('(SELECT i.patient_person_key FROM public.std_patient_ids i
                          WHERE i.patient_id IN %s)', test_ids);
    test_keys_text := format('(SELECT i.patient_person_key::text FROM public.std_patient_ids i
                               WHERE i.patient_id IN %s)', test_ids);
    test_pacs := format('(SELECT p.patient_db_uid FROM public.etl_patient_view p
                          WHERE p.id IN %s)', test_ids);
    test_acc := format('(SELECT o.accession_number FROM public.etl_orders o
                         WHERE o.accession_number IS NOT NULL AND o.patient_dbid IN %s)', test_keys_text);

    -- A study of a placeholder referrer keeps every column but the referrer's name. The column
    -- list is built from the table, so a column added later still reaches the view.
    SELECT string_agg(
               CASE WHEN a.attname IN ('referring_physician_first_name', 'referring_physician_last_name',
                                       'referring_physician_mid_name')
                    THEN format('(CASE WHEN UPPER(CONCAT_WS(%L, BTRIM(s.referring_physician_last_name),
                                                    BTRIM(s.referring_physician_first_name))) IN %s
                                       THEN NULL ELSE s.%I END)::%s AS %I',
                                ', ', ref_names, a.attname, format_type(a.atttypid, a.atttypmod), a.attname)
                    ELSE format('s.%I', a.attname) END,
               ', ' ORDER BY a.attnum)
      INTO cols
      FROM pg_attribute a
     WHERE a.attrelid = 'public.etl_didb_studies'::regclass AND a.attnum > 0 AND NOT a.attisdropped;

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.etl_didb_studies AS
         SELECT %s FROM public.etl_didb_studies s
         WHERE COALESCE(s.site_id = %s, TRUE)
           AND (s.patient_db_uid IS NULL OR s.patient_db_uid NOT IN %s)
           AND (s.accession_number IS NULL OR s.accession_number NOT IN %s)',
        cols, rh_id, test_pacs, test_acc);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.etl_orders AS
         SELECT o.* FROM public.etl_orders o
         WHERE (o.order_control IS NULL OR o.order_control NOT IN %s)
           AND (o.patient_dbid IS NULL OR o.patient_dbid NOT IN %s)', others, test_keys_text);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.hl7_oru_reports AS
         SELECT r.* FROM public.hl7_oru_reports r
         WHERE NOT EXISTS (SELECT 1 FROM public.etl_orders o
                           WHERE o.accession_number = r.accession_number
                             AND o.order_control IN %s)
           AND NOT EXISTS (SELECT 1 FROM public.etl_didb_studies s
                           WHERE s.accession_number = r.accession_number
                             AND s.site_id <> %s)
           AND (r.patient_id IS NULL OR BTRIM(r.patient_id) NOT IN %s)
           AND (r.accession_number IS NULL OR r.accession_number NOT IN %s)',
        others, rh_id, test_ids, test_acc);

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
                                 AND (o.order_control IN %s OR o.patient_dbid IN %s))',
            t, t, others, test_keys_text);
    END LOOP;

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_reports AS
         SELECT r.* FROM public.std_reports r
         WHERE NOT EXISTS (SELECT 1 FROM public.etl_orders o
                           WHERE o.accession_number = r.reported_acc_number
                             AND (o.order_control IN %s OR o.patient_dbid IN %s))',
        others, test_keys_text);

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_pps AS
         SELECT p.* FROM public.std_pps p
         WHERE NOT EXISTS (SELECT 1 FROM public.aetitle_modality_map m
                           WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(p.performing_ae_title))
                             AND COALESCE(m.modality, %L) <> %L
                             AND m.site_id <> %s)
           AND NOT EXISTS (SELECT 1 FROM public.etl_didb_studies s
                           WHERE s.study_db_uid = p.study_db_uid
                             AND s.site_id <> %s)
           AND (p.patient_person_key IS NULL OR p.patient_person_key NOT IN %s)',
        '', 'PACS', rh_id, rh_id, test_keys);

    -- Staff (0151): a listed service account is in no staff list. Matched on either RIS key or
    -- the resource ID, or on "Last, First".
    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_resources_ris AS
         SELECT r.* FROM public.std_resources_ris r
         WHERE NOT EXISTS (SELECT 1 FROM public.brayn_placeholders x
                           WHERE x.kind = %L AND x.active
                             AND (BTRIM(x.code) IN (r.resource_id_key::text, r.person_key::text, r.resource_id)
                                  OR UPPER(BTRIM(x.name)) = UPPER(CONCAT_WS(%L, BTRIM(r.last_name),
                                                                           BTRIM(r.first_name)))))',
        'staff', ', ');

    -- Report Composer logs in as its own role and reads only rpt.*, so search_path cannot reach
    -- it. Its views are pointed at rh instead, in place, so a later change to one stays covered.
    FOR v IN
        SELECT c.oid, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'rpt' AND c.relkind = 'v'
    LOOP
        EXECUTE format('CREATE OR REPLACE VIEW rpt.%I AS %s', v.relname,
            regexp_replace(pg_get_viewdef(v.oid, false),
                '\mpublic\.(etl_didb_studies|etl_orders|hl7_oru_reports|hl7_oru_analysis|ai_nlp_cache|std_worklist_arrivals|std_worklist_exam_done|std_worklist_scheduled|std_worklist_cancellations|worklist_status_history|std_reports|std_pps|std_resources_ris)\M',
                'rh.\1', 'g'));
    END LOOP;
END
$fn$;

SELECT rh_refresh_views();
