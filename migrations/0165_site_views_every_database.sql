-- Migration 0165: the rh views keep the site each database serves (item 13, before the SJH move).
-- *** LAUMC BRANCH ONLY ***
--
-- The views of migration 0145 (schema rh) hold every rule people read through: a test patient's
-- rows and a placeholder referrer's name (0151), staff who left read 'Former staff' (0157), the
-- HIS's duplicate orders left out (0162). They kept "the default site" (RH). Every migration and
-- this function run on every site database too, so in SJH's own database they kept RH's rows,
-- that is none: Report Composer there (rpt.* reads rh.*) saw 0 of SJH's studies, and a request
-- on SJH read the real tables, without any of the rules.
--
-- Now "this site" is rayd_served_site_id() (migration 0153), read when the views are rebuilt and
-- written into them as a number: RH in the main database, as before,
-- so nothing changes there; SJH in SJH's. "The other sites' RIS issuers" are those of every site
-- but the served one. Web requests on a site database now read through the views as well
-- (utils/site_db.py installs utils/rh_scope.py there), and the placeholder list is copied to
-- every site database (utils/site_db.SHARED_TABLES). Only the two definitions marked below
-- differ from 0162's function.

CREATE OR REPLACE FUNCTION rh_refresh_views() RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $fn$
DECLARE
    t text;
    v record;
    cols text;
    -- 0165: the site this database serves (was: the default site), written into the views as a
    -- number. Not a call: a function in a view runs with the reader's rights, and Report
    -- Composer's role (rpt.* reads rh.*) may not read settings. served_site is recorded before
    -- a site database's migrations (utils/site_db._bootstrap), and this runs after each batch.
    rh_id  text := COALESCE(public.rayd_served_site_id()::text, 'NULL');
    -- 0165: the RIS issuers of every other site (was: of every site but the default)
    others text := format('(SELECT ris_issuer FROM public.sites WHERE id <> %s AND ris_issuer IS NOT NULL)',
                          COALESCE(public.rayd_served_site_id()::text, 'NULL'));
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
    -- Staff who left (0157): empty while settings.staff_left_hidden is not 'true'.
    former     text := 'Former staff';
    left_login text := format('(SELECT k.key FROM public.staff_left_keys k WHERE k.kind = %L)', 'login');
    left_name  text := format('(SELECT k.key FROM public.staff_left_keys k WHERE k.kind = %L)', 'name');
    left_res   text := format('(SELECT k.key FROM public.staff_left_keys k WHERE k.kind = %L)', 'resource');
    login_cols text[] := ARRAY['reading_physician_id', 'signing_physician_id', 'rep_prelim_signed_by',
                               'rep_transcribed_by', 'rep_final_signed_by', 'rep_addendum_by', 'submitted_by',
                               'rep_study_last_composed_by', 'rep_last_accessed_by', 'imported_by'];
    pn_cols    text[] := ARRAY['operators_name', 'performing_physicians_name'];   -- DICOM LAST^FIRST
    norm text := 'NULLIF(BTRIM(REGEXP_REPLACE(LOWER(%s), ''[[:space:],^]+'', '' '', ''g'')), '''')';
    -- HIS duplicates (0162): an order still Scheduled after its day has passed. The HIS sends
    -- every order twice; one copy stays Scheduled and the RIS screen filters it out.
    -- COALESCE: an order with no status (an unmapped RIS status) or no date is never a duplicate;
    -- without it NOT NULL would drop the row.
    his_dup text := format('COALESCE(%1$s.order_status = %2$L AND %1$s.scheduled_datetime < (CURRENT_TIMESTAMP AT TIME ZONE %3$L)::date, FALSE)',
                           '%1$s', 'scheduled', 'Asia/Beirut');
    pair_left text;
BEGIN
    test_keys := format('(SELECT i.patient_person_key FROM public.std_patient_ids i
                          WHERE i.patient_id IN %s)', test_ids);
    test_keys_text := format('(SELECT i.patient_person_key::text FROM public.std_patient_ids i
                               WHERE i.patient_id IN %s)', test_ids);
    test_pacs := format('(SELECT p.patient_db_uid FROM public.etl_patient_view p
                          WHERE p.id IN %s)', test_ids);
    test_acc := format('(SELECT o.accession_number FROM public.etl_orders o
                         WHERE o.accession_number IS NOT NULL AND o.patient_dbid IN %s)', test_keys_text);

    -- A study of a placeholder referrer keeps every column but the referrer's name; a study
    -- signed, read or typed by someone who left keeps every column but that person's login or
    -- name. The column list is built from the table, so a column added later still reaches it.
    SELECT string_agg(
               CASE
                   WHEN a.attname IN ('referring_physician_first_name', 'referring_physician_last_name',
                                      'referring_physician_mid_name')
                   THEN format('(CASE WHEN UPPER(CONCAT_WS(%L, BTRIM(s.referring_physician_last_name),
                                                   BTRIM(s.referring_physician_first_name))) IN %s
                                      THEN NULL ELSE s.%I END)::%s AS %I',
                               ', ', ref_names, a.attname, format_type(a.atttypid, a.atttypmod), a.attname)
                   WHEN a.attname = ANY (login_cols)
                   THEN format('(CASE WHEN LOWER(BTRIM(SPLIT_PART(BTRIM(s.%I), %L, 1))) IN %s
                                      THEN %L ELSE s.%I END)::%s AS %I',
                               a.attname, '@', left_login, former, a.attname,
                               format_type(a.atttypid, a.atttypmod), a.attname)
                   WHEN a.attname ~ '^(reading|signing)_physician_(first|last)_name$'
                   THEN format('(CASE WHEN (LOWER(BTRIM(SPLIT_PART(BTRIM(s.%I), %L, 1))) IN %s
                                            OR %s IN %s)
                                      THEN %s ELSE s.%I END)::%s AS %I',
                               split_part(a.attname, '_physician_', 1) || '_physician_id', '@', left_login,
                               format(norm, format('CONCAT_WS(%L, s.%I, s.%I)', ' ',
                                      split_part(a.attname, '_physician_', 1) || '_physician_first_name',
                                      split_part(a.attname, '_physician_', 1) || '_physician_last_name')),
                               left_name,
                               CASE WHEN a.attname LIKE '%last_name' THEN quote_literal(former) ELSE 'NULL' END,
                               a.attname, format_type(a.atttypid, a.atttypmod), a.attname)
                   WHEN a.attname = ANY (pn_cols)
                   THEN format('(CASE WHEN %s IN %s THEN %L ELSE s.%I END)::%s AS %I',
                               format(norm, format('s.%I', a.attname)), left_name, former, a.attname,
                               format_type(a.atttypid, a.atttypmod), a.attname)
                   ELSE format('s.%I', a.attname)
               END,
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
           AND (o.patient_dbid IS NULL OR o.patient_dbid NOT IN %s)
           AND NOT %s', others, test_keys_text, format(his_dup, 'o'));

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
                                 AND (o.order_control IN %s OR o.patient_dbid IN %s OR %s))',
            t, t, others, test_keys_text, format(his_dup, 'o'));
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
    -- the resource ID, or on "Last, First". Staff who left (0157) keep their row, roles and keys;
    -- their name reads 'Former staff'.
    SELECT string_agg(
               CASE
                   WHEN a.attname IN ('last_name', 'common_name')
                   THEN format('(CASE WHEN r.resource_id_key::text IN %s THEN %L ELSE r.%I END)::%s AS %I',
                               left_res, former, a.attname, format_type(a.atttypid, a.atttypmod), a.attname)
                   WHEN a.attname IN ('first_name', 'middle_name', 'name_prefix', 'name_suffix')
                   THEN format('(CASE WHEN r.resource_id_key::text IN %s THEN NULL ELSE r.%I END)::%s AS %I',
                               left_res, a.attname, format_type(a.atttypid, a.atttypmod), a.attname)
                   ELSE format('r.%I', a.attname)
               END,
               ', ' ORDER BY a.attnum)
      INTO cols
      FROM pg_attribute a
     WHERE a.attrelid = 'public.std_resources_ris'::regclass AND a.attnum > 0 AND NOT a.attisdropped;

    EXECUTE format(
        'CREATE OR REPLACE VIEW rh.std_resources_ris AS
         SELECT %s FROM public.std_resources_ris r
         WHERE NOT EXISTS (SELECT 1 FROM public.brayn_placeholders x
                           WHERE x.kind = %L AND x.active
                             AND (BTRIM(x.code) IN (r.resource_id_key::text, r.person_key::text, r.resource_id)
                                  OR UPPER(BTRIM(x.name)) = UPPER(CONCAT_WS(%L, BTRIM(r.last_name),
                                                                           BTRIM(r.first_name)))))',
        cols, 'staff', ', ');

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
