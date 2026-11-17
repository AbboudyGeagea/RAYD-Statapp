-- Migration 0157: staff who left read "Former staff" (LAUMC to-do item 9).
-- *** LAUMC BRANCH ONLY ***
--
-- The customer (meeting of 2026-11-13): the manager sets leavers inactive on the RIS and PACS
-- herself; then, automatically, a person inactive on the RIS and/or PACS no longer has KPIs
-- shown (technician, radiologist, ...), in any period, while department totals still count
-- their studies. Having no PACS account does not count as inactive. Nothing is deleted.
--
-- What the systems hold (server, 2026-11-17):
--   RIS   std_resources_ris.active is kept (radiologists 52 inactive of 102 rows, technologists
--         50 of 136). One person has several rows (one per role, old and new resource IDs):
--         32 of the 52 inactive radiologist rows belong to someone with an active row. So a
--         person is judged on all their rows, and two RIS person records sharing one login
--         (abdallah.noufaily has two) are judged together.
--   PACS  MEDILINK.SECM_USERS.STATUS: every staff account is ACTIVE today (only patient-portal
--         accounts are SUSPENDED or TIMEOUT). IS_EXPIRED is the password, LAST_LOGIN is unused
--         (staff sign in through the hospital directory). The nightly copy now brings STATUS and
--         IN_STATE_SINCE (ETL_JOBS/etl_pacs_user_groups.py).
--   Link  The RIS resource ID holds the PACS login (esther.tannoury@ad.umcrh.com_1316529329 is
--         PACS esther.tannoury); else the e-mail address, as on the PACS account.
--
-- Who left (public.staff_status, rebuilt after every nightly ETL, utils/staff_status.py):
--   - a RIS person (anyone with a role other than referring doctor, DOC) left when none of their
--     RIS rows is active and no active person shares their login, or when they have PACS
--     accounts and none of them is ACTIVE;
--   - a PACS login linked to no RIS person left when none of its accounts is ACTIVE.
--
-- Where it shows: the rh views of migrations 0145/0151, which every page reads (and Report
-- Composer through rpt.*), replace a leaver's login and name with 'Former staff' on studies
-- and RIS staff rows. Every per-person list then shows one "Former staff" line instead of the
-- people, and every count, total and timestamp stays as it was: a study still counts as
-- signed. The ETL, the listener, the NLP worker and CRN read the real tables. Referring
-- doctors are not touched.
--
-- Off until switched on (settings.staff_left_hidden = 'true', on Report 39 or by SQL), so the
-- list is checked on production first. Report 39 (Staff status) lists everyone either way.

-- 1. PACS account status, from the nightly copy
ALTER TABLE public.std_pacs_user_groups
    ADD COLUMN IF NOT EXISTS user_status  TEXT,        -- SECM_USERS.STATUS (ACTIVE, SUSPENDED, ...)
    ADD COLUMN IF NOT EXISTS status_since TIMESTAMP;   -- SECM_USERS.IN_STATE_SINCE

-- 2. The switch
INSERT INTO public.settings (key, value) VALUES ('staff_left_hidden', 'false')
ON CONFLICT (key) DO NOTHING;

-- 3. Everyone in the RIS and PACS, with whether they left. One row per RIS person, and one per
--    PACS login that no RIS person holds. Names are staff names (std_resources_ris carries
--    them by design, migration 0063); no patient data.
DROP VIEW IF EXISTS public.staff_left_keys;
DROP MATERIALIZED VIEW IF EXISTS public.staff_status;

CREATE MATERIALIZED VIEW public.staff_status AS
WITH ris AS (
    SELECT r.resource_id_key,
           COALESCE(r.person_key, -r.resource_id_key) AS person_key,
           NULLIF(BTRIM(r.role_code), '') AS role_code,
           COALESCE(r.active, TRUE) AS active,
           CASE WHEN r.resource_id LIKE '%@%'
                THEN NULLIF(LOWER(BTRIM(SPLIT_PART(BTRIM(r.resource_id), '@', 1))), '') END AS login,
           NULLIF(LOWER(BTRIM(r.primary_email_address)), '') AS mail1,
           NULLIF(LOWER(BTRIM(r.secondary_email_address)), '') AS mail2,
           NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(r.first_name), ''), NULLIF(BTRIM(r.last_name), ''))), '') AS full_name,
           NULLIF(BTRIM(REGEXP_REPLACE(LOWER(CONCAT_WS(' ', r.first_name, r.last_name)), '[[:space:],^]+', ' ', 'g')), '') AS name_fl,
           NULLIF(BTRIM(REGEXP_REPLACE(LOWER(CONCAT_WS(' ', r.last_name, r.first_name)), '[[:space:],^]+', ' ', 'g')), '') AS name_lf,
           r.source_last_updated
    FROM public.std_resources_ris r
    WHERE NOT EXISTS (SELECT 1 FROM public.brayn_placeholders x      -- service accounts (0151)
                      WHERE x.kind = 'staff' AND x.active
                        AND (BTRIM(x.code) IN (r.resource_id_key::text, r.person_key::text, r.resource_id)
                             OR UPPER(BTRIM(x.name)) = UPPER(CONCAT_WS(', ', BTRIM(r.last_name), BTRIM(r.first_name)))))
),
person AS (
    SELECT person_key,
           bool_or(active) AS ris_active,
           array_agg(DISTINCT resource_id_key ORDER BY resource_id_key) AS resource_keys,
           string_agg(DISTINCT role_code, ', ' ORDER BY role_code) AS ris_roles,
           (array_agg(full_name ORDER BY active DESC, source_last_updated DESC NULLS LAST)
                FILTER (WHERE full_name IS NOT NULL))[1] AS name,
           array_remove(array_agg(DISTINCT name_fl) || array_agg(DISTINCT name_lf), NULL) AS names
    FROM ris
    GROUP BY person_key
    HAVING bool_or(COALESCE(role_code, '') <> 'DOC')
),
pacs AS (
    SELECT LOWER(BTRIM(SPLIT_PART(BTRIM(g.login_id), '@', 1))) AS login,
           MIN(NULLIF(LOWER(BTRIM(g.email)), '')) AS mail,
           bool_or(COALESCE(UPPER(BTRIM(g.user_status)), 'ACTIVE') = 'ACTIVE') AS pacs_active,
           string_agg(DISTINCT UPPER(BTRIM(g.user_status)), ', ') AS pacs_status,
           MAX(g.status_since) AS pacs_status_since,
           string_agg(DISTINCT BTRIM(g.group_name), ', ' ORDER BY BTRIM(g.group_name)) AS pacs_groups
    FROM public.std_pacs_user_groups g
    WHERE LOWER(BTRIM(COALESCE(g.group_name, ''))) NOT IN ('web_guests', 'web_patients')   -- patient portal
      AND NULLIF(BTRIM(g.login_id), '') IS NOT NULL
    GROUP BY 1
),
link AS (
    SELECT r.person_key, r.login, 'RIS ID' AS how
    FROM ris r JOIN person p USING (person_key)
    WHERE r.login IS NOT NULL
    UNION
    SELECT r.person_key, a.login, 'e-mail'
    FROM ris r JOIN person p USING (person_key)
    JOIN pacs a ON a.mail IN (r.mail1, r.mail2)
),
login_ris AS (          -- is any RIS person holding this login active?
    SELECT l.login, bool_or(p.ris_active) AS ris_active
    FROM link l JOIN person p USING (person_key)
    GROUP BY l.login
),
per_person AS (
    SELECT p.person_key,
           array_agg(DISTINCT l.login) FILTER (WHERE l.login IS NOT NULL) AS logins,
           string_agg(DISTINCT l.how, ', ') AS linked_by,
           bool_and(lr.ris_active IS FALSE) FILTER (WHERE l.login IS NOT NULL) AS logins_ris_inactive,
           bool_or(a.pacs_active) AS pacs_active,
           string_agg(DISTINCT a.pacs_groups, ', ') AS pacs_groups,
           string_agg(DISTINCT a.pacs_status, ', ') AS pacs_status,
           MAX(a.pacs_status_since) AS pacs_status_since
    FROM person p
    LEFT JOIN link l ON l.person_key = p.person_key
    LEFT JOIN login_ris lr ON lr.login = l.login
    LEFT JOIN pacs a ON a.login = l.login
    GROUP BY p.person_key
),
people AS (
    SELECT p.person_key, p.name, p.ris_roles, p.ris_active, p.resource_keys, p.names,
           pp.logins, pp.linked_by, pp.pacs_groups, pp.pacs_status, pp.pacs_status_since, pp.pacs_active,
           (NOT p.ris_active AND COALESCE(pp.logins_ris_inactive, TRUE)) AS ris_left,
           (pp.pacs_active IS FALSE) AS pacs_left
    FROM person p JOIN per_person pp USING (person_key)
)
SELECT 'ris:' || person_key AS staff_id, person_key, name, ris_roles, ris_active, resource_keys,
       COALESCE(logins, '{}') AS logins, names, linked_by,
       pacs_groups, pacs_status, pacs_status_since, pacs_active,
       (ris_left OR pacs_left) AS is_left,
       CASE WHEN ris_left AND pacs_left THEN 'Inactive on the RIS and PACS'
            WHEN ris_left THEN 'Inactive on the RIS'
            WHEN pacs_left THEN 'Not active on PACS' END AS left_reason
FROM people
UNION ALL
SELECT 'pacs:' || a.login, NULL, NULL, NULL, NULL, '{}', ARRAY[a.login], '{}', NULL,
       a.pacs_groups, a.pacs_status, a.pacs_status_since, a.pacs_active,
       (a.pacs_active IS FALSE),
       CASE WHEN a.pacs_active IS FALSE THEN 'Not active on PACS' END
FROM pacs a
WHERE NOT EXISTS (SELECT 1 FROM link l WHERE l.login = a.login);

CREATE UNIQUE INDEX uq_staff_status_id ON public.staff_status (staff_id);

-- 4. What the rh views replace, while the switch is on: a login, a name or a RIS resource key
--    of someone who left, never one that a current person also holds. A name needs two words.
CREATE VIEW public.staff_left_keys AS
SELECT k.kind, k.key
FROM (
    SELECT 'login' AS kind, l AS key, st.is_left FROM public.staff_status st, unnest(st.logins) l
    UNION ALL
    SELECT 'name', n, st.is_left FROM public.staff_status st, unnest(st.names) n WHERE POSITION(' ' IN n) > 0
    UNION ALL
    SELECT 'resource', rk::text, st.is_left FROM public.staff_status st, unnest(st.resource_keys) rk
) k
WHERE EXISTS (SELECT 1 FROM public.settings z WHERE z.key = 'staff_left_hidden' AND LOWER(BTRIM(z.value)) = 'true')
GROUP BY k.kind, k.key
HAVING bool_and(k.is_left);

-- 5. Migration 0151's function, with leavers replaced. Each set is small and worked out once
--    per query (a hashed subplan).
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

-- 6. Report 39, Staff status (routes/report_39.py): everyone in the RIS and PACS, grouped by
--    PACS group and RIS role, for the manager. A report like every other (CLAUDE.md rule 8):
--    the catalogue row, the licence entry; access per user and site (rule 9, nothing granted
--    here: Super user, CHT and Administrator open every report).
INSERT INTO report_template (report_id, report_name, long_description, report_sql_query,
                             required_parameters, visualization_type, is_base)
VALUES (
    39,
    'Staff Status (RIS and PACS)',
    'Everyone on the RIS and PACS (no RAYD accounts), grouped by PACS group and RIS role: active or inactive on each, how the RIS person and PACS login are linked, and their signed studies and performed exams in the period. Whoever is inactive on the RIS and/or PACS reads "Former staff" on every report once the switch is on; department totals keep their studies. Excel download for the manager.',
    NULL,   -- routes/report_39.py and utils/staff_status.py run their own queries
    'start_date,end_date',
    'table',
    TRUE
)
ON CONFLICT (report_id) DO UPDATE
    SET report_name      = EXCLUDED.report_name,
        long_description = EXCLUDED.long_description,
        is_base          = TRUE;

UPDATE settings
SET value = jsonb_set(
    value::jsonb,
    '{reports}',
    (
        SELECT jsonb_agg(DISTINCT elem::int ORDER BY elem::int)
        FROM jsonb_array_elements(COALESCE(value::jsonb->'reports', '[]'::jsonb) || '39'::jsonb) AS elem
    )
)::text
WHERE key = 'license'
  AND value::jsonb->'reports' IS NOT NULL
  AND NOT (value::jsonb->'reports' @> '39'::jsonb);
