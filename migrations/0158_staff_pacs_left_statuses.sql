-- Migration 0158: a PACS account locked after wrong passwords is not someone who left (item 9).
-- *** LAUMC BRANCH ONLY ***
--
-- Operator, 2026-11-17: a leaver is set inactive on the RIS and on PACS by hand; PACS also locks
-- an account by itself after several wrong passwords. Migration 0157 counted every PACS status
-- other than ACTIVE as "left", so a radiologist who mistyped a password would read "Former
-- staff" the next morning. Now only the statuses listed in settings.staff_pacs_left_statuses
-- count (default SUSPENDED, DISABLED, INACTIVE, DELETED); ACTIVE, a lock, TIMEOUT or any other
-- value does not. The RIS still catches every leaver, since both systems are set inactive.
-- The list is read each time staff_status is rebuilt, so it changes without a migration.
--
-- staff_status is rebuilt under a new name and swapped in, so staff_left_keys, and the rh views
-- that read it, never have to be dropped.

INSERT INTO public.settings (key, value) VALUES ('staff_pacs_left_statuses', 'SUSPENDED,DISABLED,INACTIVE,DELETED')
ON CONFLICT (key) DO NOTHING;

DROP MATERIALIZED VIEW IF EXISTS public.staff_status_new;

CREATE MATERIALIZED VIEW public.staff_status_new AS
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
left_statuses AS (      -- the PACS statuses a leaver is set to by hand (0158)
    SELECT string_to_array(UPPER(REGEXP_REPLACE(COALESCE(
               (SELECT z.value FROM public.settings z WHERE z.key = 'staff_pacs_left_statuses'),
               'SUSPENDED,DISABLED,INACTIVE,DELETED'), '[[:space:]]', '', 'g')), ',') AS list
),
pacs AS (
    SELECT LOWER(BTRIM(SPLIT_PART(BTRIM(g.login_id), '@', 1))) AS login,
           MIN(NULLIF(LOWER(BTRIM(g.email)), '')) AS mail,
           bool_or(NOT (COALESCE(UPPER(BTRIM(g.user_status)), 'ACTIVE') = ANY (ls.list))) AS pacs_active,
           string_agg(DISTINCT UPPER(BTRIM(g.user_status)), ', ') AS pacs_status,
           MAX(g.status_since) AS pacs_status_since,
           string_agg(DISTINCT BTRIM(g.group_name), ', ' ORDER BY BTRIM(g.group_name)) AS pacs_groups
    FROM public.std_pacs_user_groups g
    CROSS JOIN left_statuses ls
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
login_ris AS (
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

CREATE UNIQUE INDEX uq_staff_status_new_id ON public.staff_status_new (staff_id);

-- staff_left_keys reads the new list (same columns, so the rh views keep working) ...
CREATE OR REPLACE VIEW public.staff_left_keys AS
SELECT k.kind, k.key
FROM (
    SELECT 'login' AS kind, l AS key, st.is_left FROM public.staff_status_new st, unnest(st.logins) l
    UNION ALL
    SELECT 'name', n, st.is_left FROM public.staff_status_new st, unnest(st.names) n WHERE POSITION(' ' IN n) > 0
    UNION ALL
    SELECT 'resource', rk::text, st.is_left FROM public.staff_status_new st, unnest(st.resource_keys) rk
) k
WHERE EXISTS (SELECT 1 FROM public.settings z WHERE z.key = 'staff_left_hidden' AND LOWER(BTRIM(z.value)) = 'true')
GROUP BY k.kind, k.key
HAVING bool_and(k.is_left);

-- ... then the old one goes and the new one takes its name (views follow the object, not the name)
DROP MATERIALIZED VIEW public.staff_status;
ALTER MATERIALIZED VIEW public.staff_status_new RENAME TO staff_status;
ALTER INDEX public.uq_staff_status_new_id RENAME TO uq_staff_status_id;
