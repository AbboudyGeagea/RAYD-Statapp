-- Migration 0153: the site a database serves (meeting of 2026-11-13, item 13a).
-- *** LAUMC BRANCH ONLY ***
--
-- SJH gets its own database (utils/site_db.py), carrying the same schema and the same
-- reports as RH's. The reports keep "the site's own devices and studies" with conditions
-- that used to name the default site (SELECT id FROM sites WHERE is_default), which is RH,
-- so in SJH's database they would keep nothing. Each database serves one site: RH's the
-- default site, SJH's the site named in its settings ('served_site', written by
-- utils/site_db.py when it sets that database up). Queries now ask this function, and the
-- Python side asks utils.site_db.served_site_id(), which is the same site.
--
-- On RH's database nothing changes: no 'served_site' row, so the default site, as before.

CREATE OR REPLACE FUNCTION rayd_served_site_id() RETURNS INTEGER
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT s.id FROM public.sites s
         JOIN public.settings st ON st.key = 'served_site'
                                AND UPPER(BTRIM(st.value)) = UPPER(s.code)
         LIMIT 1),
        (SELECT id FROM public.sites WHERE is_default ORDER BY id LIMIT 1))
$$;
