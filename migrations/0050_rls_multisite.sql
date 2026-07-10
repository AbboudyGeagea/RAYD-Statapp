-- Migration 0050: Row-Level Security for multi-site isolation
-- *** LAUMC BRANCH ONLY — do NOT merge to main / single-site branches ***
--
-- Model:
--   * Web requests connect as role `rayd_app` (a NON-owner login role) → subject to RLS.
--   * Background jobs (ETL, NLP worker, MLLP listener, APScheduler) keep connecting as the
--     table owner `etl_user`, which BYPASSES RLS by design — they must see all sites to write
--     rows before site is resolved and to compute cross-site rollups. (RLS is NOT forced, so
--     the owner bypasses automatically.)
--
-- Per request the app sets the allowed-site scope (canonical site ids from `sites`,
-- = the user's granted sites ∩ their site-picker selection):
--       SELECT set_config('rayd.site_scope', '1,2', true);   -- true = transaction-local
--
-- Policies filter site_id to that set. If the GUC is unset, current_setting(...,true) returns
-- NULL → the predicate is NULL → zero rows. FAIL-CLOSED: a bug shows an empty dashboard, never
-- another site's data. Rows with NULL site_id (e.g. ORU not yet enriched) are also invisible to
-- rayd_app until enriched — visible to background/owner. Intended behaviour.

-- ---------------------------------------------------------------------------
-- 1. Application role (idempotent). Password is set at deploy time, not here.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rayd_app') THEN
        CREATE ROLE rayd_app LOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO rayd_app;
-- The app reads everything and writes its own operational tables. RLS (below) provides the
-- site isolation; these grants just let the role reach the tables at all.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA public TO rayd_app;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA public TO rayd_app;
-- Future tables created by the owner inherit the same grants.
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rayd_app;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO rayd_app;

-- ---------------------------------------------------------------------------
-- 2. Enable RLS + attach the site-scope policy to each site-scoped table.
--    NOTE RLS enabled but NOT forced -> owner etl_user bypasses (ETL/NLP/listener OK).
--    Policy applies only TO rayd_app.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    t text;
    scoped_tables text[] := ARRAY[
        'etl_didb_studies',
        'etl_orders',
        'hl7_orders',
        'hl7_oru_reports',
        'worklist_status_history',
        'analytics_snapshots'
    ];
BEGIN
    FOREACH t IN ARRAY scoped_tables LOOP
        IF EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
            EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', t || '_site_scope', t);
            EXECUTE format($p$
                CREATE POLICY %I ON public.%I
                    FOR ALL TO rayd_app
                    USING (
                        site_id = ANY (
                            string_to_array(
                                current_setting('rayd.site_scope', true), ','
                            )::int[]
                        )
                    )
            $p$, t || '_site_scope', t);
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 3. hl7_oru_analysis is joined via report_id and inherits the reports policy in
--    practice, but a DIRECT query would bypass isolation. Guard it too, scoping
--    through its parent report's site.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema='public' AND table_name='hl7_oru_analysis') THEN
        ALTER TABLE public.hl7_oru_analysis ENABLE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS hl7_oru_analysis_site_scope ON public.hl7_oru_analysis;
        CREATE POLICY hl7_oru_analysis_site_scope ON public.hl7_oru_analysis
            FOR ALL TO rayd_app
            USING (
                EXISTS (
                    SELECT 1 FROM public.hl7_oru_reports r
                    WHERE r.id = hl7_oru_analysis.report_id
                      AND r.site_id = ANY (
                          string_to_array(current_setting('rayd.site_scope', true), ',')::int[]
                      )
                )
            );
    END IF;
END $$;
