-- Migration 0137: RAY7 is renamed BRAYN (operator decision, 2026-10-25).
--
-- A FULL rename, not a display name: tables, columns, constraints, indexes, the
-- findings sequence and the two rule codes that carried the old name. Earlier
-- migrations keep the old names — they are history and have already run — so on a
-- fresh install 0119..0136 create ray7_* and this file renames them.
--
-- Renames are catalogue-only: no table is rewritten and no row moves. The rpt.*
-- worklist views and the rpt_<db> role's grants follow by OID, not by name.
--
-- Found by name rather than listed, so nothing named ray7 is missed and a second
-- run finds nothing to do.

DO $$
DECLARE r record;
BEGIN
    -- 1. Tables.
    FOR r IN SELECT c.relname
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE '%ray7%'
    LOOP
        EXECUTE format('ALTER TABLE public.%I RENAME TO %I',
                       r.relname, replace(r.relname, 'ray7', 'brayn'));
    END LOOP;

    -- 2. Columns of base tables (hl7_message_archive.ray7_status, _severity,
    --    _screened_at).
    FOR r IN SELECT c.relname, a.attname
               FROM pg_attribute a
               JOIN pg_class c ON c.oid = a.attrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'public' AND c.relkind = 'r'
                AND a.attnum > 0 AND NOT a.attisdropped AND a.attname LIKE '%ray7%'
    LOOP
        EXECUTE format('ALTER TABLE public.%I RENAME COLUMN %I TO %I',
                       r.relname, r.attname, replace(r.attname, 'ray7', 'brayn'));
    END LOOP;

    -- 3. Constraints. Renaming a primary-key or unique constraint renames its
    --    index with it, so this runs before the index pass.
    FOR r IN SELECT con.conname, cl.relname
               FROM pg_constraint con
               JOIN pg_class cl ON cl.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = cl.relnamespace
              WHERE n.nspname = 'public' AND con.conname LIKE '%ray7%'
    LOOP
        EXECUTE format('ALTER TABLE public.%I RENAME CONSTRAINT %I TO %I',
                       r.relname, r.conname, replace(r.conname, 'ray7', 'brayn'));
    END LOOP;

    -- 4. The remaining indexes, and the findings id sequence.
    FOR r IN SELECT c.relname, c.relkind
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'public' AND c.relkind IN ('i', 'S')
                AND c.relname LIKE '%ray7%'
    LOOP
        IF r.relkind = 'i' THEN
            EXECUTE format('ALTER INDEX public.%I RENAME TO %I',
                           r.relname, replace(r.relname, 'ray7', 'brayn'));
        ELSE
            EXECUTE format('ALTER SEQUENCE public.%I RENAME TO %I',
                           r.relname, replace(r.relname, 'ray7', 'brayn'));
        END IF;
    END LOOP;
END $$;


-- 5. The two rule codes named after the engine, everywhere they are stored:
--    the rule rows, the findings raised under them, and the per-sender counters.
UPDATE brayn_rules
   SET rule_code = replace(rule_code, 'RAY7_', 'BRAYN_'), updated_at = NOW()
 WHERE rule_code LIKE 'RAY7\_%';

UPDATE brayn_rules
   SET title = replace(title, 'RAY7', 'BRAYN'),
       description = replace(description, 'RAY7', 'BRAYN'),
       updated_at = NOW()
 WHERE title LIKE '%RAY7%' OR description LIKE '%RAY7%';

UPDATE brayn_findings
   SET rule_code = replace(rule_code, 'RAY7_', 'BRAYN_')
 WHERE rule_code LIKE 'RAY7\_%';

UPDATE brayn_interface_stats
   SET metric = replace(metric, 'RAY7_', 'BRAYN_')
 WHERE metric LIKE 'RAY7\_%';
