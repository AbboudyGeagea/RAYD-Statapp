-- Migration 0139: the report editor is now called Report Composer.
--
-- Philips uses its earlier name (operator, 2026-11-04), so it must appear nowhere. Only
-- what people read changes: the tables, views, rpt_ role, licence key and page permission
-- keep their names. db_migrations.py carries over the record of the renamed 0136.
--
-- The one place the name was stored rather than written in code is the AI assistant's
-- help, on databases that ran 0136 before it was reworded; on a fresh install nothing here
-- matches. It goes from an operator's own wording too (Admin > AI Docs), and nothing else
-- in that wording changes. "worklist" stays a keyword, so someone asking for the editor by
-- its earlier name still lands here.

UPDATE ai_doc_chunks
   SET keywords   = 'report composer,scheduled email,'
                    || regexp_replace(COALESCE(keywords, ''), 'dynamic[ _-]?worklists?,?', '', 'gi'),
       updated_at = NOW()
 WHERE section ~* 'dynamic[ _-]?worklist' OR keywords ~* 'dynamic[ _-]?worklist';

UPDATE ai_doc_chunks
   SET section    = regexp_replace(section, 'dynamic[ _-]?worklists?', 'Report Composer', 'gi'),
       content    = replace(regexp_replace(content, 'dynamic[ _-]?worklists?', 'Report Composer', 'gi'),
                            'Admins can share worklists and write their SQL by hand.',
                            'Admins can share reports, write their SQL by hand, and email saved reports on a schedule.'),
       updated_at = NOW()
 WHERE section ~* 'dynamic[ _-]?worklist' OR content ~* 'dynamic[ _-]?worklist';
