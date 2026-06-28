-- Migration 0059: repair report_template SQL corrupted by repeated storing_ae replacements.
--
-- Root cause: update.sh tracked applied migrations with column 'filename' but the
-- schema_migrations table (created by Flask on startup) uses column 'name'.
-- Every INSERT INTO schema_migrations (filename) silently failed, so migration 0056
-- was re-applied on every update.sh run. Each run's REPLACE deepened the nesting:
--   storing_ae → original_storing_ae → original_original_storing_ae → … (5 times)
--
-- REGEXP_REPLACE with pattern (original_)+storing_ae collapses any depth back to
-- exactly one original_ prefix in a single idempotent pass.

UPDATE report_template
SET report_sql_query = REGEXP_REPLACE(
    report_sql_query,
    '(original_)+storing_ae',
    'original_storing_ae',
    'g'
)
WHERE report_sql_query ~ '(original_)+storing_ae';

UPDATE report_template
SET base_sql = REGEXP_REPLACE(
    base_sql,
    '(original_)+storing_ae',
    'original_storing_ae',
    'g'
)
WHERE base_sql ~ '(original_)+storing_ae';
