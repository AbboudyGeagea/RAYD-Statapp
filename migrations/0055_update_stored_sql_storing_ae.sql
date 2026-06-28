-- Migration 0055: update stored SQL in report_template to use original_storing_ae
-- report_template rows contain raw SQL strings written before the
-- storing_ae → original_storing_ae column rename (migration 0052).

UPDATE report_template
SET base_sql = REPLACE(base_sql, 'storing_ae', 'original_storing_ae')
WHERE base_sql LIKE '%%storing_ae%%';
