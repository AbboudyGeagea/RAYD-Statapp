-- Migration 0056: fix storing_ae references in report_template.report_sql_query
-- Migration 0055 targeted the wrong column name (base_sql vs report_sql_query).
-- This corrects all stored report SQL to use the renamed column.

UPDATE report_template
SET report_sql_query = REPLACE(report_sql_query, 'storing_ae', 'original_storing_ae')
WHERE report_sql_query LIKE '%%storing_ae%%';
