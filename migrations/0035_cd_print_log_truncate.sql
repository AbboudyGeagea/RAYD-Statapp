-- Full reset of cd_print_log so ETL re-populates from scratch with correct timestamps.
TRUNCATE TABLE cd_print_log RESTART IDENTITY;
