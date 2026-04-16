-- Migration: 0003_set_go_live_date_june_2025
-- Update ETL cutoff from 2000-01-01 to 2025-06-01.

UPDATE go_live_config SET go_live_date = '2025-06-01';
