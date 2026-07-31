-- Migration 0095: seed device_weekly_schedule so Report 25's device utilization
-- matrix stops showing 0% for every AE title.
--
-- Root cause (confirmed against production, 2026-07-31): device_weekly_schedule
-- had zero rows. It's a manually-configured table (populated today only via the
-- Mapping Config page's AE/modality CSV upload, routes/mapping_controller.py,
-- which syncs both aetitle_modality_map AND device_weekly_schedule together) --
-- not something ETL'd automatically. With the table empty, every AE/weekday's
-- opening-minutes lookup falls through to 0, total_cap is 0 for every device,
-- and routes/report_25.py's utilization formula
--   util = round((day_load / total_cap) * 100, 1) if total_cap > 0 else 0
-- takes the else-0 branch unconditionally, regardless of how much real PPS
-- activity (std_pps) actually happened -- confirmed 574,382 std_pps rows exist
-- with real start/end times, so this was never a data problem on the load side.
--
-- Seeds one row per (aetitle, day 0-6) for every AE title already present in
-- aetitle_modality_map (satisfies device_weekly_schedule's FK by construction),
-- using that AE's own daily_capacity_minutes if set, else the column's own
-- schema default of 720 (12h). ON CONFLICT DO NOTHING makes this safe to run
-- even after someone has already set real per-device values via the Mapping
-- Config CSV upload -- it will never overwrite a value that's already there.
INSERT INTO device_weekly_schedule (aetitle, day_of_week, std_opening_minutes)
SELECT DISTINCT m.aetitle, d.day_of_week, COALESCE(m.daily_capacity_minutes, 720)
FROM aetitle_modality_map m
CROSS JOIN generate_series(0, 6) AS d(day_of_week)
ON CONFLICT (aetitle, day_of_week) DO NOTHING;
