-- Migration 0098: index std_worklist_arrivals.arrived_at.
--
-- Migrations 0090 and 0092 gave std_worklist_exam_done and std_worklist_scheduled an
-- index on their own timestamp column (idx_worklist_exam_done_exam_done_at,
-- idx_worklist_scheduled_scheduled_at), but std_worklist_arrivals (migration 0088) only
-- ever got idx_worklist_arrivals_pps_key -- an oversight, not intentional. Migration
-- 0089's uq_worklist_arrivals_site_time UNIQUE (site_worklist_key, arrived_at) creates
-- an index too, but arrived_at is the SECOND column of that composite -- useless for a
-- range scan on arrived_at alone without an equality condition on site_worklist_key
-- first.
--
-- Report 35 (Technician TAT, routes/report_35.py) filters std_worklist_arrivals by
-- arrived_at range on every request (its base CTE, the report's date-range filter) --
-- without this index that's a full sequential scan of the whole table every single page
-- load, same class of problem as the other two tables already fixed.

CREATE INDEX IF NOT EXISTS idx_worklist_arrivals_arrived_at
    ON std_worklist_arrivals (arrived_at);
