-- Migration 0112: allow unrestricted backfill runs for ORU NLP clustering jobs.
--
-- oru_nlp_jobs.days was NOT NULL, forcing every clustering run (including the
-- on-demand "Process Reports" button) to only look at reports from the last N
-- days (capped at 365 in routes/oru_analytics.py). On an install with years of
-- historical HL7 ORU backlog this makes most of ai_nlp_cache's pending count
-- permanently unreachable -- no amount of clicking "Process Reports" would ever
-- touch it, since rows outside the window are excluded from the query entirely.
--
-- days = NULL now means "no date filter" (backfill mode). The worker
-- (nlp_worker/worker.py) auto-chains backfill jobs via next_job_id until the
-- backlog is exhausted, so a single "Backfill All" click clears the whole
-- history without the user re-clicking hundreds of times.

ALTER TABLE oru_nlp_jobs ALTER COLUMN days DROP NOT NULL;
ALTER TABLE oru_nlp_jobs ADD COLUMN IF NOT EXISTS next_job_id INTEGER REFERENCES oru_nlp_jobs(id);
