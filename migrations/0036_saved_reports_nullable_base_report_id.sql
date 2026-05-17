-- Migration 0036: Make saved_reports.base_report_id nullable
-- Super Report saves use no backing report_template row; the NOT NULL constraint
-- caused a FK violation on every insert from the Super Report preset save endpoint.

ALTER TABLE public.saved_reports
    ALTER COLUMN base_report_id DROP NOT NULL;

-- Re-point the FK so orphaned rows survive report_template deletes (SET NULL instead of CASCADE)
ALTER TABLE public.saved_reports
    DROP CONSTRAINT IF EXISTS saved_reports_base_report_id_fkey;

ALTER TABLE public.saved_reports
    ADD CONSTRAINT saved_reports_base_report_id_fkey
        FOREIGN KEY (base_report_id)
        REFERENCES public.report_template(report_id)
        ON DELETE SET NULL;
