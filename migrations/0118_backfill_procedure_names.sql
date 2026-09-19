-- Migration 0118: backfill procedure_duration_map.procedure_name from the RIS order
-- text, so the reports can actually show a procedure DESCRIPTION instead of a code.
--
-- WHY THIS IS NEEDED
-- Every report that displays a procedure now renders
--     COALESCE(NULLIF(TRIM(pm.procedure_name), ''), <raw code>)
-- (report_22, report_23, report_25 via migration 0110, report_29, report_32, report_35,
-- super_report, capacity_ladder). That COALESCE silently degrades back to the raw code
-- for every row whose procedure_name is NULL — which is most of them on any install
-- where ETL_JOBS/etl_runner.py Phase 8 Step 3 ran first:
--
--     INSERT INTO procedure_duration_map (procedure_code, duration_minutes, ...)
--     SELECT DISTINCT TRIM(proc_id), 15, 1.0, 1.0 FROM etl_orders ...
--     ON CONFLICT (procedure_code) DO NOTHING
--
-- That inserts a placeholder row per distinct order code with procedure_name NULL, and
-- ETL_JOBS/etl_ris_procedures.py then could not fill it in (its own upsert was
-- ON CONFLICT DO NOTHING — fixed alongside this migration to fill a NULL name).
-- This migration does the same repair for rows already sitting in the table.
--
-- SOURCE OF THE NAME
-- etl_orders.proc_text, aggregated with MODE() WITHIN GROUP per code — the exact
-- derivation routes/mapping_controller.py already computes and shows on the Procedures
-- tab (`proc_name_map`), except that one is display-only and never persisted, so the
-- reports never see it. On a RIS-connected install ETL_JOBS/etl_ris_procedures.py is
-- the better source (SPS_CODE.DESCRIPTION); this fills the gap for codes the RIS catalog
-- does not cover and for installs with no RIS feed at all.
--
-- SAFETY
-- Fill-only and idempotent: touches ONLY rows where procedure_name IS NULL or blank, so
-- a name from the RIS catalog or a hand edit on the mapping tab is never overwritten.
-- Re-running it is a no-op. No column is added or dropped; no row is inserted or deleted.
--
-- MATERIALIZED per CLAUDE.md convention #4 — etl_orders scans with MODE() WITHIN GROUP
-- must not be inlined into the outer statement.

WITH order_names AS MATERIALIZED (
    SELECT UPPER(TRIM(proc_id))                                      AS code,
           MODE() WITHIN GROUP (ORDER BY TRIM(proc_text))            AS name
    FROM etl_orders
    WHERE proc_id   IS NOT NULL AND TRIM(proc_id)   != ''
      AND proc_text IS NOT NULL AND TRIM(proc_text) != ''
      -- SR is not a real study (CLAUDE.md convention #2); its order text is
      -- PACS-generated noise and must never become a procedure's display name.
      AND COALESCE(modality, '') != 'SR'
    GROUP BY UPPER(TRIM(proc_id))
)
UPDATE procedure_duration_map pdm
SET    procedure_name = o.name
FROM   order_names o
WHERE  o.code = UPPER(TRIM(pdm.procedure_code))
  AND  (pdm.procedure_name IS NULL OR TRIM(pdm.procedure_name) = '')
  AND  o.name IS NOT NULL
  AND  TRIM(o.name) != ''
  -- A name identical to the code is not a description — leaving it NULL keeps the
  -- mapping tab's "needs a human name" signal intact instead of hiding the gap.
  AND  UPPER(TRIM(o.name)) != UPPER(TRIM(pdm.procedure_code));
