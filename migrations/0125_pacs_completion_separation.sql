-- Migration 0125: separate the RIS "exam done" from the PACS "images stored".
--
-- THE PROBLEM. Both events arrive as ORM^O01 with ORC-1=SC and ORC-5=CM. They are
-- byte-identical in shape and mean completely different things:
--
--     RIS  CM  -> the exam finished at the device
--     PACS CM  -> the images landed in the PACS
--
-- Because the listener matched on shape alone, a RIS completion was ALSO treated
-- as a PACS study-complete: it fired the auto-done update and wrote a row into
-- hl7_scn_studies. Caught on 2026-09-18 by inspecting what the test stream had
-- actually produced, not by any test.
--
-- The only thing that distinguishes them is the SENDER, so the sender is what the
-- listener now keys on. settings.hl7_pacs_sending_app names the PACS application
-- (MSH-3); anything else carrying CM is a lifecycle event and nothing more.
--
-- Left EMPTY by default, which means the PACS-completion path stays off until a
-- site configures it. That is the safe default: an unconfigured install records
-- lifecycle completions correctly and simply has no PACS timestamp, whereas the
-- old behaviour silently attributed RIS completions to the PACS.
--
--
-- WHY BOTH ARE KEPT RATHER THAN ONE CHOSEN (operator decision, 2026-09-18:
-- "we can calculate both, RIS CM status, and PACS insert time (From SCN)")
--
-- The gap between them is itself a measurement. RIS-completed to PACS-stored is
-- image transfer and processing lag, and a widening gap is a real operational
-- signal — the same comparison the Oracle branches made between
-- etl_didb_studies.insert_time and the RIS exam-done timestamp, which had its own
-- per-modality threshold setting. Collapsing the two into one column would throw
-- that away permanently, and neither is a substitute for the other:
--
--     completed_at      feeds turnaround time. Clinical.
--     pacs_completed_at feeds the transfer-lag check. Operational.

ALTER TABLE ray7_study_state
    ADD COLUMN IF NOT EXISTS pacs_completed_at TIMESTAMP;

-- Finding studies where the images never followed the exam.
CREATE INDEX IF NOT EXISTS idx_ray7_state_pacs_pending
    ON ray7_study_state (completed_at)
    WHERE pacs_completed_at IS NULL AND completed_at IS NOT NULL;

-- MSH-3 of the PACS. Empty means "no PACS completion feed configured", and the
-- path stays inert rather than guessing from message shape.
INSERT INTO settings (key, value)
VALUES ('hl7_pacs_sending_app', '')
ON CONFLICT (key) DO NOTHING;
