-- Drop the (accession_number, acknowledged_by_id) unique key added by 0043.
--
-- 0043 assumed acknowledged_by_id was the technician. It is the user who
-- clicked Ack. Acknowledgements are keyed by (accession_number, flag_date),
-- the 0019 constraint that /api/tech/flag/acknowledge upserts on. The extra
-- key made a second acknowledgement of the same accession on another flag
-- date by the same user fail with HTTP 500 ("Failed to acknowledge").
--
-- Dropping a constraint loses no data. To restore it:
--   ALTER TABLE tech_flag_acknowledgements
--       ADD CONSTRAINT uq_tech_flag_ack_study_tech UNIQUE (accession_number, acknowledged_by_id);

ALTER TABLE tech_flag_acknowledgements DROP CONSTRAINT IF EXISTS uq_tech_flag_ack_study_tech;
