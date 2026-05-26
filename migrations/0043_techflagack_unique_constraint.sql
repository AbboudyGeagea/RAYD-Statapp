-- Prevent duplicate tech flag acknowledgements for the same study/technician pair.
--
-- Schema note (from 0019_tech_flag_acknowledgements.sql):
--   accession_number     TEXT NOT NULL
--   acknowledged_by_id   INT REFERENCES users(id)   -- the technician's user id
--
-- The column requested as "tech_user_id" in the task spec does not exist;
-- the actual FK column is acknowledged_by_id.
-- A UNIQUE constraint on (accession_number, flag_date) already exists from
-- migration 0019; this adds the per-technician uniqueness guard.

ALTER TABLE tech_flag_acknowledgements
    ADD CONSTRAINT uq_tech_flag_ack_study_tech
    UNIQUE (accession_number, acknowledged_by_id);
