-- Migration 0138: index the archive by sender and arrival time.
--
-- The rebuilt BRAYN console's Feed status shows, per sender, when its newest
-- message arrived, however long ago; the Messages screen filters by sender. Both
-- are MAX / ORDER BY received_at within one sending_app, which without this index
-- is a scan of the whole archive — the largest table on an install, and the only
-- copy of every message ever received, so it is never pruned.
CREATE INDEX IF NOT EXISTS idx_hl7_archive_sender_received
    ON hl7_message_archive (sending_app, received_at DESC);
