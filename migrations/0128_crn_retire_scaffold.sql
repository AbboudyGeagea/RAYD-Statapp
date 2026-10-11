-- Retire the July CRN scaffold (migrations 0077/0078/0101) to make room for the CRN
-- built and run at Mazloum (migrations 0129-0132).
--
-- The scaffold never sent anything (settings.crn_enabled stayed 'false', dry run
-- only). Its crn_notifications table has a different layout from the new one, so it
-- is renamed, not dropped: crn_legacy_notifications and
-- crn_legacy_notification_attempts keep whatever dry-run rows it wrote. Renaming a
-- table does not rename its indexes or sequences, so those are renamed too, or the
-- new crn_notifications would collide with "crn_notifications_pkey".
--
-- Only runs when crn_notifications still has the scaffold's layout (ack_token), so
-- a fresh install, or a re-run, is a no-op. referring_contacts is left in place
-- (nothing reads it any more); the new contact list is crn_contacts (0130).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'crn_notifications' AND column_name = 'ack_token') THEN
        ALTER TABLE IF EXISTS crn_notification_attempts RENAME TO crn_legacy_notification_attempts;
        ALTER INDEX IF EXISTS crn_notification_attempts_pkey RENAME TO crn_legacy_notification_attempts_pkey;
        ALTER INDEX IF EXISTS idx_crn_notification_attempts_notification RENAME TO idx_crn_legacy_notification_attempts_notification;
        ALTER SEQUENCE IF EXISTS crn_notification_attempts_id_seq RENAME TO crn_legacy_notification_attempts_id_seq;

        ALTER TABLE crn_notifications RENAME TO crn_legacy_notifications;
        ALTER INDEX IF EXISTS crn_notifications_pkey RENAME TO crn_legacy_notifications_pkey;
        ALTER INDEX IF EXISTS crn_notifications_report_id_key RENAME TO crn_legacy_notifications_report_id_key;
        ALTER INDEX IF EXISTS crn_notifications_ack_token_key RENAME TO crn_legacy_notifications_ack_token_key;
        ALTER INDEX IF EXISTS idx_crn_notifications_pending_escalation RENAME TO idx_crn_legacy_notifications_pending_escalation;
        ALTER SEQUENCE IF EXISTS crn_notifications_id_seq RENAME TO crn_legacy_notifications_id_seq;
    END IF;
END $$;

DELETE FROM settings WHERE key IN ('crn_enabled', 'crn_message_template');
