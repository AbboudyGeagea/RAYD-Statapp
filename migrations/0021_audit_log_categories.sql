-- 0021_audit_log_categories.sql
-- Adds event_category and resource_type to user_audit_log for system usage tracking.

ALTER TABLE user_audit_log
    ADD COLUMN IF NOT EXISTS event_category VARCHAR(20),
    ADD COLUMN IF NOT EXISTS resource_type  VARCHAR(50);

-- Back-fill category from action for existing rows
UPDATE user_audit_log SET event_category = 'auth'      WHERE action IN ('login','logout','registered','password_changed') AND event_category IS NULL;
UPDATE user_audit_log SET event_category = 'user_mgmt' WHERE event_category IS NULL;

CREATE INDEX IF NOT EXISTS idx_ual_category ON user_audit_log(event_category);
