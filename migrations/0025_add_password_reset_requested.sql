-- 0025: Add password_reset_requested flag to users
-- Users can request a password reset; admin sees a count badge and sets a temp password.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS password_reset_requested BOOLEAN NOT NULL DEFAULT FALSE;
