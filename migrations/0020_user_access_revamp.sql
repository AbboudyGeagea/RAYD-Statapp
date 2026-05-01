-- 0020_user_access_revamp.sql
-- Full revamp of user registration and access control:
--   • Profile fields on users (full_name, email, phone, department, notes)
--   • Account lifecycle fields (status, created_by, created_at, last_login, must_change_password)
--   • Audit log table (user_audit_log)

-- ── 1. Profile & lifecycle columns on users ───────────────────────
ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name       VARCHAR(200);
ALTER TABLE users ADD COLUMN IF NOT EXISTS email           VARCHAR(200);
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone           VARCHAR(50);
ALTER TABLE users ADD COLUMN IF NOT EXISTS department      VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_by      INTEGER REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at      TIMESTAMP DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login      TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS notes           TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS status          VARCHAR(20) NOT NULL DEFAULT 'active';

-- ── 2. Backfill existing users as active ──────────────────────────
UPDATE users SET status = 'active' WHERE status IS NULL OR status NOT IN ('active','pending','disabled');

-- ── 3. Audit log table ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_audit_log (
    id              SERIAL PRIMARY KEY,
    actor_user_id   INTEGER REFERENCES users(id) ON DELETE SET NULL,
    target_user_id  INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action          VARCHAR(50)  NOT NULL,
    detail          JSONB,
    ip_address      VARCHAR(45),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ual_target  ON user_audit_log(target_user_id);
CREATE INDEX IF NOT EXISTS idx_ual_created ON user_audit_log(created_at DESC);
