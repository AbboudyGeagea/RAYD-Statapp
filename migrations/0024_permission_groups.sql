-- 0024_permission_groups.sql
-- Group-based permission system: groups carry a permission set,
-- users inherit from their group and can have individual overrides.

CREATE TABLE IF NOT EXISTS permission_groups (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) UNIQUE NOT NULL,
    description TEXT DEFAULT '',
    permissions JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMP DEFAULT NOW()
);

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS group_id             INTEGER REFERENCES permission_groups(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS permission_overrides JSONB DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_users_group_id ON users (group_id);

-- Default groups (skip if they already exist)
INSERT INTO permission_groups (name, description, permissions) VALUES
(
    'Administrators',
    'Full system access — all features enabled',
    '{"can_export":true,"can_configure":true,"can_manage_users":true,"can_view_finance":true,"can_use_ai":true,"can_view_etl":true,"can_view_reports":["*"]}'::jsonb
),
(
    'Radiologists',
    'Reading physicians — full reports and AI assistant',
    '{"can_export":true,"can_configure":false,"can_manage_users":false,"can_view_finance":false,"can_use_ai":true,"can_view_etl":false,"can_view_reports":["*"]}'::jsonb
),
(
    'Technicians',
    'Imaging technicians — view reports, no export or config',
    '{"can_export":false,"can_configure":false,"can_manage_users":false,"can_view_finance":false,"can_use_ai":false,"can_view_etl":false,"can_view_reports":["*"]}'::jsonb
),
(
    'Finance',
    'Finance team — financial dashboard and export',
    '{"can_export":true,"can_configure":false,"can_manage_users":false,"can_view_finance":true,"can_use_ai":false,"can_view_etl":false,"can_view_reports":["*"]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
