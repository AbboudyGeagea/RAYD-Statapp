-- Migration 0131: HL7 Branch Role System Refactor
-- Replaces old roles (admin, viewer, viewer2, tec, finance, secretary)
-- with new roles (su, implementation, administrator, user)

BEGIN TRANSACTION;

-- Update existing users to new role system
UPDATE users SET role = 'su' WHERE role = 'admin';
UPDATE users SET role = 'implementation' WHERE role = 'tec';
UPDATE users SET role = 'administrator' WHERE role IN ('viewer', 'viewer2');
UPDATE users SET role = 'user' WHERE role IN ('finance', 'secretary');

-- Ensure no invalid roles exist
UPDATE users SET role = 'user' WHERE role NOT IN ('su', 'implementation', 'administrator', 'user');

-- Add constraint to enforce valid roles (if not already present)
-- NOTE: PostgreSQL doesn't have declarative CHECK constraints on inherited types,
-- so we rely on application-level validation in db.py ROLE_PAGE_DEFAULTS.

COMMIT;
