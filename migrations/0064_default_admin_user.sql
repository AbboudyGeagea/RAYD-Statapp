-- 0064_default_admin_user.sql
-- A fresh install can end up with an EMPTY users table: init-db/schema.sql stops
-- at the blank line inside the report_template COPY, before the users COPY runs,
-- so nobody can log in.
--
-- Creates admin / admin123 ONLY when the database has no admin at all (no user
-- named 'admin' and no user with role 'admin'). Where an admin already exists this
-- is a no-op, so it never resets anyone's password.
-- must_change_password = TRUE: the first login goes straight to "change password".
-- The notes value lets install.sh disable this account when the installer picks
-- a different admin username.

INSERT INTO users (username, password_hash, role, status, must_change_password, full_name, notes)
SELECT 'admin',
       'pbkdf2:sha256:1000000$ov3lU3TdLmPvjD4o$6f11ca854dd221729e1119fecbe60f44f5deeba6c693c10191c28a9577b308ed',
       'admin',
       'active',
       TRUE,
       'Administrator',
       'Default admin created by migration 0064'
WHERE NOT EXISTS (
    SELECT 1 FROM users WHERE username = 'admin' OR role = 'admin'
);
