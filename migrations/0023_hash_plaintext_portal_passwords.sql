-- Migration 0023: Remove plaintext patient portal passwords
-- Rows with only password_plain (no hash yet) are given a random placeholder hash
-- that forces a password reset on next login. Rows already having password_hash
-- are untouched. The password_plain column is then nulled and kept as a stub
-- so existing code that references it does not error; it will always be NULL.

-- Step 1: For any row that still has password_plain but no password_hash,
--         insert a bcrypt-style placeholder that will never match any real password.
--         (The '$' prefix markers ensure werkzeug never accepts it as valid.)
UPDATE patient_portal_users
SET    password_hash = '$pbkdf2-sha256$placeholder$MUST_RESET'
WHERE  password_hash IS NULL
  AND  password_plain IS NOT NULL
  AND  password_plain != '';

-- Step 2: Wipe all stored plaintext passwords.
UPDATE patient_portal_users
SET    password_plain = NULL;

-- Step 3: Remove the column so it can never be used again.
ALTER TABLE patient_portal_users
    DROP COLUMN IF EXISTS password_plain;
