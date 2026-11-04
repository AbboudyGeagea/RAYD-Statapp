-- Migration 0140: Microsoft 365 sign-in (OAuth) for scheduled emails.
--
-- Microsoft turns SMTP basic auth off by default at the end of December 2026, so a relay
-- on Microsoft 365 needs an app token instead of a password. No user is present when a
-- schedule runs, so the app signs in as itself (client credentials): utils/worklist_email.py,
-- IT's side in docs/LAUMC_O365_EMAIL_SETUP.md.
--
-- smtp_auth is password or oauth_o365. smtp_oauth_client_secret is encrypted with utils/crypto.

INSERT INTO settings (key, value) VALUES ('smtp_auth', 'password') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_oauth_tenant', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_oauth_client_id', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('smtp_oauth_client_secret', '') ON CONFLICT (key) DO NOTHING;
