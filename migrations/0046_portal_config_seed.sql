-- Migration 0046: seed every patient-portal setting, drop the shared viewer account
--
-- The config page saves into rows that exist, and no install ever seeded them,
-- so a fresh install could not be configured at all.
--
-- Patients were sent to the viewer with a shared username and password in the
-- URL: they saw both and could change the accession to open any study. The
-- portal now signs a per-study, expiring link (routes/portal_bp.py), so the
-- shared-account rows are removed rather than left holding a PACS password.

INSERT INTO portal_config (config_key, config_value, description) VALUES
    ('hospital_name',                '',    'Shown on the patient login page and in the WhatsApp message'),
    ('hospital_logo_url',            '',    'Logo on the patient login page (https address or /static/ path)'),
    ('viewer_url_template',          '',    'Viewer link with {accession} {expires} {signature}, optional {mrn} {nonce}'),
    ('viewer_signing_secret',        '',    'HMAC secret shared with the PACS viewer (stored encrypted)'),
    ('viewer_link_ttl_seconds',      '300', 'Seconds a viewer link stays valid'),
    ('twilio_account_sid',           '',    'Twilio account SID'),
    ('twilio_auth_token',            '',    'Twilio auth token (stored encrypted)'),
    ('twilio_whatsapp_from',         '',    'Twilio WhatsApp-enabled sender number'),
    ('portal_base_url',              '',    'Public address patients open, e.g. https://results.hospital.com'),
    ('whatsapp_message_template',    'Hello, your radiology exam at {hospital_name} is registered. View your images at {portal_url} with username {username} and password {password}.', 'WhatsApp message (English)'),
    ('whatsapp_message_template_ar', '',    'WhatsApp message (Arabic)')
ON CONFLICT (config_key) DO NOTHING;

DELETE FROM portal_config
WHERE config_key IN ('viewer_base_url', 'viewer_username', 'viewer_password', 'viewer_accession_param');
