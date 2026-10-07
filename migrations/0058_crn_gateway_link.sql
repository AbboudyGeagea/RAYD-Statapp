-- CRN slice 3b: RAYD <-> CRN gateway.
--
-- Before texting a recipient, RAYD places their notification page on the gateway
-- (crn_recipients.page_pushed_at). Every dispatcher pass collects the gateway's
-- opens and acknowledgements into crn_events; an acknowledgement sets
-- crn_notifications.acknowledged_at / acknowledged_by (the recipient) and status
-- 'acknowledged'. RAYD always calls the gateway, never the other way round.
--
-- Settings:
--   crn_gateway_url           gateway address RAYD calls, e.g. https://crn.<hospital-domain>
--   crn_gateway_api_key       shared secret (stored encrypted with utils/crypto)
--   crn_gateway_event_cursor  last gateway event collected
--   crn_hospital_name, crn_page_footer   shown on the notification page

ALTER TABLE crn_recipients ADD COLUMN IF NOT EXISTS page_pushed_at TIMESTAMP;
ALTER TABLE crn_notifications ADD COLUMN IF NOT EXISTS acknowledged_by INTEGER REFERENCES crn_recipients(id);

INSERT INTO settings (key, value) VALUES ('crn_gateway_url', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_gateway_api_key', '') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_gateway_event_cursor', '0') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_hospital_name', 'Mazloum Hospital') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('crn_page_footer', 'Confidential medical information, intended only for the recipient. Do not forward.') ON CONFLICT (key) DO NOTHING;
