-- Migration 0062: every request the burning station (RayBurn) makes to the CD burn API.
--
-- cd_burn_log only holds burns that were stored. When the CD Log stays empty, this
-- table tells the two cases apart:
--   no row at all          → nothing reached RAYD: check RayBurn's URL, the network, nginx
--   rows, outcome≠'stored' → RayBurn is sending but RAYD refused it (API key, URL, JSON)
-- Written by routes/cd_log_route.py and shown on the CD Burn Audit dashboard.
-- payload_shape keeps field names only, never values; patient names and IDs stay in cd_burn_log.

CREATE TABLE IF NOT EXISTS cd_burn_receipts (
  id SERIAL PRIMARY KEY,
  received_at TIMESTAMP NOT NULL DEFAULT now(),
  remote_addr VARCHAR(64),
  method VARCHAR(10),
  path TEXT,
  user_agent TEXT,
  http_status INT NOT NULL,
  -- stored | no_api_key_configured | bad_api_key | not_json | bad_payload | db_error | server_error | wrong_url
  outcome VARCHAR(30) NOT NULL,
  detail TEXT,
  payload_shape TEXT,
  cd_log_id INT REFERENCES cd_burn_log(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_cd_burn_receipts_received_at ON cd_burn_receipts(received_at);
