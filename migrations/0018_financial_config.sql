-- 0018: RVU financial configuration
-- Creates financial_config and financial_audit_log tables

CREATE TABLE IF NOT EXISTS financial_config (
    id          SERIAL        PRIMARY KEY,
    entity_type VARCHAR(20)   NOT NULL CHECK (entity_type IN ('global', 'modality', 'procedure')),
    entity_id   TEXT,
    usd_per_rvu NUMERIC(8,4)  NOT NULL CHECK (usd_per_rvu > 0 AND usd_per_rvu < 1000),
    notes       TEXT,
    created_at  TIMESTAMP     NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP     NOT NULL DEFAULT now()
);

-- Only one global row
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_config_global
    ON financial_config (entity_type)
    WHERE entity_type = 'global';

-- One row per (entity_type, entity_id) for modality/procedure overrides
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_config_override
    ON financial_config (entity_type, entity_id)
    WHERE entity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_financial_config_type
    ON financial_config (entity_type);

CREATE TABLE IF NOT EXISTS financial_audit_log (
    id          SERIAL        PRIMARY KEY,
    user_id     INT,
    user_name   TEXT,
    action      TEXT          NOT NULL,
    entity_type TEXT,
    entity_id   TEXT,
    old_value   NUMERIC(8,4),
    new_value   NUMERIC(8,4),
    ip_address  TEXT,
    created_at  TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_financial_audit_created
    ON financial_audit_log (created_at DESC);

-- Seed default global rate ($40/RVU)
INSERT INTO financial_config (entity_type, entity_id, usd_per_rvu, notes)
VALUES ('global', NULL, 40.0000, 'Default global rate')
ON CONFLICT DO NOTHING;
