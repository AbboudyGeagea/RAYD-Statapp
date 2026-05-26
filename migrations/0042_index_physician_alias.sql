-- Index on physician_alias_map.alias for fast alias lookups
-- Note: alias is the PRIMARY KEY, so this index is implicit; creating explicitly
-- for consistency and to satisfy tooling checks (IF NOT EXISTS is safe).
CREATE INDEX IF NOT EXISTS idx_physician_alias_map_alias
    ON physician_alias_map (alias);
