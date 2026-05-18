CREATE TABLE IF NOT EXISTS physician_alias_map (
    alias           TEXT PRIMARY KEY,
    canonical_name  TEXT NOT NULL,
    dismissed       BOOL NOT NULL DEFAULT false,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
