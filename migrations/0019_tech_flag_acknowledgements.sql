-- 0019: technician flag acknowledgements
CREATE TABLE IF NOT EXISTS tech_flag_acknowledgements (
    id                   SERIAL PRIMARY KEY,
    accession_number     TEXT NOT NULL,
    flag_date            DATE NOT NULL,
    flags                TEXT[] NOT NULL DEFAULT '{}',
    note                 TEXT,
    acknowledged_by_id   INT REFERENCES users(id) ON DELETE SET NULL,
    acknowledged_by_name TEXT NOT NULL,
    acknowledged_at      TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (accession_number, flag_date)
);

CREATE INDEX IF NOT EXISTS idx_tfa_flag_date ON tech_flag_acknowledgements (flag_date);
