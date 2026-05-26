-- Index on etl_didb_studies.accession_number for fast lookup by accession
CREATE INDEX IF NOT EXISTS idx_etl_studies_accession
    ON etl_didb_studies (accession_number);
