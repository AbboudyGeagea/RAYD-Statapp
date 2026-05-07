-- 0028: add procedure_name to procedure_duration_map
ALTER TABLE procedure_duration_map
    ADD COLUMN IF NOT EXISTS procedure_name TEXT;
