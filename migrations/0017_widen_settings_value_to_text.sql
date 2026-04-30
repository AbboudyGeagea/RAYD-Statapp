-- Migration 0017: widen settings.key and settings.value from VARCHAR(100) to TEXT
-- Required because license JSON and other long values exceed the 100-char limit.

ALTER TABLE public.settings
    ALTER COLUMN key   TYPE TEXT,
    ALTER COLUMN value TYPE TEXT;
