-- Migration: 0002_add_scheduling_entries
-- Creates the scheduling_entries table if it was not present at initial DB setup.

CREATE TABLE IF NOT EXISTS public.scheduling_entries (
    id                    SERIAL PRIMARY KEY,
    first_name            TEXT NOT NULL,
    middle_name           TEXT NOT NULL,
    last_name             TEXT NOT NULL,
    date_of_birth         DATE NOT NULL,
    referring_physician   TEXT NOT NULL,
    patient_class         TEXT NOT NULL,
    procedure_datetime    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    modality_type         VARCHAR(50) NOT NULL,
    procedures            JSONB NOT NULL DEFAULT '[]'::jsonb,
    third_party_approvals JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
