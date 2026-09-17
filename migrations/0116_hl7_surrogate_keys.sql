-- Migration 0116: stable surrogate keys for HL7-sourced studies, patients and orders.
--
-- THE PROBLEM. etl_didb_studies.study_db_uid and etl_patient_view.patient_db_uid are
-- NOT NULL BIGINT primary keys, and etl_orders.order_dbid likewise. On the Oracle
-- branches those were the PACS's own internal row IDs, handed to us for free. HL7
-- carries no such thing: it identifies a study by accession number and a patient by
-- patient ID, both text. Something has to mint the integers, and 124 report queries
-- depend on the column types staying exactly as they are.
--
-- WHY A TABLE AND NOT A HASH. The tempting answer is a deterministic hash of the
-- natural key — no lookup, no state, stable across replays by construction. It was
-- rejected because these are PRIMARY KEYS. A 64-bit hash collision does not surface
-- as an error; it silently merges two unrelated studies into one row, and the report
-- built on top shows a number that is simply wrong with nothing anywhere indicating
-- why. At a million studies the odds are small but the failure is both invisible and
-- permanent, and there is no cheap audit that would ever catch it.
--
-- A mapping table costs one indexed lookup per new natural key — paid once per study,
-- never on the read path — and in exchange the mapping is exact, inspectable, and
-- reversible. When a report shows a suspicious study_db_uid you can ask this table
-- which accession it came from. You cannot ask that of a hash.
--
-- REPLAY. The map persists, so re-running the projector over the archived messages
-- produces the same surrogate IDs it did the first time. This is the property that
-- makes replay safe: rebuilt rows land on top of themselves instead of duplicating.
-- Never TRUNCATE this table as part of a rebuild.
--
-- WHY THE SEQUENCE STARTS AT 9,000,000,000. Far above any real PACS-issued ID. If a
-- site ever runs this build against a database that already holds Oracle-sourced
-- rows — a migration from an existing install, or a side-by-side comparison during
-- cutover — a synthesised ID can never land on top of a real one. The gap is free;
-- discovering the overlap afterwards would not be.

CREATE SEQUENCE IF NOT EXISTS hl7_surrogate_id_seq
    AS BIGINT
    START WITH 9000000000
    INCREMENT BY 1
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS hl7_surrogate_keys (
    -- 'study' | 'patient' | 'order'
    entity       TEXT      NOT NULL,
    -- accession number / patient ID / placer order number, trimmed
    natural_key  TEXT      NOT NULL,
    surrogate_id BIGINT    NOT NULL DEFAULT nextval('hl7_surrogate_id_seq'),
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (entity, natural_key),
    CONSTRAINT ck_hl7_surrogate_entity CHECK (entity IN ('study', 'patient', 'order'))
);

-- IDs are unique across every entity type, not just within one. A study and a patient
-- can never share a surrogate value, so a mis-joined query produces zero rows rather
-- than plausible-looking nonsense.
CREATE UNIQUE INDEX IF NOT EXISTS ux_hl7_surrogate_id
    ON hl7_surrogate_keys (surrogate_id);


-- Resolve a natural key to its surrogate, minting one on first sight.
--
-- Returns NULL for a null/blank natural key rather than inventing an ID for nothing:
-- callers then see a NULL and can skip the row, instead of the table quietly filling
-- with surrogates for the empty string.
--
-- The read-insert-reread shape is deliberate. ON CONFLICT ... DO UPDATE would return
-- the row in one statement but writes to the primary key on every duplicate, taking a
-- row lock and producing dead tuples on what is overwhelmingly a read. DO NOTHING
-- returns no row when it conflicts, hence the re-select: under a concurrent insert of
-- the same key, the loser of the race simply reads the winner's value. Both paths end
-- at the same ID, which is the only thing that matters.
CREATE OR REPLACE FUNCTION hl7_surrogate_id(p_entity TEXT, p_natural_key TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_key TEXT;
    v_id  BIGINT;
BEGIN
    v_key := NULLIF(btrim(COALESCE(p_natural_key, '')), '');
    IF v_key IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT surrogate_id INTO v_id
      FROM hl7_surrogate_keys
     WHERE entity = p_entity AND natural_key = v_key;

    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;

    INSERT INTO hl7_surrogate_keys (entity, natural_key)
    VALUES (p_entity, v_key)
    ON CONFLICT (entity, natural_key) DO NOTHING
    RETURNING surrogate_id INTO v_id;

    IF v_id IS NULL THEN
        -- Lost the race to a concurrent insert; the winner's value is authoritative.
        SELECT surrogate_id INTO v_id
          FROM hl7_surrogate_keys
         WHERE entity = p_entity AND natural_key = v_key;
    END IF;

    RETURN v_id;
END;
$fn$;
