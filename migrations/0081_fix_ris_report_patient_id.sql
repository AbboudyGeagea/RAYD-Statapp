-- Migration 0081: fix patient_id on existing RIS-sourced hl7_oru_reports rows.
--
-- Operator identified (2026-07-27): ETL_JOBS/etl_ris_reports.py's RIS-report enrichment
-- filled hl7_oru_reports.patient_id from etl_patient_view.id -- a PACS-internal column,
-- confirmed by a real sample surfacing placeholder values ("PIX_xxxxx"), not the real
-- hospital MRN. Live HL7 messages' own PID-3 parsing is correct and untouched by this
-- bug (confirmed by operator) -- this only affects report_source='ris' rows (~475k),
-- enriched via the RIS ETL path. ETL_JOBS/etl_ris_reports.py's _ENRICH_SQL is fixed in
-- the same commit so newly-arriving reports use the correct source going forward; this
-- migration only corrects what's already loaded.
--
-- Correct source: std_patient_ids (migration 0060, RIS PATIENT_ID_LIST) -- confirmed
-- against real data (2026-07-27) that is_primary reliably marks exactly one
-- authoritative row per patient, regardless of what that ID's value looks like (a
-- short numeric string is just as valid a real MRN as a "LAU..."-prefixed one -- the
-- vendor-provided flag is the trustworthy signal, not the value's shape). Reached via
-- etl_orders.patient_dbid (= RIS patient_person_key, by existing design), matched on
-- accession_number directly.
--
-- BATCHED BY ID RANGE, not "repeat while rows match" (contrast migration 0073's DELETE,
-- where matching rows disappear each batch): this is an UPDATE, and report_source='ris'
-- stays true for a row forever regardless of whether its patient_id got fixed, so a
-- repeat-until-zero-affected loop would spin on the same first batch forever. Instead
-- walks id ranges from 0 to MAX(id), 5,000 rows/commit -- 475k rows is large enough
-- that a single transaction risks the same lock-pileup/timeout failure mode as 0073's
-- first attempt.
--
-- Only touches rows where the resolved value actually differs (IS DISTINCT FROM) and
-- only where the new join can confidently resolve something (INNER-joined via the two
-- LATERAL subqueries) -- a row that can't be resolved yet (order not loaded, no primary
-- ID on file) is left untouched rather than blanked out, so a later re-run of the ETL's
-- fixed _ENRICH_SQL can still pick it up once the underlying data catches up.

DO $$
DECLARE
    batch_size    INT := 5000;
    cur_id        INT := 0;
    max_id        INT;
    updated_count INT;
    total_updated INT := 0;
BEGIN
    SELECT COALESCE(MAX(id), 0) INTO max_id FROM hl7_oru_reports WHERE report_source = 'ris';

    WHILE cur_id < max_id LOOP
        WITH batch AS (
            SELECT o.id, o.accession_number
            FROM hl7_oru_reports o
            WHERE o.report_source = 'ris'
              AND o.id > cur_id AND o.id <= cur_id + batch_size
        ),
        resolved AS (
            SELECT b.id, pid.patient_id AS new_patient_id
            FROM batch b
            JOIN LATERAL (
                SELECT eo.patient_dbid
                FROM etl_orders eo
                WHERE eo.accession_number = b.accession_number
                  AND eo.patient_dbid ~ '^[0-9]+$'
                ORDER BY eo.last_update DESC NULLS LAST
                LIMIT 1
            ) eo ON true
            JOIN LATERAL (
                SELECT patient_id
                FROM std_patient_ids
                WHERE patient_person_key = eo.patient_dbid::bigint
                  AND UPPER(is_primary) = 'Y'
                LIMIT 1
            ) pid ON true
        )
        UPDATE hl7_oru_reports o
        SET patient_id = r.new_patient_id
        FROM resolved r
        WHERE o.id = r.id
          AND o.patient_id IS DISTINCT FROM r.new_patient_id;

        GET DIAGNOSTICS updated_count = ROW_COUNT;
        total_updated := total_updated + updated_count;
        RAISE NOTICE 'hl7_oru_reports RIS patient_id fix: id range (%, %] -- % rows updated, % total',
            cur_id, cur_id + batch_size, updated_count, total_updated;
        cur_id := cur_id + batch_size;
        COMMIT;
    END LOOP;

    RAISE NOTICE 'hl7_oru_reports RIS patient_id fix: DONE -- % total rows updated', total_updated;
END $$;
