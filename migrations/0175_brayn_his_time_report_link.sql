-- Migration 0175: BRAYN times the HIS's messages by MSH-7 and links the PACS report to the RIS
-- study (LAUMC to-do item 17, 2026-11-20). *** LAUMC BRANCH ONLY *** BRAYN's own tables only.
--
-- What production showed (BRAYN's findings, 18-19 Nov):
--   FUTURE_EVENT 147, all HIS (SAP_P): ORC-9 filled on every one, later than the message time
--     (MSH-7) on every one, from under 3 hours to over a week ahead: SAP's ORC-9 is a planned
--     time, not the transaction time BRAYN takes it for. The order's "ordered" time was that
--     planned time, so 23 studies read as ordered after the RIS scheduled them (TIME_CONTRADICTION
--     ordered > scheduled, 26.7 hours on average). The HIS's time is now MSH-7, for its orders and
--     its status messages (RQ order changed, CA cancelled), a mapping for sender SAP_P that comes
--     before the generic ORC-9 one (editable in BRAYN's mapping studio).
--   UNREPORTED 148, every one keyed by the RIS number (1003...): BRAYN keys a RIS study by ORC-3
--     and the PACS report (CARESTREAM ORU) by the accession in OBR-3, so a report never reached
--     its RIS study. The RIS's completion names the PACS accession in OBR-20: kept as
--     brayn_study_state.pacs_accession, and utils/brayn_shadow._link_report gives the RIS study
--     the report's time, whichever of the two arrives second.
-- The studies already in BRAYN are corrected below from the archive. Run the BRAYN console's
-- Re-check after this migration, not before: it re-reads the held HIS messages with the new times.

ALTER TABLE public.brayn_study_state ADD COLUMN IF NOT EXISTS pacs_accession VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_brayn_study_state_pacs_accession
    ON public.brayn_study_state (pacs_accession) WHERE pacs_accession IS NOT NULL;

INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, sort_order)
VALUES ('parsed', 'pacs_accession', 'text', 'PACS accession',
        'The accession the PACS files the study and its report under, when the message is keyed by another number (the RIS''s). Links the PACS report to the RIS study.', 118)
ON CONFLICT (target_kind, target_field) DO NOTHING;

INSERT INTO public.hl7_field_mappings
    (sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, priority, transform, notes)
VALUES ('RIS',   'status', 'parsed', 'pacs_accession', 'OBR', 20, 1, 10, 'text',
        'LAUMC RIS: OBR-20 = the PACS accession, on the completion (migration 0175)'),
       ('SAP_P', 'order',  'parsed', 'event_time',     'MSH',  7, NULL, 5, 'datetime',
        'LAUMC HIS: ORC-9 is a planned time; the order''s time is the message time (migration 0175)'),
       ('SAP_P', 'status', 'parsed', 'event_time',     'MSH',  7, NULL, 5, 'datetime',
        'LAUMC HIS: ORC-9 is a planned time; RQ / CA happen at the message time (migration 0175)')
ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority) DO NOTHING;

-- The HIS's events already logged: their time is the message's.
UPDATE public.hl7_study_events e
   SET event_time = a.message_datetime
  FROM public.hl7_message_archive a
 WHERE a.id = e.message_archive_id
   AND a.sending_app = 'SAP_P'
   AND a.message_datetime IS NOT NULL
   AND e.event_time IS DISTINCT FROM a.message_datetime;

-- Their studies: the ordered / cancelled time and the last event again from the events (only
-- ever earlier: the planned times were later than the messages). An order the RIS has not
-- scheduled yet is a provisional study keyed ~ORD:<HIS order number>, its events keyed by the
-- order number alone.
WITH x AS (
    SELECT COALESCE(accession_number, '~ORD:' || placer_order_number) AS acc,
           MIN(event_time) FILTER (WHERE canonical_state = 'ordered')   AS ordered_at,
           MIN(event_time) FILTER (WHERE canonical_state = 'cancelled') AS cancelled_at,
           MAX(event_time) AS last_event_at
      FROM public.hl7_study_events
     WHERE accession_number IS NOT NULL OR placer_order_number IS NOT NULL
     GROUP BY 1
)
UPDATE public.brayn_study_state s
   SET ordered_at    = LEAST(s.ordered_at, x.ordered_at),
       cancelled_at  = LEAST(s.cancelled_at, x.cancelled_at),
       last_event_at = CASE WHEN s.last_event_at > x.last_event_at THEN x.last_event_at ELSE s.last_event_at END,
       updated_at    = NOW()
  FROM x
 WHERE s.accession_number = x.acc
   AND (x.ordered_at < s.ordered_at OR x.cancelled_at < s.cancelled_at OR s.last_event_at > x.last_event_at);

-- The HIS's "event in the future" findings were the planned time, not a clock problem.
UPDATE public.brayn_findings
   SET resolved_at = NOW(), resolution = 'cleared',
       resolution_note = 'migration 0175: the HIS''s ORC-9 is a planned time; its time is now MSH-7'
 WHERE rule_code = 'FUTURE_EVENT' AND sending_app = 'SAP_P' AND resolved_at IS NULL;

-- "Ordered after scheduled" notes that the corrected order time answers.
UPDATE public.brayn_findings f
   SET resolved_at = NOW(), resolution = 'cleared',
       resolution_note = 'migration 0175: the order time is now the HIS''s message time'
  FROM public.brayn_study_state s
 WHERE f.rule_code = 'TIME_CONTRADICTION' AND f.message_archive_id IS NULL AND f.resolved_at IS NULL
   AND s.accession_number = f.accession_number
   AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(f.detail->'pairs', '[]'::jsonb)) p
                    WHERE p->>'earlier_rung' <> 'ordered')
   AND s.ordered_at <= LEAST(s.scheduled_at, s.arrived_at, s.started_at, s.completed_at);

-- The RIS studies already in BRAYN: the PACS accession from their archived messages (OBR-20,
-- the latest that has one), keyed like 0173 by the RIS number in ORC-3.
WITH msg AS (
    SELECT a.id, a.received_at,
           substring(a.raw_message from 'ORC\|[^\r]*') AS orc,
           substring(a.raw_message from 'OBR\|[^\r]*') AS obr
      FROM public.hl7_message_archive a
     WHERE a.sending_app = 'RIS'
), fields AS (
    SELECT id, received_at,
           NULLIF(btrim(split_part(split_part(orc, '|', 4), '^', 1)), '')  AS acc,
           NULLIF(btrim(split_part(split_part(obr, '|', 21), '^', 1)), '') AS pacs
      FROM msg
), latest AS (
    SELECT DISTINCT ON (acc) acc, pacs
      FROM fields
     WHERE acc IS NOT NULL AND pacs IS NOT NULL AND pacs <> acc
     ORDER BY acc, received_at DESC, id DESC
)
UPDATE public.brayn_study_state s
   SET pacs_accession = l.pacs
  FROM latest l
 WHERE s.accession_number = l.acc
   AND s.pacs_accession IS DISTINCT FROM l.pacs;

-- ... and the report time of the PACS study under that accession. The 15-minute sweep then
-- closes their UNREPORTED findings.
UPDATE public.brayn_study_state r
   SET reported_at = LEAST(r.reported_at, p.t), updated_at = NOW()
  FROM (SELECT accession_number, LEAST(reported_at, signed_prelim_at, signed_final_at) AS t
          FROM public.brayn_study_state) p
 WHERE r.pacs_accession = p.accession_number
   AND r.accession_number <> p.accession_number
   AND p.t IS NOT NULL
   AND (r.reported_at IS NULL OR r.reported_at > p.t);
