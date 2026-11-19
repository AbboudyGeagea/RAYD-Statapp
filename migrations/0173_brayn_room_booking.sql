-- Migration 0173: BRAYN keeps the RIS room, the appointment and the booked minutes of each study
-- (LAUMC to-do item 38, the Live Department board). *** LAUMC BRANCH ONLY ***
--
-- The board groups by RIS room and counts today's appointments per room; BRAYN had neither. In
-- LAUMC's RIS lifecycle messages (production sample, 19 Nov, every ORC-5: '', SC, A, IP, CM, CA):
--   ORC-16   the RIS room code, = aetitle_modality_map.room_code of a RIS room (XR1, MRI 1.5T,
--            SJH-MAMO, SJH-US2); 36 rooms at RH, 10 at SJH
--   OBR-36   the appointment (scheduled date/time), e.g. 20261119161500
--   OBR-27   the booked duration in minutes (10 for an X-ray, 30 for an MRI)
-- OBR-21 (BRAYN's device) is empty and PV1-3.2 (its room) is the patient's ward bed (R219-J,
-- RICU7D), so neither can place a study in an imaging room.
--
-- The three are field mappings (hl7_field_mappings, editable in BRAYN's mapping studio) for
-- sender RIS onto three new parsed fields; utils/brayn_shadow stores them on the study after
-- each message (the latest value wins: a rebooking moves the room and the time). The messages
-- archived since the feed began (18 Nov) fill the studies already open. BRAYN's own tables only.

ALTER TABLE public.brayn_study_state
    ADD COLUMN IF NOT EXISTS ris_room       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS appointment_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS booked_minutes INTEGER;

CREATE INDEX IF NOT EXISTS idx_brayn_study_state_appointment
    ON public.brayn_study_state (appointment_at) WHERE appointment_at IS NOT NULL;

INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, sort_order)
VALUES ('parsed', 'ris_room',       'text',     'RIS room code',
        'The imaging room the RIS booked or performed the exam in (its MODALITY code); the Live Department board groups by it.', 112),
       ('parsed', 'appointment_at', 'datetime', 'Appointment',
        'When the exam is booked to start; the Live Department board counts today''s per room.', 114),
       ('parsed', 'booked_minutes', 'number',   'Booked duration (minutes)',
        'How long the RIS booked the exam for; the board''s expected end.', 116)
ON CONFLICT (target_kind, target_field) DO NOTHING;

INSERT INTO public.hl7_field_mappings
    (sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, priority, transform, notes)
VALUES ('RIS', 'status', 'parsed', 'ris_room',       'ORC', 16, 1, 10, 'text',
        'LAUMC RIS: ORC-16 = the RIS room code (migration 0173)'),
       ('RIS', 'status', 'parsed', 'appointment_at', 'OBR', 36, NULL, 10, 'datetime',
        'LAUMC RIS: OBR-36 = the appointment (migration 0173)'),
       ('RIS', 'status', 'parsed', 'booked_minutes', 'OBR', 27, 1, 10, 'number',
        'LAUMC RIS: OBR-27 = the booked minutes (migration 0173)')
ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority) DO NOTHING;

-- The studies already open: each one's latest RIS message since the feed began.
WITH msg AS (
    SELECT a.id, a.received_at,
           substring(a.raw_message from 'ORC\|[^\r]*') AS orc,
           substring(a.raw_message from 'OBR\|[^\r]*') AS obr
      FROM public.hl7_message_archive a
     WHERE a.sending_app = 'RIS'
), fields AS (
    SELECT id, received_at,
           NULLIF(btrim(split_part(split_part(orc, '|', 4), '^', 1)), '') AS acc,
           NULLIF(btrim(split_part(split_part(orc, '|', 17), '^', 1)), '') AS room,
           NULLIF(btrim(split_part(obr, '|', 37)), '') AS appt,
           NULLIF(btrim(split_part(split_part(obr, '|', 28), '^', 1)), '') AS mins
      FROM msg
), latest AS (
    SELECT DISTINCT ON (acc) acc, room,
           CASE WHEN appt ~ '^\d{12}' THEN to_timestamp(left(appt, 12), 'YYYYMMDDHH24MI')::timestamp END AS appt,
           CASE WHEN mins ~ '^\d{1,3}$' THEN mins::int END AS mins
      FROM fields
     WHERE acc IS NOT NULL AND (room IS NOT NULL OR appt IS NOT NULL)
     ORDER BY acc, received_at DESC, id DESC
)
UPDATE public.brayn_study_state s
   SET ris_room = COALESCE(l.room, s.ris_room),
       appointment_at = COALESCE(l.appt, s.appointment_at),
       booked_minutes = COALESCE(l.mins, s.booked_minutes)
  FROM latest l
 WHERE s.accession_number = l.acc;
