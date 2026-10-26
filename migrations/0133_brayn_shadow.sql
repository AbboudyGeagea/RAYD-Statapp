-- Migration 0133: BRAYN on LAUMC, in SHADOW mode (2026-10-26).
--
-- The operator asked to bring BRAYN (formerly RAY7, built on the HL7 branch) to
-- LAUMC "without breaking anything". So on LAUMC it only OBSERVES:
--
--   * LAUMC's listener keeps doing exactly what it does today (fill-only report
--     upsert, HIS orders, PACS completions). After it, the raw message is copied
--     into hl7_message_archive; a background job screens the archived messages
--     and writes ONLY the tables below. Nothing here is read by a report, and
--     nothing BRAYN does reaches etl_*, hl7_orders or hl7_oru_reports.
--   * Nothing is ever held: a message BRAYN cannot identify is shown in the BRAYN
--     console as "not identified", while LAUMC stored it as usual.
--   * Off until switched on: settings.brayn_enabled = 'false'.
--
-- The tables and seed rows are the HL7 branch's current definitions (its
-- migrations 0115-0138, read from a freshly built HL7 database), created here
-- directly under their final brayn_* names. LAUMC differences, below the seeds:
--   * hl7_field_targets keeps only the 'parsed' targets: the others write straight
--     into etl_* tables, which on LAUMC belong to the Oracle ETL.
--   * Checks that need the RIS status feed (R2I, not live yet) start OFF, or every
--     HIS order and every study would raise a note: ORPHAN_EVENT, SKIPPED_RUNG,
--     ORPHAN_ORDER, STALLED_SCHEDULED, STALLED_ARRIVED, STALLED_STARTED. Turn them on
--     in the BRAYN console (Settings) once the R2I feed is live.
--
-- Additive only: new tables, new indexes, new settings rows. No existing table
-- is altered.


-- -- Tables, sequences, constraints, indexes -------------------------------






CREATE TABLE public.brayn_findings (
    id bigint NOT NULL,
    message_archive_id bigint,
    rule_code text NOT NULL,
    severity text NOT NULL,
    accession_number text,
    patient_id text,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    resolved_at timestamp without time zone,
    resolved_by integer,
    resolution text,
    resolution_note text,
    sending_app text,
    CONSTRAINT ck_brayn_findings_resolution CHECK (((resolution IS NULL) OR (resolution = ANY (ARRAY['cleared'::text, 'ignored'::text, 'fixed_upstream'::text])))),
    CONSTRAINT ck_brayn_findings_severity CHECK ((severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text])))
);

CREATE SEQUENCE public.brayn_findings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.brayn_findings_id_seq OWNED BY public.brayn_findings.id;

CREATE TABLE public.brayn_interface_stats (
    bucket_start timestamp without time zone NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    metric text NOT NULL,
    n integer DEFAULT 0 NOT NULL
);

CREATE TABLE public.brayn_ladder_profile (
    rung text NOT NULL,
    ladder_rank integer NOT NULL,
    label text NOT NULL,
    expected boolean DEFAULT false NOT NULL,
    enforce_order boolean DEFAULT false NOT NULL,
    notes text,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_brayn_ladder_rung CHECK ((rung = ANY (ARRAY['ordered'::text, 'scheduled'::text, 'arrived'::text, 'started'::text, 'completed'::text, 'signed_prelim'::text, 'signed_final'::text])))
);

CREATE TABLE public.brayn_rules (
    rule_code text NOT NULL,
    modality text DEFAULT ''::text NOT NULL,
    category text NOT NULL,
    title text NOT NULL,
    description text,
    default_severity text NOT NULL,
    severity text,
    enabled boolean DEFAULT true NOT NULL,
    threshold_minutes integer,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    threshold_pct numeric(6,2),
    CONSTRAINT ck_brayn_rules_category CHECK ((category = ANY (ARRAY['intake'::text, 'quality'::text, 'interface'::text, 'workflow'::text]))),
    CONSTRAINT ck_brayn_rules_default_severity CHECK ((default_severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text]))),
    CONSTRAINT ck_brayn_rules_severity CHECK (((severity IS NULL) OR (severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text]))))
);

CREATE TABLE public.brayn_study_state (
    accession_number text NOT NULL,
    placer_order_number text,
    patient_id text,
    modality text,
    aetitle text,
    room_name text,
    procedure_code text,
    patient_class text,
    ordered_at timestamp without time zone,
    scheduled_at timestamp without time zone,
    arrived_at timestamp without time zone,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    reported_at timestamp without time zone,
    cancelled_at timestamp without time zone,
    current_rank integer DEFAULT 0 NOT NULL,
    is_closed boolean DEFAULT false NOT NULL,
    event_count integer DEFAULT 0 NOT NULL,
    open_findings integer DEFAULT 0 NOT NULL,
    first_seen_at timestamp without time zone DEFAULT now() NOT NULL,
    last_event_at timestamp without time zone,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    patient_location text,
    procedure_text text,
    pacs_completed_at timestamp without time zone,
    signed_prelim_at timestamp without time zone,
    signed_final_at timestamp without time zone,
    visit_number text,
    is_provisional boolean DEFAULT false NOT NULL
);

CREATE TABLE public.brayn_unknown_values (
    kind text NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    value text NOT NULL,
    first_seen timestamp without time zone DEFAULT now() NOT NULL,
    last_seen timestamp without time zone DEFAULT now() NOT NULL,
    n integer DEFAULT 0 NOT NULL,
    sample_archive_id bigint
);

CREATE TABLE public.hl7_field_mappings (
    id integer NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    message_kind text DEFAULT ''::text NOT NULL,
    target_kind text DEFAULT 'parsed'::text NOT NULL,
    target_field text NOT NULL,
    segment text NOT NULL,
    field_index integer NOT NULL,
    component_index integer,
    repeat_index integer,
    priority integer DEFAULT 100 NOT NULL,
    transform text DEFAULT 'text'::text NOT NULL,
    active boolean DEFAULT true NOT NULL,
    notes text,
    updated_by integer,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_hl7_fm_component CHECK (((component_index IS NULL) OR (component_index > 0))),
    CONSTRAINT ck_hl7_fm_field_index CHECK ((field_index > 0)),
    CONSTRAINT ck_hl7_fm_target_kind CHECK ((target_kind = ANY (ARRAY['parsed'::text, 'study'::text, 'patient'::text, 'order'::text]))),
    CONSTRAINT ck_hl7_fm_transform CHECK ((transform = ANY (ARRAY['text'::text, 'upper'::text, 'datetime'::text, 'date'::text, 'birth_date'::text, 'name_xpn'::text, 'name_xcn'::text, 'number'::text])))
);

CREATE SEQUENCE public.hl7_field_mappings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.hl7_field_mappings_id_seq OWNED BY public.hl7_field_mappings.id;

CREATE TABLE public.hl7_field_targets (
    target_kind text NOT NULL,
    target_field text NOT NULL,
    data_type text NOT NULL,
    label text NOT NULL,
    description text,
    is_dangerous boolean DEFAULT false NOT NULL,
    sort_order integer DEFAULT 100 NOT NULL
);

CREATE TABLE public.hl7_message_archive (
    id bigint NOT NULL,
    message_control_id text NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    sending_facility text,
    message_type text,
    message_datetime timestamp without time zone,
    hl7_version text,
    raw_message text NOT NULL,
    source_ip text,
    received_at timestamp without time zone DEFAULT now() NOT NULL,
    parse_status text DEFAULT 'pending'::text NOT NULL,
    parse_error text,
    parsed_at timestamp without time zone,
    projected_at timestamp without time zone,
    content_hash text DEFAULT ''::text NOT NULL,
    brayn_status text DEFAULT 'pending'::text NOT NULL,
    brayn_severity text,
    brayn_screened_at timestamp without time zone,
    CONSTRAINT ck_hl7_archive_brayn_severity CHECK (((brayn_severity IS NULL) OR (brayn_severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text])))),
    CONSTRAINT ck_hl7_archive_brayn_status CHECK ((brayn_status = ANY (ARRAY['pending'::text, 'accepted'::text, 'flagged'::text, 'quarantined'::text]))),
    CONSTRAINT ck_hl7_archive_parse_status CHECK ((parse_status = ANY (ARRAY['pending'::text, 'ok'::text, 'error'::text, 'ignored'::text])))
);

CREATE SEQUENCE public.hl7_message_archive_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.hl7_message_archive_id_seq OWNED BY public.hl7_message_archive.id;

CREATE TABLE public.hl7_patients (
    patient_id text NOT NULL,
    patient_name text,
    birth_date date,
    sex text,
    patient_class text,
    patient_location text,
    last_adt_event text,
    last_message_at timestamp without time zone,
    message_archive_id bigint,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_hl7_patient_birth_date CHECK (((birth_date IS NULL) OR ((birth_date > '1880-01-01'::date) AND (birth_date < '2100-01-01'::date))))
);

CREATE TABLE public.hl7_result_status_map (
    id integer NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    result_status text NOT NULL,
    canonical_state text NOT NULL,
    ladder_rank integer NOT NULL,
    active boolean DEFAULT true NOT NULL,
    notes text,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_hl7_result_canonical CHECK ((canonical_state = ANY (ARRAY['signed_prelim'::text, 'signed_final'::text])))
);

CREATE SEQUENCE public.hl7_result_status_map_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.hl7_result_status_map_id_seq OWNED BY public.hl7_result_status_map.id;

CREATE TABLE public.hl7_status_map (
    id integer NOT NULL,
    sending_app text DEFAULT ''::text NOT NULL,
    order_control text DEFAULT ''::text NOT NULL,
    order_status text NOT NULL,
    canonical_state text NOT NULL,
    ladder_rank integer NOT NULL,
    active boolean DEFAULT true NOT NULL,
    notes text,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_hl7_status_canonical CHECK ((canonical_state = ANY (ARRAY['ordered'::text, 'scheduled'::text, 'arrived'::text, 'started'::text, 'completed'::text, 'signed_prelim'::text, 'signed_final'::text, 'cancelled'::text])))
);

CREATE SEQUENCE public.hl7_status_map_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.hl7_status_map_id_seq OWNED BY public.hl7_status_map.id;

CREATE TABLE public.hl7_study_events (
    id bigint NOT NULL,
    message_archive_id bigint,
    accession_number text,
    placer_order_number text,
    patient_id text,
    canonical_state text NOT NULL,
    ladder_rank integer NOT NULL,
    event_time timestamp without time zone NOT NULL,
    performed_by_id text,
    performed_by_name text,
    performed_by_role text,
    aetitle text,
    room_name text,
    modality text,
    procedure_code text,
    procedure_text text,
    patient_class text,
    patient_location text,
    raw_order_control text,
    raw_order_status text,
    received_at timestamp without time zone DEFAULT now() NOT NULL,
    visit_number text,
    CONSTRAINT ck_hl7_event_canonical CHECK ((canonical_state = ANY (ARRAY['ordered'::text, 'scheduled'::text, 'arrived'::text, 'started'::text, 'completed'::text, 'signed_prelim'::text, 'signed_final'::text, 'cancelled'::text]))),
    CONSTRAINT ck_hl7_event_identified CHECK (((accession_number IS NOT NULL) OR (placer_order_number IS NOT NULL)))
);

CREATE SEQUENCE public.hl7_study_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.hl7_study_events_id_seq OWNED BY public.hl7_study_events.id;

ALTER TABLE ONLY public.brayn_findings ALTER COLUMN id SET DEFAULT nextval('public.brayn_findings_id_seq'::regclass);

ALTER TABLE ONLY public.hl7_field_mappings ALTER COLUMN id SET DEFAULT nextval('public.hl7_field_mappings_id_seq'::regclass);

ALTER TABLE ONLY public.hl7_message_archive ALTER COLUMN id SET DEFAULT nextval('public.hl7_message_archive_id_seq'::regclass);

ALTER TABLE ONLY public.hl7_result_status_map ALTER COLUMN id SET DEFAULT nextval('public.hl7_result_status_map_id_seq'::regclass);

ALTER TABLE ONLY public.hl7_status_map ALTER COLUMN id SET DEFAULT nextval('public.hl7_status_map_id_seq'::regclass);

ALTER TABLE ONLY public.hl7_study_events ALTER COLUMN id SET DEFAULT nextval('public.hl7_study_events_id_seq'::regclass);

ALTER TABLE ONLY public.brayn_findings
    ADD CONSTRAINT brayn_findings_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.brayn_interface_stats
    ADD CONSTRAINT brayn_interface_stats_pkey PRIMARY KEY (bucket_start, sending_app, metric);

ALTER TABLE ONLY public.brayn_ladder_profile
    ADD CONSTRAINT brayn_ladder_profile_ladder_rank_key UNIQUE (ladder_rank);

ALTER TABLE ONLY public.brayn_ladder_profile
    ADD CONSTRAINT brayn_ladder_profile_pkey PRIMARY KEY (rung);

ALTER TABLE ONLY public.brayn_rules
    ADD CONSTRAINT brayn_rules_pkey PRIMARY KEY (rule_code, modality);

ALTER TABLE ONLY public.brayn_study_state
    ADD CONSTRAINT brayn_study_state_pkey PRIMARY KEY (accession_number);

ALTER TABLE ONLY public.brayn_unknown_values
    ADD CONSTRAINT brayn_unknown_values_pkey PRIMARY KEY (kind, sending_app, value);

ALTER TABLE ONLY public.hl7_field_mappings
    ADD CONSTRAINT hl7_field_mappings_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hl7_field_targets
    ADD CONSTRAINT hl7_field_targets_pkey PRIMARY KEY (target_kind, target_field);

ALTER TABLE ONLY public.hl7_message_archive
    ADD CONSTRAINT hl7_message_archive_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hl7_patients
    ADD CONSTRAINT hl7_patients_pkey PRIMARY KEY (patient_id);

ALTER TABLE ONLY public.hl7_result_status_map
    ADD CONSTRAINT hl7_result_status_map_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hl7_status_map
    ADD CONSTRAINT hl7_status_map_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hl7_study_events
    ADD CONSTRAINT hl7_study_events_pkey PRIMARY KEY (id);

CREATE INDEX idx_brayn_findings_accession ON public.brayn_findings USING btree (accession_number) WHERE (accession_number IS NOT NULL);

CREATE INDEX idx_brayn_findings_open ON public.brayn_findings USING btree (severity, created_at DESC) WHERE (resolved_at IS NULL);

CREATE INDEX idx_brayn_interface_stats_recent ON public.brayn_interface_stats USING btree (bucket_start DESC, sending_app);

CREATE INDEX idx_brayn_state_pacs_pending ON public.brayn_study_state USING btree (completed_at) WHERE ((pacs_completed_at IS NULL) AND (completed_at IS NOT NULL));

CREATE INDEX idx_brayn_state_patient ON public.brayn_study_state USING btree (patient_id) WHERE (patient_id IS NOT NULL);

CREATE INDEX idx_brayn_state_placer ON public.brayn_study_state USING btree (placer_order_number) WHERE (placer_order_number IS NOT NULL);

CREATE INDEX idx_brayn_state_provisional ON public.brayn_study_state USING btree (placer_order_number) WHERE is_provisional;

CREATE INDEX idx_brayn_state_stalled_arrived ON public.brayn_study_state USING btree (arrived_at) WHERE ((started_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_stalled_scheduled ON public.brayn_study_state USING btree (scheduled_at) WHERE ((arrived_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_stalled_started ON public.brayn_study_state USING btree (started_at) WHERE ((completed_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_unreported ON public.brayn_study_state USING btree (completed_at) WHERE ((reported_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_unscheduled ON public.brayn_study_state USING btree (ordered_at) WHERE ((scheduled_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_unsigned_final ON public.brayn_study_state USING btree (signed_prelim_at) WHERE ((signed_final_at IS NULL) AND (cancelled_at IS NULL) AND (NOT is_closed));

CREATE INDEX idx_brayn_state_visit ON public.brayn_study_state USING btree (visit_number, procedure_code) WHERE (visit_number IS NOT NULL);

CREATE INDEX idx_hl7_archive_brayn_open ON public.hl7_message_archive USING btree (brayn_status, received_at DESC) WHERE (brayn_status = ANY (ARRAY['flagged'::text, 'quarantined'::text]));

CREATE INDEX idx_hl7_archive_received ON public.hl7_message_archive USING btree (received_at DESC);

CREATE INDEX idx_hl7_archive_sender_received ON public.hl7_message_archive USING btree (sending_app, received_at DESC);

CREATE INDEX idx_hl7_archive_type ON public.hl7_message_archive USING btree (message_type);

CREATE INDEX idx_hl7_archive_unhealthy ON public.hl7_message_archive USING btree (parse_status, received_at DESC) WHERE (parse_status <> 'ok'::text);

-- LAUMC: nothing is ever projected (projected_at stays NULL), so the HL7 branch's
-- "unprojected" index would cover every row. The shadow worker instead reads what
-- it has not screened yet, oldest first.
CREATE INDEX idx_hl7_archive_brayn_pending ON public.hl7_message_archive USING btree (id) WHERE (brayn_status = 'pending'::text);

CREATE INDEX idx_hl7_events_accession ON public.hl7_study_events USING btree (accession_number, ladder_rank) WHERE (accession_number IS NOT NULL);

CREATE INDEX idx_hl7_events_aetitle ON public.hl7_study_events USING btree (aetitle, event_time DESC) WHERE (aetitle IS NOT NULL);

CREATE INDEX idx_hl7_events_archive ON public.hl7_study_events USING btree (message_archive_id);

CREATE INDEX idx_hl7_events_placer ON public.hl7_study_events USING btree (placer_order_number) WHERE (placer_order_number IS NOT NULL);

CREATE INDEX idx_hl7_events_state_time ON public.hl7_study_events USING btree (canonical_state, event_time DESC);

CREATE INDEX idx_hl7_events_time ON public.hl7_study_events USING btree (event_time DESC);

CREATE INDEX idx_hl7_events_visit ON public.hl7_study_events USING btree (visit_number) WHERE (visit_number IS NOT NULL);

CREATE INDEX idx_hl7_fm_lookup ON public.hl7_field_mappings USING btree (target_kind, message_kind, sending_app, priority) WHERE active;

CREATE INDEX idx_hl7_patients_updated ON public.hl7_patients USING btree (updated_at DESC);

CREATE UNIQUE INDEX ux_brayn_findings_message_rule ON public.brayn_findings USING btree (message_archive_id, rule_code) WHERE (message_archive_id IS NOT NULL);

CREATE UNIQUE INDEX ux_brayn_findings_open_absence ON public.brayn_findings USING btree (rule_code, accession_number) WHERE ((message_archive_id IS NULL) AND (resolved_at IS NULL));

CREATE UNIQUE INDEX ux_brayn_findings_open_interface ON public.brayn_findings USING btree (rule_code, sending_app) WHERE ((message_archive_id IS NULL) AND (accession_number IS NULL) AND (resolved_at IS NULL));

CREATE UNIQUE INDEX ux_hl7_archive_identity ON public.hl7_message_archive USING btree (sending_app, message_control_id, content_hash);

CREATE UNIQUE INDEX ux_hl7_fm_slot ON public.hl7_field_mappings USING btree (sending_app, message_kind, target_kind, target_field, priority);

CREATE UNIQUE INDEX ux_hl7_result_status_map ON public.hl7_result_status_map USING btree (sending_app, result_status);

CREATE UNIQUE INDEX ux_hl7_status_map_lookup ON public.hl7_status_map USING btree (sending_app, order_control, order_status);

ALTER TABLE ONLY public.brayn_findings
    ADD CONSTRAINT brayn_findings_message_archive_id_fkey FOREIGN KEY (message_archive_id) REFERENCES public.hl7_message_archive(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.hl7_patients
    ADD CONSTRAINT hl7_patients_message_archive_id_fkey FOREIGN KEY (message_archive_id) REFERENCES public.hl7_message_archive(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.hl7_study_events
    ADD CONSTRAINT hl7_study_events_message_archive_id_fkey FOREIGN KEY (message_archive_id) REFERENCES public.hl7_message_archive(id) ON DELETE CASCADE;


-- -- Seed configuration (as on the HL7 branch) ----------------------------





INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('ordered', 20, 'Ordered (NW)', false, false, 'HIS order, ORC-1 = NW. The accession does not exist yet at this rung - the RIS mints it at scheduling - so these are matched on order number. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('scheduled', 40, 'Scheduled (SC)', false, false, 'The accession is minted at this point. Walk-ins and emergencies legitimately never reach this rung. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('arrived', 60, 'Arrived (AR)', false, false, 'Patient arrived. Vendor-specific; not a standard HL7 order status. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('started', 70, 'Started (IP)', false, false, 'Exam started on the device. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('completed', 100, 'Exam done (CM)', false, false, 'Clinical completion, the TAT anchor. Distinct from a PACS CM, which means images stored and lands in pacs_completed_at. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('signed_prelim', 120, 'Signed 1 (W)', false, false, 'First signature, from OBX-11. Mapped through hl7_result_status_map, not hardcoded. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_ladder_profile (rung, ladder_rank, label, expected, enforce_order, notes, updated_at) VALUES ('signed_final', 140, 'Signed 3 (F)', false, false, 'Final signature, from OBX-11. Reaching this rung is what closes a study. [0135: "measure order" counts late deliveries of this rung in the sender''s out-of-order rate; nothing is held.]', '2026-10-26 17:36:55.664974') ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('FUTURE_EVENT', '', 'quality', 'Event timestamped in the future', 'The sender clock is ahead of ours, or a date was mistyped at the source.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('LADDER_REGRESSION', '', 'quality', 'Lifecycle went backwards', 'Retired in 0135: the state-based TIME_CONTRADICTION covers an event stamped after a higher rung.', 'warning', NULL, false, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNKNOWN_STATUS_CODE', '', 'intake', 'Status code not mapped', 'The ORC status value has no row in hl7_status_map, so the transition cannot be classified at all. Projecting it would silently lose the event.', 'critical', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('MISSING_IDENTIFIER', '', 'intake', 'No usable identifier', 'Neither an accession number nor a placer order number, so the message cannot be attached to any study.', 'critical', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('PLACEHOLDER_DATA', '', 'quality', 'Placeholder or test data', 'ER quick-registration placeholders (birth date 9999-11-11, sex U), known test patients, service accounts.', 'info', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('STALLED_SCHEDULED', '', 'workflow', 'Scheduled but never arrived', 'Scheduled, and no arrival event since. Either a no-show nobody cancelled, or the arrival feed has stopped.', 'warning', NULL, true, 2880, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('STALLED_ARRIVED', '', 'workflow', 'Arrived but never started', 'Patient arrived and the exam never started. A real wait, or a missing start event.', 'warning', NULL, true, 240, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('STALLED_STARTED', '', 'workflow', 'Started but never completed', 'An exam left open on the device, or a completion message that never arrived. Inflates every duration metric it touches.', 'warning', NULL, true, 180, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNREPORTED', '', 'workflow', 'Completed but never reported', 'The exam completed and no ORU followed.', 'warning', NULL, true, 1440, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('ORPHAN_ORDER', '', 'workflow', 'Order never scheduled', 'A HIS order whose accession was never minted by the RIS, so it can never be matched to a study.', 'warning', NULL, true, 1440, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('STALLED_UNSIGNED', '', 'workflow', 'Preliminary signed but never finalised', 'A first signature arrived and the final one never followed. The study reads as complete to everyone downstream while the report is still provisional.', 'warning', NULL, true, 2880, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('REPEATED_TRANSITION', '', 'interface', 'Repeated transition', 'The same rung reported again at a different time. Legitimate in this RIS - a worklist entry can genuinely arrive more than once - so informational, suppressed within the threshold window.', 'warning', NULL, true, 10, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNKNOWN_PROCEDURE', '', 'interface', 'Procedure code not in the catalogue', 'Expected to fire constantly until MFN master data is loaded. Kept as info so it measures the gap instead of blocking the feed.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNKNOWN_AETITLE', '', 'interface', 'Device AE title not mapped', 'The device on a Started event has no row in aetitle_modality_map, so its utilisation will not be attributed.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNKNOWN_PERFORMER', '', 'interface', 'Performer not recognised', 'The identifier in the "by whom" field resolves to nobody known. Pending MFN staff master data.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('LOGICAL_DUPLICATE', '', 'interface', 'Logical duplicate', 'Different MSH-10, but same accession, state and event time. Usually the same event reaching us by two routes.', 'warning', NULL, true, 10, '2026-10-26 17:36:55.664974', 5.00) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('BRAYN_BUDGET_EXCEEDED', '', 'interface', 'Screening budget exceeded', 'BRAYN ran out of its inline time budget and skipped the remaining rules for this message. The message was accepted and stored; it was simply screened less thoroughly. Persistent occurrences mean the ingest path is under load.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.772801', 1.00) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('BRAYN_DEGRADED', '', 'interface', 'Screening was degraded', 'BRAYN could not complete one or more of its lookups for this message, so the rules depending on them did not run. The message was accepted and stored, but it was screened with less information than usual. A run of these points at the database, not at the messages.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.772801', 1.00) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNEXPECTED_RUNG', '', 'interface', 'Status received for a rung this site does not send', 'The ladder profile marks this rung as not expected at this install, but a message carrying it arrived. Usually means the profile needs updating rather than that anything is wrong with the message.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('UNKNOWN_RESULT_STATUS', '', 'interface', 'Result status not mapped', 'The OBX-11 (or OBR-25) value has no row in hl7_result_status_map, so the report cannot be placed on the signature ladder. The report itself still lands; only the signature rung is lost.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('OUT_OF_SEQUENCE_DELIVERY', '', 'interface', 'Messages delivered out of order', 'Share of a sender''s lifecycle messages that arrived after a later rung of the same study. The data is unaffected - the lifecycle is order-independent - but a rising rate means the sender''s queue is disturbed. Measured on the rungs marked "measure order" in the ladder profile (all rungs when none are marked).', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', 10.00) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('CONTROL_ID_REUSE', '', 'interface', 'Control ID reused with different content', 'Share of a sender''s messages carrying an MSH-10 it already used for different content. Both messages are stored and both are ingested; nothing is lost. A non-zero rate is still a sender bug worth raising with its vendor.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', 1.00) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('EXACT_REDELIVERY', '', 'interface', 'Exact redelivery', 'Share of a sender''s messages that were byte-identical redeliveries. Expected at a Mirth hub; measured, not alerted, unless a threshold is set.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('TIME_CONTRADICTION', '', 'quality', 'Timestamps contradict the lifecycle', 'A later rung of this study is stamped earlier than an earlier one - completed before arrived, signed before the exam was done. Judged on the whole lifecycle after every change; cancellation closes it, otherwise a person does. Produces negative turnaround times downstream.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('SKIPPED_RUNG', '', 'quality', 'Rung skipped', 'The study is completed but an expected arrival or start is missing. Judged on the whole lifecycle: a late-delivered rung closes it.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.brayn_rules (rule_code, modality, category, title, description, default_severity, severity, enabled, threshold_minutes, updated_at, threshold_pct) VALUES ('ORPHAN_EVENT', '', 'quality', 'No order or scheduling received', 'The study has exam events but no HIS order and no scheduling message. Closes itself when either arrives; open for good means the order feed missed it.', 'warning', NULL, true, NULL, '2026-10-26 17:36:55.664974', NULL) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (1, '', 'status', 'parsed', 'accession_number', 'ORC', 3, 1, NULL, 10, 'text', true, 'filler order number', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (2, '', 'status', 'parsed', 'accession_number', 'OBR', 3, 1, NULL, 20, 'text', true, 'fallback when ORC-3 is empty', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (3, '', 'status', 'parsed', 'event_time', 'ORC', 9, NULL, NULL, 10, 'datetime', true, 'transaction time', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (4, '', 'status', 'parsed', 'performed_by_id', 'ORC', 19, 1, NULL, 10, 'text', true, 'PROVISIONAL - ORC-19 Action By, unconfirmed', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (5, '', 'status', 'parsed', 'performed_by_name', 'ORC', 19, NULL, NULL, 10, 'name_xcn', true, 'PROVISIONAL - unconfirmed', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (6, '', 'status', 'parsed', 'aetitle', 'OBR', 21, 1, NULL, 10, 'text', true, 'PROVISIONAL - no standard field carries an AE title', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (7, '', 'status', 'parsed', 'room_name', 'PV1', 3, 2, NULL, 10, 'text', true, 'PROVISIONAL - PV1-3 component 2', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (9, '', '', 'parsed', 'patient_id', 'PID', 3, 1, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (10, '', '', 'parsed', 'procedure_code', 'OBR', 4, 1, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (11, '', '', 'parsed', 'procedure_text', 'OBR', 4, 2, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (12, '', '', 'parsed', 'patient_class', 'PV1', 2, NULL, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (13, '', '', 'parsed', 'patient_location', 'PV1', 3, 1, NULL, 10, 'text', true, 'ER detection reads this', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (14, '', 'adt', 'parsed', 'patient_name', 'PID', 5, NULL, NULL, 10, 'name_xpn', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (16, '', 'adt', 'parsed', 'sex', 'PID', 8, NULL, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (17, '', 'order', 'parsed', 'placer_order_number', 'ORC', 2, 1, NULL, 10, 'text', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (18, '', 'result', 'parsed', 'accession_number', 'OBR', 3, 1, NULL, 10, 'text', true, 'ORU joins on OBR-3', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (19, '', 'result', 'parsed', 'event_time', 'OBR', 22, NULL, NULL, 10, 'datetime', true, 'results report status change', NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (15, '', 'adt', 'parsed', 'birth_date', 'PID', 7, NULL, NULL, 10, 'birth_date', true, NULL, NULL, '2026-10-26 17:36:55.220917') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_mappings (id, sending_app, message_kind, target_kind, target_field, segment, field_index, component_index, repeat_index, priority, transform, active, notes, updated_by, updated_at) VALUES (8, '', 'status', 'parsed', 'modality', 'OBR', 24, 1, NULL, 10, 'text', true, 'diagnostic service section id', NULL, '2026-10-26 17:36:55.625076') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'accession_number', 'text', 'Accession number', 'The join key to everything. Usually ORC-3 or OBR-3.', true, 10) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'placer_order_number', 'text', 'Placer order number', 'The HIS order identity, before the RIS mints an accession. Usually ORC-2.', false, 20) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'patient_id', 'text', 'Patient ID', 'Usually PID-3 component 1.', true, 30) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'event_time', 'datetime', 'Event timestamp', 'When the transition happened. Feeds arrived/started/completed and therefore every turnaround figure.', true, 40) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'raw_order_control', 'text', 'ORC-1 order control', 'Raw value, used for status lookup.', false, 50) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'raw_order_status', 'text', 'ORC-5 order status', 'Raw value, mapped to a lifecycle state.', true, 60) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'performed_by_id', 'text', 'Performed by (ID)', 'Who did it. LDAP account at LAUMC.', false, 70) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'performed_by_name', 'text', 'Performed by (name)', NULL, false, 80) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'performed_by_role', 'text', 'Performed by (role)', 'tech / resident / radiologist.', false, 90) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'aetitle', 'text', 'Device AE title', 'The performing device. No standard HL7 field carries this; the default guess is OBR-21.', false, 100) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'room_name', 'text', 'Room name', 'Default guess is PV1-3 component 2.', false, 110) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'modality', 'text', 'Modality', 'CT, MR, US. Default OBR-24.', false, 120) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'procedure_code', 'text', 'Procedure code', NULL, false, 130) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'procedure_text', 'text', 'Procedure description', NULL, false, 140) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'patient_class', 'text', 'Patient class', 'I / O / E.', false, 150) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'patient_location', 'text', 'Patient location', 'ER detection keys on this prefix, so a wrong value hides emergency studies.', true, 160) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'patient_name', 'text', 'Patient name', NULL, false, 170) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'birth_date', 'date', 'Birth date', 'Drives age at exam. Placeholders are nulled automatically.', true, 180) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_field_targets (target_kind, target_field, data_type, label, description, is_dangerous, sort_order) VALUES ('parsed', 'sex', 'text', 'Sex', NULL, false, 190) ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_result_status_map (id, sending_app, result_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (1, '', 'W', 'signed_prelim', 120, true, 'Signed 1 at this site. Not the standard HL7 meaning of W - mapped, not assumed.', '2026-10-26 17:36:55.459247') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_result_status_map (id, sending_app, result_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (2, '', 'F', 'signed_final', 140, true, 'Signed 3 at this site - the final signature.', '2026-10-26 17:36:55.459247') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_result_status_map (id, sending_app, result_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (3, '', 'P', 'signed_prelim', 120, true, 'Standard HL7 preliminary. Seeded for senders that use it instead of W.', '2026-10-26 17:36:55.459247') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_result_status_map (id, sending_app, result_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (4, '', 'C', 'signed_final', 140, true, 'Correction to a final result. Treated as final; the corrected report replaces the original.', '2026-10-26 17:36:55.459247') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (1, '', '', 'SC', 'scheduled', 40, true, 'Scheduled - accession is minted at this point', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (2, '', '', 'AR', 'arrived', 60, true, 'Patient arrived; vendor-specific, not a standard HL7 order status', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (3, '', '', 'IP', 'started', 70, true, 'In progress - exam started on the device', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (4, '', '', 'CM', 'completed', 100, true, 'Completed - the TAT anchor the cutover moves to', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (5, '', '', 'CA', 'cancelled', -1, true, 'Cancelled', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (6, '', '', 'DC', 'cancelled', -1, true, 'Discontinued', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (7, '', '', 'ER', 'cancelled', -1, true, 'Error - sender retracted the order', '2026-10-26 17:36:54.927787') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (8, '', '', 'OC', 'cancelled', -1, true, 'Order cancelled (ORC-1, with ORC-5 empty)', '2026-10-26 17:36:55.625076') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (9, '', '', 'CR', 'cancelled', -1, true, 'Cancelled as requested (ORC-1, with ORC-5 empty)', '2026-10-26 17:36:55.625076') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (10, '', '', 'OD', 'cancelled', -1, true, 'Order discontinued (ORC-1, with ORC-5 empty)', '2026-10-26 17:36:55.625076') ON CONFLICT DO NOTHING;
INSERT INTO public.hl7_status_map (id, sending_app, order_control, order_status, canonical_state, ladder_rank, active, notes, updated_at) VALUES (11, '', '', 'DR', 'cancelled', -1, true, 'Discontinued as requested (ORC-1, with ORC-5 empty)', '2026-10-26 17:36:55.625076') ON CONFLICT DO NOTHING;

-- -- LAUMC differences -------------------------------------------------------

-- No RIS status feed yet: these checks would flag every order and every study.
UPDATE brayn_rules SET enabled = FALSE, updated_at = NOW()
 WHERE modality = ''
   AND rule_code IN ('ORPHAN_EVENT', 'SKIPPED_RUNG', 'ORPHAN_ORDER',
                     'STALLED_SCHEDULED', 'STALLED_ARRIVED', 'STALLED_STARTED');

-- BRAYN observes nothing until an admin switches it on.
INSERT INTO settings (key, value) VALUES ('brayn_enabled', 'false')
ON CONFLICT (key) DO NOTHING;
