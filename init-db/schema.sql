--
-- PostgreSQL database dump
--

\restrict KJYa84OVZKyF7b91Xu734vPoVloXFpJZizpKoynDyrhLtHSfCjFcvtSa7oZbZXi

-- Dumped from database version 14.22 (Ubuntu 14.22-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.22 (Ubuntu 14.22-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: active_sessions; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.active_sessions (
    session_id character varying NOT NULL,
    user_id integer NOT NULL,
    role character varying NOT NULL,
    ip_address character varying NOT NULL,
    login_time timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.active_sessions OWNER TO etl_user;

--
-- Name: aetitle_modality_map; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.aetitle_modality_map (
    id integer NOT NULL,
    aetitle character varying NOT NULL,
    modality character varying NOT NULL,
    daily_capacity_minutes integer DEFAULT 480
);


ALTER TABLE public.aetitle_modality_map OWNER TO etl_user;

--
-- Name: aetitle_modality_map_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.aetitle_modality_map_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.aetitle_modality_map_id_seq OWNER TO etl_user;

--
-- Name: aetitle_modality_map_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.aetitle_modality_map_id_seq OWNED BY public.aetitle_modality_map.id;


--
-- Name: db_params; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.db_params (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    db_role character varying(50) NOT NULL,
    db_type character varying(50) NOT NULL,
    conn_string text,
    host character varying(100),
    username character varying(50),
    password character varying(100),
    port integer,
    sid character varying(50),
    mode character varying(50),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.db_params OWNER TO etl_user;

--
-- Name: db_params_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.db_params_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.db_params_id_seq OWNER TO etl_user;

--
-- Name: db_params_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.db_params_id_seq OWNED BY public.db_params.id;


--
-- Name: device_exceptions; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.device_exceptions (
    id integer NOT NULL,
    aetitle character varying(50),
    exception_date date NOT NULL,
    actual_opening_minutes integer NOT NULL,
    reason character varying(255)
);


ALTER TABLE public.device_exceptions OWNER TO etl_user;

--
-- Name: device_exceptions_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.device_exceptions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.device_exceptions_id_seq OWNER TO etl_user;

--
-- Name: device_exceptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.device_exceptions_id_seq OWNED BY public.device_exceptions.id;


--
-- Name: device_weekly_schedule; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.device_weekly_schedule (
    aetitle character varying(50) NOT NULL,
    day_of_week integer NOT NULL,
    std_opening_minutes integer DEFAULT 720 NOT NULL,
    CONSTRAINT device_weekly_schedule_day_of_week_check CHECK (((day_of_week >= 0) AND (day_of_week <= 6)))
);


ALTER TABLE public.device_weekly_schedule OWNER TO etl_user;

--
-- Name: etl_didb_raw_images; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_didb_raw_images (
    raw_image_db_uid bigint NOT NULL,
    patient_db_uid bigint NOT NULL,
    study_db_uid bigint NOT NULL,
    series_db_uid bigint NOT NULL,
    study_instance_uid character varying(255) NOT NULL,
    series_instance_uid character varying(255) NOT NULL,
    image_number integer,
    last_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.etl_didb_raw_images OWNER TO etl_user;

--
-- Name: etl_didb_serieses; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_didb_serieses (
    series_db_uid bigint NOT NULL,
    study_db_uid bigint NOT NULL,
    patient_db_uid bigint,
    study_instance_uid text,
    series_instance_uid text,
    series_number integer,
    modality text,
    number_of_series_images integer,
    body_part_examined text,
    protocol_name text,
    series_description text,
    series_icon_blob_len text,
    institution_name text,
    station_name text,
    manufacturer text,
    institutional_department_name text,
    last_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.etl_didb_serieses OWNER TO etl_user;

--
-- Name: etl_didb_studies; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_didb_studies (
    study_db_uid bigint NOT NULL,
    patient_db_uid bigint NOT NULL,
    study_instance_uid text,
    accession_number text,
    study_id text,
    storing_ae text,
    study_date date,
    study_description text,
    study_body_part text,
    study_age integer,
    age_at_exam numeric(5,2),
    number_of_study_series integer,
    number_of_study_images integer,
    study_status text,
    patient_class text,
    procedure_code text,
    referring_physician_first_name text,
    referring_physician_mid_name text,
    referring_physician_last_name text,
    report_status text,
    order_status text,
    last_accessed_time timestamp without time zone,
    insert_time timestamp without time zone,
    last_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    reading_physician_first_name text,
    reading_physician_last_name text,
    reading_physician_id bigint,
    signing_physician_first_name text,
    signing_physician_last_name text,
    signing_physician_id bigint,
    study_has_report boolean DEFAULT false NOT NULL,
    rep_prelim_timestamp timestamp without time zone,
    rep_prelim_signed_by text,
    rep_transcribed_by text,
    rep_transcribed_timestamp timestamp without time zone,
    rep_final_signed_by text,
    rep_final_timestamp timestamp without time zone,
    rep_addendum_by text,
    rep_addendum_timestamp timestamp without time zone,
    rep_has_addendum boolean DEFAULT false NOT NULL,
    is_linked_study boolean DEFAULT false NOT NULL,
    patient_location character varying(3),
    last_access_time character varying(50),
    study_modality character varying(50),
    study_time character varying(50),
    date_submitted character varying(50),
    submitted_by character varying(50),
    afip character varying(50),
    last_update_time character varying(50),
    rep_study_last_composed_by character varying(50),
    rep_study_last_composed_ts character varying(50),
    rep_study_voice_rec_used character varying(50),
    lock_reason character varying(50),
    sla_deadline character varying(50),
    free_date2 character varying(50),
    operators_name character varying(50),
    performing_physicians_name character varying(50),
    rep_first_voice_rec_used_time character varying(50),
    imported_by character varying(50),
    imported_by_timestamp character varying(50),
    has_report boolean,
    has_addendum boolean,
    is_linked boolean
);


ALTER TABLE public.etl_didb_studies OWNER TO etl_user;

--
-- Name: etl_image_locations; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_image_locations (
    raw_image_db_uid bigint NOT NULL,
    source_db_uid integer,
    file_system text,
    image_size_kb integer,
    file_num integer,
    image_checksum text,
    path_type integer,
    last_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.etl_image_locations OWNER TO etl_user;

--
-- Name: etl_job_log; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_job_log (
    id integer NOT NULL,
    job_name text NOT NULL,
    status text DEFAULT 'RUNNING'::text,
    start_time timestamp without time zone DEFAULT now(),
    end_time timestamp without time zone,
    records_processed integer DEFAULT 0,
    null_alerts integer DEFAULT 0,
    rows_per_second numeric(10,2),
    error_message text,
    duration_seconds numeric(10,2)
);


ALTER TABLE public.etl_job_log OWNER TO etl_user;

--
-- Name: etl_job_log_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.etl_job_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.etl_job_log_id_seq OWNER TO etl_user;

--
-- Name: etl_job_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.etl_job_log_id_seq OWNED BY public.etl_job_log.id;


--
-- Name: etl_orders; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_orders (
    order_dbid bigint NOT NULL,
    patient_dbid text,
    study_db_uid bigint,
    visit_dbid text,
    study_instance_uid text,
    proc_id text,
    proc_text text,
    scheduled_datetime timestamp without time zone,
    order_status text,
    modality text,
    has_study boolean DEFAULT false,
    last_update timestamp without time zone DEFAULT now(),
    order_control text
);


ALTER TABLE public.etl_orders OWNER TO etl_user;

--
-- Name: etl_patient_view; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_patient_view (
    patient_db_uid bigint NOT NULL,
    id text,
    birth_date date,
    sex text,
    number_of_patient_studies integer,
    number_of_patient_series integer,
    number_of_patient_images integer,
    mdl_patient_dbid text,
    fallback_id text,
    last_update timestamp without time zone,
    age_group text,
    gender character varying(50)
);


ALTER TABLE public.etl_patient_view OWNER TO etl_user;

--
-- Name: go_live_config; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.go_live_config (
    id integer NOT NULL,
    go_live_date date NOT NULL
);


ALTER TABLE public.go_live_config OWNER TO etl_user;

--
-- Name: go_live_config_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.go_live_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.go_live_config_id_seq OWNER TO etl_user;

--
-- Name: go_live_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.go_live_config_id_seq OWNED BY public.go_live_config.id;


--
-- Name: hl7_orders; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.hl7_orders (
    id integer NOT NULL,
    message_id text NOT NULL,
    message_datetime timestamp without time zone,
    message_type text,
    patient_id text,
    patient_name text,
    date_of_birth timestamp without time zone,
    gender text,
    accession_number text,
    placer_order_number text,
    procedure_code text,
    procedure_text text,
    modality text,
    scheduled_datetime timestamp without time zone,
    ordering_physician text,
    order_status text,
    raw_message text,
    received_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.hl7_orders OWNER TO etl_user;

--
-- Name: hl7_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.hl7_orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.hl7_orders_id_seq OWNER TO etl_user;

--
-- Name: hl7_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.hl7_orders_id_seq OWNED BY public.hl7_orders.id;


--
-- Name: procedure_duration_map; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.procedure_duration_map (
    id integer NOT NULL,
    procedure_code character varying NOT NULL,
    duration_minutes integer NOT NULL,
    rvu_value numeric(10,2) DEFAULT 0.0
);


ALTER TABLE public.procedure_duration_map OWNER TO etl_user;

--
-- Name: procedurecode_duration_map_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.procedurecode_duration_map_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.procedurecode_duration_map_id_seq OWNER TO etl_user;

--
-- Name: procedurecode_duration_map_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.procedurecode_duration_map_id_seq OWNED BY public.procedure_duration_map.id;


--
-- Name: report_access_control; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.report_access_control (
    id integer NOT NULL,
    user_id integer NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    report_template_id integer NOT NULL
);


ALTER TABLE public.report_access_control OWNER TO etl_user;

--
-- Name: report_access_control_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.report_access_control_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.report_access_control_id_seq OWNER TO etl_user;

--
-- Name: report_access_control_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.report_access_control_id_seq OWNED BY public.report_access_control.id;


--
-- Name: report_derivative; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.report_derivative (
    derivative_id integer NOT NULL,
    report_id integer NOT NULL,
    dimension_id integer NOT NULL,
    sql_fragment text NOT NULL,
    operator character varying(50) NOT NULL,
    description text,
    sort_order integer DEFAULT 0
);


ALTER TABLE public.report_derivative OWNER TO etl_user;

--
-- Name: report_derivative_derivative_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.report_derivative_derivative_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.report_derivative_derivative_id_seq OWNER TO etl_user;

--
-- Name: report_derivative_derivative_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.report_derivative_derivative_id_seq OWNED BY public.report_derivative.derivative_id;


--
-- Name: report_dimension; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.report_dimension (
    dimension_id integer NOT NULL,
    report_id integer NOT NULL,
    dimension_name character varying(255) NOT NULL,
    source_table character varying(255) NOT NULL,
    source_column character varying(255) NOT NULL,
    sql_type character varying(50) NOT NULL,
    operator character varying(50) NOT NULL,
    ui_type character varying(50) NOT NULL,
    domain_table character varying(255),
    required boolean DEFAULT true,
    sort_order integer DEFAULT 0,
    fact_alias character varying(10)
);


ALTER TABLE public.report_dimension OWNER TO etl_user;

--
-- Name: report_dimension_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.report_dimension_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.report_dimension_id_seq OWNER TO etl_user;

--
-- Name: report_dimension_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.report_dimension_id_seq OWNED BY public.report_dimension.dimension_id;


--
-- Name: report_template; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.report_template (
    report_id integer NOT NULL,
    report_name character varying(255) NOT NULL,
    long_description text,
    report_sql_query text,
    required_parameters text,
    created_by_user_id integer,
    creation_date timestamp without time zone,
    visualization_type character varying(50),
    is_base boolean DEFAULT true NOT NULL
);


ALTER TABLE public.report_template OWNER TO etl_user;

--
-- Name: report_template_report_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.report_template_report_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.report_template_report_id_seq OWNER TO etl_user;

--
-- Name: report_template_report_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.report_template_report_id_seq OWNED BY public.report_template.report_id;


--
-- Name: saved_reports; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.saved_reports (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    owner_user_id integer NOT NULL,
    base_report_id integer NOT NULL,
    is_public boolean DEFAULT false NOT NULL,
    filter_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    generated_sql text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.saved_reports OWNER TO etl_user;

--
-- Name: saved_reports_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.saved_reports_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.saved_reports_id_seq OWNER TO etl_user;

--
-- Name: saved_reports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.saved_reports_id_seq OWNED BY public.saved_reports.id;


--
-- Name: settings; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.settings (
    id integer NOT NULL,
    key text NOT NULL,
    value text NOT NULL
);


ALTER TABLE public.settings OWNER TO etl_user;

--
-- Name: settings_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.settings_id_seq OWNER TO etl_user;

--
-- Name: settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.settings_id_seq OWNED BY public.settings.id;


--
-- Name: summary_storage_daily; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.summary_storage_daily (
    id integer NOT NULL,
    study_date date NOT NULL,
    modality character varying(50),
    procedure_code character varying(255),
    total_gb numeric(12,4) DEFAULT 0,
    study_count integer DEFAULT 0,
    storing_ae character varying(100)
);


ALTER TABLE public.summary_storage_daily OWNER TO etl_user;

--
-- Name: summary_storage_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.summary_storage_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.summary_storage_daily_id_seq OWNER TO etl_user;

--
-- Name: summary_storage_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.summary_storage_daily_id_seq OWNED BY public.summary_storage_daily.id;


--
-- Name: patient_portal_users; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.patient_portal_users (
    id               SERIAL PRIMARY KEY,
    mrn              VARCHAR(50) UNIQUE NOT NULL,
    full_name        VARCHAR(200),
    phone            VARCHAR(30),
    accession_number VARCHAR(100),
    username         VARCHAR(50) UNIQUE NOT NULL,
    password_plain   VARCHAR(50),
    is_active        BOOLEAN DEFAULT TRUE,
    last_login       TIMESTAMP WITHOUT TIME ZONE,
    whatsapp_sent    BOOLEAN DEFAULT FALSE,
    whatsapp_sent_at TIMESTAMP WITHOUT TIME ZONE,
    created_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

ALTER TABLE public.patient_portal_users OWNER TO etl_user;


--
-- Name: portal_config; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.portal_config (
    id           SERIAL PRIMARY KEY,
    config_key   VARCHAR(100) UNIQUE NOT NULL,
    config_value TEXT,
    description  TEXT,
    updated_at   TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

ALTER TABLE public.portal_config OWNER TO etl_user;


--
-- Name: users; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.users (
    id integer NOT NULL,
    username character varying NOT NULL,
    password_hash character varying NOT NULL,
    role character varying NOT NULL
);


ALTER TABLE public.users OWNER TO etl_user;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: etl_user
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.users_id_seq OWNER TO etl_user;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: etl_user
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: aetitle_modality_map id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.aetitle_modality_map ALTER COLUMN id SET DEFAULT nextval('public.aetitle_modality_map_id_seq'::regclass);


--
-- Name: db_params id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.db_params ALTER COLUMN id SET DEFAULT nextval('public.db_params_id_seq'::regclass);


--
-- Name: device_exceptions id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_exceptions ALTER COLUMN id SET DEFAULT nextval('public.device_exceptions_id_seq'::regclass);


--
-- Name: etl_job_log id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_job_log ALTER COLUMN id SET DEFAULT nextval('public.etl_job_log_id_seq'::regclass);


--
-- Name: go_live_config id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.go_live_config ALTER COLUMN id SET DEFAULT nextval('public.go_live_config_id_seq'::regclass);


--
-- Name: hl7_orders id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.hl7_orders ALTER COLUMN id SET DEFAULT nextval('public.hl7_orders_id_seq'::regclass);


--
-- Name: procedure_duration_map id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.procedure_duration_map ALTER COLUMN id SET DEFAULT nextval('public.procedurecode_duration_map_id_seq'::regclass);


--
-- Name: report_access_control id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control ALTER COLUMN id SET DEFAULT nextval('public.report_access_control_id_seq'::regclass);


--
-- Name: report_derivative derivative_id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_derivative ALTER COLUMN derivative_id SET DEFAULT nextval('public.report_derivative_derivative_id_seq'::regclass);


--
-- Name: report_dimension dimension_id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_dimension ALTER COLUMN dimension_id SET DEFAULT nextval('public.report_dimension_id_seq'::regclass);


--
-- Name: report_template report_id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_template ALTER COLUMN report_id SET DEFAULT nextval('public.report_template_report_id_seq'::regclass);


--
-- Name: saved_reports id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.saved_reports ALTER COLUMN id SET DEFAULT nextval('public.saved_reports_id_seq'::regclass);


--
-- Name: settings id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.settings ALTER COLUMN id SET DEFAULT nextval('public.settings_id_seq'::regclass);


--
-- Name: summary_storage_daily id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.summary_storage_daily ALTER COLUMN id SET DEFAULT nextval('public.summary_storage_daily_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: device_exceptions _ae_date_uc; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_exceptions
    ADD CONSTRAINT _ae_date_uc UNIQUE (aetitle, exception_date);


--
-- Name: summary_storage_daily _date_ae_mod_proc_uc; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.summary_storage_daily
    ADD CONSTRAINT _date_ae_mod_proc_uc UNIQUE (study_date, storing_ae, modality, procedure_code);


--
-- Name: active_sessions active_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.active_sessions
    ADD CONSTRAINT active_sessions_pkey PRIMARY KEY (session_id);


--
-- Name: aetitle_modality_map aetitle_modality_map_aetitle_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.aetitle_modality_map
    ADD CONSTRAINT aetitle_modality_map_aetitle_key UNIQUE (aetitle);


--
-- Name: aetitle_modality_map aetitle_modality_map_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.aetitle_modality_map
    ADD CONSTRAINT aetitle_modality_map_pkey PRIMARY KEY (id);


--
-- Name: db_params db_params_name_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.db_params
    ADD CONSTRAINT db_params_name_key UNIQUE (name);


--
-- Name: db_params db_params_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.db_params
    ADD CONSTRAINT db_params_pkey PRIMARY KEY (id);


--
-- Name: device_exceptions device_exceptions_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_exceptions
    ADD CONSTRAINT device_exceptions_pkey PRIMARY KEY (id);


--
-- Name: device_weekly_schedule device_weekly_schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_weekly_schedule
    ADD CONSTRAINT device_weekly_schedule_pkey PRIMARY KEY (aetitle, day_of_week);


--
-- Name: etl_image_locations etl_image_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_image_locations
    ADD CONSTRAINT etl_image_locations_pkey PRIMARY KEY (raw_image_db_uid);


--
-- Name: etl_job_log etl_job_log_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_job_log
    ADD CONSTRAINT etl_job_log_pkey PRIMARY KEY (id);


--
-- Name: etl_orders etl_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_orders
    ADD CONSTRAINT etl_orders_pkey PRIMARY KEY (order_dbid);


--
-- Name: etl_patient_view etl_patient_view_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_patient_view
    ADD CONSTRAINT etl_patient_view_pkey PRIMARY KEY (patient_db_uid);


--
-- Name: go_live_config go_live_config_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.go_live_config
    ADD CONSTRAINT go_live_config_pkey PRIMARY KEY (id);


--
-- Name: hl7_orders hl7_orders_message_id_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.hl7_orders
    ADD CONSTRAINT hl7_orders_message_id_key UNIQUE (message_id);


--
-- Name: hl7_orders hl7_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.hl7_orders
    ADD CONSTRAINT hl7_orders_pkey PRIMARY KEY (id);


--
-- Name: etl_didb_raw_images pk_etl_didb_raw_images; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_raw_images
    ADD CONSTRAINT pk_etl_didb_raw_images PRIMARY KEY (raw_image_db_uid);


--
-- Name: etl_didb_serieses pk_etl_didb_serieses; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_serieses
    ADD CONSTRAINT pk_etl_didb_serieses PRIMARY KEY (series_db_uid);


--
-- Name: etl_didb_studies pk_etl_didb_studies; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_studies
    ADD CONSTRAINT pk_etl_didb_studies PRIMARY KEY (study_db_uid);


--
-- Name: procedure_duration_map procedurecode_duration_map_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.procedure_duration_map
    ADD CONSTRAINT procedurecode_duration_map_pkey PRIMARY KEY (id);


--
-- Name: procedure_duration_map procedurecode_duration_map_procedure_code_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.procedure_duration_map
    ADD CONSTRAINT procedurecode_duration_map_procedure_code_key UNIQUE (procedure_code);


--
-- Name: report_access_control report_access_control_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control
    ADD CONSTRAINT report_access_control_pkey PRIMARY KEY (id);


--
-- Name: report_derivative report_derivative_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_derivative
    ADD CONSTRAINT report_derivative_pkey PRIMARY KEY (derivative_id);


--
-- Name: report_dimension report_dimension_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_dimension
    ADD CONSTRAINT report_dimension_pkey PRIMARY KEY (dimension_id);


--
-- Name: report_template report_template_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_template
    ADD CONSTRAINT report_template_pkey PRIMARY KEY (report_id);


--
-- Name: report_template report_template_report_name_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_template
    ADD CONSTRAINT report_template_report_name_key UNIQUE (report_name);


--
-- Name: saved_reports saved_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.saved_reports
    ADD CONSTRAINT saved_reports_pkey PRIMARY KEY (id);


--
-- Name: settings settings_key_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_key_key UNIQUE (key);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (id);


--
-- Name: summary_storage_daily summary_storage_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.summary_storage_daily
    ADD CONSTRAINT summary_storage_daily_pkey PRIMARY KEY (id);


-- Removed: summary_storage_daily_study_date_modality_procedure_code_key
-- Superseded by the correct 4-column constraint _date_ae_mod_proc_uc above.


--
-- Name: report_access_control uq_report_access_control; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control
    ADD CONSTRAINT uq_report_access_control UNIQUE (report_template_id, user_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: idx_etl_raw_img_patient_uid; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_etl_raw_img_patient_uid ON public.etl_didb_raw_images USING btree (patient_db_uid);


--
-- Name: idx_etl_raw_img_series_uid; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_etl_raw_img_series_uid ON public.etl_didb_raw_images USING btree (series_db_uid);


--
-- Name: idx_etl_raw_img_study_uid; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_etl_raw_img_study_uid ON public.etl_didb_raw_images USING btree (study_db_uid);


--
-- Name: idx_hl7_orders_accession; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_hl7_orders_accession ON public.hl7_orders USING btree (accession_number);


--
-- Name: idx_hl7_orders_patient_id; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_hl7_orders_patient_id ON public.hl7_orders USING btree (patient_id);


--
-- Name: idx_hl7_orders_received_at; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_hl7_orders_received_at ON public.hl7_orders USING btree (received_at DESC);


--
-- Name: idx_hl7_orders_scheduled; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_hl7_orders_scheduled ON public.hl7_orders USING btree (scheduled_datetime);


--
-- Name: idx_img_loc_source; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_img_loc_source ON public.etl_image_locations USING btree (source_db_uid);


--
-- Name: idx_job_log_start; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_job_log_start ON public.etl_job_log USING btree (start_time DESC);


--
-- Name: idx_orders_patient_id; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_orders_patient_id ON public.etl_orders USING btree (patient_dbid);


--
-- Name: idx_orders_sched_date; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_orders_sched_date ON public.etl_orders USING btree (scheduled_datetime);


--
-- Name: idx_orders_study_uid; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_orders_study_uid ON public.etl_orders USING btree (study_db_uid);


--
-- Name: idx_raw_img_composite_lookup; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_raw_img_composite_lookup ON public.etl_didb_raw_images USING btree (series_db_uid, study_db_uid);


--
-- Name: idx_report_dimension_report; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_report_dimension_report ON public.report_dimension USING btree (report_id, sort_order);


--
-- Name: idx_saved_reports_base; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_saved_reports_base ON public.saved_reports USING btree (base_report_id);


--
-- Name: idx_saved_reports_owner; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_saved_reports_owner ON public.saved_reports USING btree (owner_user_id);


--
-- Name: idx_storage_analytics; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_storage_analytics ON public.summary_storage_daily USING btree (study_date);


--
-- Name: idx_study_final_time; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_study_final_time ON public.etl_didb_studies USING btree (rep_final_timestamp);


--
-- Name: idx_study_prelim_time; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX idx_study_prelim_time ON public.etl_didb_studies USING btree (rep_prelim_timestamp);


--
-- Name: ix_rac_report_template_id; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX ix_rac_report_template_id ON public.report_access_control USING btree (report_template_id);


--
-- Name: ix_rac_user_id; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX ix_rac_user_id ON public.report_access_control USING btree (user_id);


--
-- Name: ix_report_access_control_user_id; Type: INDEX; Schema: public; Owner: etl_user
--

CREATE INDEX ix_report_access_control_user_id ON public.report_access_control USING btree (user_id);


--
-- Name: active_sessions active_sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.active_sessions
    ADD CONSTRAINT active_sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: device_exceptions device_exceptions_aetitle_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_exceptions
    ADD CONSTRAINT device_exceptions_aetitle_fkey FOREIGN KEY (aetitle) REFERENCES public.aetitle_modality_map(aetitle) ON DELETE CASCADE;


--
-- Name: device_weekly_schedule device_weekly_schedule_aetitle_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.device_weekly_schedule
    ADD CONSTRAINT device_weekly_schedule_aetitle_fkey FOREIGN KEY (aetitle) REFERENCES public.aetitle_modality_map(aetitle) ON DELETE CASCADE;


--
-- Name: report_access_control fk_access_control_user; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control
    ADD CONSTRAINT fk_access_control_user FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: etl_image_locations fk_location_to_raw_image; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_image_locations
    ADD CONSTRAINT fk_location_to_raw_image FOREIGN KEY (raw_image_db_uid) REFERENCES public.etl_didb_raw_images(raw_image_db_uid) ON DELETE CASCADE;


--
-- Name: report_access_control fk_rac_report; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control
    ADD CONSTRAINT fk_rac_report FOREIGN KEY (report_template_id) REFERENCES public.report_template(report_id) ON DELETE CASCADE;


--
-- Name: etl_didb_raw_images fk_raw_images_to_series; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_raw_images
    ADD CONSTRAINT fk_raw_images_to_series FOREIGN KEY (series_db_uid) REFERENCES public.etl_didb_serieses(series_db_uid) ON DELETE CASCADE;


--
-- Name: etl_didb_raw_images fk_raw_images_to_study; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_raw_images
    ADD CONSTRAINT fk_raw_images_to_study FOREIGN KEY (study_db_uid) REFERENCES public.etl_didb_studies(study_db_uid) ON DELETE CASCADE;


--
-- Name: etl_didb_serieses fk_series_to_study; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_didb_serieses
    ADD CONSTRAINT fk_series_to_study FOREIGN KEY (study_db_uid) REFERENCES public.etl_didb_studies(study_db_uid) ON DELETE CASCADE;


--
-- Name: report_derivative report_derivative_dimension_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_derivative
    ADD CONSTRAINT report_derivative_dimension_id_fkey FOREIGN KEY (dimension_id) REFERENCES public.report_dimension(dimension_id) ON DELETE CASCADE;


--
-- Name: report_derivative report_derivative_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_derivative
    ADD CONSTRAINT report_derivative_report_id_fkey FOREIGN KEY (report_id) REFERENCES public.report_template(report_id) ON DELETE CASCADE;


--
-- Name: report_dimension report_dimension_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_dimension
    ADD CONSTRAINT report_dimension_report_id_fkey FOREIGN KEY (report_id) REFERENCES public.report_template(report_id) ON DELETE CASCADE;


--
-- Name: saved_reports saved_reports_base_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.saved_reports
    ADD CONSTRAINT saved_reports_base_report_id_fkey FOREIGN KEY (base_report_id) REFERENCES public.report_template(report_id) ON DELETE CASCADE;


--
-- Name: saved_reports saved_reports_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.saved_reports
    ADD CONSTRAINT saved_reports_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict KJYa84OVZKyF7b91Xu734vPoVloXFpJZizpKoynDyrhLtHSfCjFcvtSa7oZbZXi

--
-- PostgreSQL database dump
--

\restrict KpFa2gmV9BQHG54KR4rxklK1ZwpTJSKtyYfoXhv2SStduMzJCFFthoLqZysRXMG

-- Dumped from database version 14.22 (Ubuntu 14.22-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.22 (Ubuntu 14.22-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: aetitle_modality_map; Type: TABLE DATA; Schema: public; Owner: etl_user
--

COPY public.aetitle_modality_map (id, aetitle, modality, daily_capacity_minutes) FROM stdin;
16	EBW	US	480
17	CLASSIC	CR	480
18	PLATINUM_STORE	US	480
19	INGENIA	MR	480
\.


--
-- Data for Name: go_live_config; Type: TABLE DATA; Schema: public; Owner: etl_user
--

COPY public.go_live_config (id, go_live_date) FROM stdin;
1	2025-06-01
\.


--
-- Data for Name: report_template; Type: TABLE DATA; Schema: public; Owner: etl_user
--

COPY public.report_template (report_id, report_name, long_description, report_sql_query, required_parameters, created_by_user_id, creation_date, visualization_type, is_base) FROM stdin;
22	Studies Fact	Counts all studies between the selected date range. If start date is empty, uses Go-Live Date.	SELECT\r\n    s.study_db_uid,\r\n    s.procedure_code,\r\n    s.study_date,\r\n    s.storing_ae,        \r\n    m.modality,          \r\n    s.study_status,\r\n    s.patient_db_uid,\r\n    -- Add the actual Patient ID for accurate unique counting\r\n    p.fallback_id as patient_id, \r\n    p.sex,\r\n    p.age_group,\r\n    s.last_update,\r\n    s.patient_class,\r\n    -- Create a single field for the Physician Name\r\n    TRIM(CONCAT(s.referring_physician_first_name, ' ', s.referring_physician_last_name)) as referring_physician,\r\n    s.signing_physician_first_name,    \r\n    s.signing_physician_last_name,    \r\n    s.signing_physician_id,\r\n    s.patient_location\r\nFROM etl_didb_studies s\r\nLEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle\r\nLEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid\r\nWHERE 1=1	start_date,end_date	\N	\N	table	t
27	Order base Reports	Patient-level demographics and utilization base report	SELECT\r\n    o.order_dbid,\r\n    o.order_status,\r\n    o.proc_id,\r\n    o.proc_text,\r\n    o.scheduled_datetime,\r\n    o.has_study,\r\n    s.study_date,\r\n    s.storing_ae,\r\n    s.procedure_code,\r\n    p.birth_date,\r\n    p.sex,\r\n    COALESCE(m.duration_minutes, 0) as duration_minutes\r\nFROM etl_orders o\r\nLEFT JOIN etl_didb_studies s \r\n    ON s.study_db_uid = o.study_db_uid\r\nLEFT JOIN etl_patient_view p \r\n    ON p.patient_db_uid = o.patient_dbid\r\nLEFT JOIN procedure_duration_map m \r\n    ON m.procedure_code = s.procedure_code OR m.procedure_code = o.proc_id	start_date,end_date	\N	\N	table	t
23	Patient Demographics	Base report for Patients Fact	SELECT \r\n    p.patient_db_uid, \r\n    p.birth_date, \r\n    p.sex, \r\n    p.age_group,\r\n    s.age_at_exam, \r\n    p.fallback_id,\r\n    s.study_db_uid, \r\n    s.study_date, \r\n    s.storing_ae,\r\n    m.modality,\r\n    o.order_dbid,\r\n    s.patient_class,\r\n    o.proc_id\r\nFROM etl_patient_view p\r\nLEFT JOIN etl_didb_studies s ON s.patient_db_uid = p.patient_db_uid\r\nLEFT JOIN etl_orders o ON o.patient_dbid = p.patient_db_uid\r\nLEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle\r\nWHERE 1=1	start_date,end_date	\N	\N	bar	t
25	Modality Device Fact	Calculates average turnaround time (TAT) in minutes per signing physician.	SELECT \r\n    UPPER(TRIM(s.storing_ae)) as aetitle,\r\n    COALESCE(UPPER(m.modality), 'N/A') as modality,\r\n    s.study_date,\r\n    s.patient_class,\r\n    s.patient_location,\r\n    s.rep_final_signed_by as reading_radiologist,\r\n    s.procedure_code,\r\n    -- TAT Calculation\r\n    EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date))/60 as total_tat_min,\r\n    -- Work Duration from Procedure Map (Default to 15 if missing)\r\n    COALESCE(pm.duration_minutes, 15) as proc_duration,\r\n    -- RVU Value from Procedure Map (Default to 1.0 if missing)\r\n    COALESCE(pm.rvu_value, 1.0) as rvu,\r\n    -- Base Daily Capacity from Modality Map (Default to 480 if missing)\r\n    COALESCE(m.daily_capacity_minutes, 480) as base_daily_capacity,\r\n    s.patient_db_uid as patient_id\r\nFROM etl_didb_studies s\r\nLEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))\r\nLEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))\r\nWHERE s.study_date BETWEEN :start AND :end\r\n  AND s.rep_final_timestamp IS NOT NULL\r\n  AND s.rep_final_signed_by IS NOT NULL	start_date,end_date	\N	\N	bar	t
29	Storage Calculation	Calculate the Storage usage for different Modalities, Patients, Procedures in a specific period of time. 	\r\nSELECT\r\n    study_date,\r\n    COALESCE(modality, 'N/A')            AS modality,\r\n    COALESCE(procedure_code, 'UNKNOWN')   AS procedure_code,\r\n    COALESCE(storing_ae, 'Unknown')       AS storing_ae,\r\n    SUM(total_gb)                           AS total_gb,\r\n    SUM(study_count)                        AS study_count\r\nFROM summary_storage_daily\r\nWHERE study_date BETWEEN :start AND :end\r\nGROUP BY study_date, modality, procedure_code, storing_ae\r\nORDER BY total_gb DESC\r\n	start_date,end_date	\N	\N	table	t
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: etl_user
--

COPY public.users (id, username, password_hash, role) FROM stdin;
1	admin	scrypt:32768:8:1$zIVwNzev0WSBKcqf$22eb669e34f4495927d7baa0ff3c34cf838437b3fa1b92ed40e7fe2960ed1037508b83833f08e77701efcfec18ca60536fd40752f20553d027186d8a5e5b7f5c	admin
2	viewer	scrypt:32768:8:1$HdTiXFpE7DM72FFv$e9f74f207844448e77dd6cfa6edcd387e2e5d9eb6926dc26c543ede22d92648ad8d462c25c868309450f620fe594352558281ac371c3736a0541b9a75f910721	viewer
3			
4	role	pbkdf2:sha256:1000000$0zRXA9L3LeM6JpvC$7a66740d4253b8af4734b721b6520ec451e368cb654922f16b93821a97565ea5	viewer
5	roles	pbkdf2:sha256:1000000$u9gFU5HHA8DzWA8V$266aef6a641ca282437fe23ee1bef4aea2e6f8fd04e08488f0aaece7f7ad51d4	viewer
6	administrator	pbkdf2:sha256:1000000$zyyEBxFz0AdXs9CK$015f3b344b2eb43887dbd0b7065fd5aa16f592e9dc7d2123f9a3313464119450	admin
\.


--
-- Name: aetitle_modality_map_id_seq; Type: SEQUENCE SET; Schema: public; Owner: etl_user
--

SELECT pg_catalog.setval('public.aetitle_modality_map_id_seq', 19, true);


--
-- Name: go_live_config_id_seq; Type: SEQUENCE SET; Schema: public; Owner: etl_user
--

SELECT pg_catalog.setval('public.go_live_config_id_seq', 1, true);


--
-- Name: report_template_report_id_seq; Type: SEQUENCE SET; Schema: public; Owner: etl_user
--

SELECT pg_catalog.setval('public.report_template_report_id_seq', 42, true);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: etl_user
--

SELECT pg_catalog.setval('public.users_id_seq', 6, true);


--
-- Name: user_page_permissions; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.user_page_permissions (
    id SERIAL PRIMARY KEY,
    user_id integer NOT NULL REFERENCES public.users(id),
    page_key character varying(50) NOT NULL,
    is_enabled boolean DEFAULT true
);

ALTER TABLE public.user_page_permissions OWNER TO etl_user;

--
-- Name: scheduling_entries; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.scheduling_entries (
    id SERIAL PRIMARY KEY,
    first_name text NOT NULL,
    middle_name text NOT NULL,
    last_name text NOT NULL,
    date_of_birth date NOT NULL,
    referring_physician text NOT NULL,
    patient_class text NOT NULL,
    procedure_datetime timestamp without time zone NOT NULL,
    modality_type character varying(50) NOT NULL,
    procedures jsonb NOT NULL DEFAULT '[]'::jsonb,
    third_party_approvals jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);

ALTER TABLE public.scheduling_entries OWNER TO etl_user;

--
-- PostgreSQL database dump complete
--

\unrestrict KpFa2gmV9BQHG54KR4rxklK1ZwpTJSKtyYfoXhv2SStduMzJCFFthoLqZysRXMG
