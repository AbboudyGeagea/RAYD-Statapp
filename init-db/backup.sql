--
-- PostgreSQL database dump
--

\restrict YWbpvW60aNYCXVWKWtnTfofKC9maKUjXI0EXIQDJc2EMJkSHapaJmY3wBLELhNg

-- Dumped from database version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)

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
    patient_location character varying(3)
);


ALTER TABLE public.etl_didb_studies OWNER TO etl_user;

--
-- Name: etl_image_locations; Type: TABLE; Schema: public; Owner: etl_user
--

CREATE TABLE public.etl_image_locations (
    raw_images_db_uid bigint NOT NULL,
    source_db_uid integer,
    file_system text,
    image_size_kb integer,
    file_num integer,
    image_checksum text,
    path_type integer,
    lossy_indication text,
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
    age_group text
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
    key character varying(100) NOT NULL,
    value character varying(100) NOT NULL
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
-- Name: db_params id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.db_params ALTER COLUMN id SET DEFAULT nextval('public.db_params_id_seq'::regclass);


--
-- Name: etl_job_log id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_job_log ALTER COLUMN id SET DEFAULT nextval('public.etl_job_log_id_seq'::regclass);


--
-- Name: go_live_config id; Type: DEFAULT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.go_live_config ALTER COLUMN id SET DEFAULT nextval('public.go_live_config_id_seq'::regclass);


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
-- Name: etl_image_locations etl_image_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_image_locations
    ADD CONSTRAINT etl_image_locations_pkey PRIMARY KEY (raw_images_db_uid);


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


--
-- Name: summary_storage_daily summary_storage_daily_study_date_modality_procedure_code_key; Type: CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.summary_storage_daily
    ADD CONSTRAINT summary_storage_daily_study_date_modality_procedure_code_key UNIQUE (study_date, modality, procedure_code);


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
-- Name: report_access_control fk_access_control_user; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.report_access_control
    ADD CONSTRAINT fk_access_control_user FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: etl_image_locations fk_location_to_raw_image; Type: FK CONSTRAINT; Schema: public; Owner: etl_user
--

ALTER TABLE ONLY public.etl_image_locations
    ADD CONSTRAINT fk_location_to_raw_image FOREIGN KEY (raw_images_db_uid) REFERENCES public.etl_didb_raw_images(raw_image_db_uid) ON DELETE CASCADE;


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

\unrestrict YWbpvW60aNYCXVWKWtnTfofKC9maKUjXI0EXIQDJc2EMJkSHapaJmY3wBLELhNg

--
-- PostgreSQL database dump
--

\restrict NJ8Rt2cVOaucregayAHD7UcKIoXwXF29qXMYN4SaHcJOmbc00VSUhYn3mnhz9fb

-- Dumped from database version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)

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
-- Data for Name: db_params; Type: TABLE DATA; Schema: public; Owner: etl_user
--

COPY public.db_params (id, name, db_role, db_type, conn_string, host, username, password, port, sid, mode, created_at, updated_at) FROM stdin;
2	oracle_PACS	source	oracle	\N	10.10.11.50	sys	a1d2m7i4	1521	mst1	SYSDBA	2025-11-18 11:20:20.901226	2025-11-18 11:20:20.907357
1	etl_db	dest	postgres	postgresql://etl_user:$ecureC3ynbabe@localhost:5432/etl_db	localhost	\N	\N	\N	\N	\N	2025-11-18 11:20:20.901226	2025-11-18 11:20:20.907357
\.


--
-- Name: db_params_id_seq; Type: SEQUENCE SET; Schema: public; Owner: etl_user
--

SELECT pg_catalog.setval('public.db_params_id_seq', 2, true);


--
-- PostgreSQL database dump complete
--

\unrestrict NJ8Rt2cVOaucregayAHD7UcKIoXwXF29qXMYN4SaHcJOmbc00VSUhYn3mnhz9fb

