-- Migration 0066: create std_site_pps_ext (RIS SITE_PPS -> per-step QA/compliance
-- extension, LAUMC).
--
-- Despite the name, SITE_PPS is NOT a hospital-site/org lookup — it's a 1:1 extension
-- of PPS (PK is PPS_KEY itself) carrying per-performed-step QA/compliance detail: film
-- reject tracking, radiation shielding/pregnancy screening, CD-burn tracking (directly
-- relevant to the earlier CD-burning/Weasis feature ask), critical-result messaging,
-- consent, complications. PPS's own site/org resolution is still unsolved — see
-- migration 0065.
--
-- 6 of the 43 source columns are EXCLUDED — free text or a name field, same category as
-- the patient-names exclusion (migration 0060):
--   HOLDER_NAME                  — a person's name
--   RAD_NOTE                     — free-text radiologist note
--   CD_BURNED_COMMENT            — free-text comment
--   RADIOLOGIST_REVIEW_COMMENTS  — free-text comment
--   DOC_PATIENT_COMPL_COMMENTS   — free-text comment
--   DOC_STAFF_COMPL_COMMENTS     — free-text comment
-- Everything else is structured (flags, counts, dates, short codes) and is pulled.
-- Most flag-shaped columns are typed TEXT rather than BOOLEAN — no sample data
-- confirmed a clean Y/N-only value set for all of them, and a bad cast would abort the
-- whole load; safer to store raw and refine the type later once real values are visible.
--
-- Created directly in the main etl_db, no FK to std_pps enforced (avoids load-order
-- coupling — same reasoning as the other PPS lookup tables, migration 0064).

CREATE TABLE IF NOT EXISTS std_site_pps_ext (
    pps_key                      BIGINT PRIMARY KEY,
    change_room_location          TEXT,
    film_reject_number             TEXT,
    film_reject_reason              TEXT,
    filmed_at_exam_time_flag         TEXT,
    films_digital_number              TEXT,
    films_used                         TEXT,
    film_retake_number                  TEXT,
    holder_dosimeter                     TEXT,
    holder_last_menstrual_date            TIMESTAMP,
    holder_shielded                        TEXT,
    patient_shielded                        TEXT,
    education                                TEXT,
    obstetrics_ac_est_due_date_1              TIMESTAMP,
    holder_pregnant                            TEXT,
    criticalresultmessage                       TEXT,  -- exact content type (flag vs message) unconfirmed
    radiologist_present                          TEXT,
    patient_waittime_indicator                    TEXT,
    extra_views_complete                           TEXT,
    number_of_projections                           TEXT,
    number_hard_copies                               TEXT,
    time_out                                          TEXT,  -- flag or timestamp — unconfirmed, stored raw
    cd_burned                                          TEXT,
    image_sent_to                                       TEXT,
    image_sent_to_2                                      TEXT,
    image_sent_to_3                                       TEXT,
    cd_burned_coll_deliv                                   TEXT,
    cd_burned_date                                          TIMESTAMP,
    cd_burned_requested_by                                   TEXT,
    consent_gained                                            TEXT,
    performing_clinician                                       TEXT,
    nrr_ind                                                     TEXT,
    assigned_to                                                  TEXT,
    into_private_folder                                           TEXT,
    doc_patient_complication                                       TEXT,
    doc_operator_complication                                       TEXT,
    assigned_to_res                                                  BIGINT,  -- looks like a resource key
    last_update                                                       TIMESTAMP NOT NULL DEFAULT NOW()
);
