-- Migration 0121: radiologist TAT service-level thresholds, per patient class.
--
-- OPERATOR INSTRUCTION (2026-09-19): "for radiologist TAT we need to set the threshold
-- as 24 hours for In patients and 48 hours for out patients". Urgent/ER studies, which
-- Report 33 already breaks out as their own block, take the inpatient target (24 h) per
-- the same instruction.
--
-- Stored as settings rows rather than constants in the report code because this is
-- policy, not arithmetic: it changes without a deploy, and it differs per install. Same
-- prefixed-key convention as 'jci_threshold:<CODE>' (routes/report_35.py) and
-- 'oru_crit:<keyword>' (routes/oru_analytics.py). The unit is in the key name --
-- 'rad_tat_sla_hours' -- so a value read straight from the table is unambiguous; the
-- neighbouring jci_threshold rows are in MINUTES, and one unlabelled number next to the
-- other was an obvious way to get a 60x error later.
--
-- Read by utils/tat_sla.py, which every TAT-by-patient-class surface now goes through:
--   Report 31 (Operations — TAT by patient class)
--   Report 32 (Radiologists Performance — per-radiologist compliance)
--   Report 33 (KPI Detailed Reading — compliance line per modality x class block)
--   Report 25 (legacy combined dashboard — TAT-anchor matrix)
--
-- Clearing a value (setting it to '') is meaningful: that bucket then shows its raw
-- distribution with NO pass/fail judgement, matching how report_35 treats a JCI tier
-- with no configured threshold. DELETEing the row instead restores the code default
-- (24 / 24 / 48), so "no policy yet" and "back to the default" stay distinguishable.
--
-- Patient-class vocabulary is NOT seeded here. utils/tat_sla.py reuses the existing
-- 'pc_inpatient' / 'pc_outpatient' / 'pc_emergency' rows that routes/super_report.py
-- already reads, falling back to the same built-in defaults when they are absent --
-- adding a second vocabulary would just create two things to keep in sync.
--
-- Idempotent: ON CONFLICT DO NOTHING, so re-running never overwrites a threshold an
-- administrator has since tuned.

INSERT INTO settings (key, value) VALUES
    ('rad_tat_sla_hours:IN',  '24'),
    ('rad_tat_sla_hours:URG', '24'),
    ('rad_tat_sla_hours:OUT', '48')
ON CONFLICT (key) DO NOTHING;

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT key, value FROM settings
        WHERE key LIKE 'rad_tat_sla_hours:%'
        ORDER BY key
    LOOP
        RAISE NOTICE '0121: % = % h', r.key, COALESCE(NULLIF(r.value, ''), '(no target)');
    END LOOP;
END $$;
