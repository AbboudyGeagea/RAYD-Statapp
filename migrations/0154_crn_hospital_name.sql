-- Migration 0154: CRN's hospital name is LAUMC's, not Mazloum's.
-- *** LAUMC BRANCH ONLY ***
--
-- Migration 0132 came from the Mazloum branch with its seed, crn_hospital_name =
-- 'Mazloum Hospital', which shows on the page a doctor opens from a CRN text. Production
-- still had it on 2026-11-15 (seen in a sample). CRN goes live this month.
--
-- Each database gets the name of the site it serves (rayd_served_site_id(), migration
-- 0153), without the "(Main)" / "(Satellite)" tag: 'LAUMC - Rizk Hospital' here,
-- 'LAUMC - Saint John' in SJH's own database. Only the Mazloum seed or an empty value is
-- replaced; a name someone typed on Admin > CRN > Settings stays.

INSERT INTO settings (key, value)
SELECT 'crn_hospital_name', BTRIM(regexp_replace(s.name, '\s*\([^)]*\)\s*$', ''))
FROM sites s WHERE s.id = rayd_served_site_id()
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
WHERE settings.value IS NULL OR BTRIM(settings.value) IN ('', 'Mazloum Hospital');
