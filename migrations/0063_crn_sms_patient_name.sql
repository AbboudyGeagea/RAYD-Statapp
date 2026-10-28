-- CRN SMS text, at the hospital's request (2026-10-28): a short CRN line, the patient's
-- name and the link, in one SMS of at most 160 characters.
--
-- {patient} = the patient's name as received in the order (hl7_orders.patient_name, PID-5).
-- A name too long for one SMS is shortened; the link never is (utils/crn_dispatch.py).
-- Replaces whatever text was saved: the hospital asked for this one.

INSERT INTO settings (key, value) VALUES ('crn_sms_template', 'Critical result (CRN) for {patient}: {link}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
