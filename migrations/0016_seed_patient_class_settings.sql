-- Configurable patient-class → category mapping for the super report.
-- Edit these values to match the actual patient_class codes in your PACS DB.
-- pc_inpatient  : comma-separated values that count as Inpatient
-- pc_outpatient : comma-separated values that count as Outpatient
-- pc_emergency  : comma-separated values that count as Emergency/ER
--
-- To add/change a value after deployment:
--   UPDATE settings SET value = 'I,IP,INPAT' WHERE key = 'pc_inpatient';

INSERT INTO settings (key, value) VALUES
    ('pc_inpatient',  'I,IP,INPAT,INPATIENT,INN'),
    ('pc_outpatient', 'O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY'),
    ('pc_emergency',  'E,EP,ER,EMERGENCY,URG,URGENT')
ON CONFLICT (key) DO NOTHING;
