"""
utils/worklist_catalog.py
────────────────────────────────────────────────────────────────
What a Report Composer report may name: datasets, fields, groupings, measures, columns.

    catalog ──▶ the editor's tabs and pickers (GET /worklists/api/catalog)
            └─▶ the compiler, which takes EVERY identifier from here and nothing
                from the request — a definition that names something not listed is
                refused, so a column name can never be smuggled into the SQL.

Datasets are the rpt.* views of migration 0064 (Bekaa's PACS data layer). A field
lives on every dataset whose view has its column (same name everywhere, by
construction of the views).

Field types and the filters they take:
    date    none | quick | relative (prev/next N hours/days) | fixed (from–to)
    multi   pick values (a picker of the values actually present, or a fixed list)
    text    contains | equals | starts with | does not contain
    yesno   any | yes | no
    range   from / to, numbers

A date field may name, per dataset, an index-backed DATE column holding the same day
('date_col'): the compiler then bounds that column as well, so a filter on study_at
(a computed date and time) still reads etl_didb_studies through its study_date index.
"""

TABS = [
    ('primary', 'Primary'),
    ('patient', 'Patient'),
    ('study',   'Study'),
    ('report',  'Report'),
    ('nlp',     'NLP findings'),
    ('order',   'Order'),
    ('series',  'Series'),
    ('crn',     'CRN'),
]

ST, SE, O, R, F, SG, C = 'studies', 'series', 'orders', 'reports', 'findings', 'storage', 'crn'

DATASETS = {
    ST: {
        'label': 'Studies (PACS)',
        'view': 'rpt.studies',
        'hint': 'One row per PACS study, SR removed: patient, device, procedure, technologist, '
                'radiologists, report times, its PACS order and its ORU report\'s NLP result.',
        'count_label': 'Studies',
        'default_date': 'study_at',
        'default_measure': 'count',
        'list_default': ['study_at', 'accession', 'patient_id', 'modality', 'procedure',
                         'device', 'report_stage', 'technologist'],
    },
    SE: {
        'label': 'Series (PACS)',
        'view': 'rpt.series',
        'hint': 'One row per PACS series: protocol, body part examined, station, manufacturer, '
                'images, with its study\'s date and device.',
        'count_label': 'Series',
        'default_date': 'study_at',
        'default_measure': 'count',
        'list_default': ['study_at', 'accession', 'modality', 'protocol', 'series_description',
                         'body_part', 'images', 'device'],
    },
    O: {
        'label': 'Orders (PACS)',
        'view': 'rpt.orders',
        'hint': 'One row per order in PACS (etl_orders), with its study when it was performed.',
        'count_label': 'Orders',
        'default_date': 'scheduled_at',
        'default_measure': 'count',
        'list_default': ['scheduled_at', 'order_id', 'patient_id', 'procedure', 'modality',
                         'order_status', 'performed'],
    },
    R: {
        'label': 'Reports (ORU)',
        'view': 'rpt.reports',
        'hint': 'One row per ORU report received, with the NLP result and the PACS study of the '
                'same accession.',
        'count_label': 'Reports',
        'default_date': 'report_at',
        'default_measure': 'count',
        'list_default': ['report_at', 'accession', 'radiologist', 'modality', 'procedure',
                         'critical', 'findings'],
    },
    F: {
        'label': 'NLP findings',
        'view': 'rpt.findings',
        'hint': 'One row per finding the NLP affirmed in a report (negations and history removed, '
                'benign findings left out).',
        'count_label': 'Findings',
        'default_date': 'report_at',
        'default_measure': 'count',
        'list_default': ['report_at', 'accession', 'finding', 'modality', 'procedure',
                         'radiologist'],
    },
    SG: {
        'label': 'Storage',
        'view': 'rpt.storage',
        'hint': 'Gigabytes and studies stored per day, device and procedure (the nightly storage '
                'summary).',
        'count_label': 'Rows',
        'default_date': 'study_at',
        'default_measure': 'gb',
        'list_default': ['study_date', 'modality', 'device', 'procedure_code', 'gb', 'studies'],
    },
    C: {
        'label': 'CRN notifications',
        'view': 'rpt.crn',
        'hint': 'One row per Critical Result Notification: who was told, when, and how it was '
                'acknowledged (as Admin > CRN > Report).',
        'count_label': 'Notifications',
        'default_date': 'detected_at',
        'default_measure': 'count',
        'list_default': ['detected_at', 'ref_code', 'accession', 'doctor', 'radiologist',
                         'status', 'acknowledged_how', 'ack_minutes'],
    },
}

WEEKDAYS = [(1, 'Mon'), (2, 'Tue'), (3, 'Wed'), (4, 'Thu'), (5, 'Fri'), (6, 'Sat'), (7, 'Sun')]
REPORT_STAGES = [(s, s) for s in ('Final', 'Preliminary', 'Transcribed', 'Reported', 'Not reported')]
_STUDY_DAY = {ST: 'study_date', SE: 'study_date', SG: 'study_date'}

# options: 'distinct' = the values present in the dataset (picker shows them, binds
# them); a list = fixed (value, label) pairs. group: may be used to group rows.
FIELDS = [
    # ── Primary ──
    {'id': 'modality', 'label': 'Modality', 'tab': 'primary', 'type': 'multi', 'col': 'modality',
     'datasets': {ST, SE, O, R, F, SG, C}, 'options': 'distinct', 'group': True,
     'hint': 'From the AE map where the device is mapped, else what PACS sent. In Series: the '
             'series\' own modality.'},
    {'id': 'device', 'label': 'Device (AE title)', 'tab': 'primary', 'type': 'multi',
     'col': 'device', 'datasets': {ST, SE, O, R, F, SG, C}, 'options': 'distinct', 'group': True},
    {'id': 'patient_type', 'label': 'Patient type', 'tab': 'primary', 'type': 'multi',
     'col': 'patient_type', 'datasets': {ST, O, R, F}, 'options': 'distinct', 'group': True,
     'hint': 'Inpatient, Outpatient or Emergency from the patient class codes in Settings (the '
             'Super Report\'s); other codes as PACS sent them.'},
    {'id': 'report_stage', 'label': 'Report stage', 'tab': 'primary', 'type': 'multi',
     'col': 'report_stage', 'datasets': {ST, O}, 'options': REPORT_STAGES, 'group': True,
     'hint': 'Furthest report step in PACS: Final, Preliminary, Transcribed, Reported (a report '
             'with no signature time), Not reported.'},

    # ── Patient ──
    {'id': 'patient_id', 'label': 'Patient ID', 'tab': 'patient', 'type': 'text',
     'col': 'patient_id', 'datasets': {ST, SE, O, R, F, C}},
    {'id': 'sex', 'label': 'Sex', 'tab': 'patient', 'type': 'multi', 'col': 'sex',
     'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'age', 'label': 'Age at study', 'tab': 'patient', 'type': 'range', 'col': 'age',
     'datasets': {ST}, 'unit': 'years'},
    {'id': 'age_band', 'label': 'Age band', 'tab': 'patient', 'type': 'multi', 'col': 'age_band',
     'datasets': {ST}, 'options': [('0-17', '0-17'), ('18-39', '18-39'), ('40-64', '40-64'),
                                   ('65+', '65+')], 'group': True},
    {'id': 'patient_class', 'label': 'Patient class (code)', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_class', 'datasets': {ST, O, R}, 'options': 'distinct', 'group': True},
    {'id': 'patient_location', 'label': 'Patient location', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_location', 'datasets': {ST}, 'options': 'distinct', 'group': True},

    # ── Study ──
    {'id': 'study_at', 'label': 'Study date', 'tab': 'study', 'type': 'date', 'col': 'study_at',
     'datasets': {ST, SE, O, SG}, 'date_col': _STUDY_DAY,
     'hint': 'PACS study date and time. In Orders: the date of the order\'s study.'},
    {'id': 'stored_at', 'label': 'Stored in PACS', 'tab': 'study', 'type': 'date',
     'col': 'stored_at', 'datasets': {ST}},
    {'id': 'accession', 'label': 'Accession', 'tab': 'study', 'type': 'text', 'col': 'accession',
     'datasets': {ST, SE, O, R, F, C}},
    {'id': 'procedure', 'label': 'Procedure', 'tab': 'study', 'type': 'multi', 'col': 'procedure',
     'datasets': {ST, O, R, F, C}, 'options': 'distinct', 'group': True},
    {'id': 'procedure_code', 'label': 'Procedure code', 'tab': 'study', 'type': 'multi',
     'col': 'procedure_code', 'datasets': {ST, O, R, SG}, 'options': 'distinct', 'group': True},
    {'id': 'study_description', 'label': 'Study description', 'tab': 'study', 'type': 'text',
     'col': 'study_description', 'datasets': {ST, SE}, 'group': True},
    {'id': 'body_part', 'label': 'Body part', 'tab': 'study', 'type': 'multi', 'col': 'body_part',
     'datasets': {ST, SE}, 'options': 'distinct', 'group': True,
     'hint': 'Studies: the study\'s body part. Series: the body part examined by the series.'},
    {'id': 'room', 'label': 'Room', 'tab': 'study', 'type': 'multi', 'col': 'room',
     'datasets': {ST}, 'options': 'distinct', 'group': True,
     'hint': 'The device\'s room in the AE map (Modality / Procedures).'},
    {'id': 'pacs_modality', 'label': 'Modality as PACS sent', 'tab': 'study', 'type': 'multi',
     'col': 'pacs_modality', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'technologist', 'label': 'Technologist', 'tab': 'study', 'type': 'multi',
     'col': 'technologist', 'datasets': {ST}, 'options': 'distinct', 'group': True,
     'hint': 'The DICOM Operators\' Name on the study, as the modality wrote it.'},
    {'id': 'performing_physician', 'label': 'Performing physician', 'tab': 'study',
     'type': 'multi', 'col': 'performing_physician', 'datasets': {ST}, 'options': 'distinct',
     'group': True},
    {'id': 'shift', 'label': 'Shift', 'tab': 'study', 'type': 'multi', 'col': 'shift',
     'datasets': {ST}, 'options': [('Morning', 'Morning'), ('Afternoon', 'Afternoon'),
                                   ('Night', 'Night')], 'group': True,
     'hint': 'By the hour of the study time, with the shift hours set on Report 25.'},
    {'id': 'weekday', 'label': 'Weekday', 'tab': 'study', 'type': 'multi', 'col': 'weekday',
     'datasets': {ST, SE, O}, 'options': WEEKDAYS, 'group': True},
    {'id': 'week_of_month', 'label': 'Week of month', 'tab': 'study', 'type': 'multi',
     'col': 'week_of_month', 'datasets': {ST},
     'options': [(1, '1 (days 1–7)'), (2, '2'), (3, '3'), (4, '4'), (5, '5')], 'group': True},
    {'id': 'hour', 'label': 'Hour', 'tab': 'study', 'type': 'range', 'col': 'hour',
     'datasets': {ST, SE, O}, 'unit': '0–23', 'group': True,
     'hint': 'Hour of the study time (Orders: of the scheduled time). Empty when PACS has no time.'},
    {'id': 'images', 'label': 'Images', 'tab': 'study', 'type': 'range', 'col': 'images',
     'datasets': {ST, SE}, 'unit': 'images'},
    {'id': 'series_count', 'label': 'Series in the study', 'tab': 'study', 'type': 'range',
     'col': 'series', 'datasets': {ST}, 'unit': 'series'},
    {'id': 'study_status', 'label': 'PACS study status', 'tab': 'study', 'type': 'multi',
     'col': 'study_status', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'linked', 'label': 'Linked study', 'tab': 'study', 'type': 'yesno', 'col': 'linked',
     'datasets': {ST}},
    {'id': 'store_delay_minutes', 'label': 'Study time → stored in PACS (min)', 'tab': 'study',
     'type': 'range', 'col': 'store_delay_minutes', 'datasets': {ST}, 'unit': 'minutes'},

    # ── Report ──
    {'id': 'has_report', 'label': 'Has a report', 'tab': 'report', 'type': 'yesno',
     'col': 'has_report', 'datasets': {ST}},
    {'id': 'report_status', 'label': 'PACS report status', 'tab': 'report', 'type': 'multi',
     'col': 'report_status', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'final_signed_at', 'label': 'Final signature date', 'tab': 'report', 'type': 'date',
     'col': 'final_signed_at', 'datasets': {ST, O}},
    {'id': 'prelim_signed_at', 'label': 'Preliminary signature date', 'tab': 'report',
     'type': 'date', 'col': 'prelim_signed_at', 'datasets': {ST}},
    {'id': 'reading_radiologist', 'label': 'Reading radiologist', 'tab': 'report',
     'type': 'multi', 'col': 'reading_radiologist', 'datasets': {ST}, 'options': 'distinct',
     'group': True, 'hint': 'Name variants merged by the physician aliases (Modality / Procedures).'},
    {'id': 'signing_radiologist', 'label': 'Signing radiologist', 'tab': 'report',
     'type': 'multi', 'col': 'signing_radiologist', 'datasets': {ST}, 'options': 'distinct',
     'group': True, 'hint': 'Name variants merged by the physician aliases (Modality / Procedures).'},
    {'id': 'radiologist', 'label': 'Radiologist (report)', 'tab': 'report', 'type': 'multi',
     'col': 'radiologist', 'datasets': {R, F, C}, 'options': 'distinct', 'group': True,
     'hint': 'Reports and findings: the physician ID on the ORU report. CRN: the signing '
             'radiologist.'},
    {'id': 'referring', 'label': 'Referring physician', 'tab': 'report', 'type': 'text',
     'col': 'referring', 'datasets': {ST, O}, 'group': True,
     'hint': 'First and last name as PACS has them; "EXTERNAL DOC" is shown as External referrals.'},
    {'id': 'report_tat_hours', 'label': 'Report turnaround (hours)', 'tab': 'report',
     'type': 'range', 'col': 'report_tat_hours', 'datasets': {ST, R}, 'unit': 'hours',
     'hint': 'Images stored in PACS → final signature (Reports: → the ORU report time), as the '
             'Super Report measures it.'},
    {'id': 'prelim_tat_hours', 'label': 'Preliminary turnaround (hours)', 'tab': 'report',
     'type': 'range', 'col': 'prelim_tat_hours', 'datasets': {ST}, 'unit': 'hours'},
    {'id': 'has_addendum', 'label': 'Has an addendum', 'tab': 'report', 'type': 'yesno',
     'col': 'has_addendum', 'datasets': {ST}},
    {'id': 'report_at', 'label': 'Report date', 'tab': 'report', 'type': 'date',
     'col': 'report_at', 'datasets': {R, F}},
    {'id': 'oru_report', 'label': 'ORU report received', 'tab': 'report', 'type': 'yesno',
     'col': 'oru_report', 'datasets': {ST}},
    {'id': 'has_impression', 'label': 'Has an impression section', 'tab': 'report',
     'type': 'yesno', 'col': 'has_impression', 'datasets': {R}},
    {'id': 'report_words', 'label': 'Report length (words)', 'tab': 'report', 'type': 'range',
     'col': 'report_words', 'datasets': {R}, 'unit': 'words'},

    # ── NLP findings ──
    {'id': 'critical', 'label': 'Critical finding (NLP)', 'tab': 'nlp', 'type': 'yesno',
     'col': 'critical', 'datasets': {ST, R}},
    {'id': 'findings', 'label': 'Findings (NLP)', 'tab': 'nlp', 'type': 'text',
     'col': 'findings', 'datasets': {ST, R},
     'hint': 'The NLP findings of the report, comma-separated: use "contains", e.g. Pneumothorax.'},
    {'id': 'finding', 'label': 'Finding', 'tab': 'nlp', 'type': 'multi', 'col': 'finding',
     'datasets': {F}, 'options': 'distinct', 'group': True},
    {'id': 'finding_count', 'label': 'Number of findings', 'tab': 'nlp', 'type': 'range',
     'col': 'finding_count', 'datasets': {R}, 'unit': 'findings'},
    {'id': 'analysed', 'label': 'Analysed by the NLP', 'tab': 'nlp', 'type': 'yesno',
     'col': 'analysed', 'datasets': {R}},

    # ── Order ──
    {'id': 'scheduled_at', 'label': 'Scheduled date', 'tab': 'order', 'type': 'date',
     'col': 'scheduled_at', 'datasets': {ST, O},
     'hint': 'PACS scheduled time, else the order time.'},
    {'id': 'order_status', 'label': 'Order status', 'tab': 'order', 'type': 'multi',
     'col': 'order_status', 'datasets': {ST, O}, 'options': 'distinct', 'group': True},
    {'id': 'order_control', 'label': 'Order control', 'tab': 'order', 'type': 'multi',
     'col': 'order_control', 'datasets': {O}, 'options': 'distinct', 'group': True},
    {'id': 'performed', 'label': 'Performed (has a study)', 'tab': 'order', 'type': 'yesno',
     'col': 'performed', 'datasets': {O}},
    {'id': 'cancelled', 'label': 'Cancelled', 'tab': 'order', 'type': 'yesno', 'col': 'cancelled',
     'datasets': {O}, 'hint': 'Order status CA / DC, or order control CA / OC / DC / OD.'},
    {'id': 'start_delay_minutes', 'label': 'Scheduled → study time (min)', 'tab': 'order',
     'type': 'range', 'col': 'start_delay_minutes', 'datasets': {ST, O}, 'unit': 'minutes',
     'hint': 'Negative when the study started before its scheduled time.'},

    # ── Series ──
    {'id': 'study_modality', 'label': 'Study modality', 'tab': 'series', 'type': 'multi',
     'col': 'study_modality', 'datasets': {SE}, 'options': 'distinct', 'group': True},
    {'id': 'protocol', 'label': 'Protocol', 'tab': 'series', 'type': 'multi', 'col': 'protocol',
     'datasets': {SE}, 'options': 'distinct', 'group': True},
    {'id': 'series_description', 'label': 'Series description', 'tab': 'series', 'type': 'text',
     'col': 'series_description', 'datasets': {SE}, 'group': True},
    {'id': 'station', 'label': 'Station name', 'tab': 'series', 'type': 'multi', 'col': 'station',
     'datasets': {SE}, 'options': 'distinct', 'group': True},
    {'id': 'manufacturer', 'label': 'Manufacturer', 'tab': 'series', 'type': 'multi',
     'col': 'manufacturer', 'datasets': {SE}, 'options': 'distinct', 'group': True},
    {'id': 'institution', 'label': 'Institution', 'tab': 'series', 'type': 'multi',
     'col': 'institution', 'datasets': {SE}, 'options': 'distinct', 'group': True},
    {'id': 'department', 'label': 'Department', 'tab': 'series', 'type': 'multi',
     'col': 'department', 'datasets': {SE}, 'options': 'distinct', 'group': True},

    # ── CRN ──
    {'id': 'crn', 'label': 'CRN sent', 'tab': 'crn', 'type': 'yesno', 'col': 'crn',
     'datasets': {ST, R}},
    {'id': 'detected_at', 'label': 'Detected', 'tab': 'crn', 'type': 'date', 'col': 'detected_at',
     'datasets': {C}},
    {'id': 'crn_status', 'label': 'CRN status', 'tab': 'crn', 'type': 'multi', 'col': 'status',
     'datasets': {C}, 'options': 'distinct', 'group': True},
    {'id': 'acknowledged', 'label': 'Acknowledged', 'tab': 'crn', 'type': 'yesno',
     'col': 'acknowledged', 'datasets': {C}},
    {'id': 'acknowledged_how', 'label': 'How acknowledged', 'tab': 'crn', 'type': 'multi',
     'col': 'acknowledged_how', 'datasets': {C},
     'options': [(s, s) for s in ('first SMS', 'after resend', 'fallback contact', 'by phone',
                                  'not acknowledged')], 'group': True},
    {'id': 'overdue', 'label': 'Went overdue', 'tab': 'crn', 'type': 'yesno', 'col': 'overdue',
     'datasets': {C}},
    {'id': 'ack_minutes', 'label': 'Time to acknowledge (min)', 'tab': 'crn', 'type': 'range',
     'col': 'ack_minutes', 'datasets': {C}, 'unit': 'minutes'},
    {'id': 'doctor', 'label': 'Doctor notified', 'tab': 'crn', 'type': 'multi', 'col': 'doctor',
     'datasets': {C}, 'options': 'distinct', 'group': True},
]
FIELD = {f['id']: f for f in FIELDS}

DATE_BUCKETS = [('day', 'day'), ('week', 'week'), ('month', 'month'), ('quarter', 'quarter'),
                ('year', 'year')]

QUICK_DATES = [
    ('today', 'Today'), ('yesterday', 'Yesterday'), ('this_week', 'This week'),
    ('last_week', 'Last week'), ('last_7_days', 'Last 7 days'), ('last_30_days', 'Last 30 days'),
    ('this_month', 'This month'), ('last_month', 'Last month'),
    ('last_3_months', 'Last 3 complete months'), ('this_year', 'This year'),
    ('last_year', 'Last year'),
]


def _median(col, digits):
    return (f'round((percentile_cont(0.5) WITHIN GROUP (ORDER BY d.{col}))::numeric, {digits})')


# sql runs over the view aliased d. additive: may be summed into a Total.
MEASURES = {
    'count':              {'label': None, 'sql': 'COUNT(*)', 'additive': True,
                           'datasets': {ST, SE, O, R, F, C}},
    'patients':           {'label': 'Patients', 'sql': 'COUNT(DISTINCT d.patient_id)',
                           'additive': False, 'datasets': {ST, SE, O, R, F, C}},
    'studies':            {'label': 'Studies', 'sql': 'COUNT(DISTINCT d.study_uid)',
                           'additive': False, 'datasets': {SE, O}},
    'reports':            {'label': 'Reports', 'sql': 'COUNT(DISTINCT d.report_id)',
                           'additive': False, 'datasets': {F}},
    'reported':           {'label': 'With a report', 'sql': 'COUNT(*) FILTER (WHERE d.has_report)',
                           'additive': True, 'datasets': {ST}},
    'not_reported':       {'label': 'Without a report',
                           'sql': 'COUNT(*) FILTER (WHERE NOT d.has_report)', 'additive': True,
                           'datasets': {ST}},
    'final':              {'label': 'Final signed',
                           'sql': "COUNT(*) FILTER (WHERE d.report_stage = 'Final')",
                           'additive': True, 'datasets': {ST, O}},
    'addenda':            {'label': 'With an addendum',
                           'sql': 'COUNT(*) FILTER (WHERE d.has_addendum)', 'additive': True,
                           'datasets': {ST}},
    'critical':           {'label': 'Critical (NLP)', 'sql': 'COUNT(*) FILTER (WHERE d.critical)',
                           'additive': True, 'datasets': {ST, R}},
    'crn':                {'label': 'CRN sent', 'sql': 'COUNT(*) FILTER (WHERE d.crn)',
                           'additive': True, 'datasets': {ST, R}},
    'images':             {'label': 'Images', 'sql': 'SUM(d.images)', 'additive': True,
                           'datasets': {ST, SE}},
    'median_report_tat':  {'label': 'Median report TAT (h)', 'sql': _median('report_tat_hours', 2),
                           'additive': False, 'datasets': {ST, R}},
    'avg_report_tat':     {'label': 'Average report TAT (h)',
                           'sql': 'round(AVG(d.report_tat_hours)::numeric, 2)', 'additive': False,
                           'datasets': {ST, R}},
    'median_prelim_tat':  {'label': 'Median preliminary TAT (h)',
                           'sql': _median('prelim_tat_hours', 2), 'additive': False,
                           'datasets': {ST}},
    'median_store_delay': {'label': 'Median study → PACS (min)',
                           'sql': _median('store_delay_minutes', 1), 'additive': False,
                           'datasets': {ST}},
    'median_start_delay': {'label': 'Median scheduled → study (min)',
                           'sql': _median('start_delay_minutes', 1), 'additive': False,
                           'datasets': {ST, O}},
    'performed':          {'label': 'Performed', 'sql': 'COUNT(*) FILTER (WHERE d.performed)',
                           'additive': True, 'datasets': {O}},
    'not_performed':      {'label': 'Not performed',
                           'sql': 'COUNT(*) FILTER (WHERE NOT d.performed AND NOT d.cancelled)',
                           'additive': True, 'datasets': {O}},
    'cancelled':          {'label': 'Cancelled', 'sql': 'COUNT(*) FILTER (WHERE d.cancelled)',
                           'additive': True, 'datasets': {O}},
    'avg_age':            {'label': 'Average age', 'sql': 'round(AVG(d.age)::numeric, 1)',
                           'additive': False, 'datasets': {ST}},
    'gb':                 {'label': 'Storage (GB)', 'sql': 'round(SUM(d.gb)::numeric, 2)',
                           'additive': True, 'datasets': {SG}},
    'stored_studies':     {'label': 'Studies stored', 'sql': 'SUM(d.studies)', 'additive': True,
                           'datasets': {SG}},
    'acknowledged':       {'label': 'Acknowledged',
                           'sql': 'COUNT(*) FILTER (WHERE d.acknowledged)', 'additive': True,
                           'datasets': {C}},
    'overdue':            {'label': 'Went overdue', 'sql': 'COUNT(*) FILTER (WHERE d.overdue)',
                           'additive': True, 'datasets': {C}},
    'median_ack_minutes': {'label': 'Median time to acknowledge (min)',
                           'sql': _median('ack_minutes', 1), 'additive': False, 'datasets': {C}},
}

# Column labels, shared by every dataset that has the column.
LABELS = {
    'study_uid': 'Study UID', 'series_uid': 'Series UID', 'order_id': 'Order ID',
    'report_id': 'Report ID', 'crn_id': 'CRN ID', 'ref_code': 'CRN reference',
    'accession': 'Accession', 'patient_id': 'Patient ID', 'sex': 'Sex', 'age': 'Age',
    'age_band': 'Age band', 'patient_class': 'Patient class', 'patient_type': 'Patient type',
    'patient_location': 'Location', 'modality': 'Modality', 'pacs_modality': 'PACS modality',
    'study_modality': 'Study modality', 'device': 'Device', 'device_name': 'Device name',
    'room': 'Room', 'procedure_code': 'Procedure code', 'procedure': 'Procedure',
    'study_description': 'Study description', 'body_part': 'Body part',
    'study_status': 'PACS study status', 'report_status': 'PACS report status',
    'order_status': 'Order status', 'order_control': 'Order control', 'study_at': 'Study date',
    'study_date': 'Study day', 'hour': 'Hour', 'shift': 'Shift', 'weekday': 'Weekday',
    'week_of_month': 'Week of month', 'stored_at': 'Stored in PACS', 'scheduled_at': 'Scheduled',
    'scheduled_date': 'Scheduled day', 'transcribed_at': 'Transcribed',
    'prelim_signed_at': 'Prelim signed', 'final_signed_at': 'Final signed',
    'addendum_at': 'Addendum', 'report_stage': 'Report stage', 'has_report': 'Report',
    'has_addendum': 'Addendum', 'linked': 'Linked', 'technologist': 'Technologist',
    'performing_physician': 'Performing physician', 'reading_radiologist': 'Reading radiologist',
    'signing_radiologist': 'Signing radiologist', 'radiologist': 'Radiologist',
    'referring': 'Referring', 'images': 'Images', 'series': 'Series',
    'report_tat_hours': 'Report TAT (h)', 'prelim_tat_hours': 'Prelim TAT (h)',
    'store_delay_minutes': 'Study → PACS (min)', 'start_delay_minutes': 'Scheduled → study (min)',
    'oru_report': 'ORU report', 'critical': 'Critical', 'findings': 'Findings (NLP)', 'crn': 'CRN',
    'series_number': 'Series no.', 'series_description': 'Series description',
    'protocol': 'Protocol', 'station': 'Station', 'manufacturer': 'Manufacturer',
    'institution': 'Institution', 'department': 'Department', 'performed': 'Performed',
    'cancelled': 'Cancelled', 'report_at': 'Reported', 'report_date': 'Report day',
    'received_at': 'Received', 'analysed': 'NLP analysed', 'finding_count': 'Findings',
    'has_impression': 'Impression', 'report_words': 'Words', 'finding': 'Finding',
    'gb': 'GB', 'studies': 'Studies', 'doctor_code': 'Doctor code', 'doctor': 'Doctor',
    'status': 'CRN status', 'detected_at': 'Detected', 'detected_date': 'Detected day',
    'report_received_at': 'Report received', 'first_sent_at': 'First SMS',
    'acknowledged_at': 'Acknowledged at', 'acknowledged': 'Acknowledged',
    'ack_minutes': 'To acknowledge (min)', 'overdue': 'Overdue', 'resent': 'Resent',
    'fallback': 'Fallback', 'by_phone': 'By phone', 'acknowledged_how': 'How acknowledged',
}

# Columns a List may show, per dataset, in the view's order. Every column of each view.
_COLUMN_IDS = {
    ST: ['study_at', 'accession', 'patient_id', 'sex', 'age', 'age_band', 'patient_class',
         'patient_type', 'patient_location', 'modality', 'pacs_modality', 'device', 'device_name',
         'room', 'procedure_code', 'procedure', 'study_description', 'body_part', 'study_status',
         'report_status', 'order_status', 'study_date', 'hour', 'shift', 'weekday',
         'week_of_month', 'stored_at', 'scheduled_at', 'transcribed_at', 'prelim_signed_at',
         'final_signed_at', 'addendum_at', 'report_stage', 'has_report', 'has_addendum', 'linked',
         'technologist', 'performing_physician', 'reading_radiologist', 'signing_radiologist',
         'referring', 'images', 'series', 'report_tat_hours', 'prelim_tat_hours',
         'store_delay_minutes', 'start_delay_minutes', 'oru_report', 'critical', 'findings',
         'crn', 'study_uid'],
    SE: ['study_at', 'accession', 'patient_id', 'study_date', 'hour', 'weekday', 'modality',
         'study_modality', 'device', 'series_number', 'series_description', 'protocol',
         'body_part', 'images', 'station', 'manufacturer', 'institution', 'department',
         'study_description', 'series_uid', 'study_uid'],
    O: ['scheduled_at', 'order_id', 'accession', 'patient_id', 'patient_class', 'patient_type',
        'procedure_code', 'procedure', 'modality', 'device', 'scheduled_date', 'hour', 'weekday',
        'order_status', 'order_control', 'performed', 'cancelled', 'study_at', 'report_stage',
        'final_signed_at', 'referring', 'start_delay_minutes', 'study_uid'],
    R: ['report_at', 'accession', 'patient_id', 'radiologist', 'modality', 'procedure',
        'procedure_code', 'device', 'patient_class', 'patient_type', 'report_date', 'received_at',
        'study_date', 'stored_at', 'report_tat_hours', 'analysed', 'critical', 'finding_count',
        'findings', 'has_impression', 'report_words', 'crn', 'report_id'],
    F: ['report_at', 'accession', 'patient_id', 'finding', 'radiologist', 'modality',
        'procedure', 'device', 'patient_type', 'report_date', 'report_id'],
    SG: ['study_date', 'study_at', 'modality', 'device', 'procedure_code', 'gb', 'studies'],
    C: ['detected_at', 'ref_code', 'accession', 'patient_id', 'radiologist', 'doctor_code',
        'doctor', 'status', 'detected_date', 'report_received_at', 'first_sent_at',
        'acknowledged_at', 'acknowledged', 'ack_minutes', 'overdue', 'resent', 'fallback',
        'by_phone', 'acknowledged_how', 'modality', 'procedure', 'device', 'crn_id'],
}
COLUMNS = {ds: [(c, LABELS[c]) for c in cols] for ds, cols in _COLUMN_IDS.items()}


# ── The SQL reference ─────────────────────────────────────────────────────────
#
# What a hand-written worklist may query, column by column. EVERY column of every
# rpt view is described here — tests/test_worklists.py compares this against the
# views themselves, so a column added to a view without a description fails the
# suite rather than reaching an author undocumented. Meanings follow the view
# definitions in migration 0064.

_MODALITY = ('Modality: from the AE map when the device is mapped, else the modality PACS sent. '
             'SR is left out.')
_PATIENT_TYPE = ('patient_class in words: Inpatient, Outpatient or Emergency by the codes in '
                 'Settings (pc_inpatient / pc_outpatient / pc_emergency, as the Super Report); '
                 'other codes as sent.')
_STUDY_AT = ('The study\'s date and time: PACS study date plus the DICOM study time. Midnight '
             'when PACS has no time (then hour is empty).')
_TAT = ('Report turnaround in hours: images stored in PACS (insert_time) to the final '
        'signature, as the Super Report measures it.')

COLUMN_DOCS = {
    ST: {
        'study_uid':         'PACS study key (etl_didb_studies.study_db_uid). One row per study.',
        'accession':         'Accession number.',
        'patient_id':        'Patient ID (MRN) from the PACS patient record.',
        'sex':               'Sex from the PACS patient record: M, F, O ...',
        'age':               'Age in whole years at the study: PACS age at exam, else from the '
                             'birth date.',
        'age_band':          'Age group: 0-17, 18-39, 40-64 or 65+.',
        'patient_class':     'Patient class code as PACS has it (I, O, E or a site code).',
        'patient_type':      _PATIENT_TYPE,
        'patient_location':  'Patient location code (e.g. ER).',
        'modality':          _MODALITY,
        'pacs_modality':     'The modality PACS sent for the study, before the AE map.',
        'device':            'AE title of the device that stored the study.',
        'device_name':       'Display name of the device in the AE map.',
        'room':              'The device\'s room in the AE map.',
        'procedure_code':    'Procedure code on the study.',
        'procedure':         'Procedure: study description, else the PACS order\'s procedure '
                             'text, else the code.',
        'study_description': 'DICOM study description.',
        'body_part':         'Body part of the study, upper case.',
        'study_status':      'PACS study status as sent.',
        'report_status':     'PACS report status as sent.',
        'order_status':      'PACS order status on the study as sent.',
        'study_at':          _STUDY_AT,
        'study_date':        'PACS study date, without time. Indexed: the fastest date filter.',
        'hour':              'Hour of the study time, 0-23. Empty when PACS has no time.',
        'shift':             'Morning, Afternoon or Night by the hour of the study time, with the '
                             'shift hours set on Report 25.',
        'weekday':           'Weekday of the study date: 1 = Monday ... 7 = Sunday.',
        'week_of_month':     'Week of the month: 1 = days 1-7, 2 = days 8-14, ... 5 = days 29-31.',
        'stored_at':         'When PACS stored the study (insert_time).',
        'scheduled_at':      'Scheduled time of the study\'s PACS order (else its order time).',
        'transcribed_at':    'When the report was transcribed.',
        'prelim_signed_at':  'Preliminary signature of the report.',
        'final_signed_at':   'Final signature of the report.',
        'addendum_at':       'When an addendum was added.',
        'report_stage':      'Furthest report step: Final, Preliminary, Transcribed, Reported '
                             '(a report with no signature time) or Not reported.',
        'has_report':        'true when PACS has a report for the study.',
        'has_addendum':      'true when the report has an addendum.',
        'linked':            'true for a PACS linked study.',
        'technologist':      'DICOM Operators\' Name on the study ("^" read as a space).',
        'performing_physician': 'DICOM Performing Physician\'s Name on the study.',
        'reading_radiologist': 'Reading physician (first + last name), merged by the physician '
                               'aliases.',
        'signing_radiologist': 'Signing physician (first + last name, else the final signer '
                               'PACS recorded), merged by the physician aliases.',
        'referring':         'Referring physician (first + last name); EXTERNAL DOC is '
                             '"External referrals".',
        'images':            'Number of images in the study.',
        'series':            'Number of series in the study.',
        'report_tat_hours':  _TAT,
        'prelim_tat_hours':  'Images stored in PACS to the preliminary signature, in hours.',
        'store_delay_minutes': 'Study time to stored in PACS, in minutes (empty without a time).',
        'start_delay_minutes': 'Scheduled time to study time, in minutes; negative when early.',
        'oru_report':        'true when an ORU report with this accession was received.',
        'critical':          'true when the NLP found a critical finding in an ORU report of '
                             'the study.',
        'findings':          'NLP findings of the study\'s ORU reports, comma-separated.',
        'crn':               'true when a Critical Result Notification was raised for the '
                             'accession.',
    },
    SE: {
        'series_uid':        'PACS series key. One row per series.',
        'study_uid':         'PACS study key of the series\' study.',
        'accession':         'Accession number of the study.',
        'patient_id':        'Patient ID (MRN).',
        'study_at':          _STUDY_AT,
        'study_date':        'PACS study date, without time. Indexed.',
        'hour':              'Hour of the study time, 0-23.',
        'weekday':           'Weekday of the study date: 1 = Monday ... 7 = Sunday.',
        'modality':          'The series\' own modality (DICOM). SR series are left out.',
        'study_modality':    'The study\'s modality: AE map first, else PACS.',
        'device':            'AE title of the device that stored the study.',
        'series_number':     'DICOM series number.',
        'series_description': 'DICOM series description.',
        'protocol':          'DICOM protocol name.',
        'body_part':         'DICOM body part examined, upper case.',
        'images':            'Number of images in the series.',
        'station':           'DICOM station name of the device.',
        'manufacturer':      'DICOM manufacturer of the device.',
        'institution':       'DICOM institution name.',
        'department':        'DICOM institutional department name.',
        'study_description': 'DICOM description of the series\' study.',
    },
    O: {
        'order_id':          'PACS order key (etl_orders.order_dbid). One row per order.',
        'study_uid':         'PACS study key of the order\'s study, when it was performed.',
        'accession':         'Accession number of the order\'s study.',
        'patient_id':        'Patient ID: the study\'s patient, else the PACS patient the order '
                             'names.',
        'patient_class':     'Patient class code of the order\'s study.',
        'patient_type':      _PATIENT_TYPE,
        'procedure_code':    'Procedure code on the order.',
        'procedure':         'Procedure text on the order, else the study description, else '
                             'the code.',
        'modality':          'Modality: AE map of the study\'s device, else the study\'s, else '
                             'the order\'s.',
        'device':            'AE title of the device that stored the order\'s study.',
        'scheduled_at':      'PACS scheduled time, else the order time.',
        'scheduled_date':    'scheduled_at as a date without time.',
        'hour':              'Hour of scheduled_at, 0-23.',
        'weekday':           'Weekday of scheduled_at: 1 = Monday ... 7 = Sunday.',
        'order_status':      'PACS order status as sent (HL7 ORC-5 style: SC, IP, CM, CA ...).',
        'order_control':     'Order control code as sent (HL7 ORC-1 style: NW, XO, CA ...).',
        'performed':         'true when the order has a study in PACS.',
        'cancelled':         'true when the order status is CA or DC, or the order control is '
                             'CA, OC, DC or OD.',
        'study_at':          'Date and time of the order\'s study (empty until performed).',
        'report_stage':      'Report stage of the order\'s study (empty until performed).',
        'final_signed_at':   'Final signature of the study\'s report.',
        'referring':         'Referring physician of the order\'s study.',
        'start_delay_minutes': 'Scheduled time to study time, in minutes; negative when early.',
    },
    R: {
        'report_id':         'Row number of the ORU report in RAYD. One row per report.',
        'accession':         'Accession number the report is for.',
        'patient_id':        'Patient ID from the report, else from the PACS study.',
        'radiologist':       'Physician ID on the ORU report.',
        'modality':          'Modality of the PACS study (AE map first), else the report\'s.',
        'procedure':         'Procedure name on the report, else the study description, else '
                             'the code.',
        'procedure_code':    'Procedure code on the report.',
        'device':            'AE title of the PACS study\'s device.',
        'patient_class':     'Patient class code of the PACS study.',
        'patient_type':      _PATIENT_TYPE,
        'report_at':         'Report time the sender put on the report, else when RAYD received '
                             'it.',
        'report_date':       'report_at as a date without time.',
        'received_at':       'When RAYD received the report.',
        'study_date':        'PACS study date of the report\'s study.',
        'stored_at':         'When PACS stored the study.',
        'report_tat_hours':  'Images stored in PACS to report_at, in hours.',
        'analysed':          'true once the NLP worker has analysed the report.',
        'critical':          'true when the NLP found a critical finding.',
        'finding_count':     'Number of findings the NLP affirmed.',
        'findings':          'The NLP findings, comma-separated.',
        'has_impression':    'true when the report has an impression section.',
        'report_words':      'Number of words in the report text.',
        'crn':               'true when a Critical Result Notification was raised for this '
                             'report.',
    },
    F: {
        'report_id':         'Row number of the ORU report the finding is in.',
        'accession':         'Accession number of the report.',
        'patient_id':        'Patient ID of the report.',
        'radiologist':       'Physician ID on the ORU report.',
        'modality':          'Modality of the report\'s study.',
        'procedure':         'Procedure of the report.',
        'device':            'AE title of the study\'s device.',
        'patient_type':      _PATIENT_TYPE,
        'report_at':         'Report time (else when RAYD received it).',
        'report_date':       'report_at as a date without time.',
        'finding':           'One finding the NLP affirmed (a label of the diagnosis '
                             'vocabulary). One row per finding per report.',
    },
    SG: {
        'study_date':        'Study date the storage is counted under. One row per day, device '
                             'and procedure code.',
        'study_at':          'study_date as a date and time (midnight), for the date filters.',
        'modality':          _MODALITY,
        'device':            'AE title of the storing device.',
        'procedure_code':    'Procedure code.',
        'gb':                'Gigabytes stored.',
        'studies':           'Number of studies stored.',
    },
    C: {
        'crn_id':            'Row number of the notification. One row per CRN.',
        'ref_code':          'CRN reference code (CRN-XXXX-XXXX).',
        'accession':         'Accession number of the report.',
        'patient_id':        'Patient ID of the report.',
        'radiologist':       'Radiologist who signed the report.',
        'doctor_code':       'Code of the doctor notified.',
        'doctor':            'Name of the doctor notified, else the code.',
        'status':            'CRN status: detected, sent, acknowledged, overdue, unroutable ...',
        'detected_at':       'When the CRN marker was detected.',
        'detected_date':     'detected_at as a date without time.',
        'report_received_at': 'When the report reached RAYD.',
        'first_sent_at':     'When the first SMS went out.',
        'acknowledged_at':   'When the notification was acknowledged.',
        'acknowledged':      'true once acknowledged.',
        'ack_minutes':       'Report received (else detected) to acknowledged, in minutes.',
        'overdue':           'true when it went overdue.',
        'resent':            'true when the SMS was resent.',
        'fallback':          'true when it was escalated to the fallback contact.',
        'by_phone':          'true when it was acknowledged by phone.',
        'acknowledged_how':  'first SMS, after resend, fallback contact, by phone, or not '
                             'acknowledged.',
        'modality':          'Modality of the PACS study with the accession.',
        'procedure':         'Procedure of the PACS study.',
        'device':            'AE title of the PACS study\'s device.',
    },
}

# Worked examples. Every one is run against the sandbox by the test suite, so an
# example that stops working fails there first.
SQL_EXAMPLES = [
    {'title': 'Studies per modality, last month',
     'what': 'A count per modality for the previous calendar month, filtered on the indexed day.',
     'sql': "SELECT modality, COUNT(*) AS \"Studies\"\n"
            "FROM rpt.studies\n"
            "WHERE study_date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'\n"
            "  AND study_date <  date_trunc('month', CURRENT_DATE)\n"
            "GROUP BY modality\n"
            "ORDER BY 2 DESC"},
    {'title': 'Studies per technologist on the morning shift, this week',
     'what': 'Uses the shift column (Report 25\'s shift hours) and the DICOM operator.',
     'sql': "SELECT technologist, COUNT(*) AS \"Studies\"\n"
            "FROM rpt.studies\n"
            "WHERE shift = 'Morning'\n"
            "  AND study_date >= date_trunc('week', CURRENT_DATE)\n"
            "GROUP BY technologist\n"
            "ORDER BY 2 DESC"},
    {'title': 'Median report turnaround per signing radiologist, last 30 days',
     'what': 'Hours from images in PACS to final signature, per signing radiologist.',
     'sql': "SELECT signing_radiologist, COUNT(*) AS \"Studies\",\n"
            "       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY report_tat_hours)::numeric, 2)\n"
            "           AS \"Median TAT (h)\"\n"
            "FROM rpt.studies\n"
            "WHERE study_date >= CURRENT_DATE - 30 AND report_tat_hours IS NOT NULL\n"
            "GROUP BY signing_radiologist\n"
            "ORDER BY 3"},
    {'title': 'PACS orders not performed after 2 days',
     'what': 'Orders scheduled more than 48 hours ago that have no study and were not cancelled.',
     'sql': "SELECT scheduled_at, order_id, patient_id, procedure, order_status\n"
            "FROM rpt.orders\n"
            "WHERE NOT performed AND NOT cancelled\n"
            "  AND scheduled_at < LOCALTIMESTAMP - INTERVAL '2 days'\n"
            "ORDER BY scheduled_at"},
    {'title': 'Pneumothorax reports, last 90 days',
     'what': 'One finding from the NLP, with the study it came from.',
     'sql': "SELECT f.report_at, f.accession, f.modality, f.procedure, f.radiologist\n"
            "FROM rpt.findings f\n"
            "WHERE f.finding = 'Pneumothorax'\n"
            "  AND f.report_at >= CURRENT_DATE - 90\n"
            "ORDER BY f.report_at DESC"},
    {'title': 'Protocols and images per device, last 7 days',
     'what': 'Series per protocol and device with their images, from the series dataset.',
     'sql': "SELECT device, protocol, COUNT(*) AS \"Series\", SUM(images) AS \"Images\"\n"
            "FROM rpt.series\n"
            "WHERE study_date >= CURRENT_DATE - 7\n"
            "GROUP BY device, protocol\n"
            "ORDER BY device, 3 DESC"},
    {'title': 'Storage per modality, this year',
     'what': 'Gigabytes stored per modality since 1 January.',
     'sql': "SELECT modality, round(SUM(gb), 1) AS \"GB\", SUM(studies) AS \"Studies\"\n"
            "FROM rpt.storage\n"
            "WHERE study_date >= date_trunc('year', CURRENT_DATE)\n"
            "GROUP BY modality\n"
            "ORDER BY 2 DESC"},
    {'title': 'Critical studies with their CRN, last 30 days',
     'what': 'Joins studies to CRN notifications on the accession.',
     'sql': "SELECT s.study_at, s.accession, s.modality, s.findings, c.status, c.ack_minutes\n"
            "FROM rpt.studies s\n"
            "LEFT JOIN rpt.crn c ON c.accession = s.accession\n"
            "WHERE s.study_date >= CURRENT_DATE - 30 AND s.critical\n"
            "ORDER BY s.study_at DESC"},
]


def fields_for(dataset):
    return [f for f in FIELDS if dataset in f['datasets']]


def dimensions_for(dataset):
    """[(dim_id, label)] — groupable fields, and every date field by bucket."""
    out = []
    for f in fields_for(dataset):
        if f['type'] == 'date':
            for b, bl in DATE_BUCKETS:
                out.append((f"{f['id']}:{b}", f"{f['label']} — {bl}"))
        elif f.get('group'):
            out.append((f['id'], f['label']))
    return out


def measures_for(dataset):
    return [(k, m['label'] or DATASETS[dataset]['count_label'])
            for k, m in MEASURES.items() if dataset in m['datasets']]


def as_json():
    """The catalog the editor is built from."""
    def opts(f):
        o = f.get('options')
        if isinstance(o, list):
            return [{'value': v, 'label': l} for v, l in o]
        return o
    return {
        'tabs': [{'id': t, 'label': l} for t, l in TABS],
        'datasets': {k: {**{kk: vv for kk, vv in d.items() if kk != 'view'},
                         'tabs': sorted({f['tab'] for f in fields_for(k)},
                                        key=[t for t, _ in TABS].index),
                         'dimensions': [{'id': i, 'label': l} for i, l in dimensions_for(k)],
                         'measures': [{'id': i, 'label': l} for i, l in measures_for(k)],
                         'columns': [{'id': c, 'label': l} for c, l in COLUMNS[k]]}
                     for k, d in DATASETS.items()},
        'fields': [{'id': f['id'], 'label': f['label'], 'tab': f['tab'], 'type': f['type'],
                    'col': f['col'], 'datasets': sorted(f['datasets']),
                    'options': opts(f), 'hint': f.get('hint'), 'unit': f.get('unit')}
                   for f in FIELDS],
        'quick_dates': [{'id': q, 'label': l} for q, l in QUICK_DATES],
        'additive': [k for k, m in MEASURES.items() if m['additive']],
    }
