"""
utils/worklist_catalog.py
────────────────────────────────────────────────────────────────
What a Dynamic Worklist may name: datasets, fields, groupings, measures, columns.

    catalog ──▶ the editor's tabs and pickers (GET /worklists/api/catalog)
            └─▶ the compiler, which takes EVERY identifier from here and nothing
                from the request — a definition that names something not listed is
                refused, so a column name can never be smuggled into the SQL.

Datasets are the rpt.* views of migration 0136 (LAUMC: the RIS exam from the Oracle
RIS ETL, and Bekaa's PACS datasets). A field lives on every dataset whose view has its
column (same name everywhere, by construction of the views).

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
    ('exam',    'RIS exam'),
    ('study',   'Study'),
    ('report',  'Report'),
    ('nlp',     'NLP findings'),
    ('series',  'Series'),
    ('crn',     'CRN'),
]

EX, ST, SE, R, F, SG, C = 'exams', 'studies', 'series', 'reports', 'findings', 'storage', 'crn'

DATASETS = {
    EX: {
        'label': 'Exams (RIS)',
        'view': 'rpt.exams',
        'hint': 'One row per RIS exam (scheduled procedure step): booked, arrived, scanned, '
                'exam done or cancelled, technologist, priority, the RIS report\'s signature and '
                'approval, and its PACS study once there.',
        'count_label': 'Exams',
        'default_date': 'scheduled_at',
        'default_measure': 'count',
        'list_default': ['scheduled_at', 'accession', 'patient_id', 'modality', 'procedure',
                         'exam_status', 'technologist', 'arrived_at'],
    },
    ST: {
        'label': 'Studies (PACS)',
        'view': 'rpt.studies',
        'hint': 'One row per PACS study, SR removed: patient, device, procedure, technologist, '
                'radiologists, report times, its RIS order and its ORU report\'s NLP result.',
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
    R: {
        'label': 'Reports (ORU)',
        'view': 'rpt.reports',
        'hint': 'One row per report text RAYD holds (the RIS report and the ORU listener), with '
                'the NLP result and the PACS study of the same accession.',
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
RIS_REPORT = [(s, s) for s in ('Approved', 'Signed', 'Draft', 'Not reported')]
_STUDY_DAY = {ST: 'study_date', SE: 'study_date', SG: 'study_date'}

# options: 'distinct' = the values present in the dataset (picker shows them, binds
# them); a list = fixed (value, label) pairs. group: may be used to group rows.
FIELDS = [
    # ── Primary ──
    {'id': 'modality', 'label': 'Modality', 'tab': 'primary', 'type': 'multi', 'col': 'modality',
     'datasets': {EX, ST, SE, R, F, SG, C}, 'options': 'distinct', 'group': True,
     'hint': 'From the AE map where the device is mapped, else what PACS sent (RIS exams: else '
             'the RIS modality type). In Series: the series\' own modality.'},
    {'id': 'device', 'label': 'Device (AE title)', 'tab': 'primary', 'type': 'multi',
     'col': 'device', 'datasets': {EX, ST, SE, R, F, SG, C}, 'options': 'distinct', 'group': True,
     'hint': 'RIS exams: the device the RIS performed step names, else the PACS study\'s.'},
    {'id': 'site', 'label': 'Site', 'tab': 'primary', 'type': 'multi', 'col': 'site',
     'datasets': {EX, ST}, 'options': 'distinct', 'group': True,
     'hint': 'RH or SJH. RIS exams: by the order\'s RIS issuer. Studies: by the PACS site.'},
    {'id': 'patient_type', 'label': 'Patient type', 'tab': 'primary', 'type': 'multi',
     'col': 'patient_type', 'datasets': {EX, ST, R, F}, 'options': 'distinct', 'group': True,
     'hint': 'Inpatient, Outpatient or Emergency from the patient class codes in Settings (the '
             'Super Report\'s); other codes as PACS sent them. RIS exams: empty until in PACS.'},
    {'id': 'exam_status', 'label': 'RIS status', 'tab': 'primary', 'type': 'multi',
     'col': 'exam_status', 'datasets': {EX}, 'options': 'distinct', 'group': True,
     'hint': 'The exam\'s RIS status, by stage: Requested, Scheduled, Arrived, In progress, '
             'Exam done, Dictated, Preliminary, Signed, Approved, Cancelled, Discontinued.'},
    {'id': 'report_stage', 'label': 'Report stage', 'tab': 'primary', 'type': 'multi',
     'col': 'report_stage', 'datasets': {ST}, 'options': REPORT_STAGES, 'group': True,
     'hint': 'Furthest report step in PACS: Final, Preliminary, Transcribed, Reported (a report '
             'with no signature time), Not reported.'},

    # ── Patient ──
    {'id': 'patient_id', 'label': 'Patient ID', 'tab': 'patient', 'type': 'text',
     'col': 'patient_id', 'datasets': {EX, ST, SE, R, F, C}},
    {'id': 'sex', 'label': 'Sex', 'tab': 'patient', 'type': 'multi', 'col': 'sex',
     'datasets': {EX, ST}, 'options': 'distinct', 'group': True},
    {'id': 'age', 'label': 'Age at exam', 'tab': 'patient', 'type': 'range', 'col': 'age',
     'datasets': {EX, ST}, 'unit': 'years'},
    {'id': 'age_band', 'label': 'Age band', 'tab': 'patient', 'type': 'multi', 'col': 'age_band',
     'datasets': {EX, ST}, 'options': [('0-17', '0-17'), ('18-39', '18-39'), ('40-64', '40-64'),
                                       ('65+', '65+')], 'group': True},
    {'id': 'patient_class', 'label': 'Patient class (code)', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_class', 'datasets': {EX, ST, R}, 'options': 'distinct', 'group': True},
    {'id': 'patient_location', 'label': 'Patient location', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_location', 'datasets': {EX, ST}, 'options': 'distinct', 'group': True},

    # ── RIS exam ──
    {'id': 'scheduled_at', 'label': 'Scheduled for', 'tab': 'exam', 'type': 'date',
     'col': 'scheduled_at', 'datasets': {EX, ST},
     'hint': 'The RIS appointment (SCHEDULED_DATE). The fastest date for RIS exams. Studies: '
             'the appointment of the study\'s RIS order.'},
    {'id': 'booked_at', 'label': 'Booked', 'tab': 'exam', 'type': 'date', 'col': 'booked_at',
     'datasets': {EX}, 'hint': 'When the RIS first set the exam to Scheduled.'},
    {'id': 'arrived_at', 'label': 'Arrived', 'tab': 'exam', 'type': 'date', 'col': 'arrived_at',
     'datasets': {EX}, 'hint': 'When the RIS first set the exam to Arrived.'},
    {'id': 'started_at', 'label': 'Scan started', 'tab': 'exam', 'type': 'date',
     'col': 'started_at', 'datasets': {EX},
     'hint': 'Start of the RIS performed step (the modality\'s MPPS).'},
    {'id': 'exam_done_at', 'label': 'Exam done', 'tab': 'exam', 'type': 'date',
     'col': 'exam_done_at', 'datasets': {EX}, 'hint': 'When the RIS last set the exam to Exam Done.'},
    {'id': 'cancelled_at', 'label': 'Cancelled on', 'tab': 'exam', 'type': 'date',
     'col': 'cancelled_at', 'datasets': {EX}},
    {'id': 'priority', 'label': 'Priority', 'tab': 'exam', 'type': 'multi', 'col': 'priority',
     'datasets': {EX}, 'options': 'distinct', 'group': True},
    {'id': 'performed', 'label': 'Performed', 'tab': 'exam', 'type': 'yesno', 'col': 'performed',
     'datasets': {EX},
     'hint': 'Scanned, set to Exam Done or later, or in PACS.'},
    {'id': 'cancelled', 'label': 'Cancelled', 'tab': 'exam', 'type': 'yesno', 'col': 'cancelled',
     'datasets': {EX}, 'hint': 'RIS status Cancelled (any kind) or Discontinued.'},
    {'id': 'in_pacs', 'label': 'In PACS', 'tab': 'exam', 'type': 'yesno', 'col': 'in_pacs',
     'datasets': {EX}, 'hint': 'The ETL found the exam\'s study in PACS.'},
    {'id': 'wait_minutes', 'label': 'Arrived → scan start (min)', 'tab': 'exam', 'type': 'range',
     'col': 'wait_minutes', 'datasets': {EX}, 'unit': 'minutes'},
    {'id': 'start_delay_minutes', 'label': 'Scheduled → started (min)', 'tab': 'exam',
     'type': 'range', 'col': 'start_delay_minutes', 'datasets': {EX, ST}, 'unit': 'minutes',
     'hint': 'RIS appointment to scan start (Studies: to the study time). Negative when early.'},
    {'id': 'exam_minutes', 'label': 'Scan duration (min)', 'tab': 'exam', 'type': 'range',
     'col': 'exam_minutes', 'datasets': {EX}, 'unit': 'minutes'},
    {'id': 'arrival_to_done_minutes', 'label': 'Arrived → exam done (min)', 'tab': 'exam',
     'type': 'range', 'col': 'arrival_to_done_minutes', 'datasets': {EX}, 'unit': 'minutes',
     'hint': 'Report 35\'s technologist turnaround. For inpatients Arrived is set early, to '
             'send the exam to the device\'s worklist.'},

    # ── Study ──
    {'id': 'study_at', 'label': 'Study date', 'tab': 'study', 'type': 'date', 'col': 'study_at',
     'datasets': {ST, SE, SG}, 'date_col': _STUDY_DAY,
     'hint': 'PACS study date and time.'},
    {'id': 'stored_at', 'label': 'Stored in PACS', 'tab': 'study', 'type': 'date',
     'col': 'stored_at', 'datasets': {EX, ST}},
    {'id': 'accession', 'label': 'Accession', 'tab': 'study', 'type': 'text', 'col': 'accession',
     'datasets': {EX, ST, SE, R, F, C}},
    {'id': 'procedure', 'label': 'Procedure', 'tab': 'study', 'type': 'multi', 'col': 'procedure',
     'datasets': {EX, ST, R, F, C}, 'options': 'distinct', 'group': True},
    {'id': 'procedure_code', 'label': 'Procedure code', 'tab': 'study', 'type': 'multi',
     'col': 'procedure_code', 'datasets': {EX, ST, R, SG}, 'options': 'distinct', 'group': True},
    {'id': 'study_description', 'label': 'Study description', 'tab': 'study', 'type': 'text',
     'col': 'study_description', 'datasets': {ST, SE}, 'group': True},
    {'id': 'body_part', 'label': 'Body part', 'tab': 'study', 'type': 'multi', 'col': 'body_part',
     'datasets': {ST, SE}, 'options': 'distinct', 'group': True,
     'hint': 'Studies: the study\'s body part. Series: the body part examined by the series.'},
    {'id': 'room', 'label': 'Room', 'tab': 'study', 'type': 'multi', 'col': 'room',
     'datasets': {EX, ST}, 'options': 'distinct', 'group': True,
     'hint': 'The device\'s room in the AE map (Modality / Procedures).'},
    {'id': 'pacs_modality', 'label': 'Modality as PACS sent', 'tab': 'study', 'type': 'multi',
     'col': 'pacs_modality', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'technologist', 'label': 'Technologist', 'tab': 'study', 'type': 'multi',
     'col': 'technologist', 'datasets': {EX, ST}, 'options': 'distinct', 'group': True,
     'hint': 'RIS exams: the RIS technologist (TEC) of the performed step, as Report 35. '
             'Studies: the DICOM Operators\' Name, as the modality wrote it.'},
    {'id': 'performing_physician', 'label': 'Performing physician', 'tab': 'study',
     'type': 'multi', 'col': 'performing_physician', 'datasets': {ST}, 'options': 'distinct',
     'group': True},
    {'id': 'shift', 'label': 'Shift', 'tab': 'study', 'type': 'multi', 'col': 'shift',
     'datasets': {EX, ST}, 'options': [('Morning', 'Morning'), ('Afternoon', 'Afternoon'),
                                       ('Night', 'Night')], 'group': True,
     'hint': 'By the hour of the study time (RIS exams: of the scan start, else exam done, else '
             'arrival, else the appointment), with the shift hours set on Report 25.'},
    {'id': 'weekday', 'label': 'Weekday', 'tab': 'study', 'type': 'multi', 'col': 'weekday',
     'datasets': {EX, ST, SE}, 'options': WEEKDAYS, 'group': True},
    {'id': 'week_of_month', 'label': 'Week of month', 'tab': 'study', 'type': 'multi',
     'col': 'week_of_month', 'datasets': {EX, ST},
     'options': [(1, '1 (days 1–7)'), (2, '2'), (3, '3'), (4, '4'), (5, '5')], 'group': True},
    {'id': 'hour', 'label': 'Hour', 'tab': 'study', 'type': 'range', 'col': 'hour',
     'datasets': {EX, ST, SE}, 'unit': '0–23', 'group': True,
     'hint': 'Hour of the study time (RIS exams: as the shift). Empty when PACS has no time.'},
    {'id': 'images', 'label': 'Images', 'tab': 'study', 'type': 'range', 'col': 'images',
     'datasets': {EX, ST, SE}, 'unit': 'images'},
    {'id': 'series_count', 'label': 'Series in the study', 'tab': 'study', 'type': 'range',
     'col': 'series', 'datasets': {ST}, 'unit': 'series'},
    {'id': 'study_status', 'label': 'PACS study status', 'tab': 'study', 'type': 'multi',
     'col': 'study_status', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'order_status', 'label': 'PACS order status', 'tab': 'study', 'type': 'multi',
     'col': 'order_status', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'linked', 'label': 'Linked study', 'tab': 'study', 'type': 'yesno', 'col': 'linked',
     'datasets': {ST}},
    {'id': 'store_delay_minutes', 'label': 'Study time → stored in PACS (min)', 'tab': 'study',
     'type': 'range', 'col': 'store_delay_minutes', 'datasets': {ST}, 'unit': 'minutes'},

    # ── Report ──
    {'id': 'ris_report', 'label': 'RIS report', 'tab': 'report', 'type': 'multi',
     'col': 'ris_report', 'datasets': {EX}, 'options': RIS_REPORT, 'group': True,
     'hint': 'The RIS report\'s furthest step: Approved, Signed (Signed 1), Draft, Not reported.'},
    {'id': 'signed_at', 'label': 'Signed (RIS)', 'tab': 'report', 'type': 'date',
     'col': 'signed_at', 'datasets': {EX}, 'hint': 'Signed 1 of the RIS report\'s current version.'},
    {'id': 'approved_at', 'label': 'Approved (RIS)', 'tab': 'report', 'type': 'date',
     'col': 'approved_at', 'datasets': {EX}},
    {'id': 'signed_by', 'label': 'Signed by (RIS)', 'tab': 'report', 'type': 'multi',
     'col': 'signed_by', 'datasets': {EX}, 'options': 'distinct', 'group': True},
    {'id': 'approved_by', 'label': 'Approved by (RIS)', 'tab': 'report', 'type': 'multi',
     'col': 'approved_by', 'datasets': {EX}, 'options': 'distinct', 'group': True},
    {'id': 'signed_tat_hours', 'label': 'Exam done → signed (hours)', 'tab': 'report',
     'type': 'range', 'col': 'signed_tat_hours', 'datasets': {EX}, 'unit': 'hours'},
    {'id': 'has_report', 'label': 'Has a report', 'tab': 'report', 'type': 'yesno',
     'col': 'has_report', 'datasets': {ST}},
    {'id': 'report_status', 'label': 'PACS report status', 'tab': 'report', 'type': 'multi',
     'col': 'report_status', 'datasets': {ST}, 'options': 'distinct', 'group': True},
    {'id': 'final_signed_at', 'label': 'Final signature date', 'tab': 'report', 'type': 'date',
     'col': 'final_signed_at', 'datasets': {ST}},
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
     'hint': 'Reports and findings: the physician ID on the report. CRN: the signing '
             'radiologist.'},
    {'id': 'referring', 'label': 'Referring physician', 'tab': 'report', 'type': 'text',
     'col': 'referring', 'datasets': {EX, ST}, 'group': True,
     'hint': 'First and last name as PACS has them; "EXTERNAL DOC" is shown as External referrals.'},
    {'id': 'report_tat_hours', 'label': 'Report turnaround (hours)', 'tab': 'report',
     'type': 'range', 'col': 'report_tat_hours', 'datasets': {EX, ST, R}, 'unit': 'hours',
     'hint': 'Studies: images stored in PACS → final signature, as the Super Report measures it. '
             'Reports: → the report time. RIS exams: Exam Done → RIS approval.'},
    {'id': 'prelim_tat_hours', 'label': 'Preliminary turnaround (hours)', 'tab': 'report',
     'type': 'range', 'col': 'prelim_tat_hours', 'datasets': {ST}, 'unit': 'hours'},
    {'id': 'has_addendum', 'label': 'Has an addendum', 'tab': 'report', 'type': 'yesno',
     'col': 'has_addendum', 'datasets': {ST}},
    {'id': 'report_at', 'label': 'Report date', 'tab': 'report', 'type': 'date',
     'col': 'report_at', 'datasets': {R, F}},
    {'id': 'oru_report', 'label': 'Report text received', 'tab': 'report', 'type': 'yesno',
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
                           'datasets': {EX, ST, SE, R, F, C}},
    'patients':           {'label': 'Patients', 'sql': 'COUNT(DISTINCT d.patient_id)',
                           'additive': False, 'datasets': {EX, ST, SE, R, F, C}},
    'studies':            {'label': 'Studies', 'sql': 'COUNT(DISTINCT d.study_uid)',
                           'additive': False, 'datasets': {EX, SE}},
    'reports':            {'label': 'Reports', 'sql': 'COUNT(DISTINCT d.report_id)',
                           'additive': False, 'datasets': {F}},
    'performed':          {'label': 'Performed', 'sql': 'COUNT(*) FILTER (WHERE d.performed)',
                           'additive': True, 'datasets': {EX}},
    'not_performed':      {'label': 'Not performed',
                           'sql': 'COUNT(*) FILTER (WHERE NOT d.performed AND NOT d.cancelled)',
                           'additive': True, 'datasets': {EX}},
    'cancelled':          {'label': 'Cancelled', 'sql': 'COUNT(*) FILTER (WHERE d.cancelled)',
                           'additive': True, 'datasets': {EX}},
    'approved':           {'label': 'Approved (RIS)',
                           'sql': "COUNT(*) FILTER (WHERE d.ris_report = 'Approved')",
                           'additive': True, 'datasets': {EX}},
    'median_wait':        {'label': 'Median arrived → scan (min)',
                           'sql': _median('wait_minutes', 1), 'additive': False, 'datasets': {EX}},
    'median_exam':        {'label': 'Median scan duration (min)',
                           'sql': _median('exam_minutes', 1), 'additive': False, 'datasets': {EX}},
    'median_arrival_to_done': {'label': 'Median arrived → exam done (min)',
                               'sql': _median('arrival_to_done_minutes', 1), 'additive': False,
                               'datasets': {EX}},
    'median_signed_tat':  {'label': 'Median exam done → signed (h)',
                           'sql': _median('signed_tat_hours', 2), 'additive': False,
                           'datasets': {EX}},
    'reported':           {'label': 'With a report', 'sql': 'COUNT(*) FILTER (WHERE d.has_report)',
                           'additive': True, 'datasets': {ST}},
    'not_reported':       {'label': 'Without a report',
                           'sql': 'COUNT(*) FILTER (WHERE NOT d.has_report)', 'additive': True,
                           'datasets': {ST}},
    'final':              {'label': 'Final signed',
                           'sql': "COUNT(*) FILTER (WHERE d.report_stage = 'Final')",
                           'additive': True, 'datasets': {ST}},
    'addenda':            {'label': 'With an addendum',
                           'sql': 'COUNT(*) FILTER (WHERE d.has_addendum)', 'additive': True,
                           'datasets': {ST}},
    'critical':           {'label': 'Critical (NLP)', 'sql': 'COUNT(*) FILTER (WHERE d.critical)',
                           'additive': True, 'datasets': {ST, R}},
    'crn':                {'label': 'CRN sent', 'sql': 'COUNT(*) FILTER (WHERE d.crn)',
                           'additive': True, 'datasets': {ST, R}},
    'images':             {'label': 'Images', 'sql': 'SUM(d.images)', 'additive': True,
                           'datasets': {EX, ST, SE}},
    'median_report_tat':  {'label': 'Median report TAT (h)', 'sql': _median('report_tat_hours', 2),
                           'additive': False, 'datasets': {EX, ST, R}},
    'avg_report_tat':     {'label': 'Average report TAT (h)',
                           'sql': 'round(AVG(d.report_tat_hours)::numeric, 2)', 'additive': False,
                           'datasets': {EX, ST, R}},
    'median_prelim_tat':  {'label': 'Median preliminary TAT (h)',
                           'sql': _median('prelim_tat_hours', 2), 'additive': False,
                           'datasets': {ST}},
    'median_store_delay': {'label': 'Median study → PACS (min)',
                           'sql': _median('store_delay_minutes', 1), 'additive': False,
                           'datasets': {ST}},
    'median_start_delay': {'label': 'Median scheduled → started (min)',
                           'sql': _median('start_delay_minutes', 1), 'additive': False,
                           'datasets': {EX, ST}},
    'avg_age':            {'label': 'Average age', 'sql': 'round(AVG(d.age)::numeric, 1)',
                           'additive': False, 'datasets': {EX, ST}},
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
    'exam_id': 'Exam ID', 'study_uid': 'Study UID', 'series_uid': 'Series UID',
    'report_id': 'Report ID', 'crn_id': 'CRN ID', 'ref_code': 'CRN reference',
    'accession': 'Accession', 'patient_id': 'Patient ID', 'sex': 'Sex', 'age': 'Age',
    'age_band': 'Age band', 'patient_class': 'Patient class', 'patient_type': 'Patient type',
    'patient_location': 'Location', 'modality': 'Modality', 'pacs_modality': 'PACS modality',
    'ris_modality': 'RIS modality', 'study_modality': 'Study modality', 'device': 'Device',
    'device_name': 'Device name', 'room': 'Room', 'site': 'Site',
    'procedure_code': 'Procedure code', 'procedure': 'Procedure',
    'study_description': 'Study description', 'body_part': 'Body part',
    'study_status': 'PACS study status', 'report_status': 'PACS report status',
    'order_status': 'PACS order status', 'priority': 'Priority', 'exam_status': 'RIS status',
    'ris_status': 'RIS status (raw)', 'study_at': 'Study date', 'study_date': 'Study day',
    'hour': 'Hour', 'shift': 'Shift', 'weekday': 'Weekday', 'week_of_month': 'Week of month',
    'stored_at': 'Stored in PACS', 'scheduled_at': 'Scheduled for',
    'scheduled_date': 'Scheduled day', 'booked_at': 'Booked', 'arrived_at': 'Arrived',
    'started_at': 'Scan started', 'scanner_done_at': 'Scan ended', 'exam_done_at': 'Exam done',
    'cancelled_at': 'Cancelled on', 'exam_at': 'Exam time', 'transcribed_at': 'Transcribed',
    'prelim_signed_at': 'Prelim signed', 'final_signed_at': 'Final signed',
    'addendum_at': 'Addendum', 'report_stage': 'Report stage', 'has_report': 'Report',
    'has_addendum': 'Addendum', 'linked': 'Linked', 'technologist': 'Technologist',
    'performing_physician': 'Performing physician', 'reading_radiologist': 'Reading radiologist',
    'signing_radiologist': 'Signing radiologist', 'radiologist': 'Radiologist',
    'signed_at': 'Signed (RIS)', 'approved_at': 'Approved (RIS)', 'ris_report': 'RIS report',
    'signed_by': 'Signed by', 'approved_by': 'Approved by',
    'referring': 'Referring', 'images': 'Images', 'series': 'Series', 'in_pacs': 'In PACS',
    'report_tat_hours': 'Report TAT (h)', 'prelim_tat_hours': 'Prelim TAT (h)',
    'signed_tat_hours': 'Done → signed (h)', 'wait_minutes': 'Arrived → scan (min)',
    'exam_minutes': 'Scan (min)', 'arrival_to_done_minutes': 'Arrived → done (min)',
    'store_delay_minutes': 'Study → PACS (min)', 'start_delay_minutes': 'Scheduled → started (min)',
    'oru_report': 'Report text', 'critical': 'Critical', 'findings': 'Findings (NLP)', 'crn': 'CRN',
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
    EX: ['scheduled_at', 'accession', 'patient_id', 'sex', 'age', 'age_band', 'patient_class',
         'patient_type', 'patient_location', 'modality', 'ris_modality', 'device', 'device_name',
         'room', 'site', 'procedure_code', 'procedure', 'priority', 'exam_status', 'ris_status',
         'cancelled', 'performed', 'scheduled_date', 'booked_at', 'arrived_at', 'started_at',
         'scanner_done_at', 'exam_done_at', 'cancelled_at', 'exam_at', 'hour', 'shift',
         'weekday', 'week_of_month', 'technologist', 'signed_at', 'approved_at', 'ris_report',
         'signed_by', 'approved_by', 'referring', 'study_uid', 'in_pacs', 'stored_at', 'images',
         'wait_minutes', 'start_delay_minutes', 'exam_minutes', 'arrival_to_done_minutes',
         'signed_tat_hours', 'report_tat_hours', 'exam_id'],
    ST: ['study_at', 'accession', 'patient_id', 'sex', 'age', 'age_band', 'patient_class',
         'patient_type', 'patient_location', 'modality', 'pacs_modality', 'device', 'device_name',
         'room', 'site', 'procedure_code', 'procedure', 'study_description', 'body_part',
         'study_status', 'report_status', 'order_status', 'study_date', 'hour', 'shift',
         'weekday', 'week_of_month', 'stored_at', 'scheduled_at', 'transcribed_at',
         'prelim_signed_at', 'final_signed_at', 'addendum_at', 'report_stage', 'has_report',
         'has_addendum', 'linked', 'technologist', 'performing_physician', 'reading_radiologist',
         'signing_radiologist', 'referring', 'images', 'series', 'report_tat_hours',
         'prelim_tat_hours', 'store_delay_minutes', 'start_delay_minutes', 'oru_report',
         'critical', 'findings', 'crn', 'study_uid'],
    SE: ['study_at', 'accession', 'patient_id', 'study_date', 'hour', 'weekday', 'modality',
         'study_modality', 'device', 'series_number', 'series_description', 'protocol',
         'body_part', 'images', 'station', 'manufacturer', 'institution', 'department',
         'study_description', 'series_uid', 'study_uid'],
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
# definitions in migration 0136.

_MODALITY = ('Modality: from the AE map when the device is mapped, else the modality PACS sent. '
             'SR is left out.')
_PATIENT_TYPE = ('patient_class in words: Inpatient, Outpatient or Emergency by the codes in '
                 'Settings (pc_inpatient / pc_outpatient / pc_emergency, as the Super Report); '
                 'other codes as sent.')
_STUDY_AT = ('The study\'s date and time: PACS study date plus the DICOM study time. Midnight '
             'when PACS has no time (then hour is empty).')
_TAT = ('Report turnaround in hours: images stored in PACS (insert_time) to the final '
        'signature, as the Super Report measures it.')
_MRN = ('Patient ID: the RIS MRN of the study\'s RIS order, else the PACS fallback ID (as '
        'Report 22), else the PACS ID.')
_SITE = 'Site name (RH, SJH ...).'

COLUMN_DOCS = {
    EX: {
        'exam_id':           'RIS worklist key (SITE_WORKLIST_KEY, etl_orders.order_dbid). One row '
                             'per RIS exam.',
        'accession':         'Accession number (the RIS SPS ID; the same number PACS uses).',
        'patient_id':        'Patient MRN from the RIS (std_patient_ids, the primary ID first).',
        'sex':               'Sex from the RIS patient record.',
        'age':               'Age in whole years at exam_at, from the RIS birth date.',
        'age_band':          'Age group: 0-17, 18-39, 40-64 or 65+.',
        'patient_class':     'Patient class code of the PACS study (empty until in PACS).',
        'patient_type':      _PATIENT_TYPE + ' Empty until in PACS.',
        'patient_location':  'Patient location of the PACS study (empty until in PACS).',
        'modality':          'Modality: the AE map of the device, else the RIS modality type. SR '
                             'is left out.',
        'ris_modality':      'The RIS modality type (CT, MR, US ...).',
        'device':            'AE title of the device: the RIS performed step\'s, else the PACS '
                             'study\'s.',
        'device_name':       'Display name of the device in the AE map.',
        'room':              'The device\'s room in the AE map.',
        'site':              _SITE + ' The order\'s site, else the site of its RIS issuer.',
        'procedure_code':    'RIS procedure code (SPS code).',
        'procedure':         'RIS procedure description, else the code.',
        'priority':          'RIS priority of the performed step.',
        'exam_status':       'RIS status by stage, in words: Requested, Scheduled, Arrived, In '
                             'progress, Exam done, Dictated, Preliminary, Signed, Approved, '
                             'Cancelled, Discontinued.',
        'ris_status':        'The RIS status name as the RIS has it (finer than exam_status).',
        'cancelled':         'true when the RIS status is a cancellation or Discontinued.',
        'performed':         'true once scanned, set to Exam Done or later, or in PACS.',
        'scheduled_at':      'RIS appointment (SITE_WORKLIST.SCHEDULED_DATE). Indexed: the '
                             'fastest date filter.',
        'scheduled_date':    'scheduled_at as a date without time.',
        'booked_at':         'First Scheduled transition in the RIS status history.',
        'arrived_at':        'First Arrived transition in the RIS status history.',
        'started_at':        'Scan start: the RIS performed step\'s start (the modality\'s MPPS).',
        'scanner_done_at':   'Scan end: the RIS performed step\'s end.',
        'exam_done_at':      'Last Exam Done transition in the RIS status history.',
        'cancelled_at':      'Last cancellation transition, for a cancelled exam.',
        'exam_at':           'When the exam happened: scan start, else exam done, else arrival, '
                             'else the appointment. hour, shift and weekday use it.',
        'hour':              'Hour of exam_at, 0-23.',
        'shift':             'Morning, Afternoon or Night by the hour of exam_at, with the shift '
                             'hours set on Report 25.',
        'weekday':           'Weekday of exam_at: 1 = Monday ... 7 = Sunday.',
        'week_of_month':     'Week of the month of exam_at: 1 = days 1-7 ... 5 = days 29-31.',
        'technologist':      'RIS technologist (role TEC) of the performed step, as Report 35.',
        'signed_at':         'Signed 1 of the RIS report\'s current version.',
        'approved_at':       'Approval of the RIS report\'s current version.',
        'ris_report':        'Approved, Signed (Signed 1), Draft (a report with neither) or Not '
                             'reported.',
        'signed_by':         'Who signed the RIS report (Signed 1).',
        'approved_by':       'Who approved the RIS report.',
        'referring':         'Referring physician of the PACS study; EXTERNAL DOC is "External '
                             'referrals".',
        'study_uid':         'PACS study key the ETL matched (empty until in PACS).',
        'in_pacs':           'true once the ETL found the exam\'s study in PACS.',
        'stored_at':         'When PACS stored the study.',
        'images':            'Number of images in the PACS study.',
        'wait_minutes':      'Arrived to scan start, in minutes: the patient\'s wait.',
        'start_delay_minutes': 'Appointment to scan start, in minutes; negative when early.',
        'exam_minutes':      'Scan start to scan end, in minutes.',
        'arrival_to_done_minutes': 'Arrived to Exam Done, in minutes (Report 35\'s turnaround). '
                                   'Inpatients are set to Arrived early, for the device worklist.',
        'signed_tat_hours':  'Exam Done to Signed 1, in hours.',
        'report_tat_hours':  'Exam Done to RIS approval, in hours.',
    },
    ST: {
        'study_uid':         'PACS study key (etl_didb_studies.study_db_uid). One row per study.',
        'accession':         'Accession number.',
        'patient_id':        _MRN,
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
        'site':              _SITE + ' The PACS study\'s site.',
        'procedure_code':    'Procedure code on the study.',
        'procedure':         'Procedure: study description, else the RIS order\'s procedure '
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
        'scheduled_at':      'RIS appointment of the study\'s RIS order.',
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
        'start_delay_minutes': 'RIS appointment to study time, in minutes; negative when early.',
        'oru_report':        'true when a report text with this accession is in RAYD (RIS or '
                             'ORU listener).',
        'critical':          'true when the NLP found a critical finding in a report of the '
                             'study.',
        'findings':          'NLP findings of the study\'s reports, comma-separated.',
        'crn':               'true when a Critical Result Notification was raised for the '
                             'accession.',
    },
    SE: {
        'series_uid':        'PACS series key. One row per series.',
        'study_uid':         'PACS study key of the series\' study.',
        'accession':         'Accession number of the study.',
        'patient_id':        _MRN,
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
    R: {
        'report_id':         'Row number of the report in RAYD. One row per report.',
        'accession':         'Accession number the report is for.',
        'patient_id':        'Patient ID on the report (RIS reports: the RIS MRN), else the PACS '
                             'study\'s.',
        'radiologist':       'Physician ID on the report.',
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
        'report_id':         'Row number of the report the finding is in.',
        'accession':         'Accession number of the report.',
        'patient_id':        'Patient ID of the report.',
        'radiologist':       'Physician ID on the report.',
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
    {'title': 'RIS exams per technologist, last month',
     'what': 'Exams per RIS technologist with the median minutes from arrival to Exam Done, '
             'bounded on the indexed appointment.',
     'sql': "SELECT technologist, COUNT(*) AS \"Exams\",\n"
            "       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY arrival_to_done_minutes)::numeric, 1)\n"
            "           AS \"Median arrived → done (min)\"\n"
            "FROM rpt.exams\n"
            "WHERE scheduled_at >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'\n"
            "  AND scheduled_at <  date_trunc('month', CURRENT_DATE)\n"
            "  AND performed\n"
            "GROUP BY technologist\n"
            "ORDER BY 2 DESC"},
    {'title': 'RIS exams scheduled 2+ days ago and never performed',
     'what': 'Appointments of the last 30 days, more than 48 hours old, not performed and not '
             'cancelled.',
     'sql': "SELECT scheduled_at, accession, patient_id, procedure, exam_status\n"
            "FROM rpt.exams\n"
            "WHERE scheduled_at >= CURRENT_DATE - 30\n"
            "  AND scheduled_at <  LOCALTIMESTAMP - INTERVAL '2 days'\n"
            "  AND NOT performed AND NOT cancelled\n"
            "ORDER BY scheduled_at"},
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
