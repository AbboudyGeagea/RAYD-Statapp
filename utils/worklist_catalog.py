"""
utils/worklist_catalog.py
────────────────────────────────────────────────────────────────
What a Dynamic Worklist may name: datasets, fields, groupings, measures, columns.

    catalog ──▶ the editor's tabs and pickers (GET /worklists/api/catalog)
            └─▶ the compiler, which takes EVERY identifier from here and nothing
                from the request — a definition that names something not listed is
                refused, so a column name can never be smuggled into the SQL.

Datasets are the rpt.* views of migration 0136. A field lives on every dataset
whose view has its column (same name everywhere, by construction of the views).

Field types and the filters they take:
    date    none | quick | relative (prev/next N hours/days) | fixed (from–to)
    multi   pick values (a picker of the values actually present, or a fixed list)
    text    contains | equals | starts with | does not contain
    yesno   any | yes | no
    range   from / to, numbers
"""

TABS = [
    ('primary', 'Primary'),
    ('patient', 'Patient'),
    ('order',   'Order'),
    ('exam',    'Exam'),
    ('report',  'Report'),
    ('quality', 'Data quality'),
]

DATASETS = {
    'exams': {
        'label': 'Exams',
        'view': 'rpt.exams',
        'hint': 'One row per study, SR removed. Exam date is the completion (else the '
                'latest exam event); shift and hour use the start.',
        'count_label': 'Exams',
        'default_date': 'exam_at',
        'list_default': ['exam_at', 'accession', 'patient_id', 'modality', 'procedure',
                         'device', 'status', 'technician'],
    },
    'orders': {
        'label': 'HIS orders',
        'view': 'rpt.orders',
        'hint': 'One row per order the HIS sent, matched to its study by accession or '
                'order number — including orders the RIS never scheduled.',
        'count_label': 'Orders',
        'default_date': 'ordered_at',
        'list_default': ['ordered_at', 'placer_order_number', 'patient_id', 'procedure',
                         'modality', 'referring', 'status'],
    },
    'reports': {
        'label': 'Reports',
        'view': 'rpt.reports',
        'hint': 'One row per report, with its signature level and turnaround from exam '
                'completion.',
        'count_label': 'Reports',
        'default_date': 'report_at',
        'list_default': ['report_at', 'accession', 'radiologist', 'modality', 'procedure',
                         'signature', 'report_tat_hours'],
    },
}

E, O, R = 'exams', 'orders', 'reports'

WEEKDAYS = [(1, 'Mon'), (2, 'Tue'), (3, 'Wed'), (4, 'Thu'), (5, 'Fri'), (6, 'Sat'), (7, 'Sun')]

# options: 'distinct' = the values present in the dataset (picker shows them, binds
# them); a list = fixed (value, label) pairs. group: may be used to group rows.
FIELDS = [
    # ── Primary ──
    {'id': 'status', 'label': 'Status', 'tab': 'primary', 'type': 'multi', 'col': 'status',
     'datasets': {E, O}, 'options': 'distinct', 'group': True},
    {'id': 'modality', 'label': 'Modality', 'tab': 'primary', 'type': 'multi', 'col': 'modality',
     'datasets': {E, O, R}, 'options': 'distinct', 'group': True,
     'hint': 'From the device map where the device is known, else the message.'},
    {'id': 'site', 'label': 'Site', 'tab': 'primary', 'type': 'multi', 'col': 'site',
     'datasets': {E, O, R}, 'options': 'distinct', 'group': True,
     'hint': 'From the device the exam ran on (Modality / Procedures → device map).'},

    # ── Patient ──
    {'id': 'patient_id', 'label': 'Patient ID', 'tab': 'patient', 'type': 'text',
     'col': 'patient_id', 'datasets': {E, O, R}},
    {'id': 'sex', 'label': 'Sex', 'tab': 'patient', 'type': 'multi', 'col': 'sex',
     'datasets': {E}, 'options': 'distinct', 'group': True},
    {'id': 'age', 'label': 'Age at exam', 'tab': 'patient', 'type': 'range', 'col': 'age',
     'datasets': {E}, 'unit': 'years'},
    {'id': 'age_band', 'label': 'Age band', 'tab': 'patient', 'type': 'multi', 'col': 'age_band',
     'datasets': {E}, 'options': [('0-17', '0-17'), ('18-39', '18-39'), ('40-64', '40-64'),
                                  ('65+', '65+')], 'group': True},
    {'id': 'patient_type', 'label': 'Patient type', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_type', 'datasets': {E, O, R}, 'options': 'distinct', 'group': True,
     'hint': 'PV1-2: Inpatient, Outpatient, Emergency (other codes as sent).'},
    {'id': 'patient_location', 'label': 'Patient location', 'tab': 'patient', 'type': 'multi',
     'col': 'patient_location', 'datasets': {E, O}, 'options': 'distinct', 'group': True},

    # ── Order ──
    {'id': 'ordered_at', 'label': 'Order date', 'tab': 'order', 'type': 'date',
     'col': 'ordered_at', 'datasets': {E, O}},
    {'id': 'referring', 'label': 'Referring physician', 'tab': 'order', 'type': 'text',
     'col': 'referring', 'datasets': {E, O, R}, 'group': True},
    {'id': 'ordering_physician', 'label': 'Ordering physician', 'tab': 'order', 'type': 'text',
     'col': 'ordering_physician', 'datasets': {O}, 'group': True},
    {'id': 'placer_order_number', 'label': 'Order number', 'tab': 'order', 'type': 'text',
     'col': 'placer_order_number', 'datasets': {E, O}},
    {'id': 'scheduled', 'label': 'Scheduled', 'tab': 'order', 'type': 'yesno',
     'col': 'scheduled', 'datasets': {O}},
    {'id': 'performed', 'label': 'Performed', 'tab': 'order', 'type': 'yesno',
     'col': 'performed', 'datasets': {O}},
    {'id': 'cancelled', 'label': 'Cancelled', 'tab': 'order', 'type': 'yesno',
     'col': 'cancelled', 'datasets': {O}},
    {'id': 'order_to_exam_hours', 'label': 'Order → exam (hours)', 'tab': 'order',
     'type': 'range', 'col': 'order_to_exam_hours', 'datasets': {O}, 'unit': 'hours'},

    # ── Exam ──
    {'id': 'exam_at', 'label': 'Exam date', 'tab': 'exam', 'type': 'date', 'col': 'exam_at',
     'datasets': {E}},
    {'id': 'scheduled_at', 'label': 'Scheduled date', 'tab': 'exam', 'type': 'date',
     'col': 'scheduled_at', 'datasets': {E, O}},
    {'id': 'performed_at', 'label': 'Performed date', 'tab': 'exam', 'type': 'date',
     'col': 'performed_at', 'datasets': {E, O}},
    {'id': 'accession', 'label': 'Accession', 'tab': 'exam', 'type': 'text', 'col': 'accession',
     'datasets': {E, O, R}},
    {'id': 'procedure', 'label': 'Procedure', 'tab': 'exam', 'type': 'multi', 'col': 'procedure',
     'datasets': {E, O, R}, 'options': 'distinct', 'group': True},
    {'id': 'device', 'label': 'Device (AE title)', 'tab': 'exam', 'type': 'multi', 'col': 'device',
     'datasets': {E, R}, 'options': 'distinct', 'group': True},
    {'id': 'room', 'label': 'Room', 'tab': 'exam', 'type': 'multi', 'col': 'room',
     'datasets': {E}, 'options': 'distinct', 'group': True},
    {'id': 'technician', 'label': 'Technician', 'tab': 'exam', 'type': 'multi',
     'col': 'technician', 'datasets': {E}, 'options': 'distinct', 'group': True,
     'hint': 'Who the Started event names (else the Completed one).'},
    {'id': 'shift', 'label': 'Shift', 'tab': 'exam', 'type': 'multi', 'col': 'shift',
     'datasets': {E}, 'options': 'distinct', 'group': True,
     'hint': 'From the shift hours (Shift hours button), by the hour the exam started.'},
    {'id': 'week_of_month', 'label': 'Week of month', 'tab': 'exam', 'type': 'multi',
     'col': 'week_of_month', 'datasets': {E},
     'options': [(1, '1 (days 1–7)'), (2, '2'), (3, '3'), (4, '4'), (5, '5')], 'group': True},
    {'id': 'weekday', 'label': 'Weekday', 'tab': 'exam', 'type': 'multi', 'col': 'weekday',
     'datasets': {E}, 'options': WEEKDAYS, 'group': True},
    {'id': 'hour', 'label': 'Hour started', 'tab': 'exam', 'type': 'range', 'col': 'hour',
     'datasets': {E}, 'unit': '0–23', 'group': True},
    {'id': 'wait_minutes', 'label': 'Wait, arrived → started (min)', 'tab': 'exam',
     'type': 'range', 'col': 'wait_minutes', 'datasets': {E}, 'unit': 'minutes'},
    {'id': 'exam_minutes', 'label': 'Exam duration, started → completed (min)', 'tab': 'exam',
     'type': 'range', 'col': 'exam_minutes', 'datasets': {E}, 'unit': 'minutes'},

    # ── Report ──
    {'id': 'has_report', 'label': 'Has a report', 'tab': 'report', 'type': 'yesno',
     'col': 'has_report', 'datasets': {E}},
    {'id': 'report_at', 'label': 'Report date', 'tab': 'report', 'type': 'date',
     'col': 'report_at', 'datasets': {R}},
    {'id': 'final_signed_at', 'label': 'Final signature date', 'tab': 'report', 'type': 'date',
     'col': 'final_signed_at', 'datasets': {E, R}},
    {'id': 'radiologist', 'label': 'Radiologist', 'tab': 'report', 'type': 'multi',
     'col': 'radiologist', 'datasets': {E, R}, 'options': 'distinct', 'group': True},
    {'id': 'signature', 'label': 'Signature', 'tab': 'report', 'type': 'multi', 'col': 'signature',
     'datasets': {R}, 'options': [('Final', 'Final'), ('Preliminary', 'Preliminary'),
                                  ('Unspecified', 'Unspecified')], 'group': True},
    {'id': 'report_tat_hours', 'label': 'Report turnaround (hours)', 'tab': 'report',
     'type': 'range', 'col': 'report_tat_hours', 'datasets': {E, R}, 'unit': 'hours',
     'hint': 'Exam completion → final signature (the report itself where no signature level '
             'is sent).'},
    {'id': 'critical', 'label': 'Critical finding (NLP)', 'tab': 'report', 'type': 'yesno',
     'col': 'critical', 'datasets': {E, R}},

    # ── Data quality (BRAYN) ──
    {'id': 'open_quality_notes', 'label': 'Open BRAYN quality notes', 'tab': 'quality',
     'type': 'range', 'col': 'open_quality_notes', 'datasets': {E}, 'unit': 'notes',
     'hint': 'Contradictory times, skipped steps, no order received — see the BRAYN console.'},
    {'id': 'stalled', 'label': 'Stalled (BRAYN workflow)', 'tab': 'quality', 'type': 'yesno',
     'col': 'stalled', 'datasets': {E},
     'hint': 'An open stall: scheduled but never arrived, started but never completed, '
             'completed but never reported...'},
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

# sql runs over the view aliased d. additive: may be summed into a Total.
MEASURES = {
    'count':               {'label': None, 'sql': 'COUNT(*)', 'additive': True,
                            'datasets': {E, O, R}},
    'patients':            {'label': 'Patients', 'sql': 'COUNT(DISTINCT d.patient_id)',
                            'additive': False, 'datasets': {E, O, R}},
    'reported':            {'label': 'With a report',
                            'sql': 'COUNT(*) FILTER (WHERE d.has_report)', 'additive': True,
                            'datasets': {E}},
    'cancelled':           {'label': 'Cancelled',
                            'sql': "COUNT(*) FILTER (WHERE d.status = 'Cancelled')",
                            'additive': True, 'datasets': {E, O}},
    'critical':            {'label': 'Critical findings',
                            'sql': 'COUNT(*) FILTER (WHERE d.critical)', 'additive': True,
                            'datasets': {E, R}},
    'performed':           {'label': 'Performed', 'sql': 'COUNT(*) FILTER (WHERE d.performed)',
                            'additive': True, 'datasets': {O}},
    'not_performed':       {'label': 'Not performed',
                            'sql': 'COUNT(*) FILTER (WHERE NOT d.performed AND NOT d.cancelled)',
                            'additive': True, 'datasets': {O}},
    'final':               {'label': 'Final signed',
                            'sql': "COUNT(*) FILTER (WHERE d.signature = 'Final')",
                            'additive': True, 'datasets': {R}},
    'median_report_tat':   {'label': 'Median report TAT (h)',
                            'sql': 'round((percentile_cont(0.5) WITHIN GROUP '
                                   '(ORDER BY d.report_tat_hours))::numeric, 2)',
                            'additive': False, 'datasets': {E, R}},
    'median_exam_minutes': {'label': 'Median exam duration (min)',
                            'sql': 'round((percentile_cont(0.5) WITHIN GROUP '
                                   '(ORDER BY d.exam_minutes))::numeric, 1)',
                            'additive': False, 'datasets': {E}},
    'median_wait_minutes': {'label': 'Median wait (min)',
                            'sql': 'round((percentile_cont(0.5) WITHIN GROUP '
                                   '(ORDER BY d.wait_minutes))::numeric, 1)',
                            'additive': False, 'datasets': {E}},
    'median_order_to_exam': {'label': 'Median order → exam (h)',
                             'sql': 'round((percentile_cont(0.5) WITHIN GROUP '
                                    '(ORDER BY d.order_to_exam_hours))::numeric, 2)',
                             'additive': False, 'datasets': {O}},
    'avg_age':             {'label': 'Average age', 'sql': 'round(AVG(d.age)::numeric, 1)',
                            'additive': False, 'datasets': {E}},
}

# Columns a List may show, per dataset: (column, label). Every column of each view.
COLUMNS = {
    E: [('exam_at', 'Exam date'), ('accession', 'Accession'), ('patient_id', 'Patient ID'),
        ('sex', 'Sex'), ('age', 'Age'), ('patient_type', 'Patient type'),
        ('patient_location', 'Location'), ('modality', 'Modality'), ('procedure', 'Procedure'),
        ('procedure_code', 'Procedure code'), ('device', 'Device'), ('room', 'Room'),
        ('site', 'Site'), ('status', 'Status'), ('technician', 'Technician'),
        ('shift', 'Shift'), ('radiologist', 'Radiologist'), ('referring', 'Referring'),
        ('ordered_at', 'Ordered'), ('scheduled_at', 'Scheduled'), ('arrived_at', 'Arrived'),
        ('started_at', 'Started'), ('completed_at', 'Completed'), ('images_at', 'Images in PACS'),
        ('prelim_signed_at', 'Prelim signed'), ('final_signed_at', 'Final signed'),
        ('cancelled_at', 'Cancelled'), ('wait_minutes', 'Wait (min)'),
        ('exam_minutes', 'Exam (min)'), ('report_tat_hours', 'Report TAT (h)'),
        ('has_report', 'Report'), ('critical', 'Critical'),
        ('open_quality_notes', 'BRAYN notes'), ('stalled', 'Stalled'),
        ('placer_order_number', 'Order number'), ('visit_number', 'Visit')],
    O: [('ordered_at', 'Ordered'), ('placer_order_number', 'Order number'),
        ('accession', 'Accession'), ('patient_id', 'Patient ID'),
        ('patient_type', 'Patient type'), ('patient_location', 'Location'),
        ('procedure', 'Procedure'), ('procedure_code', 'Procedure code'),
        ('modality', 'Modality'), ('site', 'Site'), ('referring', 'Referring'),
        ('ordering_physician', 'Ordering physician'), ('status', 'Status'),
        ('scheduled_at', 'Scheduled'), ('performed_at', 'Performed'),
        ('order_to_schedule_hours', 'Order → scheduled (h)'),
        ('order_to_exam_hours', 'Order → exam (h)')],
    R: [('report_at', 'Reported'), ('accession', 'Accession'), ('patient_id', 'Patient ID'),
        ('radiologist', 'Radiologist'), ('modality', 'Modality'), ('procedure', 'Procedure'),
        ('device', 'Device'), ('site', 'Site'), ('patient_type', 'Patient type'),
        ('signature', 'Signature'), ('prelim_signed_at', 'Prelim signed'),
        ('final_signed_at', 'Final signed'), ('completed_at', 'Exam completed'),
        ('report_tat_hours', 'Report TAT (h)'), ('critical', 'Critical'),
        ('referring', 'Referring')],
}


# ── The SQL reference ─────────────────────────────────────────────────────────
#
# What a hand-written worklist may query, column by column. EVERY column of every
# rpt view is described here — tests/test_worklists.py compares this against the
# views themselves, so a column added to a view without a description fails the
# suite rather than reaching an engineer undocumented (operator, 2026-10-25:
# "writing SQL needs a very perfect documentation ... or disable it").
# Meanings follow the view definitions in migration 0136.

_STEP_TIME = 'Time of the {} step: the earliest message for it, hospital time.'
_PATIENT = {
    'patient_id':       'Patient ID (MRN), PID-3.',
    'patient_class':    'Patient class code as sent in PV1-2: I, O, E, or another site code.',
    'patient_type':     'patient_class in words: Inpatient, Outpatient, Emergency; other codes '
                        'as sent.',
    'patient_location': 'Point of care from PV1-3 (first component), e.g. ER or RAD.',
}

COLUMN_DOCS = {
    E: {
        'accession':         'Accession number: the RIS study number. One row per accession.',
        **{k: _PATIENT[k] for k in ('patient_id', 'patient_class', 'patient_type',
                                    'patient_location')},
        'sex':               'Sex from the patient\'s ADT (PID-8): M, F, U ...',
        'age':               'Age in whole years on the exam date, from the ADT birth date. '
                             'Empty when no birth date is known.',
        'age_band':          'Age group: 0-17, 18-39, 40-64 or 65+.',
        'modality':          'Modality: from the device map when the device is known, else the '
                             'modality the message carried (OBR-24).',
        'device':            'AE title of the device the exam ran on (OBR-21).',
        'room':              'Room from the exam messages (PV1-3.2), else the device\'s room in '
                             'the device map.',
        'site':              'Site of the device, from the device map. Empty when the device has '
                             'no site.',
        'procedure_code':    'Procedure code (OBR-4.1).',
        'procedure':         'Procedure description (OBR-4.2).',
        'status':            'Furthest step reached: Ordered, Scheduled, Arrived, Started, '
                             'Completed, Reported, Signed preliminary, Signed final, or Cancelled.',
        'ordered_at':        _STEP_TIME.format('Ordered (HIS order)'),
        'scheduled_at':      _STEP_TIME.format('Scheduled'),
        'arrived_at':        _STEP_TIME.format('Arrived'),
        'started_at':        _STEP_TIME.format('Started'),
        'completed_at':      _STEP_TIME.format('Completed'),
        'images_at':         'When PACS reported the study complete (images stored).',
        'prelim_signed_at':  'First preliminary signature of the report.',
        'final_signed_at':   'Final signature of the report.',
        'report_final_at':   'End of the report turnaround: the final signature, else the report '
                             'time when the sender sends no signature level.',
        'cancelled_at':      'When the study was cancelled.',
        'exam_at':           'The study\'s date and time: completed, else images in PACS, '
                             'started, arrived, scheduled, then the report times. The same anchor '
                             'the reports count a study under, so both agree on the day.',
        'exam_date':         'exam_at as a date without time, for grouping by day.',
        'performed_at':      'When the technologist had the patient: started, else completed or '
                             'images in PACS. hour and shift use this time.',
        'shift':             'Shift name (AM, PM, NIGHT ...) from the Shift hours, by the hour '
                             'of performed_at.',
        'week_of_month':     'Week of the month of exam_at: 1 = days 1-7, 2 = days 8-14, ... '
                             '5 = days 29-31.',
        'weekday':           'Weekday of exam_at: 1 = Monday ... 7 = Sunday.',
        'hour':              'Hour of performed_at, 0-23.',
        'technician':        'Who the Started message names (ORC-19), else the Completed one.',
        'radiologist':       'ID of the radiologist who signed the report.',
        'referring':         'Referring physician\'s name.',
        'has_report':        'true when a report was received for the study.',
        'critical':          'true when the NLP found a critical finding in the report.',
        'open_quality_notes': 'Number of open BRAYN data-quality notes on the study.',
        'stalled':           'true when the study has an open BRAYN delay note (waiting past a '
                             'limit).',
        'wait_minutes':      'Arrived to Started, in minutes.',
        'exam_minutes':      'Started to Completed, in minutes.',
        'report_tat_hours':  'Report turnaround: Completed (else images in PACS) to '
                             'report_final_at, in hours.',
        'placer_order_number': 'HIS order number (ORC-2).',
        'visit_number':      'Visit number (PV1-19).',
    },
    O: {
        'order_id':          'Row number of the order in RAYD. One row per HIS order received.',
        'ordered_at':        'When the HIS sent the order (message time, else the time RAYD '
                             'received it).',
        'order_date':        'ordered_at as a date without time.',
        'placer_order_number': 'HIS order number (ORC-2).',
        'accession':         'The study\'s accession: from the order when it carried one, else '
                             'from the study matched by order number. Empty until the RIS '
                             'schedules it.',
        **_PATIENT,
        'procedure_code':    'Procedure code on the order (OBR-4.1).',
        'procedure':         'Procedure description on the order, else the study\'s.',
        'modality':          'Modality: device map of the matched study\'s device, else the '
                             'study\'s, else the order\'s.',
        'site':              'Site of the matched study\'s device.',
        'referring':         'Referring physician on the order (PV1-8).',
        'ordering_physician': 'Ordering provider on the order (ORC-12).',
        'status':            'Furthest step of the matched study: Not scheduled, Scheduled, '
                             'Arrived, Started, Completed, Reported, or Cancelled.',
        'scheduled_at':      'When the RIS scheduled the order.',
        'performed_at':      'When the exam was done: completed, else images in PACS, else '
                             'started.',
        'scheduled':         'true once any step after the order was received (scheduled, '
                             'arrived, started, completed, images).',
        'performed':         'true once the exam started, completed or reached PACS.',
        'reported':          'true once a report or a signature was received.',
        'cancelled':         'true when the order or its study was cancelled.',
        'order_to_schedule_hours': 'ordered_at to scheduled_at, in hours.',
        'order_to_exam_hours': 'ordered_at to completed (else images in PACS), in hours.',
    },
    R: {
        'report_id':         'Row number of the report in RAYD. One row per report.',
        'accession':         'Accession number of the study the report is for.',
        'patient_id':        'Patient ID (MRN) from the report, else from the study.',
        'radiologist':       'ID of the radiologist who signed the report.',
        'modality':          'Modality: device map of the study\'s device, else the study\'s, '
                             'else the report\'s.',
        'procedure':         'Procedure description of the study, else the report\'s.',
        'device':            'AE title of the study\'s device.',
        'site':              'Site of the study\'s device.',
        'patient_class':     'Patient class code of the study (PV1-2).',
        'patient_type':      'patient_class in words: Inpatient, Outpatient, Emergency.',
        'report_at':         'Report time the sender put on the report (OBR-22, else OBR-7).',
        'report_date':       'report_at as a date without time.',
        'signature':         'Final, Preliminary, or Unspecified when the sender sends no '
                             'signature level.',
        'prelim_signed_at':  'First preliminary signature.',
        'final_signed_at':   'Final signature.',
        'completed_at':      'When the exam completed (else images in PACS).',
        'report_tat_hours':  'Report turnaround: completed_at to the final signature (else the '
                             'report time when no signature level is sent), in hours.',
        'critical':          'true when the NLP found a critical finding in the report.',
        'referring':         'Referring physician named on the report.',
    },
}

# Worked examples. Every one is run against the sandbox by the test suite, so an
# example that stops working fails there first.
SQL_EXAMPLES = [
    {'title': 'Exams per modality, last month',
     'what': 'A count per modality for the previous calendar month.',
     'sql': "SELECT modality, COUNT(*) AS \"Exams\"\n"
            "FROM rpt.exams\n"
            "WHERE exam_at >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'\n"
            "  AND exam_at <  date_trunc('month', CURRENT_DATE)\n"
            "GROUP BY modality\n"
            "ORDER BY 2 DESC"},
    {'title': 'Exams per technician on the AM shift, this week',
     'what': 'Uses the shift column (Shift hours) and the technician of the Started message.',
     'sql': "SELECT technician, COUNT(*) AS \"Exams\"\n"
            "FROM rpt.exams\n"
            "WHERE shift = 'AM'\n"
            "  AND exam_at >= date_trunc('week', CURRENT_DATE)\n"
            "GROUP BY technician\n"
            "ORDER BY 2 DESC"},
    {'title': 'HIS orders not done after 2 days',
     'what': 'Orders older than 48 hours that were neither performed nor cancelled.',
     'sql': "SELECT ordered_at, placer_order_number, patient_id, procedure, status\n"
            "FROM rpt.orders\n"
            "WHERE NOT performed AND NOT cancelled\n"
            "  AND ordered_at < LOCALTIMESTAMP - INTERVAL '2 days'\n"
            "ORDER BY ordered_at"},
    {'title': 'Median report turnaround per radiologist, last 30 days',
     'what': 'Hours from exam completion to final signature, per signing radiologist.',
     'sql': "SELECT radiologist, COUNT(*) AS \"Reports\",\n"
            "       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY report_tat_hours)::numeric, 2)\n"
            "           AS \"Median TAT (h)\"\n"
            "FROM rpt.reports\n"
            "WHERE report_at >= CURRENT_DATE - 30 AND report_tat_hours IS NOT NULL\n"
            "GROUP BY radiologist\n"
            "ORDER BY 3"},
    {'title': 'Exams with their HIS order, last 7 days',
     'what': 'Joins exams to orders on the accession to show the referring doctor and the '
             'order time beside each exam.',
     'sql': "SELECT e.exam_at, e.accession, e.modality, o.ordered_at, o.referring\n"
            "FROM rpt.exams e\n"
            "JOIN rpt.orders o ON o.accession = e.accession\n"
            "WHERE e.exam_at >= CURRENT_DATE - 7\n"
            "ORDER BY e.exam_at DESC"},
    {'title': 'Studies BRAYN has flagged',
     'what': 'Studies with open data-quality notes or waiting past a delay limit.',
     'sql': "SELECT exam_at, accession, modality, status, open_quality_notes, stalled\n"
            "FROM rpt.exams\n"
            "WHERE open_quality_notes > 0 OR stalled\n"
            "ORDER BY exam_at DESC"},
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
