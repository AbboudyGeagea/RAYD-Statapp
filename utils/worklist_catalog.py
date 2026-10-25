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

    # ── Data quality (RAY7) ──
    {'id': 'open_quality_notes', 'label': 'Open RAY7 quality notes', 'tab': 'quality',
     'type': 'range', 'col': 'open_quality_notes', 'datasets': {E}, 'unit': 'notes',
     'hint': 'Contradictory times, skipped steps, no order received — see the RAY7 console.'},
    {'id': 'stalled', 'label': 'Stalled (RAY7 workflow)', 'tab': 'quality', 'type': 'yesno',
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
        ('open_quality_notes', 'RAY7 notes'), ('stalled', 'Stalled'),
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
