"""
utils/hl7_dictionary.py
────────────────────────────────────────────────────────────────
HL7 v2.4 field and component names, for the mapping explorer.

WHY THIS EXISTS
The mapping editor used to ask an engineer to type a segment and a field number.
But knowing it is OBR-24 IS the hard part — the form assumed the answer before
you could enter it. With a dictionary the tool can show

    19-Action By [XCN]        TECH01^HADDAD^RIMA

against a real captured message, and the engineer clicks it. The knowledge comes
from the tool rather than from the person, which matters doubly here because
several of our own field positions are still educated guesses.

SCOPE: the segments this interface actually uses — MSH, PID, PV1, ORC, OBR, OBX,
NTE, MSA. Deliberately not all ~120 segments of v2.4. An unknown segment is not
an error; it renders as numbered fields with their values, which is still enough
to click and map. Z-segments are custom by definition and have no published
meaning at all, so they always render that way.

A WRONG NAME IS WORSE THAN A NUMBER. "field 19" is honestly unhelpful; "19-Action
By" that actually points at field 18 is actively misleading and would be believed.
These are transcribed from the v2.4 standard and every field this codebase
already reads is cross-checked against the parser in utils/hl7_parse.py.

FIELD NUMBERS HERE ARE SPEC NUMBERS. MSH-9 is 9. The off-by-one that makes MSH-9
land at index 8 after splitting is absorbed in utils/hl7_fieldmap._extract, so it
never reaches this file and never reaches the person configuring a mapping.
"""

# Composite data types, by component position. A field's type decides which of
# these the explorer offers when the engineer drills into it.
COMPONENTS = {
    'XPN': ['Family Name', 'Given Name', 'Middle Name', 'Suffix', 'Prefix',
            'Degree', 'Name Type Code'],
    'XCN': ['ID Number', 'Family Name', 'Given Name', 'Middle Name', 'Suffix',
            'Prefix', 'Degree', 'Source Table', 'Assigning Authority'],
    'CX':  ['ID', 'Check Digit', 'Check Digit Scheme', 'Assigning Authority',
            'Identifier Type Code', 'Assigning Facility'],
    'CE':  ['Identifier', 'Text', 'Coding System', 'Alternate Identifier',
            'Alternate Text', 'Alternate Coding System'],
    'CWE': ['Identifier', 'Text', 'Coding System', 'Alternate Identifier',
            'Alternate Text', 'Alternate Coding System'],
    'EI':  ['Entity Identifier', 'Namespace ID', 'Universal ID', 'Universal ID Type'],
    'HD':  ['Namespace ID', 'Universal ID', 'Universal ID Type'],
    'PL':  ['Point of Care', 'Room', 'Bed', 'Facility', 'Location Status',
            'Person Location Type', 'Building', 'Floor'],
    'MSG': ['Message Code', 'Trigger Event', 'Message Structure'],
    'VID': ['Version ID', 'Internationalization Code'],
    'PT':  ['Processing ID', 'Processing Mode'],
    'TS':  ['Time', 'Degree of Precision'],
    'XTN': ['Telephone Number', 'Use Code', 'Equipment Type', 'Email',
            'Country Code', 'Area Code', 'Local Number'],
    'XAD': ['Street Address', 'Other Designation', 'City', 'State', 'Zip', 'Country'],
    'XON': ['Organization Name', 'Organization Name Type', 'ID Number',
            'Check Digit', 'Check Digit Scheme', 'Assigning Authority'],
    'NDL': ['Name', 'Start Date/Time', 'End Date/Time', 'Point of Care', 'Room',
            'Bed', 'Facility', 'Location Status', 'Person Location Type'],
    'EIP': ['Placer Assigned Identifier', 'Filler Assigned Identifier'],
    'TQ':  ['Quantity', 'Interval', 'Duration', 'Start Date/Time', 'End Date/Time',
            'Priority'],
}

# (field number, name, data type). Gaps are intentional where a field is
# unused in practice — an absent entry renders as "field N", which is honest.
SEGMENTS = {
    'MSH': ('Message Header', [
        (1, 'Field Separator', 'ST'), (2, 'Encoding Characters', 'ST'),
        (3, 'Sending Application', 'HD'), (4, 'Sending Facility', 'HD'),
        (5, 'Receiving Application', 'HD'), (6, 'Receiving Facility', 'HD'),
        (7, 'Date/Time of Message', 'TS'), (8, 'Security', 'ST'),
        (9, 'Message Type', 'MSG'), (10, 'Message Control ID', 'ST'),
        (11, 'Processing ID', 'PT'), (12, 'Version ID', 'VID'),
        (13, 'Sequence Number', 'NM'), (14, 'Continuation Pointer', 'ST'),
        (15, 'Accept Acknowledgment Type', 'ID'),
        (16, 'Application Acknowledgment Type', 'ID'),
        (17, 'Country Code', 'ID'), (18, 'Character Set', 'ID'),
        (19, 'Principal Language of Message', 'CE'),
    ]),
    'MSA': ('Message Acknowledgment', [
        (1, 'Acknowledgment Code', 'ID'), (2, 'Message Control ID', 'ST'),
        (3, 'Text Message', 'ST'), (4, 'Expected Sequence Number', 'NM'),
        (6, 'Error Condition', 'CE'),
    ]),
    'PID': ('Patient Identification', [
        (1, 'Set ID', 'SI'), (2, 'Patient ID', 'CX'),
        (3, 'Patient Identifier List', 'CX'), (4, 'Alternate Patient ID', 'CX'),
        (5, 'Patient Name', 'XPN'), (6, "Mother's Maiden Name", 'XPN'),
        (7, 'Date/Time of Birth', 'TS'), (8, 'Administrative Sex', 'IS'),
        (9, 'Patient Alias', 'XPN'), (10, 'Race', 'CE'),
        (11, 'Patient Address', 'XAD'), (12, 'County Code', 'IS'),
        (13, 'Phone Number - Home', 'XTN'), (14, 'Phone Number - Business', 'XTN'),
        (15, 'Primary Language', 'CE'), (16, 'Marital Status', 'CE'),
        (17, 'Religion', 'CE'), (18, 'Patient Account Number', 'CX'),
        (19, 'SSN Number', 'ST'), (20, "Driver's License Number", 'DLN'),
        (22, 'Ethnic Group', 'CE'), (23, 'Birth Place', 'ST'),
        (24, 'Multiple Birth Indicator', 'ID'), (25, 'Birth Order', 'NM'),
        (26, 'Citizenship', 'CE'), (28, 'Nationality', 'CE'),
        (29, 'Patient Death Date and Time', 'TS'),
        (30, 'Patient Death Indicator', 'ID'),
    ]),
    'PV1': ('Patient Visit', [
        (1, 'Set ID', 'SI'), (2, 'Patient Class', 'IS'),
        (3, 'Assigned Patient Location', 'PL'), (4, 'Admission Type', 'IS'),
        (5, 'Preadmit Number', 'CX'), (6, 'Prior Patient Location', 'PL'),
        (7, 'Attending Doctor', 'XCN'), (8, 'Referring Doctor', 'XCN'),
        (9, 'Consulting Doctor', 'XCN'), (10, 'Hospital Service', 'IS'),
        (11, 'Temporary Location', 'PL'), (14, 'Admit Source', 'IS'),
        (15, 'Ambulatory Status', 'IS'), (16, 'VIP Indicator', 'IS'),
        (17, 'Admitting Doctor', 'XCN'), (18, 'Patient Type', 'IS'),
        (19, 'Visit Number', 'CX'), (20, 'Financial Class', 'FC'),
        (36, 'Discharge Disposition', 'IS'), (37, 'Discharged to Location', 'DLD'),
        (39, 'Servicing Facility', 'IS'), (44, 'Admit Date/Time', 'TS'),
        (45, 'Discharge Date/Time', 'TS'),
    ]),
    'ORC': ('Common Order', [
        (1, 'Order Control', 'ID'), (2, 'Placer Order Number', 'EI'),
        (3, 'Filler Order Number', 'EI'), (4, 'Placer Group Number', 'EI'),
        (5, 'Order Status', 'ID'), (6, 'Response Flag', 'ID'),
        (7, 'Quantity/Timing', 'TQ'), (8, 'Parent', 'EIP'),
        (9, 'Date/Time of Transaction', 'TS'), (10, 'Entered By', 'XCN'),
        (11, 'Verified By', 'XCN'), (12, 'Ordering Provider', 'XCN'),
        (13, "Enterer's Location", 'PL'), (14, 'Call Back Phone Number', 'XTN'),
        (15, 'Order Effective Date/Time', 'TS'),
        (16, 'Order Control Code Reason', 'CE'),
        (17, 'Entering Organization', 'CE'), (18, 'Entering Device', 'CE'),
        (19, 'Action By', 'XCN'), (21, 'Ordering Facility Name', 'XON'),
        (22, 'Ordering Facility Address', 'XAD'),
        (24, 'Ordering Provider Address', 'XAD'),
        (25, 'Order Status Modifier', 'CWE'),
    ]),
    'OBR': ('Observation Request', [
        (1, 'Set ID', 'SI'), (2, 'Placer Order Number', 'EI'),
        (3, 'Filler Order Number', 'EI'), (4, 'Universal Service Identifier', 'CE'),
        (5, 'Priority', 'ID'), (6, 'Requested Date/Time', 'TS'),
        (7, 'Observation Date/Time', 'TS'), (8, 'Observation End Date/Time', 'TS'),
        (10, 'Collector Identifier', 'XCN'), (11, 'Specimen Action Code', 'ID'),
        (13, 'Relevant Clinical Info', 'ST'), (15, 'Specimen Source', 'SPS'),
        (16, 'Ordering Provider', 'XCN'), (17, 'Order Callback Phone Number', 'XTN'),
        (18, 'Placer Field 1', 'ST'), (19, 'Placer Field 2', 'ST'),
        (20, 'Filler Field 1', 'ST'), (21, 'Filler Field 2', 'ST'),
        (22, 'Results Report/Status Change Date/Time', 'TS'),
        (24, 'Diagnostic Service Section ID', 'ID'), (25, 'Result Status', 'ID'),
        (26, 'Parent Result', 'PRL'), (27, 'Quantity/Timing', 'TQ'),
        (28, 'Result Copies To', 'XCN'), (29, 'Parent', 'EIP'),
        (30, 'Transportation Mode', 'ID'), (31, 'Reason for Study', 'CE'),
        (32, 'Principal Result Interpreter', 'NDL'),
        (33, 'Assistant Result Interpreter', 'NDL'),
        (34, 'Technician', 'NDL'), (35, 'Transcriptionist', 'NDL'),
        (36, 'Scheduled Date/Time', 'TS'),
        (44, 'Procedure Code', 'CE'), (45, 'Procedure Code Modifier', 'CE'),
    ]),
    'OBX': ('Observation / Result', [
        (1, 'Set ID', 'SI'), (2, 'Value Type', 'ID'),
        (3, 'Observation Identifier', 'CE'), (4, 'Observation Sub-ID', 'ST'),
        (5, 'Observation Value', 'varies'), (6, 'Units', 'CE'),
        (7, 'References Range', 'ST'), (8, 'Abnormal Flags', 'IS'),
        (11, 'Observation Result Status', 'ID'),
        (14, 'Date/Time of the Observation', 'TS'),
        (15, "Producer's ID", 'CE'), (16, 'Responsible Observer', 'XCN'),
        (17, 'Observation Method', 'CE'),
        (18, 'Equipment Instance Identifier', 'EI'),
        (19, 'Date/Time of the Analysis', 'TS'),
    ]),
    'NTE': ('Notes and Comments', [
        (1, 'Set ID', 'SI'), (2, 'Source of Comment', 'ID'),
        (3, 'Comment', 'FT'), (4, 'Comment Type', 'CE'),
    ]),
}

# Fields this pipeline already reads, so the explorer can mark them. Seeing that
# a field is ALREADY in use is what stops an engineer mapping a second source
# onto something that is working, and shows at a glance which of our guesses a
# capture is about to confirm or refute.
IN_USE = {
    ('MSH', 3): 'sending app — archive dedupe key, and PACS-vs-RIS discrimination',
    ('MSH', 4): 'sending facility',
    ('MSH', 7): 'message timestamp',
    ('MSH', 9): 'message type — decides how the message is parsed',
    ('MSH', 10): 'control ID — archive dedupe key',
    ('MSH', 12): 'HL7 version',
    ('PID', 3): 'patient ID',
    ('PID', 5): 'patient name',
    ('PID', 7): 'birth date — drives age at exam',
    ('PID', 8): 'sex',
    ('PV1', 2): 'patient class',
    ('PV1', 3): 'patient location (.1) and room (.2) — ER detection reads the prefix',
    ('PV1', 8): 'referring doctor',
    ('ORC', 1): 'order control — NW marks a new order',
    ('ORC', 2): 'placer order number',
    ('ORC', 3): 'accession number',
    ('ORC', 5): 'order status — mapped to the lifecycle state',
    ('ORC', 9): 'event timestamp',
    ('ORC', 19): 'performed by — PROVISIONAL, unconfirmed against a real message',
    ('OBR', 3): 'accession number (ORU)',
    ('OBR', 4): 'procedure code (.1) and description (.2)',
    ('OBR', 21): 'device AE title — PROVISIONAL, unconfirmed against a real message',
    ('OBR', 22): 'result timestamp (ORU)',
    ('OBR', 24): 'modality',
}


def field_info(segment, number):
    """(name, data_type) for a field, or (None, None) if undefined here."""
    defn = SEGMENTS.get(segment)
    if not defn:
        return (None, None)
    for num, name, dtype in defn[1]:
        if num == number:
            return (name, dtype)
    return (None, None)


def components_for(data_type):
    return COMPONENTS.get(data_type, [])


def describe_segment(name):
    defn = SEGMENTS.get(name)
    if defn:
        return defn[0]
    if name.startswith('Z'):
        # Z-segments are custom by definition; no standard describes them.
        return 'Custom segment (site-specific, no standard definition)'
    return 'Unrecognised segment'


def explode(raw_message):
    """
    Turn a message into the structure the explorer renders: every segment, every
    populated field, with its spec name, type, value and component breakdown.

    Empty fields are omitted. A real ORC runs to 25 fields of which five carry
    anything, and listing twenty blanks buries the five that matter.
    """
    from utils.hl7_parse import split_segments

    out = []
    for idx, raw in enumerate(split_segments(raw_message)):
        parts = raw.split('|')
        name = parts[0]
        is_msh = name == 'MSH'
        fields = []

        for i, value in enumerate(parts):
            if i == 0:
                continue
            # MSH-1 IS the separator and MSH-2 the encoding characters, so for
            # MSH the spec number is index+1; every other segment reads straight.
            number = i + 1 if is_msh else i
            value = (value or '').strip()
            if not value:
                continue

            fname, dtype = field_info(name, number)
            comps = []
            if '^' in value:
                labels = components_for(dtype)
                for ci, cval in enumerate(value.split('^'), start=1):
                    cval = cval.strip()
                    if not cval:
                        continue
                    comps.append({
                        'index': ci,
                        'label': labels[ci - 1] if ci <= len(labels) else None,
                        'value': cval,
                    })

            fields.append({
                'number': number,
                'name': fname,
                'type': dtype,
                'value': value,
                'components': comps,
                'in_use': IN_USE.get((name, number)),
            })

        out.append({
            'index': idx,
            'name': name,
            'description': describe_segment(name),
            'known': name in SEGMENTS,
            'raw': raw,
            'fields': fields,
        })
    return out
