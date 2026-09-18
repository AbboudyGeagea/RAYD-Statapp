"""
utils/master_import.py
────────────────────────────────────────────────────────────────
CSV import for the master data HL7 cannot carry.

The cutover splits reference data three ways. Most of it is derivable from the
message stream — the lifecycle, the studies, the worklist tables. Some of it is
not, because it describes the ORGANISATION rather than the work: who the staff
are and what role each holds, how many minutes a device is open, what a room is
called. No HL7 message carries any of that, and it does not change often enough
to justify one.

MFN is the eventual source; that spec is still open with the integration
specialist. This is the other half the operator asked for — "everything else
should be sent via MFN or implementation engineer import" — and it is
deliberately built so MFN becomes a second input into the same tables rather than
a parallel system. The datasets, the column contracts and the validation defined
here are what both paths will write through.

WHY VALIDATION IS SEPARATE FROM COMMIT
Every import runs as a dry pass first, returning per-row errors and a preview,
and commits only when asked. Master data is small and rarely touched, which means
mistakes in it survive for months and are attributed to the reports rather than
to the import that caused them: a mistyped role code does not break anything
loudly, it just quietly leaves a technologist out of every technician report. The
cost of a preview step is one extra click; the cost of skipping it is a figure
nobody can explain.

WHAT THIS DOES NOT TOUCH
Nothing derived from messages. An import that could overwrite the lifecycle would
let a spreadsheet silently rewrite clinical history.
"""
import csv
import io
import logging

from sqlalchemy import text
from db import db

logger = logging.getLogger("MASTER_IMPORT")


# Role codes as the RIS ETL defined them, so an imported roster and an
# Oracle-sourced one mean the same thing by 'TEC'. Report 35 filters on exactly
# this value; a site inventing its own would silently produce an empty
# technician report.
ROLE_CODES = {
    'TEC':      'Technologist',
    'RAD':      'Radiologist',
    'DOC':      'Doctor',
    'RES':      'Resident',
    'NUR':      'Nurse',
    'TRA':      'Transcriptionist',
    'REC':      'Receptionist',
    'CLERK':    'Clerk',
    'ATT':      'Attending',
    'PHYS':     'Physicist',
    'CON':      'Consultant',
    'RADAD':    'Rad Admin',
    'RISAdmin': 'RIS Administrator',
}


class Dataset:
    """One importable master-data set: its columns, its rules, its writer."""

    def __init__(self, key, label, table, columns, required, describe, writer):
        self.key = key
        self.label = label
        self.table = table
        self.columns = columns          # ordered, for the CSV template
        self.required = required
        self.describe = describe
        self.writer = writer


def _clean(row):
    return {(k or '').strip().lower().replace(' ', '_'): (v or '').strip()
            for k, v in row.items() if k}


# ── staff roster ──────────────────────────────────────────────────────────────

def _validate_staff(rows):
    """
    Per-row errors, by CSV line number.

    resource_id is the identifier the HL7 messages carry in the "by whom" field —
    an LDAP account at LAUMC. It is what links a person to the work they did, so
    a roster whose IDs do not match what the wire sends resolves nothing, and
    that is the single most likely way this import fails usefully-looking.
    """
    errors = []
    seen = {}
    for i, r in enumerate(rows, start=2):
        rid = r.get('resource_id', '')
        role = r.get('role_code', '')
        if not rid:
            errors.append((i, 'resource_id is required — it must match the value the '
                              'HL7 messages carry in the performer field'))
        elif rid.lower() in seen:
            errors.append((i, f'duplicate resource_id "{rid}", first seen on line {seen[rid.lower()]}'))
        else:
            seen[rid.lower()] = i
        if not role:
            errors.append((i, 'role_code is required'))
        elif role not in ROLE_CODES:
            errors.append((i, f'unknown role_code "{role}" — expected one of: '
                              + ', '.join(sorted(ROLE_CODES))))
        if not (r.get('last_name') or r.get('common_name')):
            errors.append((i, 'a last_name or a common_name is required, '
                              'or the person has no displayable name'))
    return errors


_STAFF_SQL = """
INSERT INTO std_resources_ris
    (resource_id_key, resource_id, role_code, role_description,
     first_name, last_name, common_name, primary_email_address,
     active, last_update)
VALUES
    (hl7_surrogate_id('staff', :resource_id), :resource_id, :role_code, :role_description,
     :first_name, :last_name, :common_name, :email, :active, NOW())
ON CONFLICT (resource_id_key) DO UPDATE SET
    resource_id           = EXCLUDED.resource_id,
    role_code             = EXCLUDED.role_code,
    role_description      = EXCLUDED.role_description,
    first_name            = EXCLUDED.first_name,
    last_name             = EXCLUDED.last_name,
    common_name           = EXCLUDED.common_name,
    primary_email_address = EXCLUDED.primary_email_address,
    active                = EXCLUDED.active,
    last_update           = NOW()
"""


def _write_staff(rows):
    n = 0
    for r in rows:
        db.session.execute(text(_STAFF_SQL), {
            'resource_id':      r['resource_id'],
            'role_code':        r['role_code'],
            'role_description': ROLE_CODES.get(r['role_code'], r['role_code']),
            'first_name':       r.get('first_name') or None,
            'last_name':        r.get('last_name') or None,
            'common_name':      r.get('common_name') or (
                ' '.join(x for x in (r.get('first_name'), r.get('last_name')) if x) or None),
            'email':            r.get('email') or None,
            # Anything but an explicit no counts as active: a roster with the
            # column omitted should import everyone rather than nobody.
            'active':           (r.get('active', '').lower() not in ('false', 'no', '0', 'n')),
        })
        n += 1
    return n


# ── device capacity ───────────────────────────────────────────────────────────

def _validate_capacity(rows):
    """
    Capacity is the DENOMINATOR of every utilisation figure.

    No HL7 message carries how long a device is open, so without this every
    utilisation percentage is either absent or computed against a default nobody
    chose. A wrong number here does not look wrong — it produces a plausible
    percentage that is simply untrue, which is why the bounds check is strict
    rather than advisory.
    """
    errors = []
    for i, r in enumerate(rows, start=2):
        if not r.get('aetitle'):
            errors.append((i, 'aetitle is required'))
        dow, mins = r.get('day_of_week', ''), r.get('open_minutes', '')
        if dow == '':
            errors.append((i, 'day_of_week is required (0=Monday … 6=Sunday)'))
        else:
            try:
                if not 0 <= int(dow) <= 6:
                    errors.append((i, f'day_of_week "{dow}" out of range — 0=Monday … 6=Sunday'))
            except ValueError:
                errors.append((i, f'day_of_week "{dow}" is not a number'))
        try:
            m = int(mins)
            if not 0 <= m <= 1440:
                errors.append((i, f'open_minutes {m} is not within a day (0–1440)'))
        except ValueError:
            errors.append((i, f'open_minutes "{mins}" is not a number'))
    return errors


def _write_capacity(rows):
    n = 0
    for r in rows:
        db.session.execute(text("""
            INSERT INTO device_weekly_schedule (aetitle, day_of_week, std_opening_minutes)
            VALUES (:ae, :dow, :mins)
            ON CONFLICT (aetitle, day_of_week) DO UPDATE SET
                std_opening_minutes = EXCLUDED.std_opening_minutes
        """), {'ae': r['aetitle'].strip(), 'dow': int(r['day_of_week']),
               'mins': int(r['open_minutes'])})
        n += 1
    return n


DATASETS = {
    'staff': Dataset(
        key='staff', label='Staff roster', table='std_resources_ris',
        columns=['resource_id', 'role_code', 'first_name', 'last_name',
                 'common_name', 'email', 'active'],
        required=['resource_id', 'role_code'],
        describe=('Who the staff are and what role each holds. resource_id must match '
                  'the identifier the HL7 messages carry in the performer field — that '
                  'link is what resolves a name onto the work. role_code TEC drives the '
                  'technician reports; RAD and RES separate radiologists from residents.'),
        writer=(_validate_staff, _write_staff)),

    'capacity': Dataset(
        key='capacity', label='Device weekly capacity', table='device_weekly_schedule',
        columns=['aetitle', 'day_of_week', 'open_minutes'],
        required=['aetitle', 'day_of_week', 'open_minutes'],
        describe=('How many minutes each device is open per weekday, 0=Monday … 6=Sunday. '
                  'This is the denominator of every utilisation figure; no HL7 message '
                  'carries it, so without it utilisation is computed against a default '
                  'nobody chose.'),
        writer=(_validate_capacity, _write_capacity)),
}


def template_csv(key):
    ds = DATASETS[key]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(ds.columns)
    if key == 'staff':
        w.writerow(['rima.haddad', 'TEC', 'Rima', 'Haddad', '', 'rima.haddad@site.org', 'true'])
        w.writerow(['dany.abouchedid', 'RAD', 'Dany', 'Abou Chedid', '', '', 'true'])
    elif key == 'capacity':
        for d in range(5):
            w.writerow(['CT64_RH', d, 720])
        w.writerow(['CT64_RH', 5, 300])
        w.writerow(['CT64_RH', 6, 0])
    return buf.getvalue()


def run_import(key, csv_text, commit=False):
    """
    Validate, and commit only when asked.

    Returns {ok, dataset, rows, errors, preview, written}. Errors are per CSV line
    so they can be fixed in the source file rather than hunted for.
    """
    if key not in DATASETS:
        return {'ok': False, 'errors': [(0, f'unknown dataset {key}')]}
    ds = DATASETS[key]
    validate, write = ds.writer

    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        rows = [_clean(r) for r in reader]
    except Exception as exc:
        return {'ok': False, 'errors': [(0, f'could not read the CSV: {exc}')]}

    if not rows:
        return {'ok': False, 'errors': [(0, 'the file has a header but no rows')]}

    missing = [c for c in ds.required if c not in rows[0]]
    if missing:
        return {'ok': False,
                'errors': [(1, 'missing required column(s): ' + ', '.join(missing)
                            + '. Expected header: ' + ', '.join(ds.columns))]}

    errors = validate(rows)
    result = {
        'ok': not errors, 'dataset': ds.label, 'rows': len(rows),
        'errors': errors[:100], 'error_count': len(errors),
        'preview': rows[:15], 'written': 0, 'committed': False,
    }
    if errors or not commit:
        return result

    # All-or-nothing. A partially applied roster is worse than none: the reports
    # would resolve some names and silently drop the rest, which reads as a data
    # problem rather than an incomplete import.
    try:
        with db.session.begin_nested():
            result['written'] = write(rows)
        db.session.commit()
        result['committed'] = True
    except Exception as exc:
        db.session.rollback()
        logger.exception("master import failed | dataset=%s", key)
        result['ok'] = False
        result['errors'] = [(0, f'import failed and nothing was written: {exc}')]
    return result
