"""
utils/crn_contacts.py
─────────────────────
Reads the hospital's referring-doctor CSV (doctor code, name, phone, email) for
CRN. Pure functions, no database: the import page previews the result of
parse_contacts_csv() and then applies it.

Tolerant by design, because the file comes from Excel exports:
  * encoding: UTF-8 (with or without BOM), else Windows-1256 (Arabic Windows,
    which also covers French accents)
  * separator: comma, semicolon, tab or pipe, sniffed from the file
  * headers: matched by common names; with no recognisable header row the
    columns are read in the order code, name, phone, email
  * empty cells and placeholders (NULL, N/A, -, nan, ...) are missing values,
    never an error
  * phones: Lebanese numbers are normalised to +961... ("03/123456" is read as
    one number), Excel artefacts like "70123456.0" are repaired, and of two
    numbers in one cell the first is used
"""
import csv
import io
import re

FIELDS = ('doctor_code', 'full_name', 'phone', 'email')
NULL_TOKENS = {'', 'null', 'none', 'nil', 'n/a', 'na', '-', '--', 'nan', '#n/a', 'undefined', '0'}
HEADER_NAMES = {
    'doctor_code': {'doctorcode', 'drcode', 'code', 'doctorid', 'drid', 'id', 'physiciancode',
                    'physicianid', 'refcode', 'referringcode'},
    'full_name':   {'fullname', 'name', 'doctorname', 'drname', 'doctor', 'physician',
                    'physicianname', 'referringdoctor'},
    'phone':       {'phone', 'mobile', 'phonenumber', 'mobilenumber', 'tel', 'telephone', 'cell',
                    'cellphone', 'gsm', 'mobilephone', 'sms'},
    'email':       {'email', 'mail', 'emailaddress', 'courriel'},
}
COUNTRY_CODE = '961'   # Lebanon
MAX_ROWS = 20000


def _key(header):
    return re.sub(r'[^a-z0-9]', '', (header or '').lower())


def _clean(value):
    """Cell text, or None for an empty or placeholder cell."""
    if value is None:
        return None
    v = str(value).replace(' ', ' ').strip()
    return None if v.lower() in NULL_TOKENS else v


def decode(data):
    """bytes -> (text, encoding used)."""
    try:
        return data.decode('utf-8-sig'), 'utf-8'
    except UnicodeDecodeError:
        return data.decode('cp1256', errors='replace'), 'windows-1256'


def _sniff_delimiter(text):
    sample = '\n'.join(text.splitlines()[:20])
    try:
        return csv.Sniffer().sniff(sample, delimiters=',;\t|').delimiter
    except csv.Error:
        return ','


def normalize_phone(raw):
    """(e164 or None, note or None). Note explains a problem or a repair."""
    v = _clean(raw)
    if v is None:
        return None, None
    note = None
    # "03/123456" or "70 / 123456" is one Lebanese number written prefix/number.
    v = re.sub(r'\b(\d{2})\s*/\s*(\d{6})\b', r'\1\2', v)
    parts = [p for p in re.split(r'\s*(?:/|;|,|\s-\s|\bor\b|\bou\b)\s*', v, flags=re.I) if re.search(r'\d', p)]
    if len(parts) > 1:
        note = f'{len(parts)} numbers in the cell, the first is used'
        v = parts[0]
    if re.fullmatch(r'\d+\.0+', v):            # Excel turned the number into a float
        v = v.split('.')[0]
    plus = v.lstrip().startswith('+')
    digits = re.sub(r'\D', '', v)
    if not digits:
        return None, 'no digits in the phone cell'
    if plus:
        e = digits
    elif digits.startswith('00'):
        e = digits[2:]
    elif digits.startswith(COUNTRY_CODE) and len(digits) in (10, 11):
        e = digits
    elif digits.startswith('0'):
        e = COUNTRY_CODE + digits[1:]
    else:
        e = COUNTRY_CODE + digits
    if e.startswith(COUNTRY_CODE):
        if len(e) - len(COUNTRY_CODE) not in (7, 8):
            return None, f'"{raw}" is not a Lebanese number (wrong length)'
    elif not 8 <= len(e) <= 15:
        return None, f'"{raw}" is not a valid international number'
    return '+' + e, note


def _clean_email(raw):
    v = _clean(raw)
    if v is None:
        return None, None
    if re.fullmatch(r'[^@\s]+@[^@\s]+\.[^@\s]+', v):
        return v.lower(), None
    return None, f'"{v}" is not an email address, ignored'


def _map_columns(header):
    """{field: column index} from a header row; fields not found are absent."""
    found = {}
    for idx, h in enumerate(header):
        k = _key(h)
        for field, names in HEADER_NAMES.items():
            if field not in found and k in names:
                found[field] = idx
    return found


def parse_contacts_csv(data):
    """
    Parse an uploaded contacts file.

    Returns a dict:
      encoding, delimiter, header_found (bool), columns {field: header text},
      rows: [{line, doctor_code, full_name, phone_raw, phone_e164, email,
              status ('ok' | 'warning' | 'skipped' | 'duplicate'), notes [str]}],
      counts {status: n}, error (str or None, set when nothing can be read)
    """
    text, encoding = decode(data if isinstance(data, bytes) else data.encode('utf-8'))
    result = {'encoding': encoding, 'delimiter': None, 'header_found': False, 'columns': {},
              'rows': [], 'counts': {}, 'error': None}
    if not text.strip():
        result['error'] = 'The file is empty.'
        return result

    delimiter = _sniff_delimiter(text)
    result['delimiter'] = delimiter
    lines = [r for r in csv.reader(io.StringIO(text), delimiter=delimiter)]
    numbered = [(i + 1, r) for i, r in enumerate(lines) if any(_clean(c) for c in r)]
    if not numbered:
        result['error'] = 'The file has no data rows.'
        return result

    mapping = _map_columns(numbered[0][1])
    if 'doctor_code' in mapping:
        result['header_found'] = True
        result['columns'] = {f: numbered[0][1][i].strip() for f, i in mapping.items()}
        data_rows = numbered[1:]
    else:
        # No recognisable header: read the columns in the agreed order.
        mapping = {f: i for i, f in enumerate(FIELDS)}
        result['columns'] = {f: f'column {i + 1}' for f, i in mapping.items()}
        data_rows = numbered
    if len(data_rows) > MAX_ROWS:
        result['error'] = f'The file has {len(data_rows):,} rows; the limit is {MAX_ROWS:,}.'
        return result

    def cell(row, field):
        i = mapping.get(field)
        return row[i] if i is not None and i < len(row) else None

    last_by_code = {}
    for line_no, row in data_rows:
        notes = []
        code = _clean(cell(row, 'doctor_code'))
        code = code.upper() if code else None
        name = _clean(cell(row, 'full_name'))
        phone_raw = _clean(cell(row, 'phone'))
        phone_e164, phone_note = normalize_phone(phone_raw)
        email, email_note = _clean_email(cell(row, 'email'))
        for n in (phone_note, email_note):
            if n:
                notes.append(n)
        if not code:
            status = 'skipped'
            notes.insert(0, 'no doctor code, row ignored')
        else:
            if not phone_e164:
                notes.append('no usable phone: CRN cannot text this doctor')
            status = 'warning' if notes else 'ok'
        entry = {'line': line_no, 'doctor_code': code, 'full_name': name, 'phone_raw': phone_raw,
                 'phone_e164': phone_e164, 'email': email, 'status': status, 'notes': notes}
        if code:
            if code in last_by_code:
                earlier = last_by_code[code]
                earlier['status'] = 'duplicate'
                earlier['notes'].insert(0, f'code repeated on line {line_no}, that line is used')
            last_by_code[code] = entry
        result['rows'].append(entry)

    counts = {}
    for r in result['rows']:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    result['counts'] = counts
    return result


def rows_to_apply(parsed):
    """The rows an import writes: one per code (the last occurrence)."""
    return [r for r in parsed['rows'] if r['status'] in ('ok', 'warning')]
