"""
utils/his_ris.py — HIS orders vs RIS orders (LAUMC to-do item 1), Report 27's second tab.

The HIS's orders reach RAYD as HL7 ORM^O01 since 2026-11-18 (MSH-3 SAP_P, hl7_listener.py into
hl7_orders, one row per message). The RIS's orders come through the nightly ETL (etl_orders,
one row per RIS order), each with the HIS order number it was created from
(etl_orders.placer_order_number = ORC-2, migration 0160). Both keep the leading zeros
(production, 18 Nov: 146 of 146 the same with or without).

    one HIS order   every message with its HIS order number: ordered at = its first message
                    (MSH-7), what it is now = its latest message (ORC-1: NW new, RQ changed,
                    CA cancelled); procedure, HIS location and class from the latest
    billed          only orders the HIS sent a new-order message for (ORC-1 = NW) count
                    (operator, 2026-11-19: "NW" is the billed order). One seen only as changes
                    (RQ; e.g. created before the feed began) is left out and counted aside; a
                    billed order cancelled later still counts, as cancelled on the HIS
    its RIS orders  the RIS orders carrying that number (all but 4 of 3,968 have one)
    outcome         cancelled on the HIS | done (a RIS order completed or its exam done) |
                    cancelled on the RIS (all of them) | open on the RIS | not on the RIS |
                    not copied yet (no RIS order, ordered after the last nightly RIS copy
                    began: the RIS side is a day late, so it cannot be missing yet)
    hospital        SAP_P sends both hospitals' orders, numbered from one sequence. An order on
                    the RIS takes its RIS order's issuer (sites.ris_issuer). One not on the RIS
                    takes the hospital the HIS names: PV1-3's 7th component, 1000 = RH and
                    2000 = SJH (production, 19 Nov: every order of 18 Nov, against its RIS
                    order's issuer), else an SJH location or entering unit ending in "-J" (EM-J,
                    ENDOS-J), else its HIS location's hospital learned from the orders on the
                    RIS; otherwise it is unknown and only counted, never listed. The matching
                    reads every site's RIS orders (public.etl_orders), then keeps this
                    database's site.
    RIS only        RIS orders created since the HIS feed began with no HIS message for
                    their number, or with no HIS number at all (created on the RIS)

Patient names are never read (hl7_orders has them; this module does not select them).
"""
import io
import statistics
from datetime import timedelta

from sqlalchemy import text

from db import db

TITLE = 'HIS orders vs RIS'
DEFAULT_HIS_APPS = ('SAP_P',)
HIS_FACILITY_SITES = {'1000': 'RH', '2000': 'SJH'}   # PV1-3.7, the HIS's hospital code
SJH_SUFFIX = '-J'                                    # SJH's HIS locations and units: EM-J, MRI-J
HIS_APPS_KEY = 'his_sending_apps'
UNKNOWN = 'Not recorded'

OUTCOMES = (
    ('done', 'Done on the RIS'),
    ('open', 'On the RIS, not done yet'),
    ('cancelled_ris', 'Cancelled on the RIS'),
    ('cancelled_his', 'Cancelled on the HIS'),
    ('not_on_ris', 'Not on the RIS'),
    ('pending', 'Not copied yet'),
)
OUTCOME_LABELS = dict(OUTCOMES)

DIMENSIONS = (
    ('location', 'HIS location'),
    ('procedure', 'Procedure'),
    ('patient_class', 'Patient class'),
    ('control', 'Last HIS message'),
)
BREAKDOWN_ROWS = 60

COLUMNS = (
    ('HIS order number', 'his_no'), ('Ordered on the HIS', 'ordered'), ('Last HIS message', 'control'),
    ('HIS messages', 'messages'), ('Procedure code', 'procedure_code'), ('Procedure', 'procedure'),
    ('HIS location', 'location'), ('Patient class', 'patient_class'), ('Patient ID', 'patient_id'),
    ('Outcome', 'outcome_label'), ('RIS orders', 'ris_orders'), ('Registered on the RIS', 'registered'),
    ('HIS to RIS (minutes)', 'his_to_ris_min'), ('Accession', 'accessions'), ('Appointment', 'appointment'),
    ('Exam done', 'exam_done'), ('HIS to exam done (hours)', 'his_to_done_h'),
)

_HIS_SQL = """
WITH msgs AS (
    SELECT BTRIM(h.placer_order_number) AS his_no,
           COALESCE(h.message_datetime, h.received_at) AS at, h.received_at,
           UPPER(BTRIM(substring(h.raw_message from 'ORC\\|([^|]*)'))) AS control,
           h.procedure_code, h.procedure_text, h.patient_location, h.patient_class, h.patient_id,
           BTRIM(split_part(split_part(substring(h.raw_message from 'PV1\\|[^\\r]*'), '|', 4), '^', 7)) AS facility,
           UPPER(BTRIM(split_part(split_part(substring(h.raw_message from 'ORC\\|[^\\r]*'), '|', 18), '^', 1))) AS entering
    FROM public.hl7_orders h
    WHERE NULLIF(BTRIM(h.placer_order_number), '') IS NOT NULL
      AND UPPER(BTRIM(split_part(h.raw_message, '|', 3))) = ANY(:apps)
),
ranked AS (
    SELECT m.*, MIN(m.at) OVER (PARTITION BY m.his_no) AS ordered_at,
           COUNT(*) OVER (PARTITION BY m.his_no) AS messages,
           BOOL_OR(m.control = 'NW') OVER (PARTITION BY m.his_no) AS billed,
           ROW_NUMBER() OVER (PARTITION BY m.his_no ORDER BY m.at DESC, m.received_at DESC) AS rn
    FROM msgs m
)
SELECT his_no, ordered_at, messages, billed, control, procedure_code, procedure_text,
       patient_location, patient_class, patient_id, facility, entering
FROM ranked
WHERE rn = 1 AND ordered_at >= :start AND ordered_at < :end
"""

# Every site's RIS orders with a HIS number from a few days before the period on (a RIS
# order is created after its HIS order), their issuer's site and their first exam done.
_RIS_SQL = """
SELECT BTRIM(o.placer_order_number) AS his_no, o.order_dbid, o.order_created_at, o.order_status,
       o.accession_number, o.scheduled_datetime, o.study_db_uid, si.id AS site_id,
       (SELECT MIN(w.exam_done_at) FROM public.std_worklist_exam_done w
         WHERE w.site_worklist_key = o.order_dbid) AS exam_done_at
FROM public.etl_orders o
LEFT JOIN public.sites si ON si.ris_issuer = o.order_control
WHERE o.placer_order_number = ANY(:numbers)          -- idx_etl_orders_placer_order_number
"""

# This site's RIS orders created in the period since the HIS feed began: with no HIS number,
# or a number the HIS never sent. Through the site's views (no other site, no duplicates).
_RIS_ONLY_SQL = """
WITH sent AS (
    SELECT DISTINCT BTRIM(h.placer_order_number) AS p FROM public.hl7_orders h
    WHERE NULLIF(BTRIM(h.placer_order_number), '') IS NOT NULL
      AND UPPER(BTRIM(split_part(h.raw_message, '|', 3))) = ANY(:apps)
)
SELECT COUNT(*) FILTER (WHERE NULLIF(BTRIM(o.placer_order_number), '') IS NULL) AS no_his_number,
       COUNT(*) FILTER (WHERE NULLIF(BTRIM(o.placer_order_number), '') IS NOT NULL AND s.p IS NULL) AS not_sent,
       COUNT(*) AS created
FROM etl_orders o
LEFT JOIN sent s ON s.p = BTRIM(o.placer_order_number)
WHERE o.order_created_at >= :start AND o.order_created_at < :end
"""


def his_apps():
    value = db.session.execute(text("SELECT value FROM settings WHERE key = :k"), {'k': HIS_APPS_KEY}).scalar()
    apps = [a.strip().upper() for a in (value or '').split(',') if a.strip()]
    return apps or list(DEFAULT_HIS_APPS)


def feed_start(apps):
    return db.session.execute(text(
        "SELECT MIN(received_at) FROM public.hl7_orders "
        "WHERE UPPER(BTRIM(split_part(raw_message, '|', 3))) = ANY(:apps)"), {'apps': apps}).scalar()


def last_ris_copy():
    """When the last successful nightly RIS orders copy began: orders after it cannot be on it."""
    return db.session.execute(text(
        "SELECT MAX(start_time) FROM etl_job_log WHERE job_name = 'ORDERS_ETL' AND status = 'SUCCESS'")).scalar()


def _clean(value):
    v = (value or '').strip()
    return v if v and v.strip('^') else None


def _minutes(a, b):
    return round((b - a).total_seconds() / 60, 1) if a and b else None


def _when(v):
    return v.strftime('%Y-%m-%d %H:%M') if v else None


def _outcome(case, copied_until):
    ris = case['ris']
    if case['control'] == 'CA':
        return 'cancelled_his'
    if any(r['order_status'] == 'CM' or r['exam_done_at'] for r in ris):
        return 'done'
    if ris and all(r['order_status'] == 'CA' for r in ris):
        return 'cancelled_ris'
    if ris:
        return 'open'
    if copied_until is None or case['ordered_at'] >= copied_until:
        return 'pending'
    return 'not_on_ris'


def _shape(row, ris, copied_until):
    case = {
        'his_no': row['his_no'], 'ordered_at': row['ordered_at'], 'messages': int(row['messages']),
        'billed': bool(row.get('billed', True)),
        'control': row['control'] or UNKNOWN, 'procedure_code': _clean(row['procedure_code']),
        'procedure': _clean(row['procedure_text']) or _clean(row['procedure_code']) or UNKNOWN,
        'location': _clean(row['patient_location']) or UNKNOWN,
        'patient_class': _clean(row['patient_class']) or UNKNOWN,
        'patient_id': _clean(row['patient_id']), 'ris': ris,
        'facility': _clean(row.get('facility')), 'entering': _clean(row.get('entering')),
    }
    case['outcome'] = _outcome(case, copied_until)
    case['outcome_label'] = OUTCOME_LABELS[case['outcome']]
    registered = min((r['order_created_at'] for r in ris if r['order_created_at']), default=None)
    done = min((r['exam_done_at'] for r in ris if r['exam_done_at']), default=None)
    appointment = min((r['scheduled_datetime'] for r in ris if r['scheduled_datetime']), default=None)
    case.update({
        'ris_orders': len(ris), 'registered_at': registered, 'exam_done_at': done,
        'registered': _when(registered), 'exam_done': _when(done), 'appointment': _when(appointment),
        'ordered': _when(row['ordered_at']),
        'accessions': ', '.join(sorted({r['accession_number'] for r in ris if r['accession_number']})) or None,
        'his_to_ris_min': _minutes(row['ordered_at'], registered),
        'his_to_done_h': round(_minutes(row['ordered_at'], done) / 60, 1) if done else None,
        'ris_sites': {r['site_id'] for r in ris if r['site_id'] is not None},
    })
    return case


def _his_site(case, site_ids):
    """The hospital the HIS message names: its PV1-3.7 code, else an SJH location or unit."""
    code = HIS_FACILITY_SITES.get(case['facility'] or '')
    if code:
        return site_ids.get(code)
    if any((v or '').upper().endswith(SJH_SUFFIX) for v in (case['location'], case['entering'])):
        return site_ids.get('SJH')
    return None


def _assign_sites(cases, site_ids):
    """An order on the RIS: its RIS orders' site. Otherwise the hospital its HIS message names,
    else its HIS location's, when every order of that location on the RIS is at one site."""
    seen = {}
    for c in cases:
        if len(c['ris_sites']) == 1 and c['location'] != UNKNOWN:
            seen.setdefault(c['location'], set()).update(c['ris_sites'])
    for c in cases:
        his_site = _his_site(c, site_ids)
        if len(c['ris_sites']) == 1:
            c['site_id'], c['site_from'] = next(iter(c['ris_sites'])), 'ris'
        elif his_site is not None:
            c['site_id'], c['site_from'] = his_site, 'his'
        elif len(seen.get(c['location'], ())) == 1:
            c['site_id'], c['site_from'] = next(iter(seen[c['location']])), 'location'
        else:
            c['site_id'], c['site_from'] = None, None


def _median(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 1) if values else None


def _block(cases):
    decided = [c for c in cases if c['outcome'] != 'pending']
    on_ris = [c for c in decided if c['outcome'] in ('done', 'open', 'cancelled_ris')]
    counts = {k: sum(c['outcome'] == k for c in cases) for k, _ in OUTCOMES}
    return {
        'orders': len(cases), **counts,
        'on_ris_pct': round(100.0 * len(on_ris) / len(decided), 1) if decided else None,
        'not_on_ris_pct': round(100.0 * counts['not_on_ris'] / len(decided), 1) if decided else None,
        'median_to_ris_min': _median([c['his_to_ris_min'] for c in cases
                                      if c['his_to_ris_min'] is not None and c['his_to_ris_min'] >= 0]),
        'ris_first': sum(1 for c in cases if c['his_to_ris_min'] is not None and c['his_to_ris_min'] < 0),
        'median_to_done_h': _median([c['his_to_done_h'] for c in cases
                                     if c['his_to_done_h'] is not None and c['his_to_done_h'] >= 0]),
        'several_ris': sum(1 for c in cases if c['ris_orders'] > 1),
    }


def _breakdown(cases, key):
    groups = {}
    for c in cases:
        groups.setdefault(c[key], []).append(c)
    order = sorted(groups, key=lambda v: (-len(groups[v]), str(v)))
    rows = [{'value': v, **_block(groups[v])} for v in order]
    return {'rows': rows[:BREAKDOWN_ROWS], 'total': len(rows)}


def build(start, end, site_id):
    """(cases, summary) for the HIS orders placed from start to end (dates, both included) at
    site_id. Cases of another site are left out; those whose site is unknown are only counted."""
    apps = his_apps()
    params = {'start': start, 'end': end + timedelta(days=1), 'apps': apps}
    his = [dict(r) for r in db.session.execute(text(_HIS_SQL), params).mappings().all()]
    ris_by_no = {}
    if his:
        for r in db.session.execute(text(_RIS_SQL), {'numbers': [h['his_no'] for h in his]}).mappings().all():
            ris_by_no.setdefault(r['his_no'], []).append(dict(r))
    copied_until = last_ris_copy()
    every = [_shape(h, ris_by_no.get(h['his_no'], []), copied_until) for h in his]
    site_ids = {code: sid for code, sid in db.session.execute(text("SELECT code, id FROM public.sites"))}
    _assign_sites(every, site_ids)
    here = [c for c in every if site_id is None or c['site_id'] == site_id]
    cases = [c for c in here if c['billed']]                  # billed = an NW message
    not_billed = len(here) - len(cases)
    unknown_site = sum(1 for c in every if c['site_id'] is None and c['billed']) if site_id is not None else 0
    cases.sort(key=lambda c: c['ordered_at'])

    started = feed_start(apps)
    ris_only = None
    if started is not None:
        from_ = max(started, _as_datetime(start))
        ris_only = dict(db.session.execute(text(_RIS_ONLY_SQL),
                                           {'start': from_, 'end': params['end'], 'apps': apps}).mappings().one())
    summary = {
        'kpi': _block(cases),
        'outcomes': [{'key': k, 'label': label, 'orders': sum(c['outcome'] == k for c in cases)} for k, label in OUTCOMES],
        'breakdowns': {k: _breakdown(cases, k) for k, _ in DIMENSIONS},
        'not_on_ris': [c for c in cases if c['outcome'] == 'not_on_ris'][:200],
        'unknown_site': unknown_site, 'not_billed': not_billed,
        'feed_start': started, 'copied_until': copied_until, 'apps': apps, 'ris_only': ris_only,
        'site_from_his': sum(1 for c in cases if c['site_from'] == 'his'),
        'site_from_location': sum(1 for c in cases if c['site_from'] == 'location'),
    }
    return cases, summary


def _as_datetime(d):
    from datetime import datetime, time
    return datetime.combine(d, time.min)


def workbook(start, end, cases, summary, site_label):
    import xlsxwriter
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False,
                                     'strings_to_urls': False, 'strings_to_numbers': False})
    sheet = book.add_worksheet('HIS vs RIS')
    bold = book.add_format({'bold': True})
    k = summary['kpi']
    sheet.write(0, 0, f'{TITLE}, {site_label}: HIS orders placed {start} to {end}', bold)
    sheet.write(1, 0, f"{k['orders']:,} HIS orders; on the RIS {k['on_ris_pct']} %; not on the RIS "
                      f"{k['not_on_ris']:,}; cancelled on the HIS {k['cancelled_his']:,}; not copied yet {k['pending']:,}")
    sheet.write(2, 0, 'One row per billed HIS order (the HIS sent it as a new order, ORC-1 NW), as its latest '
                      f"message shows it; {summary['not_billed']:,} seen only as changes are not counted. No patient names.")
    sheet.write_row(4, 0, [h for h, _ in COLUMNS], bold)
    for i, c in enumerate(cases, 5):
        sheet.write_row(i, 0, [c[key] for _, key in COLUMNS])
    sheet.freeze_panes(5, 0)
    sheet.autofilter(4, 0, 4 + len(cases), len(COLUMNS) - 1)
    sheet.set_column(0, len(COLUMNS) - 1, 16)
    book.close()
    return buf.getvalue()
