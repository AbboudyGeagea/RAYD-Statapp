"""
utils/brayn_explain.py
────────────────────────────────────────────────────────────────
BRAYN in the operator's words: what the console says about a rule, a note or a
message.

The engine speaks in rule codes and JSON detail because that is what it stores.
An interface engineer reads MSH-10 and ORC-5. This module is the only translation
between the two, so the console's wording lives in one place and the tests can
hold it to the data the engine actually writes.

WHY THIS EXISTS. The first console showed rule codes, layer names ("intake",
"rung", "ladder") and raw JSON. An HL7 engineer with ten years on interfaces could
not tell from it what was wrong, what to do, or whether a message had reached the
reports (operator, 2026-10-25). Every sentence here answers one of those three.

Two jobs:
  * rules and notes as plain sentences: a title, what the check looks at, and one
    sentence per stored note, computed from its detail at render time
  * a message explained field by field: which fields BRAYN read and as what,
    which field caused a hold, and which field may hold a missing accession
"""
import logging
import re
from datetime import datetime

from utils.hl7_dictionary import describe_segment, field_info
from utils.hl7_parse import split_segments

logger = logging.getLogger("BRAYN_EXPLAIN")


# ── rules ─────────────────────────────────────────────────────────────────────

# (title, what it checks). Titles say what is wrong, in the reader's terms; the
# rule code stays visible in small print for support.
RULES = {
    'MISSING_IDENTIFIER':   ('No accession or order number',
                             'ORC-3 / OBR-3 and ORC-2 / OBR-2 are all empty, so the message '
                             "can't be attached to a study."),
    'UNKNOWN_STATUS_CODE':  ('Status code not mapped',
                             "The ORC-5 value is not in the status map, so BRAYN can't tell "
                             'which lifecycle step it is.'),
    'TIME_CONTRADICTION':   ('Times contradict the lifecycle',
                             'A later step carries an earlier time than a step before it, '
                             'for example Completed before Arrived.'),
    'SKIPPED_RUNG':         ('A step was skipped',
                             'The study reached a step without one before it that this site sends.'),
    'ORPHAN_EVENT':         ('No order received',
                             'Exam events arrived with no HIS order (ORM NW) and no scheduling message.'),
    'FUTURE_EVENT':         ('Event time in the future',
                             'ORC-9 / OBR-22 is later than the time the message arrived. '
                             "Usually the sender's clock."),
    'PLACEHOLDER_DATA':     ('Placeholder or test data',
                             'Values a quick registration leaves behind, such as birth date 9999-11-11.'),
    'LADDER_REGRESSION':    ('Lifecycle went backwards',
                             'Retired in 0135: covered by "Times contradict the lifecycle".'),
    'STALLED_SCHEDULED':    ('Scheduled, not arrived',
                             'No Arrived within the limit: a no-show nobody cancelled, or the '
                             'arrival feed stopped.'),
    'STALLED_ARRIVED':      ('Arrived, not started',
                             'No Started within the limit: a real wait, or a missing start event.'),
    'STALLED_STARTED':      ('Started, not completed',
                             'No Completed within the limit: an exam left open on the device, '
                             'or a completion that never came.'),
    'UNREPORTED':           ('Completed, no report',
                             'No report within the limit after the exam completed.'),
    'STALLED_UNSIGNED':     ('Preliminary report, no final',
                             'A first signature and no final one within the limit.'),
    'ORPHAN_ORDER':         ('Order never scheduled',
                             'A HIS order the RIS never scheduled within the limit.'),
    'OUT_OF_SEQUENCE_DELIVERY': ('Delivered out of order',
                             'A message arrived after a later step of the same study. '
                             'BRAYN puts events in time order, so the study is unaffected.'),
    'CONTROL_ID_REUSE':     ('Same MSH-10, different content',
                             'The sender reused a control ID for a different message.'),
    'EXACT_REDELIVERY':     ('Exact resend',
                             'The same message again, byte for byte. Stored once.'),
    'LOGICAL_DUPLICATE':    ('Duplicate event',
                             'The same step for the same study twice within minutes: one '
                             'event reaching RAYD by two routes.'),
    'REPEATED_TRANSITION':  ('Repeated step',
                             'The same step for a study again, well apart in time. Some RIS '
                             'send several Arrived per exam.'),
    'UNEXPECTED_RUNG':      ("Step this site doesn't send",
                             'A status for a lifecycle step that is switched off in '
                             '"Lifecycle steps this site sends".'),
    'UNKNOWN_AETITLE':      ('Unknown device',
                             'An AE title with no modality, so its studies miss every '
                             'per-modality and per-device chart.'),
    'UNKNOWN_RESULT_STATUS': ('Result status not mapped',
                             'An OBX-11 / OBR-25 value with no meaning in the result-status map.'),
    'UNKNOWN_PROCEDURE':    ('Unknown procedure', 'A procedure code not in the catalogue.'),
    'UNKNOWN_PERFORMER':    ('Unknown performer',
                             'A technician or radiologist BRAYN does not recognise.'),
    'BRAYN_BUDGET_EXCEEDED': ('Screening too slow',
                             'BRAYN ran out of its time budget before the ACK and accepted '
                             'the message without its remaining checks.'),
    'BRAYN_DEGRADED':       ('Screening degraded',
                             'A lookup failed, so BRAYN accepted the message without its '
                             'checks. Nothing is lost; the checks were skipped.'),
}

# The four layers, in the reader's terms. Order is the order the console shows.
GROUPS = (
    ('intake',    'Held until fixed',
     "Keep a message out of the reports until its cause is fixed. Always on: a message "
     "BRAYN can't attach to a study or a step can't produce data."),
    ('quality',   'Data quality notes',
     'Judged on the whole study after every message. Never block data; most close '
     "themselves once the study's messages agree."),
    ('workflow',  'Delays',
     'Checked every 15 minutes. A note opens when a study waits past its limit and '
     'closes when the next step arrives.'),
    ('interface', 'Sender behaviour',
     'Counted per sender. An alert opens only above the limit (per hour, from 20 '
     'messages) and closes when the rate drops back.'),
)
GROUP_TITLE = {k: t for k, t, _ in GROUPS}

# Intake rules cannot be switched off from the console: a message without an
# identifier or a step produces no data whatever the setting says.
LOCKED = {'MISSING_IDENTIFIER', 'UNKNOWN_STATUS_CODE'}


def rule_title(code):
    return RULES.get(code, (code.replace('_', ' ').capitalize(), ''))[0]


def rule_what(code):
    return RULES.get(code, ('', ''))[1]


# ── time, in words ────────────────────────────────────────────────────────────

STEP = {
    'ordered': 'Ordered', 'scheduled': 'Scheduled', 'arrived': 'Arrived',
    'started': 'Started', 'completed': 'Completed', 'reported': 'Reported',
    'signed_prelim': 'Report preliminary', 'signed_final': 'Report final',
    'cancelled': 'Cancelled', 'pacs_completed': 'Images in PACS',
}


def step_label(state):
    return STEP.get(state or '', (state or '?').replace('_', ' ').capitalize())


def duration(minutes):
    """45 min · 4 h · 4 h 20 min · 2 days 3 h"""
    if minutes is None:
        return ''
    m = int(round(float(minutes)))
    if m < 60:
        return f'{m} min'
    h, m = divmod(m, 60)
    if h < 48:
        return f'{h} h' + (f' {m} min' if m else '')
    d, h = divmod(h, 24)
    return f'{d} days' + (f' {h} h' if h else '')


def _dt(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def ago(value, now):
    """'just now', '5 min ago', '3 h ago', '2 days ago' — relative to the DB clock."""
    t = _dt(value)
    if not t or not now:
        return ''
    secs = (now - t).total_seconds()
    if secs < 0:
        return 'in ' + duration(-secs / 60)
    if secs < 60:
        return 'just now'
    return duration(secs / 60) + ' ago'


def clock(value, now=None):
    """14:02 today, 'yesterday 14:02', or '22 Oct 14:02'."""
    t = _dt(value)
    if not t:
        return ''
    if now and t.date() == now.date():
        return t.strftime('%H:%M')
    if now and (now.date() - t.date()).days == 1:
        return 'yesterday ' + t.strftime('%H:%M')
    return t.strftime('%d %b %H:%M').lstrip('0')


# ── notes as sentences ────────────────────────────────────────────────────────

_STALL = {
    'STALLED_SCHEDULED': ('Scheduled', 'not arrived'),
    'STALLED_ARRIVED':   ('Arrived', 'not started'),
    'STALLED_STARTED':   ('Started', 'not completed'),
    'UNREPORTED':        ('Completed', 'no report yet'),
    'STALLED_UNSIGNED':  ('Preliminary report', 'no final yet'),
}


def note_sentence(code, detail, now=None):
    """
    One sentence for a stored note, from its detail.

    Waiting times are computed from the stored start time and the clock NOW, not
    taken from the detail: the detail records how long the study had waited when
    the note was raised, which is stale by the time anyone reads it.
    """
    d = detail if isinstance(detail, dict) else {}
    try:
        if code in _STALL:
            label, tail = _STALL[code]
            since = _dt(d.get('since'))
            waited = (now - since).total_seconds() / 60 if since and now else d.get('stalled_minutes')
            text = f'{label} {duration(waited)} ago, {tail}.'
            if d.get('threshold_minutes'):
                text += f" Limit {duration(d['threshold_minutes'])}."
            return text
        if code == 'ORPHAN_ORDER':
            when = _dt(d.get('ordered_at'))
            text = f"Order {d.get('placer_order_number') or ''} placed".replace('  ', ' ')
            text += f' {ago(when, now)}, never scheduled.' if when and now else ', never scheduled.'
            if d.get('threshold_minutes'):
                text += f" Limit {duration(d['threshold_minutes'])}."
            return text
        if code == 'TIME_CONTRADICTION':
            parts = []
            for p in d.get('pairs') or []:
                parts.append(f"{step_label(p.get('later_rung'))} ({clock(p.get('later_rung_time'), now)}) "
                             f"is earlier than {step_label(p.get('earlier_rung'))} "
                             f"({clock(p.get('earlier_rung_time'), now)})")
            return ('; '.join(parts) + '. One of these times is wrong at the sender, so this '
                    "study's turnaround times come out wrong.") if parts else rule_what(code)
        if code == 'SKIPPED_RUNG':
            missing = [step_label(m) for m in d.get('missing_rungs') or []]
            reached = next((k[:-3] for k in d if k.endswith('_at')), None)
            if missing:
                lead = f'{step_label(reached)} came in' if reached else 'The study moved on'
                return f"{lead} with no {' and no '.join(missing)}. Durations that need the missing step stay empty."
        if code == 'ORPHAN_EVENT':
            return 'Exam events arrived, but no HIS order (ORM NW) and no scheduling message.'
        if code == 'FUTURE_EVENT':
            return (f"{step_label(d.get('state'))} is stamped {clock(d.get('event_time'), now)}, "
                    f"later than the message arrived ({clock(d.get('local_now'), now)}). "
                    "Check the sender's clock.")
        if code == 'PLACEHOLDER_DATA':
            items = d.get('placeholders') or []
            return 'Placeholder data: ' + '; '.join(items) + '.' if items else rule_what(code)
        if code == 'MISSING_IDENTIFIER':
            return 'No accession in ORC-3 / OBR-3 and no order number in ORC-2 / OBR-2.'
        if code == 'UNKNOWN_STATUS_CODE':
            status, control = d.get('order_status'), d.get('order_control')
            if not status and not control:
                return ("No status code: ORC-5 and ORC-1 are both empty (or there is no ORC), "
                        "so BRAYN can't tell which step this message is.")
            if not status:
                return (f'ORC-5 is empty and ORC-1 = {control} is not a code BRAYN can classify, '
                        "so it can't tell which step this message is.")
            return (f"ORC-5 = {status} with ORC-1 = {control or '(empty)'} is not in the status map.")
        if 'rate_pct' in d:
            window = duration(d.get('window_minutes') or 60)
            return (f"{d.get('rate_pct')}% of messages in the last {window} "
                    f"({d.get('count')} of {d.get('messages')}); alert above {d.get('threshold_pct')}%.")
    except Exception:
        logger.exception("could not phrase %s note", code)
    return d.get('note') or d.get('reason') or rule_what(code)


# ── a message, field by field ─────────────────────────────────────────────────

def segments_of(raw_message):
    """[(name, [(number, value), ...])] with HL7 numbering: MSH-1 is the separator."""
    out = []
    for line in split_segments(raw_message):
        parts = line.split('|')
        name = parts[0]
        if name == 'MSH':
            fields = [(1, '|')] + [(i + 1, v) for i, v in enumerate(parts[1:], start=1)]
        else:
            fields = [(i, v) for i, v in enumerate(parts[1:], start=1)]
        out.append((name, fields))
    return out


def _value(segs, seg, num, comp=None, which=0):
    """The value at SEG-num[.comp] in the `which`-th segment of that name, or ''."""
    hits = [f for n, f in segs if n == seg]
    if which >= len(hits):
        return ''
    val = dict(hits[which]).get(num, '') or ''
    val = val.split('~')[0]
    if comp:
        parts = val.split('^')
        val = parts[comp - 1] if comp - 1 < len(parts) else ''
    return val.strip()


# Where the parser reads each attribute, in its own order of preference. Mirrors
# utils/hl7_parse.py (_identity, _clinical, _STATUS_FIELDS); a site's field mapping
# for the same target is tried first, as the parser's overlay does.
_SOURCES = (
    ('accession_number',    'Accession',              (('ORC', 3, 1), ('OBR', 3, 1))),
    ('placer_order_number', 'Order number (placer)',  (('ORC', 2, 1), ('OBR', 2, 1))),
    ('patient_id',          'Patient ID',             (('PID', 3, 1),)),
    ('visit_number',        'Visit number',           (('PV1', 19, 1),)),
    ('procedure_code',      'Procedure',              (('OBR', 4, 1),)),
    ('modality',            'Modality',               (('OBR', 24, 1), ('OBR', 19, 1), ('OBR', 17, 1))),
    ('patient_class',       'Patient class',          (('PV1', 2, None), ('PID', 18, None))),
    ('patient_location',    'Location',               (('PV1', 3, 1),)),
    ('raw_order_control',   'Order control',          (('ORC', 1, None),)),
    ('raw_order_status',    'Order status',           (('ORC', 5, None),)),
    ('aetitle',             'Device (AE title)',      (('OBR', 21, 1),)),
    ('raw_result_status',   'Result status',          (('OBX', 11, None), ('OBR', 25, None))),
)
_EVENT_TIME = {
    'status': (('ORC', 9, None), ('MSH', 7, None)),
    'order':  (('ORC', 9, None), ('OBR', 7, 1), ('MSH', 7, None)),
    'result': (('OBR', 22, None), ('OBR', 7, None)),
}
_PERFORMER = {
    'status': (('ORC', 19, None), 'Technician'),
    'order':  (('PV1', 8, None), 'Referring doctor'),
}


def _mapped_positions(sending_app, kind):
    """{target_field: [(seg, field, comp)]} from the site's active field mappings."""
    try:
        from sqlalchemy import text
        from db import db
        rows = db.session.execute(text("""
            SELECT target_field, segment, field_index, component_index
              FROM hl7_field_mappings
             WHERE active AND target_kind = 'parsed'
               AND sending_app IN ('', :app) AND message_kind IN ('', :kind)
             ORDER BY (sending_app = '') , (message_kind = ''), priority
        """), {'app': sending_app or '', 'kind': kind or ''}).mappings().all()
    except Exception:
        logger.exception("could not read field mappings for the explainer")
        return {}
    out = {}
    for r in rows:
        out.setdefault(r['target_field'], []).append(
            (r['segment'], r['field_index'], r['component_index']))
    return out


def _key(seg, num, comp=None):
    return f'{seg}-{num}' + (f'.{comp}' if comp else '')


def _shape(value):
    return re.sub(r'[A-Za-z]', 'A', re.sub(r'\d', '9', value))


def explain(raw_message, msg, *, held_rules=(), mapped=None, known_aetitle=None,
            result_mapped=None, step_expected=None, known_accessions=(), accession_shapes=()):
    """
    The message, annotated for the console.

    `msg` is the ParsedMessage for this raw message. The lookups that need the
    database are passed in (the route does them), so this stays a pure function the
    tests can drive with plain values.

    Returns {'segments': [...], 'read': [...], 'suggestions': [...]}:
      segments  every segment with every field up to its last populated one; each
                field carries its HL7 name and, where it matters, why BRAYN read it
                (used), why it is the problem (problem) or why it may be the missing
                accession (suggested)
      read      "What BRAYN read from it": label, value, source field, problem flag
      suggestions  fields that may hold the accession, best first, for a message
                held for want of one
    """
    segs = segments_of(raw_message)
    kind = getattr(msg, 'kind', None)
    mapped = mapped or {}
    used, problem, sources = {}, {}, {}

    def find(attr, defaults):
        """First position (mapped, then default) whose value is the parsed one."""
        value = getattr(msg, attr, None)
        if value in (None, ''):
            return None
        for seg, num, comp in list(mapped.get(attr, [])) + list(defaults):
            n_hits = sum(1 for n, _ in segs if n == seg)
            for which in range(max(n_hits, 1)):
                if _value(segs, seg, num, comp, which) == str(value):
                    return (seg, num, comp, which)
        return None

    for attr, label, defaults in _SOURCES:
        pos = find(attr, defaults)
        if pos:
            sources[attr] = pos
            used[(pos[0], pos[1], pos[3])] = label

    # Event time: first populated candidate, as the parser falls through.
    for seg, num, comp in _EVENT_TIME.get(kind, ()):
        if _value(segs, seg, num, comp):
            sources['event_time'] = (seg, num, comp, 0)
            used[(seg, num, 0)] = 'Time of this step' if kind != 'result' else 'Report time'
            break
    perf = _PERFORMER.get(kind)
    if perf and _value(segs, perf[0][0], perf[0][1]):
        sources['performer'] = (perf[0][0], perf[0][1], None, 0)
        used[(perf[0][0], perf[0][1], 0)] = perf[1]
    for k, label in ((('MSH', 3, 0), 'Sender'), (('MSH', 9, 0), 'Message type'),
                     (('MSH', 10, 0), 'Control ID: spots resends and reuse')):
        used.setdefault(k, label)

    # Why this message is a problem, field by field.
    if 'UNKNOWN_STATUS_CODE' in held_rules:
        seg, num = (sources.get('raw_order_status') or ('ORC', 5, None, 0))[:2]
        problem[(seg, num, 0)] = (f"{getattr(msg, 'raw_order_status', None) or 'Empty'}: not in the "
                                  'status map, so the step is unknown. Message held.')
    if 'MISSING_IDENTIFIER' in held_rules:
        for seg, num, what in (('ORC', 3, 'accession'), ('OBR', 3, 'accession'),
                               ('ORC', 2, 'order number'), ('OBR', 2, 'order number')):
            if any(n == seg for n, _ in segs):
                problem[(seg, num, 0)] = f'Empty. BRAYN looks here for the {what}.'
    if getattr(msg, 'aetitle', None) and known_aetitle is False:
        pos = sources.get('aetitle')
        if pos:
            problem[(pos[0], pos[1], pos[3])] = (f'{msg.aetitle} has no modality yet, so these '
                                                 'studies miss the per-modality charts.')
    if kind == 'result' and getattr(msg, 'raw_result_status', None) and result_mapped is False:
        pos = sources.get('raw_result_status')
        if pos:
            problem[(pos[0], pos[1], pos[3])] = (f'{msg.raw_result_status} has no meaning in the '
                                                 'result-status map.')
    if step_expected is False and sources.get('raw_order_status'):
        pos = sources['raw_order_status']
        problem[(pos[0], pos[1], pos[3])] = (f'{step_label(getattr(msg, "canonical_state", None))} '
                                             "is switched off for this site, yet this sender sends it.")

    # Fields that may hold a missing accession: exact match with a known study
    # first, then the same shape as this site's accessions.
    suggestions = []
    if 'MISSING_IDENTIFIER' in held_rules:
        skip = {'MSH', 'PID'}
        for name, fields in segs:
            if name in skip:
                continue
            for num, val in fields:
                if (name, num, 0) in used or not val:
                    continue
                fname, dtype = field_info(name, num)
                # Dates, numbers and free text never hold an accession. ST stays in:
                # OBR-18/19 (placer fields) are ST and a common home for one.
                if dtype in ('TS', 'DTM', 'DT', 'TM', 'NM', 'SI', 'TX', 'FT') and not name.startswith('Z'):
                    continue
                for ci, comp in enumerate(val.split('~')[0].split('^'), start=1):
                    comp = comp.strip()
                    if not comp or len(comp) < 4:
                        continue
                    if comp in known_accessions:
                        reason, score = f'Same value as study {comp}, already known from another message.', 2
                    elif _shape(comp) in accession_shapes:
                        reason, score = f"Same pattern as this site's accessions ({_shape(comp)}).", 1
                    else:
                        continue
                    suggestions.append({'segment': name, 'field': num, 'component': ci,
                                        'key': _key(name, num, ci), 'value': comp,
                                        'reason': reason, 'score': score})
        suggestions.sort(key=lambda s: -s['score'])

    out_segs, seen = [], {}
    for name, fields in segs:
        which = seen.get(name, 0)
        seen[name] = which + 1
        last = max((n for n, v in fields if v), default=0)
        rows = []
        for num, val in fields:
            k = (name, num, which)
            sugg = next((s for s in suggestions[:1] if s['segment'] == name and s['field'] == num), None)
            if num > last and k not in problem:
                continue
            if not val and k not in problem and k not in used and not sugg:
                continue
            fname, dtype = field_info(name, num)
            rows.append({
                'key': _key(name, num), 'number': num, 'name': fname or '',
                'value': val,
                'used': used.get(k) if k not in problem else None,
                'problem': problem.get(k),
                'suggested': sugg['reason'] if sugg else None,
            })
        out_segs.append({'name': name, 'description': describe_segment(name),
                         'index': which, 'fields': rows})

    def src(attr):
        p = sources.get(attr)
        return _key(p[0], p[1], p[2]) if p else ''

    read = []
    if kind in ('status', 'order', 'result'):
        read.append(('Study (accession)', msg.accession_number or 'not found', src('accession_number'),
                     not msg.accession_number and 'MISSING_IDENTIFIER' in held_rules))
        if msg.placer_order_number or kind == 'order':
            read.append(('Order number', msg.placer_order_number or 'not found',
                         src('placer_order_number'), False))
    if kind == 'status':
        code = msg.raw_order_status or msg.raw_order_control or ''
        step = (f'{code} → {step_label(msg.canonical_state)}' if msg.ladder_rank is not None
                else f'{code or "(empty)"} → unknown')
        read.append(('Step', step, src('raw_order_status') or src('raw_order_control'),
                     msg.ladder_rank is None))
    elif kind == 'order':
        read.append(('Step', 'NW → Ordered', src('raw_order_control') or 'ORC-1', False))
    elif kind == 'result':
        rs = msg.raw_result_status or ''
        read.append(('Report', f'{rs} → {step_label(msg.canonical_state)}' if msg.ladder_rank is not None
                     else f'{rs or "(no status)"} → not mapped', src('raw_result_status'),
                     msg.ladder_rank is None))
    if getattr(msg, 'event_time', None):
        read.append(('Time', msg.event_time.strftime('%d %b %H:%M:%S').lstrip('0'), src('event_time'), False))
    if getattr(msg, 'aetitle', None):
        read.append(('Device', msg.aetitle + (' → ' + msg.modality if msg.modality and known_aetitle is not False
                                              else ' (no modality yet)' if known_aetitle is False else ''),
                     src('aetitle'), known_aetitle is False))
    elif getattr(msg, 'modality', None):
        read.append(('Modality', msg.modality, src('modality'), False))
    if getattr(msg, 'performed_by_name', None) and kind in _PERFORMER:
        read.append((_PERFORMER[kind][1], msg.performed_by_name, src('performer'), False))
    elif getattr(msg, 'performed_by_name', None) and kind == 'result':
        read.append(('Radiologist', msg.performed_by_name, '', False))
    if getattr(msg, 'patient_id', None):
        read.append(('Patient', msg.patient_id, src('patient_id'), False))
    if kind == 'adt':
        read.append(('Event', adt_event(getattr(msg, 'message_type', '')), 'MSH-9', False))

    return {'segments': out_segs,
            'read': [{'label': l, 'value': v, 'source': s, 'problem': bool(p)} for l, v, s, p in read],
            'suggestions': suggestions[:3]}


_ADT = {'A01': 'Admit', 'A02': 'Transfer', 'A03': 'Discharge', 'A04': 'Register',
        'A05': 'Pre-admit', 'A08': 'Patient update', 'A11': 'Cancel admit',
        'A31': 'Person update', 'A34': 'Merge patient ID', 'A40': 'Merge patient'}


def adt_event(message_type):
    ev = (message_type or '').split('^')[1] if '^' in (message_type or '') else ''
    return f'{_ADT.get(ev, "ADT")} ({ev})' if ev else 'ADT'


def event_text(msg):
    """What a message says, in a few words, for the message list."""
    kind = getattr(msg, 'kind', None)
    if kind == 'order':
        return f'New order {msg.placer_order_number}' if msg.placer_order_number else 'New order'
    if kind == 'status':
        if msg.ladder_rank is None:
            code = msg.raw_order_status or msg.raw_order_control
            return f'ORC-5 = {code} (not mapped)' if code else 'Status with no code'
        return step_label(msg.canonical_state)
    if kind == 'result':
        if msg.canonical_state == 'signed_final':
            return 'Report, final'
        if msg.canonical_state == 'signed_prelim':
            return 'Report, preliminary'
        return f'Report, status {msg.raw_result_status}' if msg.raw_result_status else 'Report'
    if kind == 'adt':
        return adt_event(msg.message_type)
    return msg.message_type or 'Unrecognised message'
