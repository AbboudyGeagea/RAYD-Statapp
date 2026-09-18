"""
utils/hl7_fieldmap.py
────────────────────────────────────────────────────────────────
Applies the operator-configured field mappings from hl7_field_mappings.

    raw segments ──▶ mappings (by scope, in priority order) ──▶ ParsedMessage
                                                            └─▶ etl_* overrides

OVERLAY, NOT REPLACEMENT. The parser fills a message with its built-in logic
first; this then overlays whatever the site has configured. A mapping that
produces no value leaves the built-in answer alone, so an install with an empty
table behaves exactly as it did before this existed, and a partially-configured
site gets its own answer where it has one and the product's elsewhere.

That ordering matters more than it sounds. The alternative — mappings as the sole
source, with the parser as a fallback only when no row matches — means one badly
scoped row silently blanks a field the product could have filled. Overlay fails
toward working software.

FIELD NUMBERS HERE ARE HUMAN NUMBERS. A mapping says OBR-24 as field_index=24,
and PID-5 as field_index=5. The MSH off-by-one (MSH-1 IS the field separator, so
its fields sit one index lower after splitting) is absorbed in _extract below,
because an implementation engineer configuring a mapping should never have to
know that, and would get it wrong if asked to.
"""
import logging
import time

from sqlalchemy import text
from db import db

logger = logging.getLogger("HL7_FIELDMAP")

_CACHE_TTL = 300
_cache = {'at': 0.0, 'rows': None}


def invalidate():
    """Drop the cache so an edit in the studio applies to the next message."""
    _cache['at'] = 0.0
    _cache['rows'] = None


def _mappings():
    now = time.time()
    if _cache['rows'] is not None and (now - _cache['at']) < _CACHE_TTL:
        return _cache['rows']
    try:
        rows = [dict(r) for r in db.session.execute(text("""
            SELECT sending_app, message_kind, target_kind, target_field,
                   segment, field_index, component_index, repeat_index,
                   priority, transform
              FROM hl7_field_mappings
             WHERE active
             ORDER BY target_kind, target_field, priority
        """)).mappings().all()]
        _cache['rows'] = rows
        _cache['at'] = now
    except Exception:
        # Keep serving the previous set. Field mapping going stale for a few
        # minutes is survivable; losing it mid-message is not.
        logger.exception("could not load hl7_field_mappings; using previous set")
    return _cache['rows'] or []


def _applies(m, sending_app, kind):
    """'' in a scope column means 'any'."""
    if m['sending_app'] and m['sending_app'] != (sending_app or ''):
        return False
    if m['message_kind'] and m['message_kind'] != (kind or ''):
        return False
    return True


def _extract(segments_by_name, m):
    """Pull one configured value out of the message, or None."""
    from utils.hl7_parse import (parse_hl7_datetime, format_name, xcn_name)

    raw_segments = segments_by_name.get(m['segment'])
    if not raw_segments:
        return None

    # repeat_index selects which repetition of a repeating SEGMENT (a second OBR,
    # a third OBX). 1-based for the same reason field numbers are.
    idx = (m['repeat_index'] or 1) - 1
    if idx >= len(raw_segments):
        return None
    fields = raw_segments[idx].split('|')

    # The MSH absorption. For MSH the split puts "MSH" where MSH-1 conceptually
    # lives, so MSH-9 lands at index 8; every other segment reads naturally.
    i = m['field_index'] - 1 if m['segment'] == 'MSH' else m['field_index']
    if i < 0 or i >= len(fields):
        return None
    value = (fields[i] or '').strip()

    if m['component_index']:
        parts = value.split('^')
        c = m['component_index'] - 1
        value = parts[c].strip() if 0 <= c < len(parts) else ''

    if not value:
        return None

    t = m['transform']
    try:
        if t == 'datetime':
            return parse_hl7_datetime(value)
        if t == 'date':
            dtv = parse_hl7_datetime(value)
            return dtv.date() if dtv else None
        if t == 'upper':
            return value.upper()
        if t == 'name_xpn':
            return format_name(value)
        if t == 'name_xcn':
            return xcn_name(value)
        if t == 'number':
            return float(value)
    except Exception:
        logger.warning("transform %s failed on %s-%s value %r",
                       t, m['segment'], m['field_index'], value[:40])
        return None
    return value


def _index_segments(segments):
    """{'OBR': ['OBR|...', 'OBR|...'], ...} — repeats preserved in order."""
    out = {}
    for s in segments:
        name = s.split('|', 1)[0]
        out.setdefault(name, []).append(s)
    return out


def apply_to_message(msg, segments):
    """
    Overlay the configured 'parsed' mappings onto an already-parsed message.

    Returns the list of field names that configuration actually changed, which the
    studio's test view uses to show an engineer what their mapping did — the
    difference between "my rule fired" and "my rule matched nothing" is the whole
    question when tuning one, and without it they are guessing.
    """
    applied = []
    try:
        by_name = _index_segments(segments)
        rows = [m for m in _mappings()
                if m['target_kind'] == 'parsed'
                and _applies(m, msg.sending_app, msg.kind)]

        # Priority order, first non-empty wins. This is what makes a fallback
        # chain work: "OBR-21, or ORC-19 if that is blank".
        seen = set()
        for m in sorted(rows, key=lambda r: (r['target_field'], r['priority'])):
            field = m['target_field']
            if field in seen:
                continue
            value = _extract(by_name, m)
            if value is None:
                continue
            if hasattr(msg, field):
                setattr(msg, field, value)
                seen.add(field)
                applied.append(field)
    except Exception:
        # Never let configuration break ingestion. A site with a broken mapping
        # should fall back to the product's behaviour, not stop receiving data.
        logger.exception("field mapping overlay failed; keeping parsed values")
    return applied


def direct_overrides(msg, segments):
    """
    Configured writes that bypass the pipeline and target an etl_* column.

    Returns {'study': {...}, 'patient': {...}, 'order': {...}} of column -> value.

    These exist because the operator asked for them explicitly, and they are the
    genuinely risky half of this feature: a value written here is indistinguishable
    downstream from one the lifecycle derived properly, so a wrong mapping corrupts
    a report with no error anywhere. The catalogue marks every one of them
    dangerous and the editor warns; nothing here blocks them.
    """
    out = {'study': {}, 'patient': {}, 'order': {}}
    try:
        by_name = _index_segments(segments)
        rows = [m for m in _mappings()
                if m['target_kind'] in out and _applies(m, msg.sending_app, msg.kind)]
        for m in sorted(rows, key=lambda r: (r['target_kind'], r['target_field'], r['priority'])):
            bucket = out[m['target_kind']]
            if m['target_field'] in bucket:
                continue
            value = _extract(by_name, m)
            if value is not None:
                bucket[m['target_field']] = value
    except Exception:
        logger.exception("direct override evaluation failed; skipping overrides")
    return out


def preview(raw_message, sending_app=None, kind=None):
    """
    What every applicable mapping would extract from one message.

    Powers both studio test modes — replaying an archived message and pasting a
    sample. Returns a row per mapping with the value it produced or why it did
    not, because "no value" has several very different causes and an engineer
    tuning a position needs to tell them apart: wrong segment, wrong field
    number, empty in this message, or a transform that rejected the content.
    """
    from utils.hl7_parse import parse_message, split_segments

    msg = parse_message(raw_message)
    segments = split_segments(raw_message)
    by_name = _index_segments(segments)
    app = sending_app if sending_app is not None else msg.sending_app
    k = kind if kind is not None else msg.kind

    results = []
    for m in _mappings():
        scoped = _applies(m, app, k)
        value = _extract(by_name, m) if scoped else None
        if not scoped:
            why = 'out of scope for this message'
        elif m['segment'] not in by_name:
            why = 'no %s segment in this message' % m['segment']
        elif value is None:
            why = 'field is empty or the transform rejected it'
        else:
            why = ''
        results.append({
            'target_kind':  m['target_kind'],
            'target_field': m['target_field'],
            'source':       '%s-%s%s' % (m['segment'], m['field_index'],
                                         '.%d' % m['component_index'] if m['component_index'] else ''),
            'transform':    m['transform'],
            'priority':     m['priority'],
            'in_scope':     scoped,
            'value':        value,
            'reason':       why,
        })
    return {'message_kind': k, 'sending_app': app, 'results': results}
