"""
utils/hl7_parse.py
────────────────────────────────────────────────────────────────
HL7 v2 parsing — the canonical home for segment primitives and for the parsers
that turn a raw message into the ParsedMessage contract RAY7 screens.

    raw MLLP bytes ─▶ parse_message() ─▶ ParsedMessage ─▶ RAY7.screen() ─▶ project

WHY THIS MODULE EXISTS SEPARATELY FROM hl7_listener.py
The listener's job is sockets, framing and acknowledgement. Parsing is a different
concern with different tests — these functions are pure, take a string, return a
value, and touch neither the network nor the database, which is the only reason
any of this is testable on a machine with no PACS. The listener now imports its
segment primitives from here rather than defining its own, so there is one
implementation of "what is field 5 of the PID" rather than two that can drift.

THE MESSAGE SET, per the agreed cutover workflow:

    HIS  ─▶  ADT              demographics, class, location
    HIS  ─▶  ORM (ORC-1=NW)   the order
    RIS  ─▶  ORC-5 SC         Scheduled — when, where, by whom
    RIS  ─▶  ORC-5 AR         Arrived   — when, by whom
    RIS  ─▶  ORC-5 IP         Started   — when, where (AE title), room, by whom
    RIS  ─▶  ORC-5 CM         Completed — by whom (resident and radiologist)
    PACS ─▶  ORU              the report

ON THE FIELD POSITIONS BELOW — READ THIS BEFORE TRUSTING THEM
The four RIS status messages carry attribution ("by whom"), the performing device
and the room name. WHICH FIELD each of those lives in is not yet confirmed: no real
sample has been captured, and the integration specialist has not yet been asked.
The positions in _STATUS_FIELDS are therefore the most defensible standard choices,
not observed fact, and they are collected in one dict precisely so that correcting
them is a single edit rather than a hunt through the file.

This is safe to ship unconfirmed for one specific reason: every message is archived
verbatim before parsing, so when a real sample arrives the positions get corrected
and the archive is replayed. Nothing is lost by guessing wrong today — which is not
a licence to guess carelessly, but does mean a wrong guess costs a replay rather
than a re-send request the hospital would refuse.

Where a field is absent or unmapped the value stays None and RAY7's referential
rules (UNKNOWN_AETITLE, UNKNOWN_PERFORMER) record the gap, so a wrong guess shows
up as a measurable stream of findings rather than as silently missing data.
"""
import re
import logging
from datetime import datetime, date

from utils.ray7 import ParsedMessage, content_hash, resolve_status

logger = logging.getLogger("HL7_PARSE")


# ── Segment primitives ────────────────────────────────────────────────────────
#
# MSH INDEXING, the thing that catches everyone: splitting on '|' puts the literal
# "MSH" at index 0, and MSH-1 *is* the field separator itself, so MSH-2 lands at
# index 1 and every field after that sits at (number - 1). field(msh, 8) is MSH-9,
# the message type; field(msh, 9) is MSH-10, the control ID. For every other
# segment the mapping is the natural one, because the segment name occupies the
# slot that the separator occupies in MSH.

def split_segments(raw_message):
    """Normalise line endings and return non-empty segments."""
    text = (raw_message or '').replace('\r\n', '\r').replace('\n', '\r')
    return [s.strip() for s in text.split('\r') if s.strip()]


def seg(segments, name):
    """First segment with this name, as a list of fields, or []."""
    for s in segments:
        if s.startswith(name + '|'):
            return s.split('|')
    return []


def all_segs(segments, name):
    """Every segment with this name — OBX, NTE and PRT repeat."""
    return [s.split('|') for s in segments if s.startswith(name + '|')]


def field(segment, index, default=None):
    try:
        val = segment[index].strip()
        return val if val else default
    except IndexError:
        return default


def component(field_val, index, default=None):
    if not field_val:
        return default
    try:
        val = field_val.split('^')[index].strip()
        return val if val else default
    except IndexError:
        return default


def parse_hl7_datetime(val):
    """
    HL7 timestamp (YYYYMMDDHHMMSS and shorter) to a NAIVE datetime.

    Naive deliberately: every timestamp column in this schema is `timestamp without
    time zone`, and RAY7 compares event times against datetime.now(). Returning an
    aware value here would raise TypeError inside a rule, which fail-open swallows,
    silently retiring that rule. The offset is stripped rather than applied because
    no sender on this interface has ever emitted one.
    """
    if not val:
        return None
    val = val.strip()
    # Strip a timezone offset, but only where it really is one: a '+' or '-'
    # followed by four digits at the end. Splitting on '-' unconditionally would
    # destroy any sender that ever emits an ISO-style date.
    val = re.sub(r'[+-]\d{4}$', '', val)
    for length, fmt in ((14, '%Y%m%d%H%M%S'), (12, '%Y%m%d%H%M'), (8, '%Y%m%d')):
        if len(val) >= length:
            try:
                return datetime.strptime(val[:length], fmt)
            except ValueError:
                continue
    return None


def format_name(raw):
    """XPN to a readable name. Handles Last^First^Mid and ID^^Full Name."""
    if not raw:
        return None
    parts = [p.strip() for p in raw.split('^')]
    if len(parts) >= 3 and not parts[1] and parts[2]:
        return parts[2]
    last = parts[0] if parts else ''
    first = parts[1] if len(parts) > 1 else ''
    mid = parts[2] if len(parts) > 2 else ''
    return ' '.join(filter(None, [first, mid, last])) or None


def xcn_name(raw):
    """
    XCN (ID^Last^First^Mid) to a display name, WITHOUT folding the ID into it.

    Separate from format_name() because XCN and XPN disagree about component 1:
    XPN starts with the surname, XCN starts with the identifier. Running an XCN
    through format_name() produces names like "20000191 GERARD WAKIM" — confirmed
    against a real PV1-8 sample.
    """
    if not raw:
        return None
    return ' '.join(filter(None, [component(raw, 2, ''), component(raw, 1, '')])) or None


# Carestream's vendor-specific "email&First&Last^^timestamp" physician stamp.
_PHYSICIAN_STAMP_RE = re.compile(r'^([^&^]+@[^&^]+)&([^&^]*)&([^^]*)\^+(\d{8,14})')


def extract_signing_physician(obr_fields, result_dt_raw):
    """
    Find the physician stamp whose embedded timestamp matches the report's own
    result time — the person who signed THIS report, not whoever else touched the
    order earlier that day.

    Matched by content rather than by a fixed OBR index because this vendor extends
    OBR well past the base spec with undocumented trailing fields, and OBR-32 (the
    standard Principal Result Interpreter) was empty in the real 2026-07-27 sample.
    Trusting an index would silently pick the wrong person the day the field count
    shifts. Falls back to the first stamp found.

    Returns (email, display_name) or (None, None).
    """
    fallback = None
    for value in obr_fields:
        if not value:
            continue
        m = _PHYSICIAN_STAMP_RE.match(value)
        if not m:
            continue
        email, first, last, ts = m.groups()
        name = ' '.join(filter(None, [first.strip(), last.strip()])) or None
        if result_dt_raw and ts[:14] == result_dt_raw[:14]:
            return email, name
        if fallback is None:
            fallback = (email, name)
    return fallback if fallback else (None, None)


# ── Placeholder data ──────────────────────────────────────────────────────────
#
# ER quick-registration admits a patient before they are identified, using a
# literal 9999-11-11 birth date and sex U. Stored naively that yields a negative
# age and poisons every age-banded report, so it is neutralised at parse time and
# recorded in ParsedMessage.placeholders for RAY7 to count.

_PLACEHOLDER_DOBS = {'99991111', '00000000', '18000101'}
_MIN_PLAUSIBLE_BIRTH = date(1880, 1, 1)


def parse_birth_date(raw, placeholders):
    """Birth date as a date, or None when implausible — appending the reason."""
    if not raw:
        return None
    if raw.strip()[:8] in _PLACEHOLDER_DOBS:
        placeholders.append('birth_date=%s (quick-registration placeholder)' % raw.strip()[:8])
        return None
    parsed = parse_hl7_datetime(raw)
    if not parsed:
        return None
    value = parsed.date()
    if value < _MIN_PLAUSIBLE_BIRTH:
        placeholders.append('birth_date=%s (before 1880)' % value.isoformat())
        return None
    if value > date.today():
        placeholders.append('birth_date=%s (in the future)' % value.isoformat())
        return None
    return value


# ── Field positions for the RIS status messages ───────────────────────────────
#
# PROVISIONAL. See the module docstring: these are standard-conformant choices, not
# observed from a real message, and they are gathered here so that one correction
# fixes every parser.
#
#   performer   ORC-19 "Action By" is the field HL7 defines for who performed the
#               action a status change reports. ORC-10 (Entered By) and ORC-12
#               (Ordering Provider) are the plausible alternatives if the RIS
#               populates those instead.
#   aetitle     NOT OBR-24, though that was the first guess and it was wrong.
#               OBR-24 is Diagnostic Service Section ID, whose conventional value
#               is the MODALITY, and _clinical() above already reads it as exactly
#               that. Using one field for two different things produced study rows
#               whose study_modality read "CT64_RH" — an AE title sitting in the
#               modality column, which would have grouped every report by device
#               instead of by modality. Caught on the first live run.
#
#               No standard HL7 v2.3/2.4 field carries a DICOM AE title at all.
#               OBR-21 (Filler Field 2) is a vendor-extensible slot several PACS
#               use for the performing station, so it is the default here — still
#               a guess, but at least one that cannot corrupt another field.
#               Expect this to be absent until a real sample confirms the position;
#               RAY7 reports that honestly as UNKNOWN_AETITLE rather than the
#               pipeline inventing a device.
#   room        PV1-3 is the assigned patient location, whose second component is
#               the room by definition: point-of-care^room^bed^facility.
_STATUS_FIELDS = {
    'performer_seg':   'ORC',
    'performer_field': 19,
    'aetitle_seg':     'OBR',
    'aetitle_field':   21,
    'room_seg':        'PV1',
    'room_field':      3,
    'room_component':  1,
}


def _envelope(segments, raw_message, source_ip):
    """MSH fields plus the identity RAY7's dedupe depends on."""
    msh = seg(segments, 'MSH')
    chash = content_hash(raw_message)
    control_id = field(msh, 9)
    return {
        # A sender that leaves MSH-10 blank still needs a stable identity, or the
        # archive's NOT NULL rejects it. The content hash makes such a message
        # dedupe correctly against an identical redelivery of itself.
        'control_id':       control_id or chash,
        'sending_app':      field(msh, 2, ''),          # MSH-3
        'sending_facility': field(msh, 3),              # MSH-4
        'message_type':     field(msh, 8),              # MSH-9
        'message_datetime': parse_hl7_datetime(field(msh, 6)),   # MSH-7
        'hl7_version':      field(msh, 11),             # MSH-12
        'raw_message':      raw_message,
        'content_hash':     chash,
        'source_ip':        source_ip,
    }


def _identity(segments, msg):
    """Accession, placer order and patient ID — shared by every clinical message."""
    pid = seg(segments, 'PID')
    orc = seg(segments, 'ORC')
    obr = seg(segments, 'OBR')

    msg.patient_id = component(field(pid, 3, ''), 0)

    # Accession is the filler order number. ORC-3 and OBR-3 carry the same value
    # when both are present; which one is populated varies by sender.
    msg.accession_number = (component(field(orc, 3, ''), 0)
                            or component(field(obr, 3, ''), 0))
    # Placer order number (ORC-2) is the only handle on a HIS order before the RIS
    # mints an accession at scheduling.
    msg.placer_order_number = (component(field(orc, 2, ''), 0)
                               or component(field(obr, 2, ''), 0))


def _clinical(segments, msg):
    """Procedure, modality, class and location, where the message carries them."""
    pid = seg(segments, 'PID')
    pv1 = seg(segments, 'PV1')
    obr = seg(segments, 'OBR')

    proc = field(obr, 4, '')
    msg.procedure_code = component(proc, 0)
    msg.procedure_text = component(proc, 1) or component(proc, 2)
    msg.modality = field(obr, 24) or field(obr, 19) or field(obr, 17)

    # PV1-2 patient class, falling back to PID-18 for senders that omit PV1.
    msg.patient_class = field(pv1, 2) or field(pid, 18)
    msg.patient_location = component(field(pv1, 3, ''), 0)


def parse_adt(segments, msg):
    """
    ADT — the only source of demographics on this branch.

    Until now the listener acknowledged ADT and discarded it, which is why birth
    date and sex were unavailable and every age and sex breakdown would have come
    out empty.
    """
    pid = seg(segments, 'PID')
    msg.kind = 'adt'
    msg.patient_id = component(field(pid, 3, ''), 0)
    msg.patient_name = format_name(field(pid, 5))
    msg.birth_date = parse_birth_date(field(pid, 7), msg.placeholders)
    msg.sex = field(pid, 8)

    pv1 = seg(segments, 'PV1')
    msg.patient_class = field(pv1, 2) or field(pid, 18)
    msg.patient_location = component(field(pv1, 3, ''), 0)

    if msg.sex and msg.sex.upper() == 'U' and msg.birth_date is None:
        msg.placeholders.append('sex=U with no usable birth date (quick registration)')
    return msg


def parse_order(segments, msg):
    """ORM with ORC-1 = NW — a new order from the HIS."""
    orc = seg(segments, 'ORC')
    obr = seg(segments, 'OBR')
    pv1 = seg(segments, 'PV1')

    msg.kind = 'order'
    _identity(segments, msg)
    _clinical(segments, msg)

    msg.raw_order_control = field(orc, 1)
    msg.raw_order_status = field(orc, 5)

    # The order itself is the "ordered" moment; ORC-9 is its transaction time.
    msg.event_time = (parse_hl7_datetime(field(orc, 9))
                      or parse_hl7_datetime(component(field(obr, 7, ''), 0))
                      or msg.message_datetime)

    # PV1-8 referring doctor, XCN — not run through format_name(), see xcn_name().
    pv1_8 = field(pv1, 8, '')
    msg.performed_by_id = component(pv1_8, 0)
    msg.performed_by_name = xcn_name(pv1_8)
    msg.performed_by_role = 'referring' if msg.performed_by_id else None
    return msg


def parse_status_event(segments, msg, canonical_state, ladder_rank):
    """
    A RIS lifecycle event: Scheduled, Arrived, Started or Completed.

    The canonical state has already been resolved through hl7_status_map by the
    dispatcher, so this fills in the payload rather than deciding what the message
    means. Attribution, device and room come from _STATUS_FIELDS — provisional
    positions, see the module docstring.
    """
    orc = seg(segments, 'ORC')
    msg.kind = 'status'
    msg.canonical_state = canonical_state
    msg.ladder_rank = ladder_rank
    msg.raw_order_control = field(orc, 1)
    msg.raw_order_status = field(orc, 5)

    _identity(segments, msg)
    _clinical(segments, msg)

    # When the transition happened. ORC-9 is the sender's own transaction time;
    # falling back to MSH-7 matches the agreed rule that for arrived and started,
    # which have no database column at the source, message time IS transition time.
    msg.event_time = parse_hl7_datetime(field(orc, 9)) or msg.message_datetime

    performer = field(seg(segments, _STATUS_FIELDS['performer_seg']),
                      _STATUS_FIELDS['performer_field'], '')
    if performer:
        msg.performed_by_id = component(performer, 0)
        msg.performed_by_name = xcn_name(performer)

    aetitle = field(seg(segments, _STATUS_FIELDS['aetitle_seg']),
                    _STATUS_FIELDS['aetitle_field'])
    if aetitle:
        msg.aetitle = component(aetitle, 0) or aetitle

    room_field = field(seg(segments, _STATUS_FIELDS['room_seg']),
                       _STATUS_FIELDS['room_field'], '')
    if room_field:
        msg.room_name = component(room_field, _STATUS_FIELDS['room_component'])

    return msg


def parse_result(segments, msg):
    """
    ORU — the report. Fills only what the lifecycle and screening need.

    The report TEXT is deliberately not assembled here: hl7_listener's
    parse_oru_r01() already does that for the hl7_oru_reports row, including the
    OBX impression handling, and duplicating it would create two answers to "what
    did the radiologist write". This produces the identity-and-timing view that
    RAY7 and the projector need, nothing more.
    """
    obr = seg(segments, 'OBR')
    msg.kind = 'result'
    _identity(segments, msg)
    _clinical(segments, msg)

    result_dt_raw = field(obr, 22, '') or field(obr, 7, '')
    msg.event_time = parse_hl7_datetime(result_dt_raw)

    physician_id, physician_name = extract_signing_physician(obr, result_dt_raw)
    msg.performed_by_id = physician_id
    msg.performed_by_name = physician_name
    msg.performed_by_role = 'radiologist' if physician_id else None
    return msg


def parse_message(raw_message, source_ip=None):
    """
    Parse any inbound message into a ParsedMessage.

    Always returns a ParsedMessage, never raises and never returns None. An
    unrecognised or malformed message comes back with kind='other' and whatever
    envelope could be read, because the caller still has to archive it — a message
    we cannot parse is still the only copy of itself.
    """
    segments = split_segments(raw_message)
    msg = ParsedMessage(**_envelope(segments, raw_message, source_ip))

    try:
        mtype = (msg.message_type or '').upper()

        if mtype.startswith('ADT'):
            return parse_adt(segments, msg)

        if 'ORU' in mtype:
            return parse_result(segments, msg)

        if 'ORM' in mtype or 'OMG' in mtype or 'OMI' in mtype or 'ORR' in mtype:
            orc = seg(segments, 'ORC')
            control = field(orc, 1, '')
            status = field(orc, 5, '')

            # A new order is an order regardless of what ORC-5 says.
            if control.upper() == 'NW':
                return parse_order(segments, msg)

            state, rank = resolve_status(msg.sending_app, control, status)
            if state:
                return parse_status_event(segments, msg, state, rank)

            # An unmapped ORC-5 is not an error here — it is RAY7's
            # UNKNOWN_STATUS_CODE, which is critical precisely because the
            # transition cannot be classified. Carry the raw values through so the
            # rule can report them, and keep kind='status' so the rule applies.
            msg.kind = 'status'
            msg.raw_order_control = control or None
            msg.raw_order_status = status or None
            _identity(segments, msg)
            _clinical(segments, msg)
            msg.event_time = parse_hl7_datetime(field(orc, 9)) or msg.message_datetime
            return msg

        msg.kind = 'other'
        return msg

    except Exception:
        # Parsing must not be able to cost us a message. The envelope is already
        # populated, so the caller can still archive it and RAY7 can still see it.
        logger.exception("HL7 parse failed for control_id=%s type=%s",
                         msg.control_id, msg.message_type)
        msg.kind = 'other'
        return msg
    finally:
        # Overlay whatever this site has configured, AFTER the built-in logic and
        # regardless of how it went. Overlay rather than replace: a mapping that
        # yields nothing leaves the product's answer intact, so an empty
        # hl7_field_mappings table behaves exactly as this module did before
        # configuration existed, and a half-configured site still gets sensible
        # values everywhere it has not expressed an opinion.
        #
        # Imported here rather than at module scope because utils.hl7_fieldmap
        # imports back into this module for its transforms; a top-level import
        # would be circular.
        try:
            from utils.hl7_fieldmap import apply_to_message
            apply_to_message(msg, segments)
        except Exception:
            logger.exception("field-map overlay failed; keeping built-in parse")
