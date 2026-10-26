"""
routes/brayn_console.py
────────────────────────────────────────────────────────────────
The BRAYN console on LAUMC — the HL7 branch's console (rebuilt 2026-10-25), adapted
2026-10-26 for BRAYN in OBSERVING mode (utils/brayn_shadow.py).

On LAUMC the listener stores every message its own way and the reports come from
the Oracle ETL, so BRAYN decides nothing: it watches the HL7 feed and says what it
sees. The six screens are the same questions, in LAUMC's terms:

    status    Feed status — is every sender delivering, and can BRAYN identify it?
    fix       To fix — messages BRAYN can't identify (an unmapped status code, no
              accession), values it doesn't know, senders over a limit
    messages  Messages — the archive, each one screened or not identified, and why
    study     Study timeline — one accession: every step, the message it came from,
              its notes, and what the reports (Oracle ETL) show for it
    notes     Study notes — data quality and delays, in plain sentences
    settings  Settings — BRAYN on/off, every check, the status, result-status and
              lifecycle-step maps

"Not identified" is the HL7 branch's "held" (stored value 'quarantined'): LAUMC
stored the message anyway, only BRAYN could not place it. Fixing the cause and
pressing Re-check screens those messages again (brayn_shadow.recheck).

LAUMC has no HL7 mapping studio, so the status code, accession field and
lifecycle-step saves live here, next to the result-status and check saves. Every
write touches BRAYN's own tables only; a device's modality is mapped in LAUMC's
Mapping page, which the reports read.

ADMIN ONLY (operator, 2026-10-26): raw messages carry patient identifiers. Every
request runs on hospital time (brayn_shadow.clock), the zone BRAYN's timestamps
are written in.
"""
import json
import logging
import re
from collections import Counter
from urllib.parse import quote

from flask import Blueprint, render_template, request, jsonify, abort, redirect, g
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db
from utils import brayn_explain as X

logger = logging.getLogger("BRAYN_CONSOLE")

brayn_bp = Blueprint('brayn_console', __name__)


@brayn_bp.before_request
def _hospital_clock():
    from utils.brayn_shadow import clock
    g.brayn_clock = clock()
    g.brayn_clock.__enter__()


@brayn_bp.teardown_request
def _end_clock(exc=None):
    c = g.pop('brayn_clock', None)
    if c is not None:
        c.__exit__(None, None, None)

PER_PAGE = 50
VIEWS = ('status', 'fix', 'messages', 'study', 'notes', 'settings')
# The first console's view names, so an old link lands on the screen that took
# its job over.
OLD_VIEWS = {'summary': 'status', 'held': 'fix', 'findings': 'notes', 'health': 'status'}
NOTE_TABS = {'quality': ('quality',), 'delays': ('workflow',)}
MODALITIES = ('CT', 'MR', 'DX', 'CR', 'US', 'MG', 'NM', 'PT', 'XA', 'RF', 'BMD', 'OT')
STEPS = ('ordered', 'scheduled', 'arrived', 'started', 'completed', 'signed_prelim', 'signed_final')
STEP_COLUMN = {s: s + '_at' for s in STEPS}
# The stall note raised while a study waits at a step, by the step it waits FOR.
WAITING_FOR = {'arrived': 'STALLED_SCHEDULED', 'started': 'STALLED_ARRIVED',
               'completed': 'STALLED_STARTED', 'signed_prelim': 'UNREPORTED',
               'signed_final': 'STALLED_UNSIGNED', 'scheduled': 'ORPHAN_ORDER'}
STICKY = {'FUTURE_EVENT', 'PLACEHOLDER_DATA'}     # facts about a message: never self-close


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role != 'admin':
        abort(403)


def _can_configure():
    """Admin only, like the console itself."""
    return current_user.role == 'admin'


def _rows(sql, params=None):
    try:
        return [dict(r) for r in db.session.execute(text(sql), params or {}).mappings().all()]
    except Exception:
        logger.exception("BRAYN console query failed")
        db.session.rollback()
        return []


def _scalar(sql, params=None, default=0):
    try:
        v = db.session.execute(text(sql), params or {}).scalar()
        return default if v is None else v
    except Exception:
        logger.exception("BRAYN console scalar failed")
        db.session.rollback()
        return default


def _now():
    """The DB clock on hospital time: the one BRAYN writes its timestamps in."""
    return _scalar("SELECT LOCALTIMESTAMP", default=None)


def _parse(raw):
    from utils.hl7_parse import parse_message
    try:
        return parse_message(raw or '')
    except Exception:
        logger.exception("BRAYN console: could not parse an archived message")
        return None


# ── what needs fixing: shared by To fix, the tab badges and Feed status ───────

def _held_groups():
    """Held messages grouped by the thing to fix: one unmapped code from one sender
    is one item however many messages carry it."""
    rows = _rows("""
        SELECT f.rule_code,
               COALESCE(a.sending_app, '')                    AS sending_app,
               COALESCE(a.message_type, '')                   AS message_type,
               COALESCE(f.detail->>'order_control', '')       AS order_control,
               COALESCE(f.detail->>'order_status', '')        AS order_status,
               COUNT(DISTINCT a.id)                           AS messages,
               COUNT(DISTINCT f.accession_number)             AS studies,
               MIN(a.received_at)                             AS first_held,
               MAX(a.received_at)                             AS last_held,
               MAX(a.id)                                      AS sample_archive_id
          FROM hl7_message_archive a
          JOIN brayn_findings f ON f.message_archive_id = a.id AND f.severity = 'critical'
         WHERE a.brayn_status = 'quarantined'
         GROUP BY 1, 2, 3, 4, 5
         ORDER BY MAX(a.received_at) DESC
         LIMIT 100
    """)
    # One card per (rule, sender, code) or (rule, sender, type): merge what the SQL
    # split by message type for status codes.
    merged = {}
    for r in rows:
        key = (r['rule_code'], r['sending_app'],
               (r['order_control'], r['order_status']) if r['rule_code'] == 'UNKNOWN_STATUS_CODE'
               else r['message_type'])
        m = merged.get(key)
        if not m:
            merged[key] = r
        else:
            m['messages'] += r['messages']
            m['studies'] += r['studies']
            m['first_held'] = min(m['first_held'], r['first_held'])
            if r['last_held'] > m['last_held']:
                m['last_held'], m['sample_archive_id'] = r['last_held'], r['sample_archive_id']
    out = list(merged.values())
    try:
        from utils.brayn import resolve_status
        for g in out:
            if g['rule_code'] == 'UNKNOWN_STATUS_CODE':
                state, _ = resolve_status(g['sending_app'], g['order_control'], g['order_status'])
                g['mapped_to'] = state      # mapped since the hold: reprocess releases them
    except Exception:
        logger.exception("BRAYN console: could not check status mappings")
    return out


def _open_unknowns():
    """Unknown values that are still unknown. A device mapped since, a result status
    mapped since, or a step switched on since drops out here, with no cleanup job."""
    rows = _rows("""
        SELECT u.kind, u.sending_app, u.value, u.n, u.first_seen, u.last_seen,
               u.sample_archive_id
          FROM brayn_unknown_values u
         WHERE u.kind <> 'status_code'
           AND NOT (u.kind = 'aetitle' AND EXISTS (
                    SELECT 1 FROM aetitle_modality_map m
                     WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(u.value))
                       AND COALESCE(m.modality, '') <> ''))
         ORDER BY u.last_seen DESC
         LIMIT 200
    """)
    out = []
    try:
        from utils.brayn import resolve_result_status, is_expected, ladder_profile
        profile = ladder_profile()
        for u in rows:
            if u['kind'] == 'result_status':
                _, rank = resolve_result_status(u['sending_app'], u['value'])
                if rank is not None:
                    continue
            if u['kind'] == 'rung':
                rank = (profile.get(u['value']) or {}).get('ladder_rank')
                if rank is not None and is_expected(rank):
                    continue
            out.append(u)
    except Exception:
        logger.exception("BRAYN console: could not filter unknown values")
        out = rows
    return out


def _open_alerts():
    return _rows("""
        SELECT f.id, f.rule_code, f.severity, f.sending_app, f.detail, f.created_at
          FROM brayn_findings f
         WHERE f.message_archive_id IS NULL AND f.accession_number IS NULL
           AND f.resolved_at IS NULL
         ORDER BY f.created_at DESC
    """)


def _counts():
    row = _rows("""
        SELECT COUNT(*) FILTER (WHERE r.category = 'quality'  AND f.resolved_at IS NULL) AS quality,
               COUNT(*) FILTER (WHERE r.category = 'workflow' AND f.resolved_at IS NULL) AS delays
          FROM brayn_findings f
          JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
         WHERE f.accession_number IS NOT NULL
    """)
    c = row[0] if row else {'quality': 0, 'delays': 0}
    c['held'] = _scalar("SELECT COUNT(*) FROM hl7_message_archive WHERE brayn_status = 'quarantined'")
    c['groups'] = _held_groups()
    c['unknowns'] = _open_unknowns()
    c['alerts'] = _open_alerts()
    c['to_fix'] = len(c['groups']) + len(c['unknowns']) + len(c['alerts'])
    c['blocking'] = len(c['groups'])
    c['notes'] = (c.get('quality') or 0) + (c.get('delays') or 0)
    c['any_message'] = _scalar("SELECT EXISTS (SELECT 1 FROM hl7_message_archive)", default=False)
    return c


# ── Feed status ───────────────────────────────────────────────────────────────

def _senders(counts):
    hourly = _rows("""
        SELECT sending_app, date_trunc('hour', received_at) AS h, COUNT(*) AS n
          FROM hl7_message_archive
         WHERE received_at >= date_trunc('hour', LOCALTIMESTAMP) - INTERVAL '11 hours'
         GROUP BY 1, 2
    """)
    today = {r['sending_app']: r['n'] for r in _rows("""
        SELECT sending_app, COUNT(*) AS n FROM hl7_message_archive
         WHERE received_at >= LOCALTIMESTAMP::date GROUP BY 1
    """)}
    held = {r['sending_app']: r['n'] for r in _rows("""
        SELECT sending_app, COUNT(*) AS n FROM hl7_message_archive
         WHERE brayn_status = 'quarantined' GROUP BY 1
    """)}
    names = set(today) | set(held) | {r['sending_app'] for r in hourly}
    names |= {r['sending_app'] for r in _rows("""
        SELECT DISTINCT sending_app FROM brayn_interface_stats
         WHERE bucket_start > LOCALTIMESTAMP - INTERVAL '7 days' AND metric = 'MESSAGES'
    """)}
    now = _now()
    base = now.replace(minute=0, second=0, microsecond=0) if now else None
    alerts = Counter(a['sending_app'] or '' for a in counts['alerts'])
    out = []
    for app in sorted(n for n in names if n is not None):
        # Index-backed (0138): the newest message from this sender, however old.
        last = _scalar("SELECT MAX(received_at) FROM hl7_message_archive WHERE sending_app = :s",
                       {'s': app}, default=None)
        bars = [0] * 12
        for r in hourly:
            if r['sending_app'] == app and base:
                i = 11 - int((base - r['h']).total_seconds() // 3600)
                if 0 <= i < 12:
                    bars[i] = r['n']
        h, a = held.get(app, 0), alerts.get(app, 0)
        if h:
            status = ('bad', 'Not identified')
        elif a:
            status = ('warn', 'Check')
        elif last and now and (now - last).total_seconds() > 86400:
            status = ('plain', 'Quiet')
        else:
            status = ('ok', 'Receiving')
        out.append({'app': app or '(blank MSH-3)', 'key': app, 'last': last, 'bars': bars,
                    'peak': max(bars) or 1, 'last_hour': bars[-1], 'today': today.get(app, 0),
                    'held': h, 'alerts': a, 'status': status})
    return out


def _behaviour():
    """Per sender, the last hour: each metric as count + rate against its limit."""
    from utils.brayn_health import INTERFACE_RULES, rates
    try:
        hour = rates(60)
    except Exception:
        logger.exception("BRAYN console: rates failed")
        db.session.rollback()
        hour = {}
    limits = {r['rule_code']: r for r in _rows("""
        SELECT rule_code, threshold_pct, enabled FROM brayn_rules WHERE modality = ''
    """)}
    out = []
    for app in sorted(hour):
        counts = hour[app]
        msgs = counts.get('MESSAGES', 0)
        metrics = []
        for code in INTERFACE_RULES:
            n = counts.get(code, 0)
            lim = limits.get(code, {}).get('threshold_pct')
            if not n and lim is None:
                continue
            pct = round(100.0 * n / msgs, 1) if msgs else None
            metrics.append({'code': code, 'title': X.rule_title(code), 'n': n, 'pct': pct,
                            'limit': float(lim) if lim is not None else None,
                            'over': lim is not None and pct is not None and pct > float(lim)})
        out.append({'app': app or '(blank MSH-3)', 'messages': msgs, 'held': counts.get('HELD', 0),
                    'metrics': metrics})
    return out


# ── Messages ──────────────────────────────────────────────────────────────────

def _message_rows(show, sender, mtype, q, page):
    where, params = [], {'lim': PER_PAGE + 1, 'off': (page - 1) * PER_PAGE}
    if show == 'held':
        where.append("a.brayn_status = 'quarantined'")
    elif show == 'in':
        where.append("a.brayn_status = 'accepted'")
    elif show == 'error':
        where.append("a.parse_status = 'error'")
    if sender:
        where.append("a.sending_app = :sender"); params['sender'] = sender
    if mtype in ('ADT', 'ORM', 'ORU'):
        where.append("a.message_type LIKE :mtype"); params['mtype'] = mtype + '%'
    if q:
        params['q'], params['qlike'] = q, q + '%'
        alt = ["a.message_control_id ILIKE :qlike",
               "a.id IN (SELECT message_archive_id FROM hl7_study_events WHERE accession_number = :q "
               "OR placer_order_number = :q OR patient_id = :q)",
               "a.id IN (SELECT message_archive_id FROM brayn_findings WHERE "
               "message_archive_id IS NOT NULL AND (accession_number = :q OR patient_id = :q))"]
        if q.isdigit() and len(q) < 18:
            alt.append("a.id = :qid"); params['qid'] = int(q)
        where.append('(' + ' OR '.join(alt) + ')')
    clause = ('WHERE ' + ' AND '.join(where)) if where else ''
    rows = _rows(f"""
        SELECT a.id, a.message_control_id, a.sending_app, a.message_type, a.received_at,
               a.brayn_status, a.parse_status, a.projected_at, a.raw_message
          FROM hl7_message_archive a {clause}
         ORDER BY a.id DESC LIMIT :lim OFFSET :off
    """, params)
    more = len(rows) > PER_PAGE
    rows = rows[:PER_PAGE]
    held_why = {}
    ids = [r['id'] for r in rows if r['brayn_status'] == 'quarantined']
    if ids:
        for f in _rows("""SELECT message_archive_id, rule_code FROM brayn_findings
                           WHERE message_archive_id = ANY(:ids) AND severity = 'critical'""",
                       {'ids': ids}):
            held_why.setdefault(f['message_archive_id'], []).append(f['rule_code'])
    for r in rows:
        msg = _parse(r.pop('raw_message'))
        r['event'] = X.event_text(msg) if msg else (r['message_type'] or '')
        r['accession'] = getattr(msg, 'accession_number', None) if msg else None
        r['placer'] = getattr(msg, 'placer_order_number', None) if msg else None
        r['outcome'] = _outcome(r)
        r['why'] = ', '.join(X.rule_title(c) for c in held_why.get(r['id'], []))
    return rows, more


def _outcome(r):
    if r.get('brayn_status') == 'quarantined':
        return ('bad', 'Not identified')
    if r.get('parse_status') == 'error':
        return ('bad', 'Could not be read')
    if r.get('brayn_status') == 'accepted':
        return ('ok', 'Screened')
    return ('plain', 'Waiting')


# ── Study timeline ────────────────────────────────────────────────────────────

def _study(acc, now):
    state = _rows("SELECT * FROM brayn_study_state WHERE accession_number = :a", {'a': acc})
    if not state:
        # An order number or a provisional key finds its study too.
        state = _rows("""SELECT * FROM brayn_study_state WHERE placer_order_number = :a
                          ORDER BY is_provisional, last_event_at DESC NULLS LAST LIMIT 1""", {'a': acc})
    if not state:
        held = _rows("""SELECT f.message_archive_id FROM brayn_findings f
                         WHERE f.accession_number = :a AND f.message_archive_id IS NOT NULL LIMIT 1""",
                     {'a': acc})
        return {'accession': acc, 'state': None, 'held_only': bool(held),
                'messages': _study_messages(acc, now)} if held else None
    s = state[0]
    acc = s['accession_number']
    events = _rows("""
        SELECT e.canonical_state, e.event_time, e.message_archive_id, e.performed_by_name,
               e.raw_order_control, e.raw_order_status, a.sending_app, a.message_type
          FROM hl7_study_events e
          LEFT JOIN hl7_message_archive a ON a.id = e.message_archive_id
         WHERE e.accession_number = :a
         ORDER BY e.event_time NULLS LAST, e.id
    """, {'a': acc})
    notes = _rows("""
        SELECT f.id, f.rule_code, f.detail, f.created_at, f.message_archive_id, r.category
          FROM brayn_findings f
          LEFT JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
         WHERE f.accession_number = :a AND f.resolved_at IS NULL
         ORDER BY f.created_at DESC
    """, {'a': acc})
    try:
        from utils.brayn import ladder_profile, profile_configured
        profile, configured = ladder_profile(), profile_configured()
    except Exception:
        profile, configured = {}, False

    contradicted = set()
    for n in notes:
        if n['rule_code'] == 'TIME_CONTRADICTION':
            for p in (n['detail'] or {}).get('pairs') or []:
                contradicted |= {p.get('earlier_rung'), p.get('later_rung')}
    waiting = {n['rule_code']: n for n in notes if n['category'] == 'workflow'}

    steps = []
    reached_later = False
    for step in reversed(STEPS):
        t = s.get(STEP_COLUMN[step])
        src = next((e for e in events if e['canonical_state'] == step), None)
        if t:
            reached_later = True
            st = ('bad', 'contradicts another step') if step in contradicted else ('ok', '')
        elif configured and not (profile.get(step) or {}).get('expected'):
            st = ('off', 'not sent by this site')
        elif waiting.get(WAITING_FOR.get(step)):
            w = waiting[WAITING_FOR[step]]
            st = ('wait', X.note_sentence(w['rule_code'], w['detail'], now))
        elif reached_later:
            st = ('miss', 'never received')
        else:
            st = ('todo', 'not yet')
        steps.append({'step': step, 'label': X.step_label(step), 'time': t, 'status': st[0],
                      'note': st[1], 'source': src})
    steps.reverse()
    if s.get('cancelled_at'):
        src = next((e for e in events if e['canonical_state'] == 'cancelled'), None)
        steps.append({'step': 'cancelled', 'label': 'Cancelled', 'time': s['cancelled_at'],
                      'status': 'bad', 'note': 'study cancelled', 'source': src})

    report = _rows("""
        SELECT s.accession_number, s.study_date, s.insert_time, s.rep_final_timestamp,
               s.rep_prelim_timestamp, s.study_has_report, s.report_status, s.order_status,
               s.storing_ae, s.patient_class, s.patient_location, s.procedure_code,
               s.study_description,
               COALESCE(m.modality, s.study_modality)               AS modality,
               m.modality IS NOT NULL                               AS modality_from_device,
               CONCAT_WS(' ', s.referring_physician_first_name, s.referring_physician_last_name) AS referring,
               CONCAT_WS(' ', s.signing_physician_first_name, s.signing_physician_last_name)     AS signer,
               COALESCE(m.modality, s.study_modality, '') != 'SR'  AS counted
          FROM etl_didb_studies s
          LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
         WHERE s.accession_number = :a
         LIMIT 1
    """, {'a': acc})
    rep = report[0] if report else None
    if rep and rep['insert_time'] and rep['rep_final_timestamp']:
        rep['tat'] = X.duration((rep['rep_final_timestamp'] - rep['insert_time']).total_seconds() / 60)
    held = _scalar("""SELECT COUNT(*) FROM brayn_findings f JOIN hl7_message_archive a
                       ON a.id = f.message_archive_id
                      WHERE f.accession_number = :a AND a.brayn_status = 'quarantined'""", {'a': acc})
    study_notes = [dict(n, title=X.rule_title(n['rule_code']),
                        sentence=X.note_sentence(n['rule_code'], n['detail'], now))
                   for n in notes if n['category'] in ('quality', 'workflow')]
    return {'accession': acc, 'state': s, 'steps': steps, 'notes': study_notes,
            'report': rep, 'held': held, 'messages': _study_messages(acc, now)}


def _study_messages(acc, now):
    rows = _rows("""
        SELECT a.id, a.message_control_id, a.sending_app, a.message_type, a.received_at,
               a.brayn_status, a.parse_status, a.projected_at, a.raw_message
          FROM hl7_message_archive a
         WHERE a.id IN (SELECT message_archive_id FROM hl7_study_events WHERE accession_number = :a)
            OR a.id IN (SELECT message_archive_id FROM brayn_findings
                         WHERE accession_number = :a AND message_archive_id IS NOT NULL)
         ORDER BY a.id DESC LIMIT 100
    """, {'a': acc})
    for r in rows:
        msg = _parse(r.pop('raw_message'))
        r['event'] = X.event_text(msg) if msg else (r['message_type'] or '')
        r['outcome'] = _outcome(r)
    return rows


# ── Study notes ───────────────────────────────────────────────────────────────

def _notes(tab, q, page, now):
    params = {'lim': PER_PAGE + 1, 'off': (page - 1) * PER_PAGE}
    if tab == 'closed':
        where = ["r.category IN ('quality', 'workflow')",
                 "f.resolved_at > LOCALTIMESTAMP - INTERVAL '7 days'"]
    else:
        where = ["r.category = ANY(:cats)", "f.resolved_at IS NULL"]
        params['cats'] = list(NOTE_TABS.get(tab, ('quality',)))
    where.append("f.accession_number IS NOT NULL")
    if q:
        where.append("(f.accession_number = :q OR f.patient_id = :q)"); params['q'] = q
    rows = _rows(f"""
        SELECT f.id, f.rule_code, f.accession_number, f.patient_id, f.detail, f.created_at,
               f.resolved_at, f.resolution, f.resolution_note, f.resolved_by,
               r.category, s.modality, s.aetitle
          FROM brayn_findings f
          JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
          LEFT JOIN brayn_study_state s ON s.accession_number = f.accession_number
         WHERE {' AND '.join(where)}
         ORDER BY COALESCE(f.resolved_at, f.created_at) DESC
         LIMIT :lim OFFSET :off
    """, params)
    more = len(rows) > PER_PAGE
    out = []
    for r in rows[:PER_PAGE]:
        r['title'] = X.rule_title(r['rule_code'])
        r['sentence'] = X.note_sentence(r['rule_code'], r['detail'], now)
        r['self_closing'] = r['rule_code'] not in STICKY
        out.append(r)
    return out, more


# ── Settings ──────────────────────────────────────────────────────────────────

def _settings():
    rules = _rows("""
        SELECT rule_code, modality, category, title, description, default_severity, severity,
               enabled, threshold_minutes, threshold_pct
          FROM brayn_rules
         ORDER BY rule_code, modality
    """)
    base = {r['rule_code']: dict(r, per_modality=[]) for r in rules if not r['modality']}
    for r in rules:
        if r['modality'] and r['rule_code'] in base:
            base[r['rule_code']]['per_modality'].append(r)
    groups = []
    for cat, title, desc in X.GROUPS:
        items = []
        for r in sorted((r for r in base.values() if r['category'] == cat), key=lambda r: X.rule_title(r['rule_code'])):
            if r['rule_code'] == 'LADDER_REGRESSION':
                continue                      # retired in 0135
            r['plain'] = X.rule_title(r['rule_code'])
            r['what'] = X.rule_what(r['rule_code'])
            r['locked'] = r['rule_code'] in X.LOCKED
            r['level'] = r['severity'] or r['default_severity']
            # Delays are read in hours, everything else in its stored unit; '4', not '4.0'.
            if r['threshold_pct'] is not None:
                r['limit'], r['unit'], r['field'] = f"{float(r['threshold_pct']):g}", '%', 'threshold_pct'
            elif r['threshold_minutes'] is not None and cat == 'workflow':
                r['limit'], r['unit'], r['field'] = f"{r['threshold_minutes'] / 60.0:g}", 'h', 'threshold_minutes'
            elif r['threshold_minutes'] is not None:
                r['limit'], r['unit'], r['field'] = f"{r['threshold_minutes']:g}", 'min', 'threshold_minutes'
            else:
                r['limit'], r['unit'], r['field'] = None, '', None
            items.append(r)
        groups.append({'category': cat, 'title': title, 'description': desc, 'rules': items})
    status_map = _rows("""
        SELECT sending_app, order_control, order_status, canonical_state, active
          FROM hl7_status_map ORDER BY ladder_rank, order_status, sending_app
    """)
    result_map = _rows("""
        SELECT sending_app, result_status, canonical_state FROM hl7_result_status_map
         ORDER BY ladder_rank, result_status
    """)
    ladder = _rows("""SELECT rung, ladder_rank, label, expected, enforce_order
                        FROM brayn_ladder_profile ORDER BY ladder_rank""")
    return {'groups': groups, 'status_map': status_map, 'result_map': result_map, 'ladder': ladder}


# ── the page ──────────────────────────────────────────────────────────────────

@brayn_bp.route('/brayn')
@login_required
def console():
    _require_access()
    view = request.args.get('view', 'status')
    view = OLD_VIEWS.get(view, view)
    if view == 'find':
        # The header's one search box: a study number opens its timeline, anything
        # else (MSH-10, patient ID, archive #) searches the messages.
        q = (request.args.get('q', '') or '').strip()[:80]
        if q and _scalar("""SELECT EXISTS (SELECT 1 FROM brayn_study_state
                             WHERE accession_number = :q OR placer_order_number = :q)""",
                         {'q': q}, default=False):
            return redirect(f'/brayn?view=study&acc={quote(q)}')
        return redirect('/brayn?view=messages' + (f'&q={quote(q)}' if q else ''))
    if view not in VIEWS:
        view = 'status'
    try:
        page = max(1, min(int(request.args.get('page', 1)), 10000))
    except ValueError:
        page = 1
    q = (request.args.get('q', '') or '').strip()[:80]
    now = _now()
    counts = _counts()
    from utils.brayn_shadow import enabled, reset_switch
    reset_switch()
    ctx = {'view': view, 'page': page, 'q': q, 'now': now, 'c': counts,
           'can_configure': _can_configure(), 'X': X, 'modalities': MODALITIES,
           'enabled': enabled()}

    if view == 'status':
        ctx['senders'] = _senders(counts)
        ctx['behaviour'] = _behaviour()
        ctx['today'] = _scalar("SELECT COUNT(*) FROM hl7_message_archive WHERE received_at >= LOCALTIMESTAMP::date")
        ctx['today_held'] = _scalar("""SELECT COUNT(*) FROM hl7_message_archive
                                        WHERE received_at >= LOCALTIMESTAMP::date
                                          AND brayn_status = 'quarantined'""")
    elif view == 'fix':
        ctx['recent'] = _rows("""
            SELECT f.rule_code, f.accession_number, f.sending_app, f.resolution, f.resolved_at,
                   u.username
              FROM brayn_findings f LEFT JOIN users u ON u.id = f.resolved_by
             WHERE f.resolved_by IS NOT NULL AND f.resolved_at > LOCALTIMESTAMP - INTERVAL '24 hours'
             ORDER BY f.resolved_at DESC LIMIT 8
        """)
    elif view == 'messages':
        show = request.args.get('show', 'all')
        show = show if show in ('all', 'in', 'held', 'error') else 'all'
        sender = (request.args.get('sender', '') or '')[:60]
        mtype = request.args.get('type', '')
        mtype = mtype if mtype in ('ADT', 'ORM', 'ORU') else ''
        ctx['rows'], ctx['more'] = _message_rows(show, sender, mtype, q, page)
        ctx.update(show=show, sender=sender, mtype=mtype,
                   sender_options=[r['sending_app'] for r in _rows("""
                       SELECT DISTINCT sending_app FROM brayn_interface_stats
                        WHERE metric = 'MESSAGES' AND sending_app IS NOT NULL ORDER BY 1""")])
    elif view == 'study':
        acc = (request.args.get('acc', '') or '').strip()[:80]
        ctx['acc'] = acc
        ctx['study'] = _study(acc, now) if acc else None
        if not acc:
            ctx['recent_studies'] = _rows("""
                SELECT accession_number, modality, aetitle, open_findings, last_event_at
                  FROM brayn_study_state WHERE NOT is_provisional
                 ORDER BY last_event_at DESC NULLS LAST LIMIT 12
            """)
    elif view == 'notes':
        tab = request.args.get('tab', 'quality')
        tab = tab if tab in ('quality', 'delays', 'closed') else 'quality'
        ctx['tab'] = tab
        ctx['rows'], ctx['more'] = _notes(tab, q, page, now)
    elif view == 'settings':
        ctx.update(_settings())

    return render_template('brayn_console.html', **ctx)


# ── one message, explained ────────────────────────────────────────────────────

@brayn_bp.route('/brayn/message/<int:archive_id>')
@login_required
def message_detail(archive_id):
    """
    The raw message, verbatim, explained field by field.

    Verbatim matters: several field positions in the status parser are still
    educated guesses, and this is where an engineer confirms what a sender
    actually put where, without asking anyone to resend.
    """
    _require_access()
    try:
        row = db.session.execute(text("""
            SELECT id, message_control_id, sending_app, sending_facility, message_type,
                   message_datetime, hl7_version, received_at, source_ip,
                   parse_status, parse_error, brayn_status, brayn_severity,
                   projected_at, raw_message
              FROM hl7_message_archive WHERE id = :id
        """), {'id': archive_id}).mappings().first()
        if not row:
            return jsonify({'error': 'not found'}), 404
        out = dict(row)
        raw = out.pop('raw_message') or ''
        now = _now()
        findings = _rows("""
            SELECT rule_code, severity, detail, created_at, resolved_at
              FROM brayn_findings WHERE message_archive_id = :id
             ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
        """, {'id': archive_id})
        held_rules = [f['rule_code'] for f in findings
                      if f['severity'] == 'critical' and out['brayn_status'] == 'quarantined']
        msg = _parse(raw)
        explained = X.explain(raw, msg, held_rules=held_rules, **_explain_lookups(raw, msg, held_rules)) \
            if msg else {'segments': [], 'read': [], 'suggestions': []}

        outcome = _outcome(out)
        if out['brayn_status'] == 'quarantined':
            reason = ' '.join(X.note_sentence(f['rule_code'], f['detail'], now)
                              for f in findings if f['rule_code'] in held_rules)
            verdict = {'state': 'held', 'title': 'Not identified',
                       'text': (reason + ' ' if reason else '') + "LAUMC stored the message as usual; "
                               "only BRAYN can't place it. Fix the cause, then re-check."}
        elif out['parse_status'] == 'error':
            verdict = {'state': 'held', 'title': 'Could not be read',
                       'text': f"{out.get('parse_error') or 'BRAYN raised an error while reading it.'} "
                               'LAUMC stored the message as usual.'}
        elif out['brayn_status'] == 'accepted':
            acc = getattr(msg, 'accession_number', None)
            what = X.event_text(msg) if msg else ''
            verdict = {'state': 'in', 'title': 'Screened',
                       'text': (f'Added to study {acc}: {what}.' if acc else
                                f'Recorded: {what}.')}
        else:
            verdict = {'state': 'wait', 'title': 'Waiting', 'text': 'Received; BRAYN has not screened it yet.'}

        out.update(explained)
        out['verdict'] = verdict
        out['outcome'] = outcome
        out['ack'] = ''     # LAUMC's listener answers; BRAYN does not record what
        out['accession'] = getattr(msg, 'accession_number', None) if msg else None
        out['message_kind'] = getattr(msg, 'kind', None) if msg else None
        out['event'] = X.event_text(msg) if msg else ''
        out['can_configure'] = _can_configure()
        out['findings'] = [{'rule_code': f['rule_code'], 'title': X.rule_title(f['rule_code']),
                            'sentence': X.note_sentence(f['rule_code'], f['detail'], now),
                            'severity': f['severity'], 'resolved': f['resolved_at'] is not None}
                           for f in findings]
        out['received'] = X.clock(out['received_at'], now)
        return jsonify(json.loads(json.dumps(out, default=str)))
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: message detail failed | id=%s", archive_id)
        return jsonify({'error': 'lookup failed'}), 500


def _explain_lookups(raw, msg, held_rules):
    """The database facts explain() needs, fetched here so it stays pure."""
    out = {}
    try:
        from utils.brayn import _known_aetitles, is_expected, profile_configured
        out['mapped'] = X._mapped_positions(msg.sending_app, msg.kind)
        if getattr(msg, 'aetitle', None):
            out['known_aetitle'] = msg.aetitle.upper() in _known_aetitles()
        if msg.kind == 'result' and msg.raw_result_status:
            out['result_mapped'] = msg.ladder_rank is not None
        if msg.kind == 'status' and msg.ladder_rank is not None and profile_configured():
            out['step_expected'] = is_expected(msg.ladder_rank)
    except Exception:
        logger.exception("BRAYN console: explain lookups failed")
    if 'MISSING_IDENTIFIER' in held_rules:
        values = set()
        for name, fields in X.segments_of(raw):
            if name in ('MSH', 'PID'):
                continue
            for _, v in fields:
                for c in (v or '').split('~')[0].split('^'):
                    if 4 <= len(c.strip()) <= 64:
                        values.add(c.strip())
        if values:
            out['known_accessions'] = {r['accession_number'] for r in _rows(
                "SELECT accession_number FROM brayn_study_state WHERE accession_number = ANY(:v)",
                {'v': list(values)[:500]})}
        sample = [r['accession_number'] for r in _rows("""
            SELECT accession_number FROM brayn_study_state WHERE NOT is_provisional
             ORDER BY last_event_at DESC NULLS LAST LIMIT 500""")]
        shapes = Counter(X._shape(a) for a in sample if a)
        out['accession_shapes'] = {s for s, n in shapes.items() if n >= max(2, len(sample) * 0.05)}
    return out


# ── actions ───────────────────────────────────────────────────────────────────

@brayn_bp.route('/brayn/held/replay', methods=['POST'])
@login_required
def replay_held():
    """
    Re-check the messages BRAYN could not identify (same address as on the HL7
    branch, so the page's script is shared).

    The operator fixes the cause first (maps the status code, points the accession
    at the right field), then presses this. Each message is screened again under
    today's configuration; those that now pass add their steps to their study, the
    rest stay "not identified" with their finding refreshed. LAUMC's own data is
    not involved either way. Optionally limited to one rule and one sender, which
    is what a To-fix card sends.
    """
    _require_access()
    body = request.json or {}
    rule = (request.form.get('rule') or body.get('rule') or '').strip()
    sender = body.get('sender')

    sql = "SELECT a.id FROM hl7_message_archive a WHERE a.brayn_status = 'quarantined'"
    params = {}
    if rule:
        sql += """ AND EXISTS (SELECT 1 FROM brayn_findings f
                                WHERE f.message_archive_id = a.id AND f.rule_code = :rule)"""
        params['rule'] = rule
    if sender is not None:
        sql += " AND COALESCE(a.sending_app, '') = :sender"
        params['sender'] = str(sender)
    ids = [r['id'] for r in _rows(sql + " ORDER BY a.id", params)]
    if not ids:
        return jsonify({'status': 'ok', 'replayed': 0, 'released': 0, 'still_held': 0})

    try:
        from utils.brayn_shadow import recheck
        recheck(ids)
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: re-check failed")
        return jsonify({'status': 'error', 'error': 're-check failed; nothing was changed'}), 500

    still = _scalar("SELECT COUNT(*) FROM hl7_message_archive "
                    "WHERE id = ANY(:ids) AND brayn_status = 'quarantined'", {'ids': ids})
    return jsonify({'status': 'ok', 'replayed': len(ids), 'released': len(ids) - still,
                    'still_held': still})


@brayn_bp.route('/brayn/finding/<int:finding_id>/resolve', methods=['POST'])
@login_required
def resolve(finding_id):
    """
    Close a finding. 'ignored' ("Don't flag again") also stops a study note from
    reopening itself for that study.

    Resolving does NOT put held data into the reports — fixing the cause and
    reprocessing does — said in the response so the button cannot imply a power it
    does not have.
    """
    _require_access()
    body = request.json or {}
    resolution = request.form.get('resolution') or body.get('resolution') or 'cleared'
    note = (request.form.get('note') or body.get('note') or '')[:2000]
    if resolution not in ('cleared', 'ignored', 'fixed_upstream'):
        return jsonify({'error': 'invalid resolution'}), 400
    try:
        db.session.execute(text("""
            UPDATE brayn_findings
               SET resolved_at = NOW(), resolved_by = :uid,
                   resolution = :res, resolution_note = :note
             WHERE id = :id AND resolved_at IS NULL
        """), {'id': finding_id, 'uid': current_user.id, 'res': resolution, 'note': note})
        db.session.commit()
        return jsonify({'status': 'ok',
                        'note': 'Closed. This does not identify a message: fix the cause, '
                                'then re-check.'})
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: resolve failed | id=%s", finding_id)
        return jsonify({'error': 'resolve failed'}), 500


@brayn_bp.route('/brayn/switch', methods=['POST'])
@login_required
def switch():
    """Turn BRAYN on or off. Off stops archiving and screening from the next
    message; what was already seen stays. LAUMC's own path is the same either way."""
    _require_access()
    on = str((request.json or {}).get('enabled')).lower() in ('1', 'true', 'yes', 'on')
    try:
        from utils.brayn_shadow import set_enabled
        set_enabled(on)
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: switch failed")
        return jsonify({'error': 'save failed'}), 500
    _audit('brayn_switched', {'enabled': on})
    return jsonify({'status': 'ok', 'enabled': on})


# Rank is derived from the step, never typed in: it is what the lifecycle rules
# compare (as in the HL7 branch's mapping studio). Cancellation is off the ladder.
STATE_RANK = {'ordered': 20, 'scheduled': 40, 'arrived': 60, 'started': 70,
              'completed': 100, 'signed_prelim': 120, 'signed_final': 140,
              'cancelled': -1}
STATUS_STATES = ('ordered', 'scheduled', 'arrived', 'started', 'completed', 'cancelled')


def _drop_cache(key):
    try:
        from utils.brayn import _cache
        _cache[key] = (0.0, None)
    except Exception:
        pass


@brayn_bp.route('/brayn/status-code/save', methods=['POST'])
@login_required
def save_status_code():
    """Give an ORC-5 value its lifecycle step, for every sender or one."""
    _require_access()
    body = request.json or {}
    state = str(body.get('canonical_state') or '').strip()
    code = str(body.get('order_status') or '').strip().upper()
    if state not in STATUS_STATES:
        return jsonify({'error': 'choose a lifecycle step'}), 400
    if not code or len(code) > 20:
        return jsonify({'error': 'status code is required'}), 400
    try:
        db.session.execute(text("""
            INSERT INTO hl7_status_map
                (sending_app, order_control, order_status, canonical_state,
                 ladder_rank, active, notes, updated_at)
            VALUES (:app, :ctl, :status, :state, :rank, TRUE, 'set from the BRAYN console', NOW())
            ON CONFLICT (sending_app, order_control, order_status) DO UPDATE SET
                canonical_state = EXCLUDED.canonical_state, ladder_rank = EXCLUDED.ladder_rank,
                active = TRUE, updated_at = NOW()
        """), {'app': str(body.get('sending_app') or '').strip(),
               'ctl': str(body.get('order_control') or '').strip(),
               'status': code, 'state': state, 'rank': STATE_RANK[state]})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: status code save failed | %s", code)
        return jsonify({'error': 'save failed'}), 500
    _drop_cache('status_map')
    _audit('brayn_status_code_mapped', {'order_status': code, 'canonical_state': state})
    return jsonify({'status': 'ok'})


@brayn_bp.route('/brayn/field-map/save', methods=['POST'])
@login_required
def save_field_map():
    """
    Tell BRAYN which field a sender puts the accession in — the "pick the field" of
    a message with no identifier. BRAYN's own reading only (target 'parsed'); LAUMC's
    listener keeps its own field map.
    """
    _require_access()
    body = request.json or {}
    seg = str(body.get('segment') or '').strip().upper()
    field = str(body.get('target_field') or '').strip()
    if field not in ('accession_number', 'placer_order_number'):
        return jsonify({'error': 'only the accession or order number can be set here'}), 400
    if not re.fullmatch(r'[A-Z][A-Z0-9]{2}', seg) or seg == 'MSH':
        return jsonify({'error': 'choose a field outside MSH'}), 400
    try:
        fidx = int(body.get('field_index'))
        comp = int(body['component_index']) if body.get('component_index') not in (None, '') else None
    except (TypeError, ValueError):
        return jsonify({'error': 'field numbers are whole numbers'}), 400
    if fidx < 1 or (comp is not None and comp < 1):
        return jsonify({'error': 'field numbers start at 1'}), 400
    try:
        db.session.execute(text("""
            INSERT INTO hl7_field_mappings
                (sending_app, message_kind, target_kind, target_field, segment, field_index,
                 component_index, priority, transform, active, notes, updated_by, updated_at)
            VALUES (:app, :kind, 'parsed', :field, :seg, :fidx, :comp, 1, 'text', TRUE,
                    'set from the BRAYN console', :uid, NOW())
            ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority)
            DO UPDATE SET segment = EXCLUDED.segment, field_index = EXCLUDED.field_index,
                          component_index = EXCLUDED.component_index, active = TRUE,
                          notes = EXCLUDED.notes, updated_by = EXCLUDED.updated_by,
                          updated_at = NOW()
        """), {'app': str(body.get('sending_app') or '').strip(),
               'kind': str(body.get('message_kind') or '').strip(),
               'field': field, 'seg': seg, 'fidx': fidx, 'comp': comp, 'uid': current_user.id})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: field map save failed")
        return jsonify({'error': 'save failed'}), 500
    try:
        from utils import hl7_fieldmap
        hl7_fieldmap.invalidate()
    except Exception:
        pass
    _audit('brayn_field_mapped', {'target_field': field, 'segment': seg, 'field_index': fidx,
                                  'component_index': comp,
                                  'sending_app': str(body.get('sending_app') or '')})
    return jsonify({'status': 'ok'})


@brayn_bp.route('/brayn/ladder-profile/save', methods=['POST'])
@login_required
def save_ladder():
    """Which lifecycle steps this site sends, and which have their delivery order
    counted. Counting order needs the step on, so off forces it off."""
    _require_access()
    body = request.json or {}
    rung = str(body.get('rung') or '').strip()
    if rung not in STATE_RANK or rung == 'cancelled':
        return jsonify({'error': 'unknown step'}), 400
    flag = lambda k: str(body.get(k)).lower() in ('1', 'true', 'yes', 'on')
    expected = flag('expected')
    enforce = flag('enforce_order') and expected
    try:
        db.session.execute(text("""
            UPDATE brayn_ladder_profile
               SET expected = :e, enforce_order = :o, updated_at = NOW()
             WHERE rung = :r
        """), {'r': rung, 'e': expected, 'o': enforce})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: ladder save failed | %s", rung)
        return jsonify({'error': 'save failed'}), 500
    _drop_cache('ladder')
    _audit('brayn_step_changed', {'rung': rung, 'expected': expected, 'enforce_order': enforce})
    return jsonify({'status': 'ok', 'rung': rung, 'expected': expected, 'enforce_order': enforce})


@brayn_bp.route('/brayn/result-status/save', methods=['POST'])
@login_required
def save_result_status():
    """
    Give an unmapped OBX-11 value its meaning: preliminary or final report.

    No screen could do this before — hl7_result_status_map was seed-and-psql only.
    Upserted per sender (or '' for every sender) on the table's own unique key;
    rows are only ever added or changed, never removed, like every seeded map.
    """
    _require_access()
    if not _can_configure():
        return jsonify({'error': 'only administrators can map result statuses'}), 403
    body = request.json or {}
    value = str(body.get('result_status') or '').strip().upper()
    state = str(body.get('canonical_state') or '').strip()
    rank = {'signed_prelim': 120, 'signed_final': 140}.get(state)
    if not value or len(value) > 8:
        return jsonify({'error': 'result status is required'}), 400
    if rank is None:
        return jsonify({'error': 'choose preliminary or final'}), 400
    try:
        db.session.execute(text("""
            INSERT INTO hl7_result_status_map
                (sending_app, result_status, canonical_state, ladder_rank, active, notes, updated_at)
            VALUES (:app, :v, :s, :r, TRUE, 'set from the BRAYN console', NOW())
            ON CONFLICT (sending_app, result_status) DO UPDATE SET
                canonical_state = EXCLUDED.canonical_state, ladder_rank = EXCLUDED.ladder_rank,
                active = TRUE, updated_at = NOW()
        """), {'app': str(body.get('sending_app') or '').strip(), 'v': value, 's': state, 'r': rank})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: result status save failed | %s", value)
        return jsonify({'error': 'save failed'}), 500
    try:
        from utils.brayn import _cache
        _cache['result_map'] = (0.0, None)
    except Exception:
        pass
    _audit('brayn_result_status_mapped', {'result_status': value, 'canonical_state': state})
    return jsonify({'status': 'ok'})


@brayn_bp.route('/brayn/rules/save', methods=['POST'])
@login_required
def save_rule():
    """
    Change one check's switch, level or limit — the first screen that can; until
    the rebuild these lived only in brayn_rules.

    The two intake checks cannot be switched off or demoted: a message with no
    identifier or no step produces no data whatever the setting says, so the switch
    would only make the console stop saying why the data is missing.
    """
    _require_access()
    if not _can_configure():
        return jsonify({'error': 'only administrators can change checks'}), 403
    body = request.json or {}
    code = str(body.get('rule_code') or '').strip()
    modality = str(body.get('modality') or '').strip().upper()
    row = _rows("SELECT * FROM brayn_rules WHERE rule_code = :c AND modality = :m",
                {'c': code, 'm': modality})
    if not row:
        return jsonify({'error': 'unknown check'}), 404
    row = row[0]
    sets, params = [], {'c': code, 'm': modality}

    if 'enabled' in body:
        enabled = str(body['enabled']).lower() in ('1', 'true', 'yes', 'on')
        if code in X.LOCKED and not enabled:
            return jsonify({'error': 'this check is always on'}), 400
        sets.append('enabled = :enabled'); params['enabled'] = enabled
    if 'severity' in body:
        sev = str(body['severity']).strip().lower()
        allowed = ('critical',) if code in X.LOCKED else ('info', 'warning')
        if sev not in allowed:
            return jsonify({'error': f'level must be one of {", ".join(allowed)}'}), 400
        sets.append('severity = :sev'); params['sev'] = sev
    for fieldname, lo, hi in (('threshold_pct', 0.0, 100.0), ('threshold_minutes', 1, 60 * 24 * 60)):
        if fieldname in body:
            if row[fieldname] is None:
                return jsonify({'error': f'this check has no {fieldname.replace("_", " ")}'}), 400
            try:
                val = float(body[fieldname])
            except (TypeError, ValueError):
                return jsonify({'error': 'the limit must be a number'}), 400
            if not lo <= val <= hi:
                return jsonify({'error': f'the limit must be between {lo:g} and {hi:g}'}), 400
            sets.append(f'{fieldname} = :{fieldname}')
            params[fieldname] = int(round(val)) if fieldname == 'threshold_minutes' else val
    if not sets:
        return jsonify({'error': 'nothing to change'}), 400
    try:
        db.session.execute(text(f"""
            UPDATE brayn_rules SET {', '.join(sets)}, updated_at = NOW()
             WHERE rule_code = :c AND modality = :m
        """), params)
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: rule save failed | %s", code)
        return jsonify({'error': 'save failed'}), 500
    try:
        from utils.brayn import _cache
        _cache['rules'] = (0.0, None)
    except Exception:
        pass
    _audit('brayn_rule_changed', {'rule_code': code, 'modality': modality,
                                  **{k: v for k, v in params.items() if k not in ('c', 'm')}})
    return jsonify({'status': 'ok'})


def _audit(action, detail):
    try:
        from utils.audit import log_event
        log_event(action, category='config', resource_type='brayn', detail=detail)
    except Exception:
        logger.exception("BRAYN console: audit log failed for %s", action)
