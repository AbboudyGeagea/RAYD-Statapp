"""
routes/brayn_console.py
────────────────────────────────────────────────────────────────
The BRAYN console — the operator's window onto the screening engine.

An engine whose output nobody can see is an engine nobody trusts, and a held-back
queue nobody can open is worse than none at all: data stops reaching the reports
and there is no visible reason why.

ONE VIEW PER QUESTION, matching BRAYN's four layers (migration 0135):

    summary   is anything wrong? — open items per layer and rule
    held      ① what is NOT in the reports, grouped by the thing to fix
    findings  ② / ④ the notes on studies (quality, workflow), filterable
    health    ③ how each sender is behaving, as rates, plus the unknown values
    messages  the archive itself

HELD IS GROUPED BY REASON, not by study. Since the rebuild only a message BRAYN
cannot identify or classify is held, and the fix is always configuration — map this
status code, find where this sender puts its accession — after which "Replay held
messages" re-screens them. There is no release-without-fixing any more: a held
message has no study to attach to or no rung to record, so forcing it through could
not produce data.

Gated on the existing 'can_view_etl' permission rather than a new key: BRAYN
occupies the position the ETL used to, and raw messages carry patient identifiers.
"""
import json
import logging

from flask import Blueprint, render_template, request, jsonify, abort, current_app, redirect
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, user_has_page

logger = logging.getLogger("BRAYN_CONSOLE")

brayn_bp = Blueprint('brayn_console', __name__)

PER_PAGE = 50
LAYERS = ('intake', 'quality', 'interface', 'workflow')


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role != 'su' and not user_has_page(current_user, 'can_view_etl'):
        abort(403)


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


def _summary():
    out = {}
    row = _rows("""
        SELECT COUNT(*) FILTER (WHERE r.category = 'quality'   AND f.resolved_at IS NULL) AS open_quality,
               COUNT(*) FILTER (WHERE r.category = 'workflow'  AND f.resolved_at IS NULL) AS open_workflow,
               COUNT(*) FILTER (WHERE r.category = 'interface' AND f.resolved_at IS NULL) AS open_alerts,
               COUNT(*) FILTER (WHERE f.resolved_at IS NOT NULL)                          AS resolved
          FROM brayn_findings f
          LEFT JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
    """)
    out.update(row[0] if row else {})
    row = _rows("""
        SELECT COUNT(*) AS messages,
               COUNT(*) FILTER (WHERE brayn_status = 'quarantined') AS held,
               COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '24 hours') AS last_24h
          FROM hl7_message_archive
    """)
    out.update(row[0] if row else {})
    out['studies'] = _scalar("SELECT COUNT(*) FROM brayn_study_state WHERE NOT is_provisional")
    out['unknown_values'] = _scalar("SELECT COUNT(*) FROM brayn_unknown_values")
    return out


def _health():
    """Per sender: the last hour and the last 24 hours, every metric as count + rate."""
    from utils.brayn_health import INTERFACE_RULES, rates
    try:
        hour, day = rates(60), rates(24 * 60)
    except Exception:
        logger.exception("BRAYN console: rates failed")
        db.session.rollback()
        hour, day = {}, {}
    senders = []
    for app in sorted(set(hour) | set(day)):
        row = {'sending_app': app or '(blank MSH-3)', 'metrics': []}
        for window, counts in (('hour', hour.get(app, {})), ('day', day.get(app, {}))):
            row[window + '_messages'] = counts.get('MESSAGES', 0)
            row[window + '_held'] = counts.get('HELD', 0)
        for code in INTERFACE_RULES:
            h, d = hour.get(app, {}).get(code, 0), day.get(app, {}).get(code, 0)
            if h or d:
                row['metrics'].append({
                    'code': code, 'hour': h, 'day': d,
                    'hour_pct': round(100.0 * h / row['hour_messages'], 1) if row['hour_messages'] else None,
                    'day_pct': round(100.0 * d / row['day_messages'], 1) if row['day_messages'] else None,
                })
        senders.append(row)
    return senders


@brayn_bp.route('/brayn')
@login_required
def console():
    _require_access()
    view     = request.args.get('view', 'summary')
    severity = request.args.get('severity', '')
    rule     = request.args.get('rule', '')
    layer    = request.args.get('layer', '')
    show     = request.args.get('show', 'open')
    search   = (request.args.get('q', '') or '').strip()
    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page = 1
    offset = (page - 1) * PER_PAGE

    ctx = {'view': view, 'severity': severity, 'rule': rule, 'layer': layer, 'show': show,
           'search': search, 'page': page, 'per_page': PER_PAGE, 'layers': LAYERS,
           'summary': _summary(), 'rows': [], 'total': 0, 'alerts': [], 'unknown': []}

    if view == 'summary':
        # One row per rule, grouped by layer: a reader can see at a glance that the
        # interface is noisy and that nothing is held, or the reverse.
        ctx['rows'] = _rows("""
            SELECT COALESCE(r.category, 'intake') AS layer, f.rule_code,
                   (ARRAY['info','warning','critical'])[
                       MAX(array_position(ARRAY['info','warning','critical'], f.severity))
                   ] AS severity,
                   COUNT(*)                                       AS total,
                   COUNT(*) FILTER (WHERE f.resolved_at IS NULL)  AS open,
                   COUNT(DISTINCT f.accession_number)             AS studies,
                   MAX(f.created_at)                              AS last_seen,
                   MAX(r.title) AS title
              FROM brayn_findings f
              LEFT JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
             GROUP BY COALESCE(r.category, 'intake'), f.rule_code
             ORDER BY array_position(ARRAY['intake','quality','interface','workflow'],
                                     COALESCE(r.category, 'intake')),
                      COUNT(*) FILTER (WHERE f.resolved_at IS NULL) DESC
        """)

    elif view == 'findings':
        where, params = [], {'lim': PER_PAGE, 'off': offset}
        if show == 'open':
            where.append("f.resolved_at IS NULL")
        elif show == 'resolved':
            where.append("f.resolved_at IS NOT NULL")
        if severity:
            where.append("f.severity = :severity"); params['severity'] = severity
        if rule:
            where.append("f.rule_code = :rule"); params['rule'] = rule
        if layer in LAYERS:
            where.append("COALESCE(r.category, 'intake') = :layer"); params['layer'] = layer
        if search:
            where.append("(f.accession_number ILIKE :q OR f.patient_id ILIKE :q "
                         "OR a.message_control_id ILIKE :q OR f.sending_app ILIKE :q)")
            params['q'] = f'%{search}%'
        clause = ('WHERE ' + ' AND '.join(where)) if where else ''
        joins = """
              FROM brayn_findings f
              LEFT JOIN hl7_message_archive a ON a.id = f.message_archive_id
              LEFT JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
        """
        ctx['total'] = _scalar(f"SELECT COUNT(*) {joins} {clause}",
                               {k: v for k, v in params.items() if k not in ('lim', 'off')})
        ctx['rows'] = _rows(f"""
            SELECT f.id, f.rule_code, f.severity, f.accession_number, f.patient_id,
                   f.sending_app, f.detail, f.created_at, f.resolved_at, f.resolution,
                   f.message_archive_id, a.message_type, a.message_control_id,
                   r.title, COALESCE(r.category, 'intake') AS layer
              {joins} {clause}
             ORDER BY CASE f.severity WHEN 'critical' THEN 1
                                      WHEN 'warning' THEN 2 ELSE 3 END,
                      f.created_at DESC
             LIMIT :lim OFFSET :off
        """, params)

    elif view == 'held':
        # Grouped by the thing to fix. The same unmapped code from the same sender is
        # one item however many messages carry it.
        ctx['total'] = _scalar("""
            SELECT COUNT(*) FROM hl7_message_archive WHERE brayn_status = 'quarantined'
        """)
        ctx['rows'] = _rows("""
            SELECT f.rule_code,
                   COALESCE(a.sending_app, '')                       AS sending_app,
                   CASE f.rule_code
                        WHEN 'UNKNOWN_STATUS_CODE'
                        THEN COALESCE(f.detail->>'order_control', '') || ' / ' ||
                             COALESCE(f.detail->>'order_status', '(empty)')
                        ELSE COALESCE(a.message_type, '') END        AS what,
                   COUNT(DISTINCT a.id)                              AS messages,
                   COUNT(DISTINCT f.accession_number)                AS studies,
                   MIN(a.received_at)                                AS first_held,
                   MAX(a.received_at)                                AS last_held,
                   MAX(a.id)                                         AS sample_archive_id
              FROM hl7_message_archive a
              JOIN brayn_findings f ON f.message_archive_id = a.id AND f.severity = 'critical'
             WHERE a.brayn_status = 'quarantined'
             GROUP BY 1, 2, 3
             ORDER BY MAX(a.received_at) DESC
             LIMIT :lim OFFSET :off
        """, {'lim': PER_PAGE, 'off': offset})

    elif view == 'health':
        ctx['rows'] = _health()
        ctx['alerts'] = _rows("""
            SELECT f.id, f.rule_code, f.severity, f.sending_app, f.detail, f.created_at, r.title
              FROM brayn_findings f
              LEFT JOIN brayn_rules r ON r.rule_code = f.rule_code AND r.modality = ''
             WHERE f.message_archive_id IS NULL AND f.accession_number IS NULL
               AND f.resolved_at IS NULL
             ORDER BY f.created_at DESC
        """)
        ctx['unknown'] = _rows("""
            SELECT kind, sending_app, value, n, first_seen, last_seen, sample_archive_id
              FROM brayn_unknown_values
             ORDER BY last_seen DESC
             LIMIT 200
        """)

    elif view == 'messages':
        where, params = [], {'lim': PER_PAGE, 'off': offset}
        if search:
            where.append("(a.message_control_id ILIKE :q OR a.sending_app ILIKE :q "
                         "OR a.message_type ILIKE :q)")
            params['q'] = f'%{search}%'
        if severity:      # reused as the status filter on this view
            where.append("a.brayn_status = :st"); params['st'] = severity
        clause = ('WHERE ' + ' AND '.join(where)) if where else ''

        ctx['total'] = _scalar(f"SELECT COUNT(*) FROM hl7_message_archive a {clause}",
                               {k: v for k, v in params.items() if k not in ('lim', 'off')})
        ctx['rows'] = _rows(f"""
            SELECT a.id, a.message_control_id, a.sending_app, a.message_type,
                   a.received_at, a.brayn_status, a.brayn_severity,
                   (a.projected_at IS NOT NULL) AS projected,
                   (SELECT COUNT(*) FROM brayn_findings x WHERE x.message_archive_id = a.id)
                     AS findings
              FROM hl7_message_archive a {clause}
             ORDER BY a.id DESC LIMIT :lim OFFSET :off
        """, params)

    ctx['rule_options'] = _rows("""
        SELECT rule_code, COUNT(*) FILTER (WHERE resolved_at IS NULL) AS open
          FROM brayn_findings GROUP BY rule_code ORDER BY 1
    """)
    ctx['pages'] = max(1, -(-ctx['total'] // PER_PAGE)) if ctx['total'] else 1
    return render_template('brayn_console.html', **ctx)


@brayn_bp.route('/ray7')
@brayn_bp.route('/ray7/<path:rest>')
def old_address(rest=''):
    """BRAYN was called RAY7 until migration 0137; old bookmarks land on the new page."""
    qs = request.query_string.decode()
    return redirect('/brayn' + ('/' + rest if rest else '') + ('?' + qs if qs else ''), code=301)


@brayn_bp.route('/brayn/message/<int:archive_id>')
@login_required
def message_detail(archive_id):
    """
    The raw message, verbatim, plus every finding against it.

    Verbatim matters: several field positions in the status parser are still
    educated guesses, and this is where an engineer confirms what a sender
    actually put where, without asking anyone to resend.
    """
    _require_access()
    try:
        msg = db.session.execute(text("""
            SELECT id, message_control_id, sending_app, sending_facility, message_type,
                   message_datetime, hl7_version, received_at, source_ip,
                   parse_status, parse_error, brayn_status, brayn_severity,
                   projected_at, raw_message
              FROM hl7_message_archive WHERE id = :id
        """), {'id': archive_id}).mappings().first()
        if not msg:
            return jsonify({'error': 'not found'}), 404

        findings = _rows("""
            SELECT rule_code, severity, detail, created_at, resolved_at
              FROM brayn_findings WHERE message_archive_id = :id
             ORDER BY CASE severity WHEN 'critical' THEN 1
                                    WHEN 'warning' THEN 2 ELSE 3 END
        """, {'id': archive_id})

        out = dict(msg)
        # One HL7 segment per line. The wire format is carriage-return delimited,
        # which a browser renders as one unreadable line.
        out['segments'] = [s for s in (out.pop('raw_message') or '')
                           .replace('\r\n', '\r').replace('\n', '\r').split('\r') if s]
        out['findings'] = findings
        return jsonify(json.loads(json.dumps(out, default=str)))
    except Exception:
        logger.exception("BRAYN console: message detail failed | id=%s", archive_id)
        return jsonify({'error': 'lookup failed'}), 500


@brayn_bp.route('/brayn/held/replay', methods=['POST'])
@login_required
def replay_held():
    """
    Re-screen the held messages — the release, now that holding means "cannot be
    identified or classified".

    The operator fixes the cause first (maps the status code, points the accession
    mapping at the right field), then presses this. Each held message is parsed and
    screened again under today's configuration; those that now pass take the normal
    accept path — events, state, legacy writes, projection, quality notes — and the
    rest stay held with their finding refreshed. Optionally limited to one rule.

    rescreen=True, unlike the old acknowledge: the human's judgement here is the
    configuration change, and the machine has to confirm it actually took.
    """
    _require_access()
    body = request.json or {}
    rule = (request.form.get('rule') or body.get('rule') or '').strip()

    sql = "SELECT a.id FROM hl7_message_archive a WHERE a.brayn_status = 'quarantined'"
    params = {}
    if rule:
        sql += """ AND EXISTS (SELECT 1 FROM brayn_findings f
                                WHERE f.message_archive_id = a.id AND f.rule_code = :rule)"""
        params['rule'] = rule
    ids = [r['id'] for r in _rows(sql + " ORDER BY a.id", params)]
    if not ids:
        return jsonify({'status': 'ok', 'replayed': 0, 'released': 0, 'still_held': 0})

    try:
        from utils.hl7_replay import replay
        replay(current_app._get_current_object(), archive_ids=ids, rescreen=True)
    except Exception:
        logger.exception("BRAYN console: replay of held messages failed")
        return jsonify({'status': 'error', 'error': 'replay failed; nothing was lost, '
                                                    'the messages are still held'}), 500

    still = _scalar("SELECT COUNT(*) FROM hl7_message_archive "
                    "WHERE id = ANY(:ids) AND brayn_status = 'quarantined'", {'ids': ids})
    return jsonify({'status': 'ok', 'replayed': len(ids), 'released': len(ids) - still,
                    'still_held': still})


@brayn_bp.route('/brayn/finding/<int:finding_id>/resolve', methods=['POST'])
@login_required
def resolve(finding_id):
    """
    Close a finding. 'ignored' also stops a study note from reopening itself.

    Resolving does NOT put held data into the reports — fixing the cause and
    replaying does — said in the response so the button cannot imply a power it
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
                        'note': 'Finding closed. This does not release held messages — '
                                'fix the cause, then use "Replay held messages".'})
    except Exception:
        db.session.rollback()
        logger.exception("BRAYN console: resolve failed | id=%s", finding_id)
        return jsonify({'error': 'resolve failed'}), 500
