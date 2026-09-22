"""
routes/ray7_console.py
────────────────────────────────────────────────────────────────
The RAY7 console — the operator's window onto the screening engine.

Seventeen rules write findings into ray7_findings, and without this the only way
to read any of it was psql. An engine whose output nobody can see is an engine
nobody trusts, and a held-back queue nobody can open is worse than none at all:
studies stop reaching the reports and there is no visible reason why.

FOUR VIEWS, NOT ONE LONG LIST.
The first version rendered up to 300 findings and 100 held messages in two flat
tables. On a real feed that is thousands of rows of which perhaps five matter,
and a page that shows everything shows nothing — the reader cannot tell a
recurring known issue from today's new one.

    summary     one row per rule: how many, how bad, when last seen
    findings    the flat list, paginated, filtered
    held        studies not reaching the reports, and why
    messages    browse the archive itself

Summary is the default because the first question is always "is anything wrong",
not "show me everything". Every count on it links through to the filtered detail,
so the long list is somewhere you arrive deliberately rather than somewhere you
land.

Admin-only, gated on the existing 'can_view_etl' permission rather than a new
key: RAY7 occupies the position the ETL used to, and raw messages carry patient
identifiers.
"""
import json
import logging

from flask import Blueprint, render_template, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, user_has_page

logger = logging.getLogger("RAY7_CONSOLE")

ray7_bp = Blueprint('ray7_console', __name__)

PER_PAGE = 50


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role != 'admin' and not user_has_page(current_user, 'can_view_etl'):
        abort(403)


def _rows(sql, params=None):
    try:
        return [dict(r) for r in db.session.execute(text(sql), params or {}).mappings().all()]
    except Exception:
        logger.exception("RAY7 console query failed")
        return []


def _scalar(sql, params=None, default=0):
    try:
        v = db.session.execute(text(sql), params or {}).scalar()
        return default if v is None else v
    except Exception:
        logger.exception("RAY7 console scalar failed")
        return default


def _summary():
    out = {}
    row = _rows("""
        SELECT COUNT(*) FILTER (WHERE severity='critical' AND resolved_at IS NULL) AS open_critical,
               COUNT(*) FILTER (WHERE severity='warning'  AND resolved_at IS NULL) AS open_warning,
               COUNT(*) FILTER (WHERE severity='info'     AND resolved_at IS NULL) AS open_info,
               COUNT(*) FILTER (WHERE resolved_at IS NOT NULL)                     AS resolved
          FROM ray7_findings
    """)
    out.update(row[0] if row else {})
    row = _rows("""
        SELECT COUNT(*) AS messages,
               COUNT(*) FILTER (WHERE ray7_status='quarantined') AS held,
               COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '24 hours') AS last_24h
          FROM hl7_message_archive
    """)
    out.update(row[0] if row else {})
    out['studies'] = _scalar("SELECT COUNT(*) FROM ray7_study_state")
    # The consequence, stated as a number: studies whose data is incomplete in the
    # reports because a message about them was held back.
    out['studies_affected'] = _scalar("""
        SELECT COUNT(DISTINCT f.accession_number)
          FROM ray7_findings f
          JOIN hl7_message_archive a ON a.id = f.message_archive_id
         WHERE a.ray7_status = 'quarantined' AND f.accession_number IS NOT NULL
    """)
    return out


@ray7_bp.route('/ray7')
@login_required
def console():
    _require_access()
    view     = request.args.get('view', 'summary')
    severity = request.args.get('severity', '')
    rule     = request.args.get('rule', '')
    show     = request.args.get('show', 'open')
    search   = (request.args.get('q', '') or '').strip()
    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page = 1
    offset = (page - 1) * PER_PAGE

    ctx = {'view': view, 'severity': severity, 'rule': rule, 'show': show,
           'search': search, 'page': page, 'per_page': PER_PAGE,
           'summary': _summary(), 'rows': [], 'total': 0}

    if view == 'summary':
        # One row per rule. This is the whole point of the redesign: a reader can
        # see at a glance that UNKNOWN_AETITLE has fired 4,000 times and is
        # cosmetic, while CONTROL_ID_REUSE has fired twice and is not.
        ctx['rows'] = _rows("""
            SELECT f.rule_code, f.severity,
                   COUNT(*)                                   AS total,
                   COUNT(*) FILTER (WHERE f.resolved_at IS NULL) AS open,
                   COUNT(DISTINCT f.accession_number)          AS studies,
                   MAX(f.created_at)                           AS last_seen,
                   r.title, r.category, r.description
              FROM ray7_findings f
              LEFT JOIN ray7_rules r ON r.rule_code = f.rule_code AND r.modality = ''
             GROUP BY f.rule_code, f.severity, r.title, r.category, r.description
             ORDER BY CASE f.severity WHEN 'critical' THEN 1
                                      WHEN 'warning'  THEN 2 ELSE 3 END,
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
        if search:
            where.append("(f.accession_number ILIKE :q OR f.patient_id ILIKE :q "
                         "OR a.message_control_id ILIKE :q)")
            params['q'] = f'%{search}%'
        clause = ('WHERE ' + ' AND '.join(where)) if where else ''

        ctx['total'] = _scalar(f"""
            SELECT COUNT(*) FROM ray7_findings f
            LEFT JOIN hl7_message_archive a ON a.id = f.message_archive_id {clause}
        """, {k: v for k, v in params.items() if k not in ('lim', 'off')})

        ctx['rows'] = _rows(f"""
            SELECT f.id, f.rule_code, f.severity, f.accession_number, f.patient_id,
                   f.detail, f.created_at, f.resolved_at, f.resolution,
                   f.message_archive_id, a.message_type, a.sending_app,
                   a.message_control_id, r.title
              FROM ray7_findings f
              LEFT JOIN hl7_message_archive a ON a.id = f.message_archive_id
              LEFT JOIN ray7_rules r ON r.rule_code = f.rule_code AND r.modality = ''
              {clause}
             ORDER BY CASE f.severity WHEN 'critical' THEN 1
                                      WHEN 'warning' THEN 2 ELSE 3 END,
                      f.created_at DESC
             LIMIT :lim OFFSET :off
        """, params)

    elif view == 'held':
        # Grouped by STUDY, not by message. The operator's question is "which
        # studies are wrong in the reports", and one study can have several held
        # messages; listing messages makes them count the same study repeatedly.
        ctx['total'] = _scalar("""
            SELECT COUNT(*) FROM hl7_message_archive WHERE ray7_status = 'quarantined'
        """)
        ctx['rows'] = _rows("""
            SELECT COALESCE(f.accession_number, '(no accession)') AS accession,
                   COUNT(DISTINCT a.id)              AS messages,
                   MIN(a.received_at)                AS first_held,
                   MAX(a.received_at)                AS last_held,
                   string_agg(DISTINCT f.rule_code, ', ') AS reasons,
                   string_agg(DISTINCT a.message_type, ', ') AS types,
                   MAX(a.id)                         AS sample_archive_id,
                   BOOL_OR(s.accession_number IS NOT NULL) AS study_exists
              FROM hl7_message_archive a
              JOIN ray7_findings f ON f.message_archive_id = a.id AND f.severity = 'critical'
              LEFT JOIN ray7_study_state s ON s.accession_number = f.accession_number
             WHERE a.ray7_status = 'quarantined'
             GROUP BY COALESCE(f.accession_number, '(no accession)')
             ORDER BY MAX(a.received_at) DESC
             LIMIT :lim OFFSET :off
        """, {'lim': PER_PAGE, 'off': offset})

    elif view == 'messages':
        where, params = [], {'lim': PER_PAGE, 'off': offset}
        if search:
            where.append("(a.message_control_id ILIKE :q OR a.sending_app ILIKE :q "
                         "OR a.message_type ILIKE :q)")
            params['q'] = f'%{search}%'
        if severity:      # reused as the status filter on this view
            where.append("a.ray7_status = :st"); params['st'] = severity
        clause = ('WHERE ' + ' AND '.join(where)) if where else ''

        ctx['total'] = _scalar(f"SELECT COUNT(*) FROM hl7_message_archive a {clause}",
                               {k: v for k, v in params.items() if k not in ('lim', 'off')})
        ctx['rows'] = _rows(f"""
            SELECT a.id, a.message_control_id, a.sending_app, a.message_type,
                   a.received_at, a.ray7_status, a.ray7_severity,
                   (a.projected_at IS NOT NULL) AS projected,
                   (SELECT COUNT(*) FROM ray7_findings x WHERE x.message_archive_id = a.id)
                     AS findings
              FROM hl7_message_archive a {clause}
             ORDER BY a.id DESC LIMIT :lim OFFSET :off
        """, params)

    ctx['rule_options'] = _rows("""
        SELECT rule_code, COUNT(*) FILTER (WHERE resolved_at IS NULL) AS open
          FROM ray7_findings GROUP BY rule_code ORDER BY 1
    """)
    ctx['pages'] = max(1, -(-ctx['total'] // PER_PAGE)) if ctx['total'] else 1
    return render_template('ray7_console.html', **ctx)


@ray7_bp.route('/ray7/message/<int:archive_id>')
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
                   parse_status, parse_error, ray7_status, ray7_severity,
                   projected_at, raw_message
              FROM hl7_message_archive WHERE id = :id
        """), {'id': archive_id}).mappings().first()
        if not msg:
            return jsonify({'error': 'not found'}), 404

        findings = _rows("""
            SELECT rule_code, severity, detail, created_at, resolved_at
              FROM ray7_findings WHERE message_archive_id = :id
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
        logger.exception("RAY7 console: message detail failed | id=%s", archive_id)
        return jsonify({'error': 'lookup failed'}), 500


@ray7_bp.route('/ray7/study/<path:accession>/acknowledge', methods=['POST'])
@login_required
def acknowledge_study(accession):
    """
    Acknowledge a quarantined study and RELEASE it into the reports.

    This is the counterpart to resolve() below, and the difference is the whole
    point. Resolving closes a flag and explicitly does not move data. Acknowledging
    says "I have looked at this, the lifecycle is sound, let it through" — and a
    verdict that does not actually release the data would be a button that lies,
    because a quarantined study is invisible in every report until something
    reprojects it.

    STUDY-LEVEL, NOT MESSAGE-LEVEL. A scrambled lifecycle quarantines several
    messages of one study; acknowledging them one at a time would be busywork and
    would leave the study half-projected between clicks. One acknowledgement covers
    the accession.

    The replay runs with rescreen=False. Re-screening would judge these messages
    against a ray7_study_state that now contains the very rungs they were flagged
    for arriving after, so OUT_OF_SEQUENCE_DELIVERY would fire again and
    re-quarantine what was just cleared. The human's judgement replaces the
    machine's here; that is what acknowledging means.
    """
    _require_access()
    body = request.json or {}
    note = (request.form.get('note') or body.get('note') or '')[:2000]

    archive_ids = [
        r['message_archive_id'] for r in _rows("""
            SELECT DISTINCT f.message_archive_id
              FROM ray7_findings f
             WHERE f.accession_number = :acc
               AND f.resolved_at IS NULL
               AND f.message_archive_id IS NOT NULL
        """, {'acc': accession})
        if r.get('message_archive_id')
    ]

    try:
        db.session.execute(text("""
            UPDATE ray7_findings
               SET resolved_at = NOW(), resolved_by = :uid,
                   resolution = 'cleared',
                   resolution_note = :note
             WHERE accession_number = :acc AND resolved_at IS NULL
        """), {'acc': accession, 'uid': current_user.id,
               'note': note or 'acknowledged in the RAY7 console'})

        # 'flagged' rather than 'accepted': the message genuinely had findings and
        # that should stay visible. What changes is that it is no longer WITHHELD.
        # The audit of who released it, and when, lives on the finding rows above.
        if archive_ids:
            db.session.execute(text("""
                UPDATE hl7_message_archive
                   SET ray7_status = 'flagged'
                 WHERE id = ANY(:ids) AND ray7_status = 'quarantined'
            """), {'ids': archive_ids})
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("RAY7 console: acknowledge failed | acc=%s", accession)
        return jsonify({'error': 'acknowledge failed'}), 500

    released = 0
    if archive_ids:
        try:
            from utils.hl7_replay import replay
            stats = replay(current_app._get_current_object(),
                           archive_ids=archive_ids, rescreen=False)
            released = stats.get('projected', 0)
        except Exception:
            # The findings are already closed and committed. Say so plainly rather
            # than reporting a clean release that did not happen — the operator can
            # retry, and a silent half-success here is how a study goes missing.
            logger.exception("RAY7 console: release replay failed | acc=%s", accession)
            return jsonify({
                'status': 'partial',
                'error': 'findings were cleared but the study could not be reprojected',
                'accession': accession,
            }), 500

    return jsonify({'status': 'ok', 'accession': accession,
                    'messages_released': len(archive_ids),
                    'projected': released,
                    'note': 'Findings acknowledged and the study reprojected into the reports.'})


@ray7_bp.route('/ray7/finding/<int:finding_id>/resolve', methods=['POST'])
@login_required
def resolve(finding_id):
    """
    Close a finding.

    Resolving does NOT put the held-back data into the reports. The projection
    either happened or it did not; clearing the flag changes neither. Getting the
    data in means fixing the cause and replaying, which is what the archive
    exists for — said in the response so the button cannot imply a power it does
    not have.
    """
    _require_access()
    body = request.json or {}
    resolution = request.form.get('resolution') or body.get('resolution') or 'cleared'
    note = (request.form.get('note') or body.get('note') or '')[:2000]
    if resolution not in ('cleared', 'ignored', 'fixed_upstream'):
        return jsonify({'error': 'invalid resolution'}), 400
    try:
        db.session.execute(text("""
            UPDATE ray7_findings
               SET resolved_at = NOW(), resolved_by = :uid,
                   resolution = :res, resolution_note = :note
             WHERE id = :id AND resolved_at IS NULL
        """), {'id': finding_id, 'uid': current_user.id, 'res': resolution, 'note': note})
        db.session.commit()
        return jsonify({'status': 'ok',
                        'note': 'Finding closed. This does not add the held-back data '
                                'to the reports — fix the cause, then replay with '
                                'python app.py -m'})
    except Exception:
        db.session.rollback()
        logger.exception("RAY7 console: resolve failed | id=%s", finding_id)
        return jsonify({'error': 'resolve failed'}), 500
