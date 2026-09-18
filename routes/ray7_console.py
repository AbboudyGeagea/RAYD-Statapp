"""
routes/ray7_console.py
────────────────────────────────────────────────────────────────
The RAY7 console — the operator's window onto the screening engine.

Seventeen rules write findings into ray7_findings, and until this page existed the
only way to read any of it was psql. An engine whose output nobody can see is an
engine nobody trusts, and a quarantine queue nobody can open is worse than no
quarantine at all: data stops reaching the reports and there is no visible reason
why.

Four things this has to answer, because they are the questions actually asked when
a feed misbehaves:

    Is anything wrong right now?        the severity tiles
    What exactly is wrong?              the findings queue
    What data is being withheld?        the quarantine list
    What did the sender actually send?  the raw message, verbatim

ADMIN-ONLY, and gated on the existing 'can_view_etl' permission rather than a new
key. RAY7 occupies the position the ETL used to, the people who would have been
given ETL visibility are the same people who need this, and adding a permission
key means touching the role defaults and every existing user's grants for no gain.

Raw messages contain patient identifiers, which is the other reason this is not a
viewer-level page.
"""
import json
import logging

from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, user_has_page

logger = logging.getLogger("RAY7_CONSOLE")

ray7_bp = Blueprint('ray7_console', __name__)


def _require_access():
    if not current_user.is_authenticated:
        abort(401)
    if current_user.role != 'admin' and not user_has_page(current_user, 'can_view_etl'):
        abort(403)


def _summary():
    """The tiles. One query per concern, each hitting an index."""
    out = {}
    try:
        row = db.session.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE severity = 'critical' AND resolved_at IS NULL) AS open_critical,
              COUNT(*) FILTER (WHERE severity = 'warning'  AND resolved_at IS NULL) AS open_warning,
              COUNT(*) FILTER (WHERE severity = 'info'     AND resolved_at IS NULL) AS open_info,
              COUNT(*) FILTER (WHERE resolved_at IS NOT NULL)                       AS resolved
            FROM ray7_findings
        """)).mappings().first()
        out.update(dict(row or {}))

        row = db.session.execute(text("""
            SELECT
              COUNT(*)                                                    AS messages,
              COUNT(*) FILTER (WHERE ray7_status = 'quarantined')         AS quarantined,
              COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '24 hours') AS last_24h,
              MAX(received_at)                                            AS newest
            FROM hl7_message_archive
        """)).mappings().first()
        out.update(dict(row or {}))

        # Deliberately surfaced as a tile: an install that has received nothing is
        # the single most common "RAY7 is broken" report, and it is almost always
        # the interface, not the engine.
        out['studies'] = db.session.execute(
            text("SELECT COUNT(*) FROM ray7_study_state")).scalar()
    except Exception:
        logger.exception("RAY7 console: summary failed")
    return out


@ray7_bp.route('/ray7')
@login_required
def console():
    _require_access()
    severity = request.args.get('severity', '')
    rule     = request.args.get('rule', '')
    show     = request.args.get('show', 'open')      # open | resolved | all
    search   = (request.args.get('q', '') or '').strip()

    where, params = [], {}
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

    findings, rules, quarantine = [], [], []
    try:
        findings = [dict(r) for r in db.session.execute(text(f"""
            SELECT f.id, f.rule_code, f.severity, f.accession_number, f.patient_id,
                   f.detail, f.created_at, f.resolved_at, f.resolution,
                   f.message_archive_id,
                   a.message_type, a.sending_app, a.message_control_id,
                   r.title, r.category
              FROM ray7_findings f
              LEFT JOIN hl7_message_archive a ON a.id = f.message_archive_id
              LEFT JOIN ray7_rules r ON r.rule_code = f.rule_code AND r.modality = ''
              {clause}
             ORDER BY CASE f.severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                      f.created_at DESC
             LIMIT 300
        """), params).mappings().all()]

        rules = [dict(r) for r in db.session.execute(text("""
            SELECT f.rule_code, COUNT(*) AS n
              FROM ray7_findings f WHERE f.resolved_at IS NULL
             GROUP BY f.rule_code ORDER BY 1
        """)).mappings().all()]

        # Quarantine is the consequential list: these messages exist, parsed fine,
        # and are being deliberately withheld from the reporting tables.
        quarantine = [dict(r) for r in db.session.execute(text("""
            SELECT a.id, a.message_control_id, a.sending_app, a.message_type,
                   a.received_at, a.ray7_severity,
                   (SELECT string_agg(rule_code, ', ')
                      FROM ray7_findings x
                     WHERE x.message_archive_id = a.id AND x.severity = 'critical') AS reasons
              FROM hl7_message_archive a
             WHERE a.ray7_status = 'quarantined'
             ORDER BY a.received_at DESC LIMIT 100
        """)).mappings().all()]
    except Exception:
        logger.exception("RAY7 console: query failed")

    return render_template(
        'ray7_console.html',
        summary=_summary(), findings=findings, rule_counts=rules,
        quarantine=quarantine, severity=severity, rule=rule, show=show, search=search,
    )


@ray7_bp.route('/ray7/message/<int:archive_id>')
@login_required
def message_detail(archive_id):
    """
    The raw message, verbatim, plus every finding against it.

    Verbatim matters: when a field position turns out to be wrong — and several in
    the status parser are still educated guesses — this is where you confirm what
    the sender actually put where, without asking them to resend anything.
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

        findings = [dict(r) for r in db.session.execute(text("""
            SELECT rule_code, severity, detail, created_at, resolved_at
              FROM ray7_findings WHERE message_archive_id = :id
             ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
        """), {'id': archive_id}).mappings().all()]

        out = dict(msg)
        # Split on CR so each HL7 segment is its own line in the viewer. The wire
        # format is carriage-return delimited, which a browser renders as one
        # unreadable line.
        out['segments'] = [s for s in (out.pop('raw_message') or '')
                           .replace('\r\n', '\r').replace('\n', '\r').split('\r') if s]
        out['findings'] = findings
        return jsonify(json.loads(json.dumps(out, default=str)))
    except Exception:
        logger.exception("RAY7 console: message detail failed | id=%s", archive_id)
        return jsonify({'error': 'lookup failed'}), 500


@ray7_bp.route('/ray7/finding/<int:finding_id>/resolve', methods=['POST'])
@login_required
def resolve(finding_id):
    """
    Close a finding.

    Resolving does NOT re-admit a quarantined message's data on its own — the
    projection already happened or did not. Re-admitting means fixing the cause
    (map the status code, correct the field position) and replaying, which is what
    the archive exists for. Saying so in the response keeps the button from
    implying a power it does not have.
    """
    _require_access()
    resolution = (request.form.get('resolution') or
                  (request.json or {}).get('resolution') or 'cleared')
    note = (request.form.get('note') or (request.json or {}).get('note') or '')[:2000]
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
        return jsonify({
            'status': 'ok',
            'note': 'Finding closed. To re-admit withheld data, fix the cause and '
                    'replay: python app.py -m',
        })
    except Exception:
        db.session.rollback()
        logger.exception("RAY7 console: resolve failed | id=%s", finding_id)
        return jsonify({'error': 'resolve failed'}), 500
