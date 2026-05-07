"""
routes/custom_reports.py — Custom Report Composer
"""

import json
import logging
from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify, abort, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db
from utils.permissions import resolve_permission
from routes.report_widgets import WIDGET_CATALOGUE, FINANCIAL_KEYS, run_widget
from routes.report_cache import get_filter_options

custom_reports_bp = Blueprint("custom_reports", __name__)
logger = logging.getLogger("CUSTOM_REPORTS")

PRIMARY_CONN = "oracle_PACS"


# ── Guards ────────────────────────────────────────────────────────────────────

def _can_access():
    return current_user.is_authenticated and current_user.role in ("admin", "viewer", "viewer2")


def _can_finance():
    return resolve_permission(current_user, "can_view_finance")


def _ensure_tables():
    """Idempotent — creates tables if migration hasn't run yet."""
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS custom_reports (
            id            SERIAL PRIMARY KEY,
            title         TEXT NOT NULL,
            created_by    INTEGER REFERENCES users(id) ON DELETE SET NULL,
            visibility    VARCHAR(20) DEFAULT 'shared',
            has_financial BOOLEAN DEFAULT FALSE,
            filters_json  JSONB DEFAULT '{}',
            created_at    TIMESTAMP DEFAULT NOW(),
            updated_at    TIMESTAMP DEFAULT NOW()
        )
    """))
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS custom_report_sections (
            id           SERIAL PRIMARY KEY,
            report_id    INTEGER NOT NULL REFERENCES custom_reports(id) ON DELETE CASCADE,
            section_type VARCHAR(50) NOT NULL,
            position     INTEGER NOT NULL DEFAULT 0,
            config_json  JSONB DEFAULT '{}'
        )
    """))
    db.session.commit()


def _available_sources():
    """
    Return list of available PG data sources (excludes oracle connections).
    Always includes the main etl_db. Adds provisioned system-type DBs and
    any non-oracle db_params entries.
    """
    sources = [{"name": "etl_db", "label": "Main DB (etl_db)", "type": "postgres"}]

    # Provisioned system-type databases
    try:
        rows = db.session.execute(text(
            "SELECT system_type, db_name FROM system_type_databases WHERE is_active = TRUE ORDER BY system_type"
        )).fetchall()
        for r in rows:
            sources.append({"name": r.db_name, "label": f"{r.system_type} ({r.db_name})", "type": "postgres"})
    except Exception:
        pass

    # Non-oracle connections from db_params
    try:
        rows = db.session.execute(text("""
            SELECT name, db_type FROM db_params
            WHERE db_type NOT ILIKE '%oracle%'
              AND name != :primary
            ORDER BY name
        """), {"primary": PRIMARY_CONN}).fetchall()
        for r in rows:
            sources.append({"name": r.name, "label": f"{r.name} ({r.db_type})", "type": r.db_type})
    except Exception:
        pass

    return sources


def _visible_report_or_404(report_id):
    """Load a report the current user is allowed to see."""
    row = db.session.execute(
        text("SELECT * FROM custom_reports WHERE id = :id"),
        {"id": report_id}
    ).mappings().fetchone()
    if not row:
        abort(404)
    row = dict(row)
    # Finance-restricted reports require can_view_finance
    if row.get("has_financial") and not _can_finance():
        abort(403)
    # Private reports (future): only owner or admin
    return row


def _editable_or_403(row):
    if current_user.role == "admin":
        return
    if row.get("created_by") != current_user.id:
        abort(403)


# ── List ──────────────────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom")
@login_required
def report_list():
    if not _can_access():
        abort(403)
    _ensure_tables()

    can_finance = _can_finance()

    rows = db.session.execute(text("""
        SELECT r.id, r.title, r.visibility, r.has_financial,
               r.created_at, r.updated_at,
               u.username AS creator,
               (SELECT COUNT(*) FROM custom_report_sections s WHERE s.report_id = r.id) AS section_count
        FROM custom_reports r
        LEFT JOIN users u ON u.id = r.created_by
        WHERE r.visibility = 'shared'
           OR r.created_by = :uid
           OR :is_admin
        ORDER BY r.updated_at DESC
    """), {"uid": current_user.id, "is_admin": current_user.role == "admin"}).mappings().fetchall()

    reports = [dict(r) for r in rows if not (r["has_financial"] and not can_finance)]

    return render_template(
        "custom_reports_list.html",
        reports=reports,
        can_finance=can_finance,
    )


# ── Composer (new) ────────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/new")
@login_required
def composer_new():
    if not _can_access():
        abort(403)
    _ensure_tables()

    can_finance = _can_finance()
    catalogue   = [w for w in WIDGET_CATALOGUE if not w["financial"] or can_finance]
    sources     = _available_sources()
    fopts       = get_filter_options(db)

    today      = date.today()
    date_from  = str(today.replace(day=1))
    date_to    = str(today)

    return render_template(
        "custom_reports_composer.html",
        report=None,
        sections=[],
        catalogue=catalogue,
        sources=sources,
        filter_options=fopts,
        can_finance=can_finance,
        default_date_from=date_from,
        default_date_to=date_to,
    )


# ── Composer (edit) ───────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/<int:report_id>/edit")
@login_required
def composer_edit(report_id):
    if not _can_access():
        abort(403)
    _ensure_tables()

    report      = _visible_report_or_404(report_id)
    _editable_or_403(report)
    can_finance = _can_finance()
    catalogue   = [w for w in WIDGET_CATALOGUE if not w["financial"] or can_finance]
    sources     = _available_sources()
    fopts       = get_filter_options(db)

    sec_rows = db.session.execute(text("""
        SELECT id, section_type, position, config_json
        FROM custom_report_sections
        WHERE report_id = :rid ORDER BY position
    """), {"rid": report_id}).mappings().fetchall()
    sections = [dict(s) for s in sec_rows]

    return render_template(
        "custom_reports_composer.html",
        report=report,
        sections=sections,
        catalogue=catalogue,
        sources=sources,
        filter_options=fopts,
        can_finance=can_finance,
        default_date_from=report["filters_json"].get("date_from", ""),
        default_date_to=report["filters_json"].get("date_to", ""),
    )


# ── Save (create or update) ───────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/save", methods=["POST"])
@login_required
def save_report():
    if not _can_access():
        abort(403)
    _ensure_tables()

    data       = request.get_json() or {}
    report_id  = data.get("id")
    title      = (data.get("title") or "").strip()
    filters    = data.get("filters") or {}
    sections   = data.get("sections") or []   # [{type, config}]

    if not title:
        return jsonify({"error": "Title is required"}), 400

    # Determine if any financial widget is used
    section_types = [s.get("type") for s in sections]
    has_financial = any(t in FINANCIAL_KEYS for t in section_types)

    # Finance widgets need the permission
    if has_financial and not _can_finance():
        return jsonify({"error": "You do not have permission to use financial widgets"}), 403

    visibility = "restricted" if has_financial else "shared"

    if report_id:
        # Update
        existing = db.session.execute(
            text("SELECT created_by FROM custom_reports WHERE id = :id"),
            {"id": report_id}
        ).fetchone()
        if not existing:
            return jsonify({"error": "Report not found"}), 404
        if current_user.role != "admin" and existing[0] != current_user.id:
            return jsonify({"error": "Not authorised to edit this report"}), 403

        db.session.execute(text("""
            UPDATE custom_reports
            SET title=:title, filters_json=:fj, has_financial=:hf,
                visibility=:vis, updated_at=NOW()
            WHERE id=:id
        """), {
            "title": title, "fj": json.dumps(filters),
            "hf": has_financial, "vis": visibility, "id": report_id
        })
        db.session.execute(
            text("DELETE FROM custom_report_sections WHERE report_id = :id"),
            {"id": report_id}
        )
    else:
        row = db.session.execute(text("""
            INSERT INTO custom_reports (title, created_by, visibility, has_financial, filters_json)
            VALUES (:title, :uid, :vis, :hf, :fj)
            RETURNING id
        """), {
            "title": title, "uid": current_user.id,
            "vis": visibility, "hf": has_financial,
            "fj": json.dumps(filters)
        }).fetchone()
        report_id = row[0]

    for pos, sec in enumerate(sections):
        sec_type = sec.get("type", "")
        if sec_type not in {w["key"] for w in WIDGET_CATALOGUE}:
            continue
        if sec_type in FINANCIAL_KEYS and not _can_finance():
            continue
        db.session.execute(text("""
            INSERT INTO custom_report_sections (report_id, section_type, position, config_json)
            VALUES (:rid, :st, :pos, :cj)
        """), {
            "rid": report_id, "st": sec_type,
            "pos": pos, "cj": json.dumps(sec.get("config") or {})
        })

    db.session.commit()
    return jsonify({"ok": True, "id": report_id})


# ── View ──────────────────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/<int:report_id>")
@login_required
def view_report(report_id):
    if not _can_access():
        abort(403)
    _ensure_tables()

    report = _visible_report_or_404(report_id)
    fopts  = get_filter_options(db)

    sec_rows = db.session.execute(text("""
        SELECT id, section_type, position, config_json
        FROM custom_report_sections
        WHERE report_id = :rid ORDER BY position
    """), {"rid": report_id}).mappings().fetchall()
    sections = [dict(s) for s in sec_rows]

    return render_template(
        "custom_reports_view.html",
        report=report,
        sections=sections,
        filter_options=fopts,
        can_finance=_can_finance(),
    )


# ── Data endpoint ─────────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/<int:report_id>/data")
@login_required
def report_data(report_id):
    if not _can_access():
        abort(403)

    report = _visible_report_or_404(report_id)

    # Build filters — URL params override saved defaults
    saved = report.get("filters_json") or {}
    today = date.today()
    filters = {
        "date_from":    request.args.get("date_from")    or saved.get("date_from") or str(today.replace(day=1)),
        "date_to":      request.args.get("date_to")      or saved.get("date_to")   or str(today),
        "modality":     request.args.get("modality")     or saved.get("modality")  or None,
        "physician_id": request.args.get("physician_id") or saved.get("physician_id") or None,
        "patient_class":request.args.get("patient_class")or saved.get("patient_class") or None,
    }

    sec_rows = db.session.execute(text("""
        SELECT id, section_type, position, config_json
        FROM custom_report_sections
        WHERE report_id = :rid ORDER BY position
    """), {"rid": report_id}).mappings().fetchall()

    results = []
    for sec in sec_rows:
        sec_type = sec["section_type"]
        # Skip financial sections if user lacks permission
        if sec_type in FINANCIAL_KEYS and not _can_finance():
            continue
        try:
            data = run_widget(db, sec_type, filters, sec["config_json"] or {})
            results.append({
                "id":       sec["id"],
                "type":     sec_type,
                "position": sec["position"],
                "config":   sec["config_json"] or {},
                "data":     data,
            })
        except Exception as e:
            logger.error(f"Widget {sec_type} failed: {e}", exc_info=True)
            results.append({
                "id":       sec["id"],
                "type":     sec_type,
                "position": sec["position"],
                "config":   sec["config_json"] or {},
                "error":    str(e),
            })

    return jsonify({"ok": True, "sections": results, "filters": filters})


# ── Delete ────────────────────────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/<int:report_id>/delete", methods=["POST"])
@login_required
def delete_report(report_id):
    if not _can_access():
        abort(403)

    row = db.session.execute(
        text("SELECT created_by FROM custom_reports WHERE id = :id"), {"id": report_id}
    ).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    if current_user.role != "admin" and row[0] != current_user.id:
        return jsonify({"error": "Not authorised"}), 403

    db.session.execute(text("DELETE FROM custom_reports WHERE id = :id"), {"id": report_id})
    db.session.commit()
    return jsonify({"ok": True})


# ── Available sources endpoint ────────────────────────────────────────────────

@custom_reports_bp.route("/reports/custom/sources")
@login_required
def available_sources():
    return jsonify({"ok": True, "sources": _available_sources()})
