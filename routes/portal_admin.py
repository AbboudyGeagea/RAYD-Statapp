"""
routes/portal_admin.py
----------------------
Admin management for the patient portal:
  - /admin/portal          → user list, search, status toggle
  - /admin/portal/reset/<id> → reset password + resend WhatsApp
  - /admin/portal/config   → edit portal_config settings

Wire up in registry.py:
    from routes.portal_admin import portal_admin_bp
    app.register_blueprint(portal_admin_bp)
"""

import re
import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, user_has_page
from routes.portal_bp import (_generate_password, _send_whatsapp, _get_config,
                              PORTAL_ENCRYPTED_KEYS, viewer_link_ttl)

logger = logging.getLogger("PORTAL_ADMIN")
portal_admin_bp = Blueprint("portal_admin", __name__)


def _require_portal_access():
    """Abort 403 for demo users (always) and non-permitted non-admins."""
    from sqlalchemy import text as _t
    demo_row = db.session.execute(
        _t("SELECT value FROM settings WHERE key = 'demo_user'")
    ).fetchone()
    demo_username = (demo_row[0] or '').strip() if demo_row else ''
    if demo_username and current_user.username == demo_username:
        abort(403)
    if current_user.role != 'admin' and not user_has_page(current_user, 'patient_portal'):
        abort(403)


@portal_admin_bp.route("/admin/portal")
@login_required
def portal_users():
    _require_portal_access()
    """Patient portal user list with search and filters."""
    search  = request.args.get("q", "").strip()
    status  = request.args.get("status", "all")
    page    = max(request.args.get("page", 1, type=int), 1)
    per_page = 50

    filters = ["1=1"]
    params  = {}

    if search:
        filters.append("(mrn ILIKE :q OR full_name ILIKE :q OR phone ILIKE :q OR accession_number ILIKE :q)")
        params["q"] = f"%{search}%"

    if status == "active":
        filters.append("is_active = TRUE")
    elif status == "inactive":
        filters.append("is_active = FALSE")
    elif status == "no_whatsapp":
        filters.append("(whatsapp_sent = FALSE OR whatsapp_sent IS NULL)")

    where = " AND ".join(filters)
    offset = (page - 1) * per_page

    users = db.session.execute(text(f"""
        SELECT id, mrn, full_name, phone, accession_number,
               username, is_active,
               last_login, whatsapp_sent, whatsapp_sent_at, created_at
        FROM patient_portal_users
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT :lim OFFSET :off
    """), {**params, "lim": per_page, "off": offset}).mappings().fetchall()

    total = db.session.execute(
        text(f"SELECT COUNT(*) FROM patient_portal_users WHERE {where}"), params
    ).scalar()

    stats = db.session.execute(text("""
        SELECT
            COUNT(*)                                          AS total,
            COUNT(*) FILTER (WHERE is_active = TRUE)         AS active,
            COUNT(*) FILTER (WHERE whatsapp_sent = TRUE)     AS sent,
            COUNT(*) FILTER (WHERE last_login IS NOT NULL)   AS logged_in
        FROM patient_portal_users
    """)).mappings().fetchone()

    return render_template("portal_admin.html",
                           users=users,
                           stats=stats,
                           search=search,
                           status_filter=status,
                           page=page,
                           per_page=per_page,
                           total=total,
                           total_pages=(total + per_page - 1) // per_page)


@portal_admin_bp.route("/admin/portal/reset/<int:user_id>", methods=["POST"])
@login_required
def reset_password(user_id):
    _require_portal_access()
    """Generate new password, update DB, resend WhatsApp."""
    row = db.session.execute(
        text("SELECT * FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).mappings().fetchone()

    if not row:
        return jsonify({"success": False, "error": "User not found"}), 404

    new_password = _generate_password()
    config       = _get_config()

    from werkzeug.security import generate_password_hash
    new_hash = generate_password_hash(new_password, method='pbkdf2:sha256')

    db.session.execute(text("""
        UPDATE patient_portal_users
        SET password_hash  = :pwd_hash,
            whatsapp_sent  = FALSE,
            updated_at     = NOW()
        WHERE id = :id
    """), {"pwd_hash": new_hash, "id": user_id})
    db.session.commit()

    # Resend WhatsApp
    wa_success = False
    wa_error   = None
    if row['phone']:
        wa_success, wa_error = _send_whatsapp(
            row['phone'], row['mrn'], new_password,
            row['accession_number'], config
        )
        if wa_success:
            db.session.execute(text("""
                UPDATE patient_portal_users
                SET whatsapp_sent = TRUE, whatsapp_sent_at = NOW()
                WHERE id = :id
            """), {"id": user_id})
            db.session.commit()

    return jsonify({
        "success":      True,
        "new_password": new_password,
        "whatsapp_sent": wa_success,
        "whatsapp_error": wa_error
    })


@portal_admin_bp.route("/admin/portal/toggle/<int:user_id>", methods=["POST"])
@login_required
def toggle_user(user_id):
    _require_portal_access()
    """Activate or deactivate a portal user."""
    row = db.session.execute(
        text("SELECT is_active FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).fetchone()

    if not row:
        return jsonify({"success": False}), 404

    new_status = not row[0]
    db.session.execute(
        text("UPDATE patient_portal_users SET is_active = :s, updated_at = NOW() WHERE id = :id"),
        {"s": new_status, "id": user_id}
    )
    db.session.commit()
    return jsonify({"success": True, "is_active": new_status})


@portal_admin_bp.route("/admin/portal/resend/<int:user_id>", methods=["POST"])
@login_required
def resend_whatsapp(user_id):
    _require_portal_access()
    """Resend WhatsApp with existing password."""
    row = db.session.execute(
        text("SELECT * FROM patient_portal_users WHERE id = :id"),
        {"id": user_id}
    ).mappings().fetchone()

    if not row or not row['phone']:
        return jsonify({"success": False, "error": "No phone number on file"}), 400

    config = _get_config()
    # Re-send requires a new password since we never store plaintext.
    new_password = _generate_password()
    from werkzeug.security import generate_password_hash
    db.session.execute(text("""
        UPDATE patient_portal_users
        SET password_hash = :h, updated_at = NOW() WHERE id = :id
    """), {"h": generate_password_hash(new_password, method='pbkdf2:sha256'), "id": user_id})
    db.session.commit()
    success, error = _send_whatsapp(
        row['phone'], row['mrn'], new_password,
        row['accession_number'], config
    )

    if success:
        db.session.execute(text("""
            UPDATE patient_portal_users
            SET whatsapp_sent = TRUE, whatsapp_sent_at = NOW()
            WHERE id = :id
        """), {"id": user_id})
        db.session.commit()

    return jsonify({"success": success, "error": error})

# ── ADD THIS ROUTE to routes/portal_admin.py ──────────────────────────────
# Place it anywhere after the existing imports, before or after portal_config()

@portal_admin_bp.route("/admin/portal/test-whatsapp", methods=["POST"])
@login_required
def test_whatsapp():
    _require_portal_access()
    """Send a test WhatsApp message to verify Twilio credentials."""
    data   = request.get_json()
    phone  = (data or {}).get("phone", "").strip()
    if not phone:
        return jsonify({"success": False, "error": "No phone number provided"})

    config = _get_config()
    success, error = _send_whatsapp(
        phone=phone,
        mrn="TEST",
        password=_generate_password(),
        accession="TEST-001",
        config=config
    )
    return jsonify({"success": success, "error": error})

_PORTAL_CONFIG_ALLOWED_KEYS = frozenset({
    'hospital_name', 'hospital_logo_url',
    'viewer_url_template', 'viewer_signing_secret', 'viewer_link_ttl_seconds',
    'twilio_account_sid', 'twilio_auth_token', 'twilio_whatsapp_from',
    'portal_base_url',
    'whatsapp_message_template', 'whatsapp_message_template_ar',
})
_VIEWER_TEMPLATE_REQUIRED = ('{accession}', '{expires}', '{signature}')


def _config_errors(values):
    """Reasons the submitted settings cannot be saved (empty list = fine)."""
    errors = []
    tpl = values.get('viewer_url_template', '')
    if tpl:
        if not re.match(r'^https?://', tpl, re.I):
            errors.append("Viewer link template must start with https:// (or http://).")
        missing = [p for p in _VIEWER_TEMPLATE_REQUIRED if p not in tpl]
        if missing:
            errors.append("Viewer link template must contain " + ", ".join(missing)
                          + " — an unsigned link would let patients open any study.")
    secret = values.get('viewer_signing_secret', '')
    if secret and len(secret) < 32:
        errors.append("Viewer signing secret must be at least 32 characters.")
    ttl = values.get('viewer_link_ttl_seconds', '')
    if ttl and not (ttl.isdigit() and 30 <= int(ttl) <= 3600):
        errors.append("Viewer link lifetime must be between 30 and 3600 seconds.")
    for key in ('portal_base_url', 'hospital_logo_url'):
        v = values.get(key, '')
        if v and not (re.match(r'^https?://', v, re.I) or (key == 'hospital_logo_url' and v.startswith('/static/'))):
            errors.append(f"{key.replace('_', ' ').capitalize()} must be an http(s) address.")
    return errors


@portal_admin_bp.route("/admin/portal/config", methods=["GET", "POST"])
@login_required
def portal_config():
    _require_portal_access()
    """View and edit portal configuration."""
    if request.method == "POST":
        from utils.crypto import encrypt as _enc
        values = {
            key[4:]: value.strip()
            for key, value in request.form.items()
            if key.startswith("cfg_") and key[4:] in _PORTAL_CONFIG_ALLOWED_KEYS  # mass-assignment guard
        }
        errors = _config_errors(values)
        if errors:
            for e in errors:
                flash(e, "danger")
            return redirect(url_for("portal_admin.portal_config"))

        for config_key, val in values.items():
            if config_key in PORTAL_ENCRYPTED_KEYS:
                if not val:
                    continue  # blank secret field = keep the stored value
                val = _enc(val)
            db.session.execute(text("""
                INSERT INTO portal_config (config_key, config_value, updated_at)
                VALUES (:k, :v, NOW())
                ON CONFLICT (config_key) DO UPDATE
                SET config_value = EXCLUDED.config_value, updated_at = NOW()
            """), {"v": val, "k": config_key})
        db.session.commit()
        flash("Configuration saved.", "success")
        return redirect(url_for("portal_admin.portal_config"))

    rows = db.session.execute(
        text("SELECT config_key, config_value FROM portal_config")
    ).fetchall()
    # Secrets never go back to the browser: the page only learns whether one is set.
    cfg = {k: ('' if k in PORTAL_ENCRYPTED_KEYS else (v or '')) for k, v in rows}
    secret_set = {k: bool(v) for k, v in rows if k in PORTAL_ENCRYPTED_KEYS}
    cfg['viewer_link_ttl_seconds'] = str(viewer_link_ttl(cfg))
    return render_template("portal_config.html", cfg=cfg, secret_set=secret_set)
