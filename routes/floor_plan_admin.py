"""
routes/floor_plan_admin.py
───────────────────────────────────────────────────────────────
Admin tool for placing devices (AE titles) on a per-site 2D floor-plan
image — the position data Live AE Status's spatial redesign renders
against (see migrations/0071_add_device_floor_positions.sql).

Admin-only (not license-gated — a config tool, like DB Manager).
Positions are stored as percentages (0.00-100.00) of the canvas, so
placement is independent of the uploaded image's actual resolution.

Registered in registry.py:
    from routes.floor_plan_admin import floor_plan_bp
    app.register_blueprint(floor_plan_bp)
"""
import os
import logging
from flask import Blueprint, render_template, request, jsonify, current_app, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

logger = logging.getLogger("FLOOR_PLAN_ADMIN")
floor_plan_bp = Blueprint('floor_plan_admin', __name__, url_prefix='/admin/floor-plan')

_ALLOWED_EXT = {'png', 'jpg', 'jpeg', 'webp'}
_UPLOAD_SUBDIR = 'floor_plans'


def _upload_dir():
    d = os.path.join(current_app.static_folder, _UPLOAD_SUBDIR)
    os.makedirs(d, exist_ok=True)
    return d


@floor_plan_bp.route('/')
@login_required
def floor_plan_page():
    if current_user.role != 'admin':
        abort(403)
    sites = db.session.execute(text(
        "SELECT id, code, name FROM sites WHERE active ORDER BY code"
    )).mappings().fetchall()
    return render_template('floor_plan_admin.html', sites=sites)


@floor_plan_bp.route('/api/state')
@login_required
def api_state():
    if current_user.role != 'admin':
        abort(403)
    site_id = request.args.get('site_id', type=int)
    if not site_id:
        return jsonify({"error": "site_id required"}), 400
    try:
        plan = db.session.execute(text(
            "SELECT image_path, image_width, image_height FROM floor_plans WHERE site_id = :sid"
        ), {"sid": site_id}).mappings().fetchone()
        devices = db.session.execute(text("""
            SELECT aetitle, modality, COALESCE(display_aetitle, aetitle) AS label,
                   floor_x, floor_y
            FROM aetitle_modality_map
            WHERE site_id = :sid
            ORDER BY modality, aetitle
        """), {"sid": site_id}).mappings().fetchall()
        return jsonify({
            "plan": dict(plan) if plan else None,
            "devices": [dict(d) for d in devices],
        })
    except Exception as e:
        db.session.rollback()
        logger.error(f"Floor plan state error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@floor_plan_bp.route('/api/upload', methods=['POST'])
@login_required
def api_upload():
    if current_user.role != 'admin':
        abort(403)
    site_id = request.form.get('site_id', type=int)
    file = request.files.get('image')
    # width/height computed client-side (Image.onload) — no server-side image
    # library dependency needed, positions are percentage-based anyway.
    width = request.form.get('width', type=int)
    height = request.form.get('height', type=int)
    if not site_id or not file or not file.filename:
        return jsonify({"error": "site_id and image are required"}), 400
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in _ALLOWED_EXT:
        return jsonify({"error": f"Unsupported file type: .{ext}"}), 400

    try:
        filename = f"site_{site_id}.{ext}"
        file.save(os.path.join(_upload_dir(), filename))
        rel_path = f"{_UPLOAD_SUBDIR}/{filename}"

        db.session.execute(text("""
            INSERT INTO floor_plans (site_id, image_path, image_width, image_height, uploaded_by, uploaded_at)
            VALUES (:sid, :path, :w, :h, :uid, NOW())
            ON CONFLICT (site_id) DO UPDATE
                SET image_path = EXCLUDED.image_path, image_width = EXCLUDED.image_width,
                    image_height = EXCLUDED.image_height, uploaded_by = EXCLUDED.uploaded_by,
                    uploaded_at = NOW()
        """), {"sid": site_id, "path": rel_path, "w": width, "h": height, "uid": current_user.id})
        db.session.commit()
        return jsonify({"ok": True, "image_path": rel_path})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Floor plan upload error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@floor_plan_bp.route('/api/position', methods=['POST'])
@login_required
def api_position():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    aetitle = (data.get('aetitle') or '').strip()
    x, y = data.get('x'), data.get('y')
    if not aetitle or x is None or y is None:
        return jsonify({"error": "aetitle, x, y are required"}), 400
    try:
        x = max(0.0, min(100.0, float(x)))
        y = max(0.0, min(100.0, float(y)))
    except (TypeError, ValueError):
        return jsonify({"error": "x/y must be numeric"}), 400
    try:
        db.session.execute(text("""
            UPDATE aetitle_modality_map
            SET floor_x = :x, floor_y = :y, floor_positioned_by = :uid, floor_positioned_at = NOW()
            WHERE aetitle = :aetitle
        """), {"x": x, "y": y, "uid": current_user.id, "aetitle": aetitle})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Floor plan position error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@floor_plan_bp.route('/api/unplace', methods=['POST'])
@login_required
def api_unplace():
    if current_user.role != 'admin':
        abort(403)
    data = request.get_json(force=True) or {}
    aetitle = (data.get('aetitle') or '').strip()
    if not aetitle:
        return jsonify({"error": "aetitle is required"}), 400
    try:
        db.session.execute(text("""
            UPDATE aetitle_modality_map
            SET floor_x = NULL, floor_y = NULL, floor_positioned_by = NULL, floor_positioned_at = NULL
            WHERE aetitle = :aetitle
        """), {"aetitle": aetitle})
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Floor plan unplace error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
