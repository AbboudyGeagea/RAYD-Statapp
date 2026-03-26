"""
routes/preferences.py
─────────────────────
User preference endpoints:
  POST /user/theme      — toggle dark/light, persisted to users.ui_theme
  POST /user/favorite   — add/remove a report from users.favorites (JSON array)
  GET  /user/favorites  — return current favorites list
"""

import json
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from db import db, User

preferences_bp = Blueprint("preferences", __name__)


@preferences_bp.route("/user/theme", methods=["POST"])
@login_required
def set_theme():
    data  = request.get_json(force=True)
    theme = data.get("theme", "dark")
    if theme not in ("dark", "light"):
        return jsonify({"error": "invalid theme"}), 400
    user = db.session.get(User, current_user.id)
    user.ui_theme = theme
    db.session.commit()
    return jsonify({"ok": True, "theme": theme})


@preferences_bp.route("/user/favorite", methods=["POST"])
@login_required
def toggle_favorite():
    data      = request.get_json(force=True)
    report_id = int(data.get("report_id", 0))
    if not report_id:
        return jsonify({"error": "report_id required"}), 400

    user = db.session.get(User, current_user.id)
    favs = json.loads(user.favorites or "[]")

    if report_id in favs:
        favs.remove(report_id)
        action = "removed"
    else:
        favs.append(report_id)
        action = "added"

    user.favorites = json.dumps(favs)
    db.session.commit()
    return jsonify({"ok": True, "action": action, "favorites": favs})


@preferences_bp.route("/user/favorites", methods=["GET"])
@login_required
def get_favorites():
    user = db.session.get(User, current_user.id)
    favs = json.loads(user.favorites or "[]")
    return jsonify({"favorites": favs})
