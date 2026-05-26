from flask import Blueprint, jsonify
from sqlalchemy import text

health_bp = Blueprint("health", __name__)


@health_bp.route("/health")
def health():
    return jsonify({"status": "ok", "service": "rayd-statapp"})


@health_bp.route("/readiness")
def readiness():
    from db import db
    checks = {}
    all_ready = True

    # DB check
    try:
        db.session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
        all_ready = False

    # NLP worker check (indirect — last hl7_oru_analysis row)
    try:
        row = db.session.execute(text(
            "SELECT analyzed_at FROM hl7_oru_analysis ORDER BY analyzed_at DESC LIMIT 1"
        )).fetchone()
        if row is None:
            checks["nlp_worker"] = "unknown"
        else:
            from datetime import datetime, timezone
            age_sec = (datetime.now(timezone.utc).replace(tzinfo=None) - row[0]).total_seconds()
            if age_sec < 86400:  # within last 24h
                checks["nlp_worker"] = "ok"
            else:
                checks["nlp_worker"] = f"stale: last_analyzed_at={row[0].isoformat()}"
    except Exception as e:
        checks["nlp_worker"] = f"error: {e}"

    status_code = 200 if all_ready else 503
    return jsonify({
        "status": "ready" if all_ready else "not_ready",
        "checks": checks,
    }), status_code
