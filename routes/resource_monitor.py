"""
routes/resource_monitor.py
──────────────────────────
Resource Monitor — admin-only dashboard that surfaces query audit log data.

Endpoints:
  GET /admin/resource-monitor          → renders resource_monitor.html
  GET /admin/resource-monitor/data     → JSON for charts and metric cards
  GET /admin/resource-monitor/row/<id> → JSON for per-row detail (filters + queries)
"""
import json
import logging
from flask import Blueprint, render_template, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db

logger = logging.getLogger("RESOURCE_MONITOR")

resource_monitor_bp = Blueprint("resource_monitor", __name__)


@resource_monitor_bp.route("/admin/resource-monitor")
@login_required
def resource_monitor_page():
    if current_user.role != "admin":
        abort(403)
    return render_template("resource_monitor.html")


@resource_monitor_bp.route("/admin/resource-monitor/data")
@login_required
def resource_monitor_data():
    if current_user.role != "admin":
        abort(403)

    try:
        # ── Summary cards ─────────────────────────────────────────────
        summary_row = db.session.execute(text("""
            SELECT
              COUNT(*)::int                                              AS total_requests,
              ROUND(AVG(total_ms)::numeric, 0)::int                     AS avg_ms,
              ROUND(AVG(CASE WHEN cache_hit THEN 1.0 ELSE 0.0 END)*100, 1) AS cache_hit_pct,
              MAX(total_ms)::int                                         AS max_ms,
              (SELECT endpoint FROM query_audit_log
               WHERE captured_at > NOW()-INTERVAL '10 days'
               ORDER BY total_ms DESC LIMIT 1)                          AS slowest_endpoint
            FROM query_audit_log
            WHERE captured_at > NOW() - INTERVAL '10 days'
              AND http_status < 400
        """)).fetchone()

        summary = {
            "total_requests":   summary_row[0] or 0,
            "avg_ms":           summary_row[1] or 0,
            "cache_hit_pct":    float(summary_row[2] or 0),
            "max_ms":           summary_row[3] or 0,
            "slowest_endpoint": summary_row[4] or "-",
        }

        # ── 10-day trend per endpoint ─────────────────────────────────
        trend_rows = db.session.execute(text("""
            SELECT
              DATE(captured_at AT TIME ZONE 'Asia/Beirut') AS day,
              endpoint,
              ROUND(AVG(total_ms)::numeric, 0)::int AS avg_ms,
              COUNT(*)::int AS requests
            FROM query_audit_log
            WHERE captured_at > NOW() - INTERVAL '10 days'
              AND http_status < 400
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """)).fetchall()

        # Collect top-6 endpoints by total request count
        ep_counts: dict = {}
        for row in trend_rows:
            ep_counts[row[1]] = ep_counts.get(row[1], 0) + row[3]
        top6 = set(sorted(ep_counts, key=lambda e: ep_counts[e], reverse=True)[:6])

        trend = []
        for row in trend_rows:
            ep = row[1] if row[1] in top6 else "Other"
            trend.append({
                "day":      row[0].isoformat() if row[0] else None,
                "endpoint": ep,
                "avg_ms":   row[2],
                "requests": row[3],
            })

        # ── Top 10 slowest requests ───────────────────────────────────
        slow_rows = db.session.execute(text("""
            SELECT id, captured_at AT TIME ZONE 'Asia/Beirut' AS ts,
                   endpoint, total_ms, query_count, username, cache_hit, http_status
            FROM query_audit_log
            WHERE captured_at > NOW() - INTERVAL '10 days'
            ORDER BY total_ms DESC
            LIMIT 10
        """)).fetchall()

        slowest = []
        for row in slow_rows:
            slowest.append({
                "id":          row[0],
                "ts":          row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else None,
                "endpoint":    row[2],
                "total_ms":    float(row[3]),
                "query_count": row[4],
                "username":    row[5] or "—",
                "cache_hit":   row[6],
                "http_status": row[7],
            })

        return jsonify({
            "summary": summary,
            "trend":   trend,
            "slowest": slowest,
        })

    except Exception as e:
        logger.error(f"[ResourceMonitor] data endpoint error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@resource_monitor_bp.route("/admin/resource-monitor/row/<int:row_id>")
@login_required
def resource_monitor_row(row_id):
    if current_user.role != "admin":
        abort(403)

    try:
        row = db.session.execute(text("""
            SELECT filters, queries, total_ms, query_count, cache_hit, username, captured_at
            FROM query_audit_log
            WHERE id = :id
        """), {"id": row_id}).fetchone()

        if not row:
            return jsonify({"error": "Row not found"}), 404

        filters = row[0] if isinstance(row[0], dict) else json.loads(row[0] or '{}')
        queries = row[1] if isinstance(row[1], list) else json.loads(row[1] or '[]')

        return jsonify({
            "filters":     filters,
            "queries":     queries,
            "total_ms":    float(row[2]),
            "query_count": row[3],
            "cache_hit":   row[4],
            "username":    row[5] or "—",
            "captured_at": row[6].strftime("%Y-%m-%d %H:%M:%S") if row[6] else None,
        })

    except Exception as e:
        logger.error(f"[ResourceMonitor] row detail error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
