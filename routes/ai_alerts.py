"""
routes/ai_alerts.py
────────────────────
Pure SQL anomaly detection alerts — no LLM inference.

Detects:
  - TAT spikes vs 4-week rolling baseline
  - Storage ingestion growth week-over-week
  - Per-device volume outliers (z-score > 2σ)
"""
import logging
from flask import Blueprint, jsonify
from flask_login import login_required
from sqlalchemy import text
from db import db

logger = logging.getLogger(__name__)
ai_alerts_bp = Blueprint("ai_alerts", __name__)


@ai_alerts_bp.route("/ai/alerts")
@login_required
def alerts():
    findings = []
    try:
        with db.engine.connect() as conn:

            # TAT spike: this week vs prior 4-week rolling average
            tat_rows = conn.execute(text("""
                WITH weekly AS (
                    SELECT DATE_TRUNC('week', study_date) AS wk,
                           ROUND(AVG(EXTRACT(EPOCH FROM (rep_final_timestamp - study_date)) / 60)::numeric, 1) AS avg_tat
                    FROM etl_didb_studies
                    WHERE study_date >= CURRENT_DATE - INTERVAL '5 weeks'
                      AND rep_final_timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1 DESC
                )
                SELECT wk, avg_tat FROM weekly LIMIT 5
            """)).fetchall()
            if len(tat_rows) >= 2:
                cur_tat  = float(tat_rows[0][1] or 0)
                baseline = sum(float(r[1] or 0) for r in tat_rows[1:]) / len(tat_rows[1:])
                if baseline > 0:
                    pct = (cur_tat - baseline) / baseline * 100
                    if abs(pct) >= 20:
                        findings.append({
                            "type": "tat",
                            "severity": "high" if abs(pct) >= 30 else "medium",
                            "msg": f"TAT {'↑' if pct > 0 else '↓'} {abs(pct):.0f}% this week "
                                   f"({cur_tat:.0f}m) vs 4-week baseline ({baseline:.0f}m)",
                        })

            # Storage growth anomaly: this week vs previous week
            stor_rows = {r[0]: float(r[1] or 0) for r in conn.execute(text("""
                SELECT CASE WHEN study_date >= CURRENT_DATE - 7 THEN 'current' ELSE 'prior' END,
                       ROUND(SUM(total_gb)::numeric, 2)
                FROM summary_storage_daily
                WHERE study_date >= CURRENT_DATE - 14
                GROUP BY 1
            """)).fetchall()}
            cur_gb, prev_gb = stor_rows.get("current", 0), stor_rows.get("prior", 0)
            if prev_gb > 0:
                stor_pct = (cur_gb - prev_gb) / prev_gb * 100
                if stor_pct >= 25:
                    findings.append({
                        "type": "storage",
                        "severity": "high" if stor_pct >= 50 else "medium",
                        "msg": f"Storage ingestion ↑ {stor_pct:.0f}% this week "
                               f"({cur_gb:.1f} GB) vs last week ({prev_gb:.1f} GB)",
                    })

            # Device volume outliers: this-week count > prior avg + 2σ
            for r in conn.execute(text("""
                WITH weekly_ae AS (
                    SELECT storing_ae, DATE_TRUNC('week', study_date) AS wk, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= CURRENT_DATE - INTERVAL '6 weeks'
                      AND storing_ae IS NOT NULL
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT storing_ae, AVG(cnt) AS avg_cnt, STDDEV(cnt) AS std_cnt
                    FROM weekly_ae WHERE wk < DATE_TRUNC('week', CURRENT_DATE)
                    GROUP BY storing_ae
                ),
                this_week AS (
                    SELECT storing_ae, cnt FROM weekly_ae
                    WHERE wk = DATE_TRUNC('week', CURRENT_DATE)
                )
                SELECT t.storing_ae, t.cnt, s.avg_cnt, s.std_cnt
                FROM this_week t JOIN stats s ON t.storing_ae = s.storing_ae
                WHERE s.std_cnt > 0 AND t.cnt > s.avg_cnt + 2 * s.std_cnt
                ORDER BY (t.cnt - s.avg_cnt) / s.std_cnt DESC LIMIT 3
            """)).fetchall():
                ae, cnt, avg, std = r
                z = (float(cnt) - float(avg)) / float(std)
                findings.append({
                    "type": "utilization", "severity": "medium",
                    "msg": f"{ae}: {cnt} studies this week vs avg {avg:.0f} (z = {z:.1f}σ above normal)",
                })

    except Exception as e:
        logger.error(f"[ai_alerts] error: {e}")
        return jsonify({"alerts": [], "error": str(e)})

    return jsonify({"alerts": findings, "count": len(findings), "clean": len(findings) == 0})
