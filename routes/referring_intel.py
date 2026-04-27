"""
routes/referring_intel.py
--------------------------
Referring Physician Intelligence — historical cohort analysis per referring doctor.
All data sourced from the daily ETL snapshot (etl_didb_studies + hl7_oru_analysis).
"""

import logging
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required
from sqlalchemy import text
from db import db

logger = logging.getLogger("REFERRING_INTEL")
referring_intel_bp = Blueprint("referring_intel", __name__)

_MJ = "LEFT JOIN aetitle_modality_map m ON s.storing_ae = m.aetitle"
_SR = "COALESCE(m.modality, s.study_modality, '') != 'SR'"
_PHY = "TRIM(CONCAT(s.referring_physician_first_name, ' ', s.referring_physician_last_name))"


# ─────────────────────────────────────────────
#  PAGE
# ─────────────────────────────────────────────

@referring_intel_bp.route("/viewer/referring-intel")
@login_required
def referring_intel_page():
    return render_template("referring_intel.html")


# ─────────────────────────────────────────────
#  PHYSICIAN LIST
# ─────────────────────────────────────────────

@referring_intel_bp.route("/viewer/referring-intel/list")
@login_required
def referring_intel_list():
    try:
        rows = db.session.execute(text(f"""
            SELECT
                {_PHY} AS physician,
                COUNT(*)                       AS total_studies,
                COUNT(DISTINCT s.patient_db_uid) AS total_patients,
                MAX(s.study_date)::text         AS last_study,
                ROUND(
                    COUNT(*) FILTER (WHERE s.study_date >= CURRENT_DATE - INTERVAL '30 days')
                    * 100.0 / NULLIF(COUNT(*), 0), 1
                ) AS pct_last_30d
            FROM etl_didb_studies s {_MJ}
            WHERE s.referring_physician_last_name IS NOT NULL
              AND s.referring_physician_last_name != ''
              AND {_SR}
            GROUP BY 1
            ORDER BY total_studies DESC
            LIMIT 300
        """)).mappings().fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        logger.error(f"Physician list error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  PHYSICIAN DETAIL
# ─────────────────────────────────────────────

@referring_intel_bp.route("/viewer/referring-intel/detail")
@login_required
def referring_intel_detail():
    physician = request.args.get("physician", "").strip()
    months    = min(int(request.args.get("months", 24)), 60)
    if not physician:
        return jsonify({"error": "physician required"}), 400

    p = {"physician": physician, "months": months}

    try:
        # ── Summary KPIs ──────────────────────────────────────────────
        kpi = db.session.execute(text(f"""
            SELECT
                COUNT(*)                            AS total_studies,
                COUNT(DISTINCT s.patient_db_uid)    AS total_patients,
                MIN(s.study_date)::text             AS first_study,
                MAX(s.study_date)::text             AS last_study,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.insert_time)) / 60.0
                ) FILTER (WHERE s.rep_final_timestamp IS NOT NULL
                            AND s.insert_time IS NOT NULL
                            AND s.rep_final_timestamp > s.insert_time
                )::numeric, 1)                      AS median_tat_min,
                COUNT(*) FILTER (WHERE s.study_has_report = TRUE
                                    OR s.rep_final_timestamp IS NOT NULL) AS reported_count,
                ROUND(
                    COUNT(*) FILTER (WHERE s.study_date >= CURRENT_DATE - INTERVAL '30 days')
                    * 100.0 / NULLIF(COUNT(*), 0), 1
                )                                   AS pct_last_30d
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
        """), p).mappings().fetchone()

        # Department median TAT baseline (last 90 days)
        dept_tat = db.session.execute(text(f"""
            SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.insert_time)) / 60.0
            )::numeric, 1)
            FROM etl_didb_studies s {_MJ}
            WHERE s.rep_final_timestamp IS NOT NULL
              AND s.insert_time IS NOT NULL
              AND s.rep_final_timestamp > s.insert_time
              AND s.study_date >= CURRENT_DATE - INTERVAL '90 days'
              AND {_SR}
        """)).scalar()

        # ── Monthly volume trend ───────────────────────────────────────
        trend = db.session.execute(text(f"""
            SELECT
                TO_CHAR(DATE_TRUNC('month', s.study_date), 'YYYY-MM') AS month,
                COUNT(*)                           AS studies,
                COUNT(DISTINCT s.patient_db_uid)   AS patients
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
              AND s.study_date >= CURRENT_DATE - (:months * INTERVAL '1 month')
            GROUP BY 1 ORDER BY 1
        """), p).mappings().fetchall()

        # ── Monthly TAT trend ─────────────────────────────────────────
        tat_trend = db.session.execute(text(f"""
            SELECT
                TO_CHAR(DATE_TRUNC('month', s.study_date), 'YYYY-MM') AS month,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.insert_time)) / 60.0
                )::numeric, 1) AS median_tat_min,
                COUNT(*)       AS cnt
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
              AND s.rep_final_timestamp IS NOT NULL
              AND s.insert_time IS NOT NULL
              AND s.rep_final_timestamp > s.insert_time
              AND s.study_date >= CURRENT_DATE - (:months * INTERVAL '1 month')
            GROUP BY 1 ORDER BY 1
        """), p).mappings().fetchall()

        # ── Modality mix ───────────────────────────────────────────────
        modality = db.session.execute(text(f"""
            SELECT
                COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
                COUNT(*) AS cnt
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
            GROUP BY 1 ORDER BY cnt DESC LIMIT 12
        """), p).mappings().fetchall()

        # ── Body parts ─────────────────────────────────────────────────
        body_parts = db.session.execute(text(f"""
            SELECT
                COALESCE(NULLIF(TRIM(s.study_body_part), ''), 'Unspecified') AS part,
                COUNT(*) AS cnt
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
            GROUP BY 1 ORDER BY cnt DESC LIMIT 10
        """), p).mappings().fetchall()

        # ── NLP findings from hl7_oru_analysis ────────────────────────
        findings = db.session.execute(text(f"""
            SELECT label, COUNT(*) AS cnt
            FROM (
                SELECT unnest(a.affirmed_labels) AS label
                FROM hl7_oru_reports r
                JOIN hl7_oru_analysis a ON a.report_id = r.id
                JOIN etl_didb_studies s ON s.accession_number = r.accession_number
                WHERE {_PHY} = :physician
                  AND a.affirmed_labels IS NOT NULL
                  AND array_length(a.affirmed_labels, 1) > 0
            ) sub
            GROUP BY label ORDER BY cnt DESC LIMIT 20
        """), p).mappings().fetchall()

        # Critical findings rate
        critical = db.session.execute(text(f"""
            SELECT
                COUNT(*)                                         AS total_reports,
                COUNT(*) FILTER (WHERE a.is_critical = TRUE)    AS critical_count
            FROM hl7_oru_reports r
            JOIN hl7_oru_analysis a ON a.report_id = r.id
            JOIN etl_didb_studies s ON s.accession_number = r.accession_number
            WHERE {_PHY} = :physician
        """), p).mappings().fetchone()

        # ── Patient return rate ────────────────────────────────────────
        # Only referrals from this physician within the selected period.
        # COUNT(DISTINCT) per bucket ensures each patient is counted once
        # regardless of how many return visits they had.
        return_stats = db.session.execute(text(f"""
            WITH phy_visits AS (
                SELECT DISTINCT s.patient_db_uid, s.study_date
                FROM etl_didb_studies s {_MJ}
                WHERE {_PHY} = :physician AND {_SR}
                  AND s.study_date >= CURRENT_DATE - (:months * INTERVAL '1 month')
            ),
            gaps AS (
                SELECT
                    patient_db_uid,
                    study_date,
                    LAG(study_date) OVER (
                        PARTITION BY patient_db_uid ORDER BY study_date
                    ) AS prev_date
                FROM phy_visits
            )
            SELECT
                COUNT(DISTINCT patient_db_uid)                                    AS total_patients,
                COUNT(DISTINCT patient_db_uid) FILTER (
                    WHERE prev_date IS NOT NULL AND (study_date - prev_date) <= 30
                )                                                                  AS return_30d,
                COUNT(DISTINCT patient_db_uid) FILTER (
                    WHERE prev_date IS NOT NULL AND (study_date - prev_date) <= 90
                )                                                                  AS return_90d,
                COUNT(DISTINCT patient_db_uid) FILTER (
                    WHERE prev_date IS NOT NULL AND (study_date - prev_date) <= 365
                )                                                                  AS return_365d
            FROM gaps
        """), p).mappings().fetchone()

        # ── Patient class mix ──────────────────────────────────────────
        patient_class = db.session.execute(text(f"""
            SELECT
                COALESCE(NULLIF(TRIM(s.patient_class), ''), 'Unknown') AS class,
                COUNT(*) AS cnt
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
            GROUP BY 1 ORDER BY cnt DESC
        """), p).mappings().fetchall()

        # ── Age distribution (buckets) ─────────────────────────────────
        age_dist = db.session.execute(text(f"""
            SELECT
                CASE
                    WHEN s.age_at_exam < 10  THEN '0-9'
                    WHEN s.age_at_exam < 20  THEN '10-19'
                    WHEN s.age_at_exam < 30  THEN '20-29'
                    WHEN s.age_at_exam < 40  THEN '30-39'
                    WHEN s.age_at_exam < 50  THEN '40-49'
                    WHEN s.age_at_exam < 60  THEN '50-59'
                    WHEN s.age_at_exam < 70  THEN '60-69'
                    WHEN s.age_at_exam < 80  THEN '70-79'
                    ELSE '80+'
                END AS bucket,
                COUNT(*) AS cnt
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
              AND s.age_at_exam IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """), p).mappings().fetchall()

        # ── Recent 20 studies ─────────────────────────────────────────
        recent = db.session.execute(text(f"""
            SELECT
                s.study_date::text,
                s.accession_number,
                COALESCE(m.modality, s.study_modality, '—')               AS modality,
                COALESCE(NULLIF(TRIM(s.study_description), ''),
                         s.study_body_part, '—')                          AS description,
                s.report_status,
                s.patient_class,
                ROUND(
                    EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.insert_time)) / 60.0
                ) AS tat_min
            FROM etl_didb_studies s {_MJ}
            WHERE {_PHY} = :physician AND {_SR}
            ORDER BY s.study_date DESC, s.study_db_uid DESC
            LIMIT 20
        """), p).mappings().fetchall()

        return jsonify({
            "physician":    physician,
            "kpi":          dict(kpi) if kpi else {},
            "dept_tat_min": float(dept_tat) if dept_tat else None,
            "trend":        [dict(r) for r in trend],
            "tat_trend":    [dict(r) for r in tat_trend],
            "modality":     [dict(r) for r in modality],
            "body_parts":   [dict(r) for r in body_parts],
            "findings":     [dict(r) for r in findings],
            "critical":     dict(critical) if critical else {},
            "return_stats": dict(return_stats) if return_stats else {},
            "patient_class":[dict(r) for r in patient_class],
            "age_dist":     [dict(r) for r in age_dist],
            "recent":       [dict(r) for r in recent],
        })

    except Exception as e:
        logger.error(f"Referring intel detail error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
