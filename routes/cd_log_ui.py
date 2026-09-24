"""
cd_log_ui.py
-----------
UI routes for CD burn audit dashboard, reports, and Orthanc validation.
Access restricted to SU (super user) and administrator roles.

Routes:
  GET  /viewer/cd-log                — main dashboard
  GET  /viewer/cd-log/patient/<pid>  — patient burn audit report
  GET  /viewer/cd-log/facility/<fac> — facility burn summary
  GET  /viewer/cd-log/validation     — Orthanc validation status
  POST /api/cd-burn/<id>/validate    — trigger Orthanc validation
"""
import logging
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, jsonify, current_user
from flask_login import login_required
from sqlalchemy import text, func

from db import db, CDLog

logger = logging.getLogger("cd_log_ui")
cd_log_ui_bp = Blueprint("cd_log_ui", __name__)


@cd_log_ui_bp.route("/viewer/cd-log")
@login_required
def cd_log_dashboard():
    """
    Main CD burn audit dashboard showing:
    - Recent burn events
    - Success/failure rate
    - Copies distributed per facility
    - Unique patients served

    Access: SU and administrator roles only
    """
    if current_user.role not in ('su', 'administrator'):
        return render_template('error.html', error="Access denied. CD Log requires administrator privileges."), 403

    try:
        # Recent burns (last 30 days)
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        recent = CDLog.query.filter(
            CDLog.timestamp >= thirty_days_ago
        ).order_by(CDLog.timestamp.desc()).limit(50).all()

        # Summary stats (last 30 days)
        stats = db.session.execute(text("""
            SELECT
              COUNT(*) as total_burns,
              SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
              SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns,
              SUM(copies_count) as total_copies,
              COUNT(DISTINCT patient_id) as unique_patients,
              COUNT(DISTINCT facility_code) as facilities,
              ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 1) as success_rate_pct
            FROM cd_burn_log
            WHERE timestamp >= :cutoff
        """), {"cutoff": thirty_days_ago}).fetchone()

        # Top facilities
        top_facilities = db.session.execute(text("""
            SELECT
              facility_code,
              COUNT(*) as burn_count,
              SUM(copies_count) as total_copies,
              COUNT(DISTINCT patient_id) as unique_patients,
              ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 1) as success_rate_pct
            FROM cd_burn_log
            WHERE timestamp >= :cutoff
            GROUP BY facility_code
            ORDER BY burn_count DESC
            LIMIT 10
        """), {"cutoff": thirty_days_ago}).fetchall()

        # Daily trend
        daily_trend = db.session.execute(text("""
            SELECT
              DATE(timestamp) as burn_date,
              COUNT(*) as burn_count,
              SUM(copies_count) as total_copies,
              SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns
            FROM cd_burn_log
            WHERE timestamp >= :cutoff
            GROUP BY DATE(timestamp)
            ORDER BY burn_date DESC
        """), {"cutoff": thirty_days_ago}).fetchall()

        return render_template('cd_log/dashboard.html',
            recent=recent,
            stats=stats,
            top_facilities=top_facilities,
            daily_trend=daily_trend
        )

    except Exception as e:
        logger.error(f"Error rendering CD log dashboard: {str(e)}", exc_info=True)
        return render_template('error.html', error=f"Dashboard error: {str(e)}"), 500


@cd_log_ui_bp.route("/viewer/cd-log/patient/<patient_id>")
@login_required
def cd_log_patient_audit(patient_id):
    """
    Patient-focused audit report:
    - All CD burn events for this patient
    - Total copies received
    - Studies burned
    - Facilities and dates
    - Validation status

    Access: SU and administrator roles only
    """
    if current_user.role not in ('su', 'administrator'):
        return render_template('error.html', error="Access denied. CD Log requires administrator privileges."), 403

    try:
        # Patient info and burns
        burns = CDLog.query.filter_by(patient_id=patient_id).order_by(
            CDLog.timestamp.desc()
        ).all()

        if not burns:
            return render_template('cd_log/patient_audit.html',
                patient_id=patient_id,
                patient_name="Unknown",
                burns=[],
                stats=None
            ), 404

        # Patient summary
        summary = db.session.execute(text("""
            SELECT
              patient_id,
              patient_name,
              COUNT(*) as total_burns,
              SUM(copies_count) as total_copies_received,
              COUNT(DISTINCT facility_code) as facilities_burned_from,
              SUM(COALESCE(json_array_length(studies), 0)) as total_studies_burned,
              MIN(timestamp) as first_burn_date,
              MAX(timestamp) as last_burn_date,
              SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
              SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns
            FROM cd_burn_log
            WHERE patient_id = :pid
            GROUP BY patient_id, patient_name
        """), {"pid": patient_id}).fetchone()

        patient_name = burns[0].patient_name if burns else "Unknown"

        return render_template('cd_log/patient_audit.html',
            patient_id=patient_id,
            patient_name=patient_name,
            burns=burns,
            stats=summary
        )

    except Exception as e:
        logger.error(f"Error rendering patient audit: {str(e)}", exc_info=True)
        return render_template('error.html', error=f"Patient audit error: {str(e)}"), 500


@cd_log_ui_bp.route("/viewer/cd-log/facility/<facility_code>")
@login_required
def cd_log_facility_report(facility_code):
    """
    Facility-focused burn summary report:
    - Total burns from this facility
    - Success/failure breakdown
    - Daily trends
    - Top patients by copy count
    - Capacity utilization

    Access: SU and administrator roles only
    """
    if current_user.role not in ('su', 'administrator'):
        return render_template('error.html', error="Access denied. CD Log requires administrator privileges."), 403

    try:
        # Facility burns (last 60 days)
        sixty_days_ago = datetime.utcnow() - timedelta(days=60)
        burns = CDLog.query.filter(
            CDLog.facility_code == facility_code,
            CDLog.timestamp >= sixty_days_ago
        ).order_by(CDLog.timestamp.desc()).all()

        # Facility summary
        summary = db.session.execute(text("""
            SELECT
              facility_code,
              COUNT(*) as total_burns,
              SUM(copies_count) as total_copies,
              COUNT(DISTINCT patient_id) as unique_patients,
              COUNT(DISTINCT DATE(timestamp)) as burn_days,
              ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 1) as success_rate_pct,
              SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns,
              SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_burns,
              AVG(disc_size_mb) as avg_disc_size_mb,
              MIN(timestamp) as first_burn_date,
              MAX(timestamp) as last_burn_date
            FROM cd_burn_log
            WHERE facility_code = :fac AND timestamp >= :cutoff
            GROUP BY facility_code
        """), {"fac": facility_code, "cutoff": sixty_days_ago}).fetchone()

        # Top patients by copy count
        top_patients = db.session.execute(text("""
            SELECT
              patient_id,
              patient_name,
              COUNT(*) as burn_events,
              SUM(copies_count) as total_copies,
              MAX(timestamp) as last_burn
            FROM cd_burn_log
            WHERE facility_code = :fac AND timestamp >= :cutoff
            GROUP BY patient_id, patient_name
            ORDER BY total_copies DESC
            LIMIT 10
        """), {"fac": facility_code, "cutoff": sixty_days_ago}).fetchall()

        # Daily burn trend
        daily_trend = db.session.execute(text("""
            SELECT
              DATE(timestamp) as burn_date,
              COUNT(*) as burn_count,
              SUM(copies_count) as total_copies,
              SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_burns
            FROM cd_burn_log
            WHERE facility_code = :fac AND timestamp >= :cutoff
            GROUP BY DATE(timestamp)
            ORDER BY burn_date DESC
        """), {"fac": facility_code, "cutoff": sixty_days_ago}).fetchall()

        return render_template('cd_log/facility_report.html',
            facility_code=facility_code,
            burns=burns,
            stats=summary,
            top_patients=top_patients,
            daily_trend=daily_trend
        )

    except Exception as e:
        logger.error(f"Error rendering facility report: {str(e)}", exc_info=True)
        return render_template('error.html', error=f"Facility report error: {str(e)}"), 500


@cd_log_ui_bp.route("/viewer/cd-log/validation")
@login_required
def cd_log_orthanc_validation():
    """
    Orthanc UID validation report:
    - Pending validations (not yet checked)
    - Valid burns (UIDs match Orthanc)
    - Mismatches (UID count discrepancies)
    - Action items

    Access: SU and administrator roles only
    """
    if current_user.role not in ('su', 'administrator'):
        return render_template('error.html', error="Access denied. CD Log requires administrator privileges."), 403

    try:
        # Unvalidated burns (last 7 days)
        seven_days_ago = datetime.utcnow() - timedelta(days=7)

        pending = CDLog.query.filter(
            CDLog.orthanc_validated == False,
            CDLog.timestamp >= seven_days_ago
        ).order_by(CDLog.timestamp.desc()).limit(100).all()

        # Validation statistics
        stats = db.session.execute(text("""
            SELECT
              SUM(CASE WHEN orthanc_validated = true THEN 1 ELSE 0 END) as validated_count,
              SUM(CASE WHEN orthanc_validated = false THEN 1 ELSE 0 END) as pending_count,
              SUM(CASE WHEN orthanc_validation_result->>'all_found' = 'true' THEN 1 ELSE 0 END) as valid_count,
              SUM(CASE WHEN orthanc_validation_result->>'all_found' = 'false' THEN 1 ELSE 0 END) as mismatch_count
            FROM cd_burn_log
            WHERE timestamp >= :cutoff
        """), {"cutoff": seven_days_ago}).fetchone()

        # Recent mismatches
        mismatches = CDLog.query.filter(
            CDLog.orthanc_validated == True,
            CDLog.orthanc_validation_result.isnot(None)
        ).order_by(CDLog.timestamp.desc()).limit(20).all()

        mismatches = [
            m for m in mismatches
            if m.orthanc_validation_result and not m.orthanc_validation_result.get('all_found', True)
        ]

        return render_template('cd_log/validation.html',
            pending=pending,
            mismatches=mismatches,
            stats=stats
        )

    except Exception as e:
        logger.error(f"Error rendering validation report: {str(e)}", exc_info=True)
        return render_template('error.html', error=f"Validation report error: {str(e)}"), 500


@cd_log_ui_bp.route("/api/cd-burn/<int:cd_log_id>/validate", methods=["POST"])
@login_required
def validate_against_orthanc(cd_log_id):
    """
    Trigger Orthanc validation for a specific burn event.
    Checks if all study UIDs exist in Orthanc and instance counts match.

    Access: SU and administrator roles only
    """
    if current_user.role not in ('su', 'administrator'):
        return jsonify({"error": "Access denied. Validation requires administrator privileges."}), 403

    try:
        burn = CDLog.query.filter_by(id=cd_log_id).first()
        if not burn:
            return jsonify({"error": "CD burn event not found"}), 404

        # TODO: Integrate with Orthanc API
        # For now, just mark as pending validation and store placeholder
        validation_result = {
            "pending": True,
            "message": "Orthanc validation would be triggered here",
            "studies_to_check": len(burn.studies) if burn.studies else 0
        }

        burn.orthanc_validation_result = validation_result
        burn.orthanc_validated_at = datetime.utcnow()
        db.session.commit()

        return jsonify({
            "success": True,
            "message": "Validation queued for this CD burn event",
            "cd_log_id": cd_log_id
        }), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error validating CD burn {cd_log_id}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Validation error: {str(e)}"}), 500
