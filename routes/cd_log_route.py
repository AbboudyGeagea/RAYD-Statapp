"""
cd_log_route.py
---------------
REST API for receiving and storing DICOM CD/DVD burn events.

The burning station POSTs each burn here; RAYD no longer connects to the
burning app's database (the old CDSURF Oracle pull is gone).

Endpoints:
  POST /api/cd-burn       — receive and store CD burn event (X-API-Key, no login)
  GET  /api/cd-burn       — list recent CD burn events (admin only)
  GET  /api/cd-burn/<id>  — retrieve a specific CD burn event (admin only)

The POST is exempt from CSRF and from the login wall (app.py). It is guarded
instead by a shared key: the station sends `X-API-Key: <CD_BURN_API_KEY>`,
with CD_BURN_API_KEY set in .env. With no key configured, every POST is refused.
"""
import hmac
import logging
import os
from datetime import datetime
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy.exc import IntegrityError

from db import db, CDLog

logger = logging.getLogger("cd_log")
cd_log_bp = Blueprint("cd_log", __name__, url_prefix="/api")


def _parse_timestamp(value):
    """ISO-8601 → naive local time (the container runs in TZ=Asia/Beirut, like
    every other timestamp in the DB). A 'Z' or offset is converted, not dropped."""
    try:
        ts = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return datetime.now()
    if ts.tzinfo is not None:
        ts = ts.astimezone().replace(tzinfo=None)
    return ts


@cd_log_bp.route("/cd-burn", methods=["POST"])
def receive_cd_burn_event():
    """
    Receive a CD burn event from an external application.

    Expected JSON structure:
    {
      "event_type": "cd_burned",
      "timestamp": "2026-09-24T14:32:15Z",
      "burn_mode": "auto_combine",
      "burn_location": "local_dvd",
      "patient": {...},
      "studies": [...],
      "burn_details": {...},
      "result": {...},
      "source": {...}
    }
    """
    expected_key = os.environ.get("CD_BURN_API_KEY", "")
    if not expected_key:
        logger.error("CD burn event refused: CD_BURN_API_KEY is not set in .env")
        return jsonify({"error": "CD burn API is not configured on this server"}), 503
    if not hmac.compare_digest(request.headers.get("X-API-Key", ""), expected_key):
        logger.warning(f"CD burn event refused: bad or missing X-API-Key from {request.remote_addr}")
        return jsonify({"error": "Invalid or missing API key"}), 401

    try:
        data = request.get_json(silent=True)

        if not isinstance(data, dict):
            return jsonify({"error": "No JSON payload provided"}), 400

        # Extract nested fields
        patient = data.get("patient") or {}
        burn_details = data.get("burn_details") or {}
        result = data.get("result") or {}
        source = data.get("source") or {}
        studies = data.get("studies") or []

        if not all(isinstance(x, dict) for x in (patient, burn_details, result, source)):
            return jsonify({"error": "patient, burn_details, result and source must be JSON objects"}), 400
        if not isinstance(studies, list):
            return jsonify({"error": "studies must be a JSON array"}), 400

        timestamp = _parse_timestamp(data.get("timestamp"))

        # Create CD log entry
        cd_log = CDLog(
            event_type=data.get("event_type", "cd_burned"),
            timestamp=timestamp,
            burn_mode=data.get("burn_mode"),
            burn_location=data.get("burn_location"),

            patient_id=patient.get("patient_id"),
            patient_name=patient.get("patient_name"),
            patient_dob=patient.get("patient_dob"),

            studies=studies,

            copies_count=burn_details.get("copies_count") or 1,
            disc_format=burn_details.get("disc_format"),
            disc_size_mb=burn_details.get("disc_size_mb"),
            disc_label=burn_details.get("disc_label"),
            burn_duration_seconds=burn_details.get("burn_duration_seconds"),

            status=result.get("status", "success"),
            error_message=result.get("error_message"),

            operator_id=source.get("operator_id"),
            facility_code=source.get("facility_code"),
            app_version=source.get("app_version"),

            orthanc_validated=False,
            orthanc_validation_result=None,
        )

        db.session.add(cd_log)
        db.session.commit()

        logger.info(
            f"CD burn event logged: patient_id={patient.get('patient_id')}, "
            f"studies={len(studies)}, operator={source.get('operator_id')}, "
            f"facility={source.get('facility_code')}"
        )

        return jsonify({
            "success": True,
            "cd_log_id": cd_log.id,
            "message": "CD burn event recorded successfully"
        }), 201

    except IntegrityError as e:
        db.session.rollback()
        logger.error(f"Database integrity error: {str(e)}")
        return jsonify({"error": "Database integrity error"}), 409

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing CD burn event: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@cd_log_bp.route("/cd-burn", methods=["GET"])
@login_required
def list_cd_burn_events():
    """
    List CD burn events with pagination and optional filtering.

    Access: admin only

    Query parameters:
      - limit: number of records (default 50, max 500)
      - offset: pagination offset (default 0)
      - facility_code: filter by facility
      - patient_id: filter by patient
      - status: filter by status (success/error)
    """
    if current_user.role != 'admin':
        return jsonify({"error": "Access denied. CD Log requires administrator privileges."}), 403

    try:
        limit = min(int(request.args.get("limit", 50)), 500)
        offset = int(request.args.get("offset", 0))

        query = CDLog.query

        # Apply optional filters
        if request.args.get("facility_code"):
            query = query.filter_by(facility_code=request.args.get("facility_code"))
        if request.args.get("patient_id"):
            query = query.filter_by(patient_id=request.args.get("patient_id"))
        if request.args.get("status"):
            query = query.filter_by(status=request.args.get("status"))

        total = query.count()
        events = query.order_by(CDLog.timestamp.desc()).limit(limit).offset(offset).all()

        return jsonify({
            "success": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "events": [
                {
                    "id": e.id,
                    "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                    "patient_id": e.patient_id,
                    "patient_name": e.patient_name,
                    "studies_count": len(e.studies) if e.studies else 0,
                    "status": e.status,
                    "disc_label": e.disc_label,
                    "facility_code": e.facility_code,
                    "operator_id": e.operator_id,
                }
                for e in events
            ]
        }), 200

    except Exception as e:
        logger.error(f"Error listing CD burn events: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@cd_log_bp.route("/cd-burn/<int:cd_log_id>", methods=["GET"])
@login_required
def get_cd_burn_event(cd_log_id):
    """
    Retrieve a specific CD burn event by ID.

    Access: admin only
    """
    if current_user.role != 'admin':
        return jsonify({"error": "Access denied. CD Log requires administrator privileges."}), 403

    try:
        event = CDLog.query.filter_by(id=cd_log_id).first()

        if not event:
            return jsonify({"error": "CD burn event not found"}), 404

        return jsonify({
            "success": True,
            "event": {
                "id": event.id,
                "event_type": event.event_type,
                "timestamp": event.timestamp.isoformat() if event.timestamp else None,
                "burn_mode": event.burn_mode,
                "burn_location": event.burn_location,
                "patient": {
                    "patient_id": event.patient_id,
                    "patient_name": event.patient_name,
                    "patient_dob": str(event.patient_dob) if event.patient_dob else None,
                },
                "studies": event.studies or [],
                "burn_details": {
                    "copies_count": event.copies_count,
                    "disc_format": event.disc_format,
                    "disc_size_mb": float(event.disc_size_mb) if event.disc_size_mb else None,
                    "disc_label": event.disc_label,
                    "burn_duration_seconds": event.burn_duration_seconds,
                },
                "result": {
                    "status": event.status,
                    "error_message": event.error_message,
                },
                "source": {
                    "operator_id": event.operator_id,
                    "facility_code": event.facility_code,
                    "app_version": event.app_version,
                },
                "orthanc_validation": {
                    "validated": event.orthanc_validated,
                    "result": event.orthanc_validation_result,
                    "validated_at": event.orthanc_validated_at.isoformat() if event.orthanc_validated_at else None,
                },
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "updated_at": event.updated_at.isoformat() if event.updated_at else None,
            }
        }), 200

    except Exception as e:
        logger.error(f"Error retrieving CD burn event {cd_log_id}: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
