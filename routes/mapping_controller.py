from flask import Blueprint, request, render_template, flash, redirect, url_for, jsonify, abort
from flask_login import login_required, current_user
from db import db, AETitleModalityMap, ProcedureDurationMap, DeviceException
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import json

mapping_bp = Blueprint('mapping', __name__, url_prefix='/mapping', template_folder='../templates')

@mapping_bp.route('', methods=['GET'])
@login_required
def mapping_page():
    if current_user.role != 'admin':
        flash("Unauthorized access.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    modality_mappings = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    duration_mappings = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    
    # Calculate Monday of current week
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday()) 
    end_of_week = start_of_week + timedelta(days=6)

    exceptions = DeviceException.query.filter(
        DeviceException.exception_date >= start_of_week,
        DeviceException.exception_date <= end_of_week
    ).all()

    exceptions_lookup = {
        f"{ex.aetitle}_{ex.exception_date.strftime('%Y-%m-%d')}": ex.actual_opening_minutes 
        for ex in exceptions
    }
    
    return render_template(
        'mapping.html',
        modality_mappings=modality_mappings,
        duration_mappings=duration_mappings,
        exceptions_json=json.dumps(exceptions_lookup)
    )

@mapping_bp.route('/upload/modality', methods=['POST'])
@login_required
def upload_modality_map():
    if current_user.role != 'admin': return abort(403)
    file = request.files.get('file')
    if not file: return redirect(url_for('mapping.mapping_page'))

    try:
        df = pd.read_csv(file)
        # Expected columns: aetitle, modality, daily_capacity_minutes
        for _, row in df.iterrows():
            mapping = AETitleModalityMap.query.filter_by(aetitle=row['aetitle']).first()
            if mapping:
                mapping.modality = row['modality']
                mapping.daily_capacity_minutes = int(row['daily_capacity_minutes'])
            else:
                db.session.add(AETitleModalityMap(
                    aetitle=row['aetitle'],
                    modality=row['modality'],
                    daily_capacity_minutes=int(row['daily_capacity_minutes'])
                ))
        db.session.commit()
        flash("Modality Map updated successfully!", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {str(e)}", "danger")
    
    return redirect(url_for('mapping.mapping_page'))

@mapping_bp.route('/upload/procedure', methods=['POST'])
@login_required
def upload_procedure_map():
    if current_user.role != 'admin': return abort(403)
    file = request.files.get('file')
    if not file: return redirect(url_for('mapping.mapping_page'))

    try:
        df = pd.read_csv(file)
        # Expected columns: procedure_code, duration_minutes, rvu_value
        for _, row in df.iterrows():
            mapping = ProcedureDurationMap.query.filter_by(procedure_code=str(row['procedure_code'])).first()
            if mapping:
                mapping.duration_minutes = int(row['duration_minutes'])
                mapping.rvu_value = float(row['rvu_value'])
            else:
                db.session.add(ProcedureDurationMap(
                    procedure_code=str(row['procedure_code']),
                    duration_minutes=int(row['duration_minutes']),
                    rvu_value=float(row['rvu_value'])
                ))
        db.session.commit()
        flash("Procedure Map updated successfully!", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {str(e)}", "danger")
    
    return redirect(url_for('mapping.mapping_page'))

@mapping_bp.route('/device/grid/save', methods=['POST'])
@login_required
def save_grid_changes():
    if current_user.role != 'admin': return abort(403)
    data = request.get_json(force=True)
    updates = data.get('updates', [])
    try:
        for item in updates:
            exc_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
            existing = DeviceException.query.filter_by(aetitle=item['aetitle'], exception_date=exc_date).first()
            if existing:
                existing.actual_opening_minutes = int(item['value'])
                if not existing.reason: existing.reason = "Manual Adjustment"
            else:
                db.session.add(DeviceException(
                    aetitle=item['aetitle'],
                    exception_date=exc_date,
                    actual_opening_minutes=int(item['value']),
                    reason="Manual Adjustment"
                ))
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

@mapping_bp.route('/device/exception/save', methods=['POST'])
@login_required
def save_device_exception():
    if current_user.role != 'admin': return abort(403)
    data = request.get_json(force=True)
    try:
        exc_date = datetime.strptime(data['date'], '%Y-%m-%d').date()
        existing = DeviceException.query.filter_by(aetitle=data['aetitle'], exception_date=exc_date).first()
        if existing:
            existing.actual_opening_minutes = int(data['minutes'])
            existing.reason = data.get('reason', 'Manual Adjustment')
        else:
            db.session.add(DeviceException(
                aetitle=data['aetitle'],
                exception_date=exc_date,
                actual_opening_minutes=int(data['minutes']),
                reason=data.get('reason', 'Manual Adjustment')
            ))
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
