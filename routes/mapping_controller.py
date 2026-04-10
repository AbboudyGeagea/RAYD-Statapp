from flask import Blueprint, request, render_template, flash, redirect, url_for, jsonify, abort, Response
from flask_login import login_required, current_user
# Import the CLASS names from your db file
from db import db, AETitleModalityMap, ProcedureDurationMap, DeviceException, DeviceWeeklySchedule
import pandas as pd
from datetime import datetime, timedelta
import json
import csv
import io
import logging

mapping_bp = Blueprint('mapping', __name__, url_prefix='/mapping')

# --- HELPER FOR UPSERT LOGIC ---
def get_or_create(model, **kwargs):
    instance = db.session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        db.session.add(instance)
        return instance, True

@mapping_bp.route('/export/modality')
@login_required
def export_modality_csv():
    if current_user.role != 'admin': return abort(403)
    rows = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['aetitle', 'modality', 'daily_capacity_minutes'])
    for r in rows:
        sched = next((s for s in r.weekly_schedules if s.day_of_week == 0), None)
        cap = sched.std_opening_minutes if sched else 720
        w.writerow([r.aetitle, r.modality, cap])
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=modality_map.csv'}
    )


@mapping_bp.route('/export/procedure')
@login_required
def export_procedure_csv():
    if current_user.role != 'admin': return abort(403)
    rows = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['procedure_code', 'duration_minutes', 'rvu_value'])
    for r in rows:
        w.writerow([r.procedure_code, r.duration_minutes, r.rvu_value])
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=procedure_map.csv'}
    )


@mapping_bp.route('', methods=['GET'])
@login_required
def mapping_page():
    if current_user.role != 'admin': return abort(403)

    modality_mappings = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    duration_mappings = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday()) 
    end_of_week = start_of_week + timedelta(days=6)

    exceptions = DeviceException.query.filter(
        DeviceException.exception_date >= start_of_week,
        DeviceException.exception_date <= end_of_week
    ).all()

    exceptions_lookup = {
        f"{ex.aetitle.upper()}_{ex.exception_date.strftime('%Y-%m-%d')}": ex.actual_opening_minutes 
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
    file = request.files.get('file')
    if not file: return redirect(url_for('mapping.mapping_page'))

    try:
        df = pd.read_csv(file)
        # Clean headers to lowercase
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        for _, row in df.iterrows():
            ae = str(row['aetitle']).strip().upper()
            mod = str(row['modality']).strip().upper()
            # Handle capacity: use CSV value or default to 480
            try:
                cap = int(float(row['daily_capacity_minutes'])) if pd.notna(row['daily_capacity_minutes']) else 480
            except:
                cap = 480

            # 1. Sync Parent (AETitleModalityMap) — keep both tables in sync
            parent = AETitleModalityMap.query.filter_by(aetitle=ae).first()
            if not parent:
                parent = AETitleModalityMap(aetitle=ae, modality=mod, daily_capacity_minutes=cap)
                db.session.add(parent)
            else:
                parent.modality = mod
                parent.daily_capacity_minutes = cap
            
            # Flush tells the DB about the parent so the Foreign Key doesn't fail
            db.session.flush()

            # 2. Sync 7 Days of Schedule (device_weekly_schedule)
            for d in range(7):
                # Filter by both AETitle AND Day (Composite Key)
                sched = DeviceWeeklySchedule.query.filter_by(aetitle=ae, day_of_week=d).first()
                if sched:
                    sched.std_opening_minutes = cap
                else:
                    db.session.add(DeviceWeeklySchedule(
                        aetitle=ae, 
                        day_of_week=d, 
                        std_opening_minutes=cap
                    ))
        
        db.session.commit()
        flash("Modality & Weekly Schedules synchronized successfully.", "success")
    except Exception as e:
        db.session.rollback()
        # This will show you exactly if it's a DB error or a naming error
        flash(f"Upload Error: {str(e)}", "danger")
        print(f"DEBUG ERROR: {str(e)}") # Check your terminal for this!
        
    return redirect(url_for('mapping.mapping_page'))
    
    
@mapping_bp.route('/upload/procedure', methods=['POST'])
@login_required
def upload_procedure_map():
    file = request.files.get('file')
    if not file: return redirect(url_for('mapping.mapping_page'))

    try:
        df = pd.read_csv(file)
        df.columns = [str(c).strip().lower() for c in df.columns]

        # LAYER OF PROTECTION: Schema Validation
        required = {'procedure_code', 'duration_minutes', 'rvu_value'}
        if not required.issubset(df.columns):
            flash("Upload Aborted: CSV headers must be procedure_code, duration_minutes, rvu_value", "danger")
            return redirect(url_for('mapping.mapping_page'))

        # LAYER OF PROTECTION: Data Integrity Check (Dry Run)
        for idx, row in df.iterrows():
            try:
                # Ensure we can actually convert these before doing any DB work
                _ = int(float(row['duration_minutes']))
                _ = float(row['rvu_value'])
                if pd.isna(row['procedure_code']): raise ValueError("Empty Code")
            except Exception:
                flash(f"Protection Alert: Row {idx+2} contains invalid data. Entire upload canceled.", "danger")
                return redirect(url_for('mapping.mapping_page'))

        # If we passed the dry run, proceed to UPSERT
        for _, row in df.iterrows():
            p_code = str(row['procedure_code']).strip().upper()
            duration = int(float(row['duration_minutes']))
            rvu = float(row['rvu_value'])

            mapping, created = get_or_create(ProcedureDurationMap, procedure_code=p_code)
            mapping.duration_minutes = duration
            mapping.rvu_value = rvu

        db.session.commit()
        flash(f"Success: {len(df)} procedures verified and updated.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Procedure DB Error: {str(e)}", "danger")
    return redirect(url_for('mapping.mapping_page'))

@mapping_bp.route('/device/grid/save', methods=['POST'])
@login_required
def save_grid_changes():
    data = request.get_json(force=True)
    updates = data.get('updates', [])
    try:
        for item in updates:
            ae = str(item['aetitle']).strip().upper()
            exc_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
            val = int(item['value'])
            
            # Logic for Point #3: Store in DeviceException
            existing = DeviceException.query.filter_by(aetitle=ae, exception_date=exc_date).first()
            if existing:
                existing.actual_opening_minutes = val
            else:
                db.session.add(DeviceException(
                    aetitle=ae, 
                    exception_date=exc_date, 
                    actual_opening_minutes=val, 
                    reason="Grid Adjustment"
                ))
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

# NEW: Inline edit for individual procedures (Point #4)
@mapping_bp.route('/procedure/update', methods=['POST'])
@login_required
def update_single_procedure():
    data = request.get_json(force=True)
    try:
        p_code = str(data['code']).strip().upper()
        mapping = ProcedureDurationMap.query.filter_by(procedure_code=p_code).first()
        if mapping:
            mapping.duration_minutes = int(data['duration'])
            mapping.rvu_value = float(data.get('rvu', mapping.rvu_value))
            db.session.commit()
            return jsonify({"status": "success"})
        return jsonify({"status": "error", "message": "Procedure not found"}), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
