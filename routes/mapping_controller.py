# routes/mapping_controller.py
from flask import Blueprint, request, render_template, flash, redirect, url_for, jsonify, abort
from flask_login import login_required, current_user
from db import db, AETitleModalityMap, ProcedureDurationMap
import pandas as pd
from io import StringIO

mapping_bp = Blueprint('mapping', __name__, url_prefix='/mapping', template_folder='../templates')

@mapping_bp.route('', methods=['GET'])
@login_required
def mapping_page():
    if current_user.role != 'admin':
        flash("Unauthorized access.", "danger")
        return redirect(url_for('viewer.viewer_dashboard'))

    modality_mappings = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    duration_mappings = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    
    return render_template(
        'mapping.html',
        modality_mappings=modality_mappings,
        duration_mappings=duration_mappings
    )

@mapping_bp.route('/upload', methods=['POST'])
@login_required
def mapping_upload():
    if current_user.role != 'admin':
        return abort(403)

    file = request.files.get('file')
    mapping_type = request.form.get('mapping_type')

    if not file or not file.filename.endswith('.csv'):
        flash("Invalid file: Please upload a .csv file", "error")
        return redirect(url_for('mapping.mapping_page'))

    try:
        raw_data = file.stream.read().decode("utf-8")
        df = pd.read_csv(StringIO(raw_data))
        df.columns = df.columns.str.strip().str.lower()

        if mapping_type == 'modality':
            # Expected columns: aetitle, modality, daily_capacity_minutes
            for _, row in df.iterrows():
                ae = str(row.get('aetitle', '')).strip()
                mod = str(row.get('modality', '')).strip()
                # Default to 480 if missing or invalid
                cap = row.get('daily_capacity_minutes', 480) 
                
                if not ae: continue
                
                rec = AETitleModalityMap.query.filter_by(aetitle=ae).first()
                if rec:
                    rec.modality = mod
                    rec.daily_capacity_minutes = int(cap)
                else:
                    db.session.add(AETitleModalityMap(
                        aetitle=ae, 
                        modality=mod, 
                        daily_capacity_minutes=int(cap)
                    ))

        elif mapping_type == 'duration':
            # Expected columns: procedure_code, duration_minutes, rvu_value
            for _, row in df.iterrows():
                code = str(row.get('procedure_code', '')).strip()
                mins = row.get('duration_minutes', 0)
                rvu = row.get('rvu_value', 0.0) # New RVU field
                
                if not code: continue

                rec = ProcedureDurationMap.query.filter_by(procedure_code=code).first()
                if rec:
                    rec.duration_minutes = int(mins)
                    rec.rvu_value = float(rvu)
                else:
                    db.session.add(ProcedureDurationMap(
                        procedure_code=code, 
                        duration_minutes=int(mins),
                        rvu_value=float(rvu)
                    ))
        
        db.session.commit()
        flash(f"Successfully synchronized {mapping_type} mappings.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Sync failed: {str(e)}", "danger")

    return redirect(url_for('mapping.mapping_page'))

@mapping_bp.route('/add', methods=['POST'])
@login_required
def mapping_add():
    if current_user.role != 'admin':
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    data = request.get_json(force=True)
    m_type = data.get('mapping_type')
    f = data.get('fields', {})

    try:
        if m_type == 'modality':
            db.session.add(AETitleModalityMap(
                aetitle=f['aetitle'], 
                modality=f['modality'],
                daily_capacity_minutes=int(f.get('daily_capacity_minutes', 480))
            ))
        else:
            db.session.add(ProcedureDurationMap(
                procedure_code=f['procedure_code'], 
                duration_minutes=int(f['duration_minutes']),
                rvu_value=float(f.get('rvu_value', 0.0))
            ))
        db.session.commit()
        return jsonify({"status": "success", "message": "Saved"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
