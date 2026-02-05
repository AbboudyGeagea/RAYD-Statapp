from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
import pandas as pd
from io import StringIO
from db import db, AETitleModalityMap, ProcedureDurationMap, User
from flask_login import login_required, current_user

mapper_editor_bp = Blueprint('mapper_editor', __name__, url_prefix='/mapper-editor')

# ----------------------------
# Access Control Decorator
# ----------------------------
def mapping_access_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Using current_user (Flask-Login) is more reliable than raw session
        if not current_user.is_authenticated or current_user.role not in ['admin', 'report_admin']:
            flash('Access denied. Admin privileges required.', 'danger')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

# ----------------------------
# 1 & 2: Main Page (List & Upsert)
# ----------------------------
@mapper_editor_bp.route('/', methods=['GET', 'POST'])
@login_required
@mapping_access_required
def mapping_list():
    if request.method == 'POST':
        mapping_type = request.form.get('mapping_type')
        file = request.files.get('file')

        if not file or file.filename == '':
            flash('No file selected.', 'warning')
            return redirect(url_for('mapper_editor.mapping_list'))

        try:
            df = pd.read_csv(StringIO(file.stream.read().decode("UTF8")))
            
            if mapping_type == 'modality':
                # Logic: Upsert (Delete existing keys found in CSV, then re-insert)
                for _, row in df.iterrows():
                    # Check for existing record by Primary Key (aetitle)
                    existing = AETitleModalityMap.query.filter_by(aetitle=row['aetitle']).first()
                    if existing:
                        existing.modality = row['modality']
                    else:
                        new_rec = AETitleModalityMap(aetitle=row['aetitle'], modality=row['modality'])
                        db.session.add(new_rec)
                
            elif mapping_type == 'duration':
                for _, row in df.iterrows():
                    # Check for existing record by Primary Key (procedure_code)
                    existing = ProcedureDurationMap.query.filter_by(procedure_code=row['procedure_code']).first()
                    if existing:
                        existing.duration_minutes = int(row['duration_minutes'])
                    else:
                        new_rec = ProcedureDurationMap(
                            procedure_code=row['procedure_code'], 
                            duration_minutes=int(row['duration_minutes'])
                        )
                        db.session.add(new_rec)

            db.session.commit()
            flash(f'Successfully processed {mapping_type} mappings.', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error processing file: {str(e)}', 'danger')
        
        return redirect(url_for('mapper_editor.mapping_list'))

    # Requirement 1: Show from DB the mappings from 2 different tables
    modality_data = AETitleModalityMap.query.all()
    duration_data = ProcedureDurationMap.query.all()
    
    return render_template('mapping_upload.html', 
                           modality_mappings=modality_data, 
                           duration_mappings=duration_data)

# ----------------------------
# 3: Manual Add (POST)
# ----------------------------
@mapper_editor_bp.route('/add', methods=['POST'])
@login_required
@mapping_access_required
def manual_add():
    mapping_type = request.form.get('mapping_type')
    try:
        if mapping_type == 'modality':
            new_rec = AETitleModalityMap(
                aetitle=request.form.get('aetitle'),
                modality=request.form.get('modality')
            )
        else:
            new_rec = ProcedureDurationMap(
                procedure_code=request.form.get('procedure_code'),
                duration_minutes=int(request.form.get('duration_minutes'))
            )
        db.session.add(new_rec)
        db.session.commit()
        flash('Manual entry added.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error adding entry: {str(e)}', 'danger')
    
    return redirect(url_for('mapper_editor.mapping_list'))

# ----------------------------
# 4: Delete (POST or DELETE)
# ----------------------------
@mapper_editor_bp.route('/delete/<string:m_type>/<path:id_val>', methods=['POST'])
@login_required
@mapping_access_required
def manual_delete(m_type, id_val):
    try:
        if m_type == 'modality':
            record = AETitleModalityMap.query.get(id_val)
        else:
            record = ProcedureDurationMap.query.get(id_val)
            
        if record:
            db.session.delete(record)
            db.session.commit()
            flash('Record deleted.', 'info')
    except Exception as e:
        db.session.rollback()
        flash(f'Delete failed: {str(e)}', 'danger')
        
    return redirect(url_for('mapper_editor.mapping_list'))
