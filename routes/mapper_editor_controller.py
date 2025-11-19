#mapper_editor_controller.py
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify, current_app
from functools import wraps
import pandas as pd
from io import StringIO
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from db import db, AEModalityMap, ProcedureDurationMap
import logging
import os

# ----------------------------
# Configure Logging
# ----------------------------
LOG_FILE = os.path.join(os.getcwd(), "mapping_upload.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ----------------------------
# Define Blueprint
# ----------------------------
mapper_editor_bp = Blueprint('mapper_editor', __name__)

# ----------------------------
# Access Control Decorator
# ----------------------------
def mapping_access_required(f):
    """Ensure user is logged in and authorized for mapping management."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session or not session.get('logged_in'):
            flash('Please log in to access this utility.', 'danger')
            return redirect(url_for('admin.login'))

        user_role = session.get('role')
        if user_role not in ['dashboard', 'report_admin']:
            flash(f'Access denied. Your role ({user_role}) is not authorized for mapping management.', 'danger')
            return redirect(url_for('dashboard.index'))

        return f(*args, **kwargs)
    return decorated_function

# ----------------------------
# Validate CSV Headers
# ----------------------------
def validate_csv_headers(df, expected_headers):
    """Validate that CSV contains required headers."""
    missing = [col for col in expected_headers if col not in df.columns]
    return missing

# =========================================================================
# MAPPING UPLOAD AND DISPLAY ROUTE
# =========================================================================
@mapper_editor_bp.route('/mapping-upload', methods=['GET', 'POST'])
@mapping_access_required
def mapping_upload():
    """
    Handles display of current mappings and upload/processing of new mapping CSV files.
    Supports AE Title To Modality and Procedure Code To Duration mappings.
    """
    if request.method == 'POST':
        mapping_type = request.form.get('mapping_type')

        if 'file' not in request.files:
            flash('No file part in the request.', 'danger')
            return redirect(url_for('admin.mapping_page'))

        file = request.files['file']
        if file.filename == '':
            flash('No file selected for upload.', 'danger')
            return redirect(url_for('admin.mapping_page'))

        try:
            # Read CSV into DataFrame
            stream = StringIO(file.stream.read().decode("UTF8"))
            df = pd.read_csv(stream)

            mapping_objects = []
            rows_processed = 0

            # ----------------------------
            # AE Title → Modality Mapping
            # ----------------------------
            if mapping_type == 'modality':
                expected_headers = ['aetitle', 'modality']
                missing = validate_csv_headers(df, expected_headers)
                if missing:
                    flash(f'Missing columns: {missing}. Please check CSV header.', 'danger')
                    return redirect(url_for('admin.mapping_page'))

                df = df[expected_headers].dropna()
                for _, row in df.iterrows():
                    mapping_objects.append({
                        'aetitle': row['aetitle'],
                        'modality': row['modality']
                    })
                    rows_processed += 1

                db.session.begin_nested()
                db.session.bulk_update_mappings(AEModalityMap, mapping_objects)
                db.session.commit()

            # ----------------------------
            # Procedure Code to Duration Mapping
            # ----------------------------
            elif mapping_type == 'duration':
                expected_headers = ['procedurecode', 'duration_minutes']
                missing = validate_csv_headers(df, expected_headers)
                if missing:
                    flash(f'Missing columns: {missing}. Please check CSV header.', 'danger')
                    return redirect(url_for('admin.mapping_page'))

                df = df[expected_headers].dropna()
                for _, row in df.iterrows():
                    try:
                        duration = int(row['duration_minutes'])
                        if duration < 0:
                            raise ValueError("Duration must be non-negative")
                    except ValueError:
                        current_app.logger.warning(f"Skipping invalid duration: {row['duration_minutes']}")
                        continue

                    mapping_objects.append({
                        'procedurecode': row['procedurecode'],
                        'duration_minutes': duration
                    })
                    rows_processed += 1

                db.session.begin_nested()
                db.session.bulk_update_mappings(ProcedureDurationMap, mapping_objects)
                db.session.commit()

            else:
                flash('Invalid mapping type specified.', 'danger')
                return redirect(url_for('admin.mapping_page'))

            # Log upload details
            logging.info(f"User: {session.get('username')} uploaded {rows_processed} rows for {mapping_type} mapping.")
            flash(f'Successfully updated/inserted {rows_processed} {mapping_type} mapping rows.', 'success')

        except KeyError as e:
            db.session.rollback()
            flash(f'Missing expected column: {e}.', 'danger')
        except IntegrityError:
            db.session.rollback()
            flash('Database integrity error. Ensure no duplicate primary keys.', 'danger')
        except SQLAlchemyError as e:
            db.session.rollback()
            flash(f'Database error: {e}', 'danger')
        except Exception as e:
            db.session.rollback()
            flash(f'Unexpected error: {e}', 'danger')

        return redirect(url_for('admin.mapping_page'))

    # ----------------------------
    # GET Request → Display Current Mappings
    # ----------------------------
    modality_mappings = AEModalityMap.query.all()
    duration_mappings = ProcedureDurationMap.query.all()

    return render_template('mapping_upload.html',
                           modality_mappings=modality_mappings,
                           duration_mappings=duration_mappings,
                           role=session.get('role'))

# =========================================================================
# AJAX ENDPOINT: EDIT MAPPING
# =========================================================================
@mapper_editor_bp.route('/mapping-edit', methods=['POST'])
@mapping_access_required
def mapping_edit():
    """
    Handles inline edit for AE Title → Modality or Procedure → Duration mappings.
    Expects JSON: { "mapping_type": "modality|duration", "id": <int>, "field": <str>, "value": <str> }
    """
    data = request.get_json()
    mapping_type = data.get('mapping_type')
    record_id = data.get('id')
    field = data.get('field')
    value = data.get('value')

    try:
        if mapping_type == 'modality':
            mapping = AEModalityMap.query.get(record_id)
            if not mapping:
                return jsonify({"status": "error", "message": "Mapping not found"}), 404
            if field not in ['aetitle', 'modality']:
                return jsonify({"status": "error", "message": "Invalid field"}), 400
            setattr(mapping, field, value)

        elif mapping_type == 'duration':
            mapping = ProcedureDurationMap.query.get(record_id)
            if not mapping:
                return jsonify({"status": "error", "message": "Mapping not found"}), 404
            if field == 'duration_minutes':
                try:
                    value = int(value)
                    if value < 0:
                        return jsonify({"status": "error", "message": "Duration must be non-negative"}), 400
                except ValueError:
                    return jsonify({"status": "error", "message": "Invalid duration"}), 400
            if field not in ['procedurecode', 'duration_minutes']:
                return jsonify({"status": "error", "message": "Invalid field"}), 400
            setattr(mapping, field, value)

        else:
            return jsonify({"status": "error", "message": "Invalid mapping type"}), 400

        db.session.commit()
        logging.info(f"User {session.get('username')} edited {mapping_type} ID {record_id}: {field} → {value}")
        return jsonify({"status": "success", "message": "Mapping updated successfully"})

    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================================================================
# AJAX ENDPOINT: DELETE MAPPING
# =========================================================================
@mapper_editor_bp.route('/mapping-delete', methods=['DELETE'])
@mapping_access_required
def mapping_delete():
    """
    Handles deletion of a mapping record.
    Expects JSON: { "mapping_type": "modality|duration", "id": <int> }
    """
    data = request.get_json()
    mapping_type = data.get('mapping_type')
    record_id = data.get('id')

    try:
        if mapping_type == 'modality':
            mapping = AEModalityMap.query.get(record_id)
        elif mapping_type == 'duration':
            mapping = ProcedureDurationMap.query.get(record_id)
        else:
            return jsonify({"status": "error", "message": "Invalid mapping type"}), 400

        if not mapping:
            return jsonify({"status": "error", "message": "Mapping not found"}), 404

        db.session.delete(mapping)
        db.session.commit()
        logging.info(f"User {session.get('username')} deleted {mapping_type} ID {record_id}")
        return jsonify({"status": "success", "message": "Mapping deleted successfully"})

    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
