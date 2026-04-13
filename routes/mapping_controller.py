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
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    rows = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['aetitle', 'modality', 'room_name', 'daily_capacity_minutes'])
    for r in rows:
        sched = next((s for s in r.weekly_schedules if s.day_of_week == 0), None)
        cap = sched.std_opening_minutes if sched else 720
        w.writerow([r.aetitle, r.modality, r.room_name or '', cap])
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
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    rows = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['procedure_code', 'duration_minutes', 'rvu_value', 'modality'])
    for r in rows:
        w.writerow([r.procedure_code, r.duration_minutes, r.rvu_value, r.modality or ''])
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
    
    # Procedure-modality conflicts detected by ETL
    from sqlalchemy import text as _t
    try:
        conflicts = db.session.execute(
            _t("SELECT procedure_code, modalities, sample_count FROM procedure_modality_conflicts ORDER BY sample_count DESC")
        ).fetchall()
        conflict_codes = {c.procedure_code for c in conflicts}
    except Exception:
        conflicts = []
        conflict_codes = set()

    # Fuzzy candidates (70-89% match) awaiting human confirmation
    try:
        fuzzy_candidates = db.session.execute(
            _t("SELECT procedure_code, suggested_modality, match_score, matched_via FROM procedure_fuzzy_candidates ORDER BY match_score DESC")
        ).fetchall()
        fuzzy_map = {f.procedure_code: f for f in fuzzy_candidates}
    except Exception:
        fuzzy_candidates = []
        fuzzy_map = {}

    # Canonical groups — approved groups with their member codes
    try:
        raw_groups = db.session.execute(_t("""
            SELECT g.id, g.canonical_name, g.approved, g.approved_by, g.approved_at,
                   ARRAY_AGG(m.procedure_code ORDER BY m.procedure_code) AS member_codes,
                   ARRAY_AGG(m.similarity_score ORDER BY m.procedure_code) AS scores
            FROM procedure_canonical_groups g
            JOIN procedure_canonical_members m ON m.group_id = g.id
            GROUP BY g.id, g.canonical_name, g.approved, g.approved_by, g.approved_at
            ORDER BY g.approved ASC, g.detected_at DESC
        """)).fetchall()
        canonical_groups = [dict(r._mapping) for r in raw_groups]
    except Exception:
        canonical_groups = []

    # Pending pairs — candidate duplicates awaiting human review
    try:
        pending_pairs = db.session.execute(_t("""
            SELECT id, code_a, code_b,
                   code_similarity, desc_similarity,
                   desc_a, desc_b
            FROM procedure_duplicate_candidates
            WHERE status = 'pending'
            ORDER BY desc_similarity DESC, code_similarity DESC
        """)).fetchall()
        pending_pairs = [dict(r._mapping) for r in pending_pairs]
    except Exception:
        pending_pairs = []

    return render_template(
        'mapping.html',
        modality_mappings=modality_mappings,
        duration_mappings=duration_mappings,
        exceptions_json=json.dumps(exceptions_lookup),
        conflicts=conflicts,
        conflict_codes=conflict_codes,
        fuzzy_map=fuzzy_map,
        canonical_groups=canonical_groups,
        pending_pairs=pending_pairs,
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

            # Room name (optional column)
            room = str(row.get('room_name', '')).strip() if 'room_name' in df.columns and pd.notna(row.get('room_name')) else None

            # 1. Sync Parent (AETitleModalityMap) — keep both tables in sync
            parent = AETitleModalityMap.query.filter_by(aetitle=ae).first()
            if not parent:
                parent = AETitleModalityMap(aetitle=ae, modality=mod, room_name=room, daily_capacity_minutes=cap)
                db.session.add(parent)
            else:
                parent.modality = mod
                parent.daily_capacity_minutes = cap
                if room:
                    parent.room_name = room
            
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
            flash("Upload Aborted: CSV headers must include procedure_code, duration_minutes, rvu_value", "danger")
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

            modality = str(row.get('modality', '')).strip().upper() if 'modality' in df.columns and pd.notna(row.get('modality')) else None

            mapping, created = get_or_create(ProcedureDurationMap, procedure_code=p_code)
            mapping.duration_minutes = duration
            mapping.rvu_value = rvu
            if modality:
                mapping.modality = modality

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
            reason = str(item.get('reason', 'Grid Adjustment') or 'Grid Adjustment').strip()
            existing = DeviceException.query.filter_by(aetitle=ae, exception_date=exc_date).first()
            if existing:
                existing.actual_opening_minutes = val
                if reason and reason != 'Grid Adjustment':
                    existing.reason = reason
            else:
                db.session.add(DeviceException(
                    aetitle=ae,
                    exception_date=exc_date,
                    actual_opening_minutes=val,
                    reason=reason
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
            dur = int(data.get('duration', 0))
            if dur > 0:
                mapping.duration_minutes = dur
            rvu = data.get('rvu')
            if rvu is not None and str(rvu).strip():
                mapping.rvu_value = float(rvu)
            if 'modality' in data:
                mapping.modality = str(data['modality']).strip().upper() or None
            db.session.commit()
            return jsonify({"status": "success"})
        return jsonify({"status": "error", "message": "Procedure not found"}), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/approve', methods=['POST'])
@login_required
def approve_canonical_group():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        group_id = int(data['group_id'])
        canonical_name = str(data['canonical_name']).strip()
        if not canonical_name:
            return jsonify({"status": "error", "message": "Canonical name required"}), 400
        db.session.execute(_t("""
            UPDATE procedure_canonical_groups
            SET canonical_name = :name,
                approved = TRUE,
                approved_by = :user,
                approved_at = NOW()
            WHERE id = :id
        """), {"name": canonical_name, "user": current_user.username, "id": group_id})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/delete', methods=['POST'])
@login_required
def delete_canonical_group():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        group_id = int(data['group_id'])
        db.session.execute(_t("DELETE FROM procedure_canonical_groups WHERE id = :id"), {"id": group_id})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/confirm-pair', methods=['POST'])
@login_required
def confirm_pair():
    """Mark a candidate pair as confirmed and add both codes to a canonical group."""
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        pair_id   = int(data['pair_id'])
        canon_name = str(data.get('canonical_name', '')).strip()
        if not canon_name:
            return jsonify({"status": "error", "message": "Canonical name is required"}), 400

        # Fetch the pair
        pair = db.session.execute(
            _t("SELECT code_a, code_b, desc_similarity FROM procedure_duplicate_candidates WHERE id = :id"),
            {"id": pair_id}
        ).fetchone()
        if not pair:
            return jsonify({"status": "error", "message": "Pair not found"}), 404

        # Create a new canonical group
        group_row = db.session.execute(_t("""
            INSERT INTO procedure_canonical_groups (canonical_name, approved, approved_by, approved_at)
            VALUES (:name, TRUE, :user, NOW())
            RETURNING id
        """), {"name": canon_name, "user": current_user.username}).fetchone()
        group_id = group_row[0]

        # Add both codes as members (upsert — code may already be in another group)
        for code in (pair.code_a, pair.code_b):
            db.session.execute(_t("""
                INSERT INTO procedure_canonical_members (procedure_code, group_id, similarity_score)
                VALUES (:code, :gid, :score)
                ON CONFLICT (procedure_code) DO UPDATE
                    SET group_id = EXCLUDED.group_id,
                        similarity_score = EXCLUDED.similarity_score,
                        added_at = NOW()
            """), {"code": code, "gid": group_id, "score": float(pair.desc_similarity)})

        # Mark the pair as confirmed
        db.session.execute(_t("""
            UPDATE procedure_duplicate_candidates
            SET status = 'confirmed', group_id = :gid, reviewed_at = NOW()
            WHERE id = :id
        """), {"gid": group_id, "id": pair_id})

        db.session.commit()
        return jsonify({"status": "success", "group_id": group_id})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/reject-pair', methods=['POST'])
@login_required
def reject_pair():
    """Mark a candidate pair as rejected (different procedures)."""
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        pair_id = int(data['pair_id'])
        db.session.execute(_t("""
            UPDATE procedure_duplicate_candidates
            SET status = 'rejected', reviewed_at = NOW()
            WHERE id = :id
        """), {"id": pair_id})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
