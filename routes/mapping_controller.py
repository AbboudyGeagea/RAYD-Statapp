from flask import Blueprint, request, render_template, flash, redirect, url_for, jsonify, abort, Response
from flask_login import login_required, current_user
# Import the CLASS names from your db file
from db import db, AETitleModalityMap, ProcedureDurationMap, DeviceException, DeviceWeeklySchedule, user_has_page
from utils.permissions import permission_required
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
@permission_required('can_export')
def export_modality_csv():
    if current_user.role not in ('admin', 'viewer', 'viewer2') and not user_has_page(current_user, 'mapping'): return abort(403)
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    rows = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['aetitle', 'modality', 'room_name', 'daily_capacity_minutes', 'display_aetitle'])
    for r in rows:
        sched = next((s for s in r.weekly_schedules if s.day_of_week == 0), None)
        cap = sched.std_opening_minutes if sched else 720
        w.writerow([r.aetitle, r.modality, r.room_name or '', cap, r.display_aetitle or ''])
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=modality_map.csv'}
    )


@mapping_bp.route('/export/procedure')
@login_required
@permission_required('can_export')
def export_procedure_csv():
    if current_user.role not in ('admin', 'viewer', 'viewer2') and not user_has_page(current_user, 'mapping'): return abort(403)
    from flask import current_app, jsonify
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403
    rows = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['procedure_code', 'procedure_name', 'duration_minutes', 'clinical_rvu', 'technical_rvu', 'modality'])
    for r in rows:
        w.writerow([r.procedure_code, r.procedure_name or '', r.duration_minutes,
                    r.clinical_rvu, r.technical_rvu, r.modality or ''])
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=procedure_map.csv'}
    )


@mapping_bp.route('', methods=['GET'])
@login_required
@permission_required('can_configure')
def mapping_page():
    if current_user.role not in ('admin', 'viewer', 'viewer2') and not user_has_page(current_user, 'mapping'): return abort(403)

    modality_mappings = AETitleModalityMap.query.order_by(AETitleModalityMap.aetitle).all()

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

    # Fast count for the Procedures tab badge only — no etl_orders scan
    from sqlalchemy import text as _t
    try:
        review_count = db.session.execute(_t("""
            SELECT (SELECT COUNT(*) FROM procedure_canonical_groups WHERE source = 'ai_suggested' AND approved = FALSE)
                 + (SELECT COUNT(*) FROM procedure_duplicate_candidates WHERE status = 'pending') AS total
        """)).scalar() or 0
    except Exception:
        review_count = 0

    return render_template(
        'mapping.html',
        modality_mappings=modality_mappings,
        exceptions_json=json.dumps(exceptions_lookup),
        review_count=int(review_count),
    )


@mapping_bp.route('/procedures-tab')
@login_required
def procedures_tab():
    """Lazy-loaded HTML fragment for the Procedures tab."""
    if current_user.role not in ('admin', 'viewer', 'viewer2') and not user_has_page(current_user, 'mapping'): return abort(403)

    from sqlalchemy import text as _t
    import json as _json

    duration_mappings = ProcedureDurationMap.query.order_by(ProcedureDurationMap.procedure_code).all()

    # Build a code→description lookup from etl_orders for procedures whose
    # procedure_name is NULL (i.e. never manually named).
    try:
        _name_rows = db.session.execute(_t("""
            SELECT UPPER(TRIM(proc_id)) AS code,
                   MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(proc_text))) AS name
            FROM etl_orders
            WHERE proc_id IS NOT NULL AND TRIM(proc_id) != ''
              AND proc_text IS NOT NULL AND TRIM(proc_text) != ''
            GROUP BY UPPER(TRIM(proc_id))
        """)).fetchall()
        proc_name_map = {r.code: r.name for r in _name_rows}
    except Exception:
        proc_name_map = {}

    try:
        conflicts = db.session.execute(
            _t("SELECT procedure_code, modalities, sample_count FROM procedure_modality_conflicts ORDER BY sample_count DESC")
        ).fetchall()
        conflict_codes = {c.procedure_code for c in conflicts}
    except Exception:
        conflicts = []
        conflict_codes = set()

    try:
        fuzzy_candidates = db.session.execute(
            _t("SELECT procedure_code, suggested_modality, match_score, matched_via FROM procedure_fuzzy_candidates ORDER BY match_score DESC")
        ).fetchall()
        fuzzy_map = {f.procedure_code: f for f in fuzzy_candidates}
    except Exception:
        fuzzy_map = {}

    # Single etl_orders scan — proc_descs MATERIALIZED and shared by both sub-queries
    try:
        row = db.session.execute(_t("""
            WITH proc_descs AS MATERIALIZED (
                SELECT UPPER(TRIM(proc_id)) AS procedure_code,
                       MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(proc_text))) AS proc_text
                FROM etl_orders
                WHERE proc_id IS NOT NULL AND TRIM(proc_id) != ''
                  AND proc_text IS NOT NULL AND TRIM(proc_text) != ''
                GROUP BY UPPER(TRIM(proc_id))
            ),
            ai_agg AS (
                SELECT g.id, g.canonical_name, g.cluster_confidence,
                       ARRAY_AGG(m.procedure_code ORDER BY m.procedure_code)                    AS member_codes,
                       ARRAY_AGG(COALESCE(d.proc_text, m.procedure_code) ORDER BY m.procedure_code) AS member_descs,
                       ARRAY_AGG(m.member_approved ORDER BY m.procedure_code)                   AS member_approved,
                       MODE() WITHIN GROUP (ORDER BY p.modality)                                AS group_modality
                FROM procedure_canonical_groups g
                JOIN procedure_canonical_members m ON m.group_id = g.id
                LEFT JOIN procedure_duration_map p ON UPPER(p.procedure_code) = UPPER(m.procedure_code)
                LEFT JOIN proc_descs d ON d.procedure_code = UPPER(TRIM(m.procedure_code))
                WHERE g.source = 'ai_suggested' AND g.approved = FALSE
                GROUP BY g.id, g.canonical_name, g.cluster_confidence
            )
            SELECT
                COALESCE(
                    (SELECT json_agg(row_to_json(a) ORDER BY a.cluster_confidence DESC NULLS LAST) FROM ai_agg a),
                    '[]'::json
                ) AS ai_groups,
                COALESCE(
                    (SELECT json_agg(sub)
                     FROM (
                         SELECT p.procedure_code,
                                COALESCE(d.proc_text, p.procedure_code) AS description,
                                p.modality
                         FROM procedure_duration_map p
                         LEFT JOIN proc_descs d ON d.procedure_code = UPPER(TRIM(p.procedure_code))
                         WHERE UPPER(p.procedure_code) NOT IN (SELECT UPPER(procedure_code) FROM procedure_canonical_members)
                         ORDER BY p.procedure_code
                         LIMIT 300
                     ) sub),
                    '[]'::json
                ) AS unclustered_procs
        """)).fetchone()

        def _as_json_list(value):
            # psycopg2 (via SQLAlchemy) auto-decodes json/jsonb result columns
            # into native Python objects, so `row[0]`/`row[1]` here are already
            # a list, NOT a JSON string. Calling json.loads() on that raises
            # TypeError: the JSON object must be str, bytes or bytearray, not
            # list — invisible in dev with empty tables (COALESCE('[]'::json)
            # decodes to [] which is falsy, so the buggy loads() call is
            # skipped), but guaranteed to fire on every request once ai_groups
            # or unclustered_procs actually has rows. Handle both shapes so
            # this is correct regardless of driver-level auto-decoding.
            if not value:
                return []
            if isinstance(value, (list, dict)):
                return value
            return _json.loads(value)

        ai_groups          = _as_json_list(row[0]) if row else []
        unclustered_procs  = _as_json_list(row[1]) if row else []
    except Exception:
        logging.getLogger("mapping").exception("procedures_tab: ai_groups/unclustered_procs query failed")
        ai_groups = []
        unclustered_procs = []

    return render_template(
        '_mapping_proc_partial.html',
        duration_mappings=duration_mappings,
        proc_name_map=proc_name_map,
        conflicts=conflicts,
        conflict_codes=conflict_codes,
        fuzzy_map=fuzzy_map,
        ai_groups=ai_groups,
        unclustered_procs=unclustered_procs,
    )


@mapping_bp.route('/upload/modality', methods=['POST'])
@login_required
def upload_modality_map():
    if current_user.role != 'admin': return abort(403)
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

            # Description (optional column)
            desc = str(row.get('description', '')).strip() if 'description' in df.columns and pd.notna(row.get('description')) else None

            # Display alias (optional column)
            display_ae = str(row.get('display_aetitle', '')).strip().upper() if 'display_aetitle' in df.columns and pd.notna(row.get('display_aetitle')) else None
            if display_ae == '':
                display_ae = None

            # 1. Sync Parent (AETitleModalityMap) — keep both tables in sync.
            # Case-insensitive lookup: aetitle_modality_map.aetitle has a case-SENSITIVE
            # UNIQUE constraint, but not every writer normalizes to uppercase (e.g. the
            # LAUMC RIS Modality ETL used to insert AE_TITLE verbatim) — an exact-case
            # filter_by() would silently miss an existing mixed-case row and insert a
            # duplicate instead of updating it.
            from sqlalchemy import func as _f
            parent = AETitleModalityMap.query.filter(_f.upper(AETitleModalityMap.aetitle) == ae).first()
            if not parent:
                parent = AETitleModalityMap(aetitle=ae, modality=mod, room_name=room, daily_capacity_minutes=cap, description=desc, display_aetitle=display_ae)
                db.session.add(parent)
            else:
                parent.modality = mod
                parent.daily_capacity_minutes = cap
                if room:
                    parent.room_name = room
                if desc is not None:
                    parent.description = desc or None
                if display_ae is not None:
                    parent.display_aetitle = display_ae
            
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
        logging.getLogger("mapping").exception("AE/modality CSV upload failed")
        flash(f"Upload Error: {str(e)}", "danger")
        
    return redirect(url_for('mapping.mapping_page'))
    
    
@mapping_bp.route('/upload/procedure', methods=['POST'])
@login_required
def upload_procedure_map():
    if current_user.role != 'admin': return abort(403)
    file = request.files.get('file')
    if not file: return redirect(url_for('mapping.mapping_page'))

    try:
        df = pd.read_csv(file)
        df.columns = [str(c).strip().lower() for c in df.columns]

        # LAYER OF PROTECTION: Schema Validation
        # Accept old single-column format (rvu_value) for backward compatibility
        has_split_rvu = 'clinical_rvu' in df.columns and 'technical_rvu' in df.columns
        has_legacy_rvu = 'rvu_value' in df.columns
        required = {'procedure_code', 'duration_minutes'}
        if not required.issubset(df.columns) or (not has_split_rvu and not has_legacy_rvu):
            flash("Upload Aborted: CSV headers must include procedure_code, duration_minutes, and either clinical_rvu + technical_rvu (or legacy rvu_value)", "danger")
            return redirect(url_for('mapping.mapping_page'))
        has_name_col = 'procedure_name' in df.columns

        # LAYER OF PROTECTION: Data Integrity Check (Dry Run)
        for idx, row in df.iterrows():
            try:
                _ = int(float(row['duration_minutes']))
                if has_split_rvu:
                    _ = float(row['clinical_rvu'])
                    _ = float(row['technical_rvu'])
                else:
                    _ = float(row['rvu_value'])
                if pd.isna(row['procedure_code']): raise ValueError("Empty Code")
            except Exception:
                flash(f"Protection Alert: Row {idx+2} contains invalid data. Entire upload canceled.", "danger")
                return redirect(url_for('mapping.mapping_page'))

        # If we passed the dry run, proceed to UPSERT
        for _, row in df.iterrows():
            p_code = str(row['procedure_code']).strip().upper()
            duration = int(float(row['duration_minutes']))
            if has_split_rvu:
                clinical_rvu  = float(row['clinical_rvu'])  or 1.0
                technical_rvu = float(row['technical_rvu']) or 1.0
            else:
                legacy = float(row['rvu_value']) or 1.0
                clinical_rvu  = legacy
                technical_rvu = legacy

            modality = str(row.get('modality', '')).strip().upper() if 'modality' in df.columns and pd.notna(row.get('modality')) else None
            name = str(row.get('procedure_name', '')).strip() if has_name_col and pd.notna(row.get('procedure_name')) else None

            mapping, created = get_or_create(ProcedureDurationMap, procedure_code=p_code)
            mapping.duration_minutes = duration
            mapping.clinical_rvu  = clinical_rvu
            mapping.technical_rvu = technical_rvu
            if modality:
                mapping.modality = modality
            if name:
                mapping.procedure_name = name

        db.session.commit()
        flash(f"Success: {len(df)} procedures verified and updated.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Procedure DB Error: {str(e)}", "danger")
    return redirect(url_for('mapping.mapping_page'))

@mapping_bp.route('/device/grid/save', methods=['POST'])
@login_required
def save_grid_changes():
    if current_user.role != 'admin': return abort(403)
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
        from utils.audit import log_event
        log_event('mapping_saved', category='config', resource_type='device_exceptions',
                  detail={'count': len(updates)})
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

# NEW: Inline edit for individual procedures (Point #4)
@mapping_bp.route('/procedure/update', methods=['POST'])
@login_required
def update_single_procedure():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import func as _f
    data = request.get_json(force=True)
    try:
        p_code = str(data['code']).strip().upper()
        # Case-insensitive lookup: procedure_code isn't guaranteed to be stored
        # uppercase (e.g. ETL Phase 8 Step 3 / live_feed.add_procedure insert the
        # raw-case code), so an exact match against the upper-cased incoming code
        # silently misses those rows and the edit appears to not persist.
        mapping = ProcedureDurationMap.query.filter(_f.upper(ProcedureDurationMap.procedure_code) == p_code).first()
        if mapping:
            dur = int(data.get('duration', 0))
            if dur > 0:
                mapping.duration_minutes = dur
            if 'clinical_rvu' in data and str(data['clinical_rvu']).strip():
                mapping.clinical_rvu = max(float(data['clinical_rvu']), 0)
            if 'technical_rvu' in data and str(data['technical_rvu']).strip():
                mapping.technical_rvu = max(float(data['technical_rvu']), 0)
            if 'modality' in data:
                mapping.modality = str(data['modality']).strip().upper() or None
            if 'name' in data:
                mapping.procedure_name = str(data['name']).strip() or None
            db.session.commit()
            return jsonify({"status": "success"})
        return jsonify({"status": "error", "message": "Procedure not found"}), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/procedure/delete', methods=['POST'])
@login_required
def delete_procedure():
    """Delete a procedure from the catalog (admin only)."""
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t, func as _f
    data = request.get_json(force=True)
    try:
        p_code = str(data['code']).strip().upper()
        # Case-insensitive lookup — see update_single_procedure for why procedure_code
        # can't be assumed to already be stored uppercase.
        mapping = ProcedureDurationMap.query.filter(_f.upper(ProcedureDurationMap.procedure_code) == p_code).first()
        if not mapping:
            return jsonify({"status": "error", "message": "Procedure not found"}), 404
        # Remove from canonical members first to avoid FK violation (case-insensitive —
        # members may have been added with different casing than procedure_duration_map)
        db.session.execute(_t(
            "DELETE FROM procedure_canonical_members WHERE UPPER(procedure_code) = :code"
        ), {"code": p_code})
        db.session.delete(mapping)
        db.session.commit()
        from utils.audit import log_event
        log_event('procedure_deleted', category='config', resource_type='procedure_duration_map',
                  detail={'procedure_code': p_code})
        return jsonify({"status": "success"})
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


@mapping_bp.route('/canonical/set-modality', methods=['POST'])
@login_required
def set_canonical_modality():
    """Apply a modality to all procedure codes in a canonical group."""
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        group_id = int(data['group_id'])
        modality = str(data.get('modality') or '').strip().upper() or None

        # Get all member codes for this group
        members = db.session.execute(
            _t("SELECT procedure_code FROM procedure_canonical_members WHERE group_id = :gid"),
            {"gid": group_id}
        ).fetchall()
        codes = [r[0].strip().upper() for r in members if r[0]]
        if not codes:
            return jsonify({"status": "error", "message": "Group has no members"}), 404

        # Case-insensitive match — procedure_duration_map.procedure_code isn't
        # guaranteed to already be uppercase (see update_single_procedure), so an
        # exact ANY() match against member codes can silently update zero rows.
        db.session.execute(_t("""
            UPDATE procedure_duration_map
            SET modality = :mod
            WHERE UPPER(procedure_code) = ANY(:codes)
        """), {"mod": modality, "codes": codes})
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
        pair_id    = int(data['pair_id'])
        canon_name = str(data.get('canonical_name', '')).strip()
        modality   = str(data.get('modality') or '').strip().upper() or None
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

        # Apply modality to both procedure codes if provided (case-insensitive —
        # see update_single_procedure for why procedure_code casing can't be assumed)
        if modality:
            db.session.execute(_t("""
                UPDATE procedure_duration_map
                SET modality = :mod
                WHERE UPPER(procedure_code) IN (UPPER(:a), UPPER(:b))
            """), {"mod": modality, "a": pair.code_a, "b": pair.code_b})

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

@mapping_bp.route('/canonical/approve-member', methods=['POST'])
@login_required
def approve_member():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        code = str(data['code']).strip().upper()
        db.session.execute(_t(
            "UPDATE procedure_canonical_members SET member_approved = TRUE WHERE procedure_code = :code"
        ), {"code": code})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/move-member', methods=['POST'])
@login_required
def move_member():
    """Move a code to a different cluster, or delete from all clusters (unclustered)."""
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        code     = str(data['code']).strip().upper()
        group_id = data.get('group_id')
        if group_id is None:
            db.session.execute(_t(
                "DELETE FROM procedure_canonical_members WHERE procedure_code = :code"
            ), {"code": code})
        else:
            db.session.execute(_t("""
                UPDATE procedure_canonical_members
                SET group_id = :gid, member_approved = NULL
                WHERE procedure_code = :code
            """), {"gid": int(group_id), "code": code})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/add-to-cluster', methods=['POST'])
@login_required
def add_to_cluster():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        code     = str(data['code']).strip().upper()
        group_id = int(data['group_id'])
        db.session.execute(_t("""
            INSERT INTO procedure_canonical_members (procedure_code, group_id, member_approved)
            VALUES (:code, :gid, NULL)
            ON CONFLICT (procedure_code) DO UPDATE SET group_id = EXCLUDED.group_id, member_approved = NULL
        """), {"code": code, "gid": group_id})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/add-cluster', methods=['POST'])
@login_required
def add_cluster():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        name = str(data.get('name', 'New Cluster')).strip()
        row = db.session.execute(_t("""
            INSERT INTO procedure_canonical_groups (canonical_name, approved, source, detected_at)
            VALUES (:name, FALSE, 'ai_suggested', NOW())
            RETURNING id
        """), {"name": name}).fetchone()
        db.session.commit()
        return jsonify({"status": "success", "group_id": row[0]})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/ae/delete', methods=['POST'])
@login_required
def delete_ae_entry():
    """Delete an AE title and its associated schedule / exceptions (CASCADE)."""
    if current_user.role != 'admin': return abort(403)
    data = request.get_json(force=True)
    ae = str(data.get('aetitle', '')).strip().upper()
    if not ae:
        return jsonify({"status": "error", "message": "aetitle required"}), 400
    try:
        # Case-insensitive lookup — see upload_modality_map() for why an exact-case
        # filter_by() can silently miss a pre-existing mixed-case row.
        from sqlalchemy import func as _f
        entry = AETitleModalityMap.query.filter(_f.upper(AETitleModalityMap.aetitle) == ae).first()
        if not entry:
            return jsonify({"status": "error", "message": "AE title not found"}), 404
        db.session.delete(entry)
        db.session.commit()
        from utils.audit import log_event
        log_event('ae_deleted', category='config', resource_type='aetitle_modality_map',
                  detail={'aetitle': ae})
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/ae/update', methods=['POST'])
@login_required
def update_ae_entry():
    """Inline update for an AE title row (modality, room_name, description)."""
    if current_user.role != 'admin': return abort(403)
    data = request.get_json(force=True)
    try:
        ae = str(data['aetitle']).strip().upper()
        # Case-insensitive lookup — see upload_modality_map() for why an exact-case
        # filter_by() can silently miss a pre-existing mixed-case row.
        from sqlalchemy import func as _f
        entry = AETitleModalityMap.query.filter(_f.upper(AETitleModalityMap.aetitle) == ae).first()
        if not entry:
            return jsonify({"status": "error", "message": "AE title not found"}), 404
        if 'modality' in data:
            entry.modality = str(data['modality']).strip().upper() or entry.modality
        if 'room_name' in data:
            entry.room_name = str(data['room_name']).strip() or None
        if 'description' in data:
            entry.description = str(data['description']).strip() or None
        if 'display_aetitle' in data:
            val = str(data['display_aetitle']).strip().upper()
            entry.display_aetitle = val or None
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@mapping_bp.route('/canonical/rename-cluster', methods=['POST'])
@login_required
def rename_cluster():
    if current_user.role != 'admin': return abort(403)
    from sqlalchemy import text as _t
    data = request.get_json(force=True)
    try:
        group_id = int(data['group_id'])
        name     = str(data['name']).strip()
        db.session.execute(_t(
            "UPDATE procedure_canonical_groups SET canonical_name = :name WHERE id = :id"
        ), {"name": name, "id": group_id})
        db.session.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# HL7 STATUS CODES — the studio surface for hl7_status_map
# ══════════════════════════════════════════════════════════════════════════════
#
# On the HL7 branch the exam lifecycle arrives as status codes in ORC, and which
# codes mean what varies by hospital. The seeded defaults (SC/AR/IP/CM) came from
# one integration; a site using anything else would previously have needed a
# migration or psql to be understood at all, because an unmapped code is rated
# CRITICAL by RAY7 and its message quarantined. That is the right behaviour — an
# unclassified transition silently dropped would be far worse — but it does mean
# the mapping has to be editable by an operator, not only by a developer.
#
# The part that makes this more than a CRUD screen is _unmapped_codes(). RAY7
# already records every code it could not classify, so rather than asking someone
# to guess what their RIS emits, the page lists the codes the site has actually
# sent, with counts and a first/last-seen window. Mapping becomes a response to
# evidence instead of documentation archaeology.

_CANONICAL_STATES = ['scheduled', 'arrived', 'started', 'completed', 'cancelled']

# Rank is derived from the state, never typed in. It is not a free choice — it is
# what makes RAY7's sequence rules and the projector agree on what "further along"
# means, and letting someone put arrived above completed would invert the
# lifecycle everywhere downstream. Cancellation is off the ladder, hence -1.
_STATE_RANK = {'scheduled': 40, 'arrived': 60, 'started': 70,
               'completed': 100, 'cancelled': -1}


def _invalidate_status_cache():
    """RAY7 caches the map for five minutes; drop it so an edit takes effect on
    the next message rather than whenever the TTL happens to lapse."""
    try:
        from utils.ray7 import _cache
        _cache['status_map'] = (0.0, None)
    except Exception:
        pass


def _unmapped_codes():
    """
    Codes RAY7 has seen and could not classify, newest first.

    Read from the findings rather than a separate log: UNKNOWN_STATUS_CODE already
    carries the sending app and both ORC values in its detail payload, so there is
    nothing extra to record and nothing that can drift out of step with what the
    engine actually did.
    """
    try:
        return [dict(r) for r in db.session.execute(_t("""
            SELECT detail ->> 'sending_app'   AS sending_app,
                   detail ->> 'order_control' AS order_control,
                   detail ->> 'order_status'  AS order_status,
                   COUNT(*)                   AS seen,
                   MIN(created_at)            AS first_seen,
                   MAX(created_at)            AS last_seen
              FROM ray7_findings
             WHERE rule_code = 'UNKNOWN_STATUS_CODE'
               AND detail ->> 'order_status' IS NOT NULL
             GROUP BY 1, 2, 3
             ORDER BY MAX(created_at) DESC
             LIMIT 50
        """)).mappings().all()]
    except Exception:
        logging.getLogger("MAPPING").exception("could not read unmapped status codes")
        return []


@mapping_bp.route('/status-codes-tab')
@login_required
def status_codes_tab():
    """Lazy-loaded HTML fragment for the HL7 Status Codes tab."""
    if current_user.role not in ('admin', 'viewer', 'viewer2') \
            and not user_has_page(current_user, 'mapping'):
        return abort(403)
    try:
        rows = [dict(r) for r in db.session.execute(_t("""
            SELECT id, sending_app, order_control, order_status, canonical_state,
                   ladder_rank, active, notes
              FROM hl7_status_map
             ORDER BY ladder_rank DESC, order_status
        """)).mappings().all()]
    except Exception:
        logging.getLogger("MAPPING").exception("could not load hl7_status_map")
        rows = []
    return render_template('_status_codes_tab.html', rows=rows,
                           unmapped=_unmapped_codes(), states=_CANONICAL_STATES)


@mapping_bp.route('/status-code/save', methods=['POST'])
@login_required
@permission_required('can_configure')
def save_status_code():
    """Create or update one mapping row."""
    if current_user.role != 'admin':
        return abort(403)
    d = request.get_json() or {}
    state = (d.get('canonical_state') or '').strip()
    if state not in _CANONICAL_STATES:
        return jsonify({'error': 'unknown canonical state'}), 400
    code = (d.get('order_status') or '').strip().upper()
    if not code:
        return jsonify({'error': 'order status code is required'}), 400

    try:
        db.session.execute(_t("""
            INSERT INTO hl7_status_map
                (sending_app, order_control, order_status, canonical_state,
                 ladder_rank, active, notes, updated_at)
            VALUES (:app, :ctl, :status, :state, :rank, :active, :notes, NOW())
            ON CONFLICT (sending_app, order_control, order_status) DO UPDATE SET
                canonical_state = EXCLUDED.canonical_state,
                ladder_rank     = EXCLUDED.ladder_rank,
                active          = EXCLUDED.active,
                notes           = EXCLUDED.notes,
                updated_at      = NOW()
        """), {
            'app':    (d.get('sending_app') or '').strip(),
            'ctl':    (d.get('order_control') or '').strip(),
            'status': code,
            'state':  state,
            'rank':   _STATE_RANK[state],
            'active': bool(d.get('active', True)),
            'notes':  (d.get('notes') or '').strip() or None,
        })
        db.session.commit()
        _invalidate_status_cache()
        return jsonify({
            'status': 'ok',
            'note': 'Mapping saved. Messages already quarantined for this code are not '
                    're-admitted automatically — replay them with: python app.py -m',
        })
    except Exception as e:
        db.session.rollback()
        logging.getLogger("MAPPING").exception("status map save failed")
        return jsonify({'error': str(e)[:200]}), 500


@mapping_bp.route('/status-code/delete', methods=['POST'])
@login_required
@permission_required('can_configure')
def delete_status_code():
    """
    Remove a mapping.

    Worth understanding before clicking: any future message carrying this code
    becomes UNKNOWN_STATUS_CODE, which RAY7 rates critical and quarantines. To
    retire a code without that consequence, set it inactive instead — the row
    stays and history keeps the meaning it was ingested with.
    """
    if current_user.role != 'admin':
        return abort(403)
    row_id = (request.get_json() or {}).get('id')
    if not row_id:
        return jsonify({'error': 'id required'}), 400
    try:
        db.session.execute(_t("DELETE FROM hl7_status_map WHERE id = :id"), {'id': row_id})
        db.session.commit()
        _invalidate_status_cache()
        return jsonify({'status': 'ok'})
    except Exception as e:
        db.session.rollback()
        logging.getLogger("MAPPING").exception("status map delete failed")
        return jsonify({'error': str(e)[:200]}), 500


# ══════════════════════════════════════════════════════════════════════════════
# HL7 FIELD MAPPING — the studio surface for hl7_field_mappings
# ══════════════════════════════════════════════════════════════════════════════
#
# The half of the field-mapping feature that makes it usable by the people it was
# built for. The table and the engine landed first; without this an implementation
# engineer would still be editing rows in psql, which is the situation the whole
# feature exists to end.
#
# The test endpoint is the part that earns its keep. Field positions are guesses
# until proven against real traffic — three of the seeded ones are still marked
# PROVISIONAL — and the difference between "my rule fired" and "my rule matched
# nothing" is the entire question when tuning one. Both modes the operator asked
# for are supported: replay a message this site actually received, or paste a
# sample when no traffic has arrived yet (a first install, or a vendor's spec
# document).

_TRANSFORMS = ['text', 'upper', 'datetime', 'date', 'name_xpn', 'name_xcn', 'number']
_MESSAGE_KINDS = ['', 'adt', 'order', 'status', 'result']


@mapping_bp.route('/field-map-tab')
@login_required
def field_map_tab():
    """Lazy-loaded HTML fragment for the HL7 Field Mapping tab."""
    if current_user.role not in ('admin', 'viewer', 'viewer2') \
            and not user_has_page(current_user, 'mapping'):
        return abort(403)
    log = logging.getLogger("MAPPING")
    rows, targets, recent = [], [], []
    try:
        rows = [dict(r) for r in db.session.execute(_t("""
            SELECT m.id, m.sending_app, m.message_kind, m.target_kind, m.target_field,
                   m.segment, m.field_index, m.component_index, m.repeat_index,
                   m.priority, m.transform, m.active, m.notes, m.updated_at,
                   COALESCE(tg.is_dangerous, FALSE) AS is_dangerous,
                   tg.label, tg.data_type
              FROM hl7_field_mappings m
              LEFT JOIN hl7_field_targets tg
                     ON tg.target_kind = m.target_kind AND tg.target_field = m.target_field
             ORDER BY m.target_kind, m.target_field, m.priority
        """)).mappings().all()]

        targets = [dict(r) for r in db.session.execute(_t("""
            SELECT target_kind, target_field, data_type, label, description,
                   is_dangerous, sort_order
              FROM hl7_field_targets ORDER BY target_kind, sort_order, target_field
        """)).mappings().all()]

        # Offered for the "test against real traffic" picker. Newest first, and
        # labelled by type and sender so an engineer can find the message shape
        # they are actually trying to map.
        recent = [dict(r) for r in db.session.execute(_t("""
            SELECT id, message_type, sending_app, message_control_id, received_at
              FROM hl7_message_archive
             ORDER BY id DESC LIMIT 40
        """)).mappings().all()]
    except Exception:
        log.exception("could not load field mapping tab")

    return render_template('_field_map_tab.html', rows=rows, targets=targets,
                           recent=recent, transforms=_TRANSFORMS,
                           message_kinds=_MESSAGE_KINDS)


@mapping_bp.route('/field-map/save', methods=['POST'])
@login_required
@permission_required('can_configure')
def save_field_map():
    """
    Create or update one mapping.

    target_kind/target_field are checked against hl7_field_targets. That is not a
    policy restriction — the operator chose to allow direct etl_* targets — it is
    typo protection: a target that does not exist maps a value into nothing at
    all, and nothing anywhere would ever report that.
    """
    if current_user.role != 'admin':
        return abort(403)
    d = request.get_json() or {}
    log = logging.getLogger("MAPPING")

    kind  = (d.get('target_kind') or 'parsed').strip()
    field = (d.get('target_field') or '').strip()
    seg   = (d.get('segment') or '').strip().upper()
    try:
        fidx = int(d.get('field_index'))
    except (TypeError, ValueError):
        return jsonify({'error': 'field number must be a whole number'}), 400
    if fidx < 1:
        return jsonify({'error': 'field numbers start at 1'}), 400
    if not seg or not field:
        return jsonify({'error': 'segment and target are both required'}), 400

    transform = (d.get('transform') or 'text').strip()
    if transform not in _TRANSFORMS:
        return jsonify({'error': 'unknown transform'}), 400

    def _opt_int(key):
        v = d.get(key)
        if v in (None, '', 'null'):
            return None
        try:
            iv = int(v)
        except (TypeError, ValueError):
            return None
        return iv if iv > 0 else None

    try:
        tgt = db.session.execute(_t("""
            SELECT data_type, is_dangerous FROM hl7_field_targets
             WHERE target_kind = :k AND target_field = :f
        """), {'k': kind, 'f': field}).mappings().first()
        if not tgt:
            return jsonify({'error': f'unknown target {kind}.{field}'}), 400

        # A type mismatch is a warning, not a refusal. Writing a raw HL7 timestamp
        # into a date column without the matching transform is almost always a
        # mistake, but "almost always" is not "always", and the operator asked for
        # freedom here. Say so and save it.
        warning = None
        if tgt['data_type'] in ('datetime', 'date') and transform not in ('datetime', 'date'):
            warning = (f"{field} expects a {tgt['data_type']} but the transform is "
                       f"'{transform}'. The value will most likely be rejected at "
                       f"ingest. Consider the '{tgt['data_type']}' transform.")
        elif tgt['is_dangerous']:
            warning = (f"{field} is a high-impact target: a wrong value here changes "
                       f"derived figures without producing any error. Verify it with "
                       f"Test before relying on it, and remember replay can undo it.")

        db.session.execute(_t("""
            INSERT INTO hl7_field_mappings
                (sending_app, message_kind, target_kind, target_field, segment,
                 field_index, component_index, repeat_index, priority, transform,
                 active, notes, updated_by, updated_at)
            VALUES (:app, :kind, :tkind, :tfield, :seg, :fidx, :cidx, :ridx,
                    :prio, :transform, :active, :notes, :uid, NOW())
            ON CONFLICT (sending_app, message_kind, target_kind, target_field, priority)
            DO UPDATE SET
                segment         = EXCLUDED.segment,
                field_index     = EXCLUDED.field_index,
                component_index = EXCLUDED.component_index,
                repeat_index    = EXCLUDED.repeat_index,
                transform       = EXCLUDED.transform,
                active          = EXCLUDED.active,
                notes           = EXCLUDED.notes,
                updated_by      = EXCLUDED.updated_by,
                updated_at      = NOW()
        """), {
            'app':   (d.get('sending_app') or '').strip(),
            'kind':  (d.get('message_kind') or '').strip(),
            'tkind': kind, 'tfield': field, 'seg': seg, 'fidx': fidx,
            'cidx':  _opt_int('component_index'),
            'ridx':  _opt_int('repeat_index'),
            'prio':  _opt_int('priority') or 100,
            'transform': transform,
            'active': bool(d.get('active', True)),
            'notes': (d.get('notes') or '').strip() or None,
            'uid':   current_user.id,
        })
        db.session.commit()

        from utils.hl7_fieldmap import invalidate
        invalidate()

        return jsonify({'status': 'ok', 'warning': warning})
    except Exception as e:
        db.session.rollback()
        log.exception("field map save failed")
        return jsonify({'error': str(e)[:200]}), 500


@mapping_bp.route('/field-map/delete', methods=['POST'])
@login_required
@permission_required('can_configure')
def delete_field_map():
    """
    Remove a mapping.

    Harmless by design: the parser falls back to its built-in position, so
    deleting a row restores the product default rather than leaving a gap.
    """
    if current_user.role != 'admin':
        return abort(403)
    row_id = (request.get_json() or {}).get('id')
    if not row_id:
        return jsonify({'error': 'id required'}), 400
    try:
        db.session.execute(_t("DELETE FROM hl7_field_mappings WHERE id = :id"),
                           {'id': row_id})
        db.session.commit()
        from utils.hl7_fieldmap import invalidate
        invalidate()
        return jsonify({'status': 'ok'})
    except Exception as e:
        db.session.rollback()
        logging.getLogger("MAPPING").exception("field map delete failed")
        return jsonify({'error': str(e)[:200]}), 500


@mapping_bp.route('/field-map/test', methods=['POST'])
@login_required
def test_field_map():
    """
    Run every applicable mapping against one message and report what it extracted.

    Two sources, both of which the operator asked for:
      archive_id  — a message this site really received. The better evidence, and
                    the payoff of storing every message verbatim.
      raw_message — a pasted sample, for a first install with no traffic yet, or
                    for checking a layout from a vendor's interface spec.

    Read-only. Nothing is written, nothing is screened, no findings are raised —
    an engineer must be able to experiment without leaving marks in the audit
    trail or tripping RAY7.
    """
    if current_user.role not in ('admin', 'viewer', 'viewer2') \
            and not user_has_page(current_user, 'mapping'):
        return abort(403)
    d = request.get_json() or {}
    raw = d.get('raw_message')

    try:
        if not raw and d.get('archive_id'):
            row = db.session.execute(_t(
                "SELECT raw_message FROM hl7_message_archive WHERE id = :id"
            ), {'id': int(d['archive_id'])}).first()
            if not row:
                return jsonify({'error': 'no archived message with that id'}), 404
            raw = row[0]
        if not raw or not raw.strip():
            return jsonify({'error': 'nothing to test — pick a message or paste one'}), 400

        from utils.hl7_fieldmap import preview
        from utils.hl7_parse import parse_message, split_segments

        out = preview(raw)
        msg = parse_message(raw)

        # The resulting message alongside the per-mapping breakdown: the mappings
        # explain HOW a value was found, this shows WHAT the pipeline would
        # actually carry forward, including fields no mapping touched.
        out['parsed'] = {
            k: (str(v) if v is not None else None)
            for k, v in (
                ('kind', msg.kind), ('accession_number', msg.accession_number),
                ('placer_order_number', msg.placer_order_number),
                ('patient_id', msg.patient_id), ('canonical_state', msg.canonical_state),
                ('ladder_rank', msg.ladder_rank), ('event_time', msg.event_time),
                ('performed_by_id', msg.performed_by_id),
                ('performed_by_name', msg.performed_by_name),
                ('aetitle', msg.aetitle), ('room_name', msg.room_name),
                ('modality', msg.modality), ('procedure_code', msg.procedure_code),
                ('procedure_text', msg.procedure_text),
                ('patient_class', msg.patient_class),
                ('patient_location', msg.patient_location),
                ('patient_name', msg.patient_name), ('birth_date', msg.birth_date),
                ('sex', msg.sex),
            )
        }
        out['segments'] = split_segments(raw)
        out['placeholders'] = list(msg.placeholders)
        return jsonify(out)
    except Exception as e:
        logging.getLogger("MAPPING").exception("field map test failed")
        return jsonify({'error': str(e)[:300]}), 500
