"""
routes/patient_journey.py
-------------------------
Patient Journey: the lifecycle of one encounter, from order through technician
exam, PACS arrival and each reporting milestone.

Moved out of Report 25's tabs into its own page under Operations (operator
request, 2026-10-05). Access uses the "patient_journey" page key, which
viewer/viewer2 get by default and an admin can grant to anyone else.
"""

from datetime import datetime as _dt

from flask import Blueprint, render_template, request, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, user_has_page

patient_journey_bp = Blueprint("patient_journey", __name__)


def _require_access():
    if not (current_user.role == "admin" or user_has_page(current_user, "patient_journey")):
        abort(403)


@patient_journey_bp.route("/patient-journey")
@login_required
def patient_journey_page():
    _require_access()
    return render_template("patient_journey.html")


@patient_journey_bp.route("/patient-journey/data")
@login_required
def patient_journey_api():
    _require_access()
    pid       = (request.args.get('pid', '') or '').strip()
    accession = (request.args.get('accession', '') or '').strip()

    if not pid and not accession:
        return jsonify({'studies': [], 'error': 'Provide patient ID or accession number'})

    try:
        accessions = set()

        # ── Find accessions by accession number ───────────────────────────────
        if accession:
            rows = db.session.execute(text(
                "SELECT DISTINCT accession_number FROM etl_didb_studies "
                "WHERE accession_number ILIKE :acc LIMIT 15"
            ), {'acc': f'%{accession}%'}).fetchall()
            accessions.update(r[0] for r in rows if r[0])

        # ── Find accessions by patient ID: HL7 orders, then the PACS patient ──
        if pid:
            try:
                rows = db.session.execute(text(
                    "SELECT DISTINCT accession_number FROM hl7_orders "
                    "WHERE patient_id ILIKE :pid AND accession_number IS NOT NULL LIMIT 20"
                ), {'pid': f'%{pid}%'}).fetchall()
                accessions.update(r[0] for r in rows if r[0])
            except Exception:
                db.session.rollback()
            # HL7 orders only exist from April 2026; older studies are found
            # through the PACS patient ID (etl_patient_view.fallback_id).
            rows = db.session.execute(text(
                "SELECT DISTINCT s.accession_number FROM etl_didb_studies s "
                "JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid "
                "WHERE p.fallback_id ILIKE :pid AND s.accession_number IS NOT NULL LIMIT 20"
            ), {'pid': f'%{pid}%'}).fetchall()
            accessions.update(r[0] for r in rows if r[0])

        if not accessions:
            return jsonify({'studies': [], 'error': None, 'message': 'No matching studies found'})

        accn_list = list(accessions)[:15]

        # ── Batch fetch studies (1 query for all accessions) ─────────────────
        study_rows = db.session.execute(text("""
            SELECT DISTINCT ON (s.accession_number)
                s.accession_number,
                s.study_date::text                                                AS study_date,
                s.study_time,
                COALESCE(s.study_description, '')                                 AS study_description,
                COALESCE(m.modality, s.study_modality, 'Unknown')                 AS modality,
                COALESCE(s.patient_class, '')                                     AS patient_class,
                COALESCE(s.patient_location, '')                                  AS patient_location,
                s.insert_time,
                s.rep_prelim_timestamp,
                s.rep_transcribed_timestamp,
                s.rep_final_timestamp,
                NULLIF(TRIM(CONCAT(
                    COALESCE(s.signing_physician_first_name,''), ' ',
                    COALESCE(s.signing_physician_last_name,'')
                )), '')                                                            AS radiologist,
                s.rep_final_signed_by,
                p.fallback_id                                                      AS pacs_patient_id
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m
                ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid
            WHERE s.accession_number = ANY(:accns)
              AND COALESCE(m.modality, s.study_modality, '') != 'SR'
        """), {'accns': accn_list}).mappings().fetchall()
        studies_map = {r['accession_number']: dict(r) for r in study_rows}

        # ── Batch fetch hl7_orders (1 query for all accessions) ──────────────
        orders_map = {}  # accn -> list of order dicts
        try:
            order_rows = db.session.execute(text("""
                SELECT
                    accession_number,
                    received_at,
                    scheduled_datetime,
                    done_at,
                    done_by,
                    order_status,
                    COALESCE(procedure_text, procedure_code, '') AS procedure,
                    modality   AS order_modality,
                    patient_id AS order_pid
                FROM hl7_orders
                WHERE accession_number = ANY(:accns)
                ORDER BY accession_number, received_at NULLS LAST
            """), {'accns': accn_list}).mappings().fetchall()
            for r in order_rows:
                orders_map.setdefault(r['accession_number'], []).append(dict(r))
        except Exception:
            db.session.rollback()

        # ── Build timeline per accession (pure Python, no more DB calls) ─────
        def _ev(events, ts, ev_type, label, detail='', by=None):
            if ts is None:
                return
            events.append({
                'ts':     str(ts),
                'type':   ev_type,
                'label':  label,
                'detail': detail,
                'by':     str(by) if by else None,
            })

        results = []
        for accn in accn_list:
            study  = studies_map.get(accn)
            if not study:
                continue
            orders = orders_map.get(accn, [])

            events  = []
            pid_val = None
            for o in orders:
                pid_val = pid_val or o.get('order_pid')
                _ev(events, o.get('received_at'),       'order_received', 'Order Received',
                    o.get('procedure') or '')
                _ev(events, o.get('scheduled_datetime'), 'scheduled',      'Exam Scheduled',
                    f"Status: {o.get('order_status') or '?'}")
                _ev(events, o.get('done_at'),            'tech_done',      'Exam Completed by Tech',
                    f"Modality: {o.get('order_modality') or ''}",
                    o.get('done_by'))

            _ev(events, study.get('insert_time'),               'pacs_in',     'Arrived in PACS',
                f"Modality: {study.get('modality','')}")
            _ev(events, study.get('rep_prelim_timestamp'),      'prelim',      'Preliminary Report', '')
            _ev(events, study.get('rep_transcribed_timestamp'), 'transcribed', 'Transcribed', '')
            _ev(events, study.get('rep_final_timestamp'),       'final',       'Final Report Signed',
                '', study.get('radiologist') or study.get('rep_final_signed_by'))

            events.sort(key=lambda x: x['ts'])
            for i in range(1, len(events)):
                try:
                    t1 = _dt.fromisoformat(str(events[i-1]['ts']).replace('Z', '').split('.')[0])
                    t2 = _dt.fromisoformat(str(events[i]['ts']).replace('Z', '').split('.')[0])
                    events[i]['gap_min'] = round((t2 - t1).total_seconds() / 60)
                except Exception:
                    events[i]['gap_min'] = None

            results.append({
                'accession':        accn,
                'study_date':       study.get('study_date', ''),
                'modality':         study.get('modality', ''),
                'patient_id':       pid_val or study.get('pacs_patient_id') or '',
                'patient_class':    study.get('patient_class', ''),
                'patient_location': study.get('patient_location', ''),
                'description':      study.get('study_description', ''),
                'events':           events,
            })

        results.sort(key=lambda x: x['study_date'], reverse=True)
        return jsonify({'studies': results, 'error': None})

    except Exception as e:
        db.session.rollback()
        return jsonify({'studies': [], 'error': str(e)}), 500
