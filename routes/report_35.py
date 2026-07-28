"""
routes/report_35.py
--------------------
Report 35 — Technician TAT & Compliance Monitoring.

Standalone extraction of report_25's "Technicians" tab. Ports report_25's
technician-monitoring section from compute_bg_data() (routes/report_25.py,
lines ~817-1219 originally) — the section of that function that was already
independent of the shared report_template id=25 query used by report_25's
other tabs:

  - Technician monitoring: per-technician / per-modality TAT compliance,
    flagged exams (before-scheduled / too-early / overlap / too-late),
    never-marked-done overdue exams, daily TAT trend. Runs its own SQL
    directly against hl7_orders (+ procedure_duration_map).

NOTE: this report previously also carried a "Reporting Cadence Analysis"
(per-radiologist signing-time heatmap, derived from
etl_didb_studies.rep_final_timestamp) here, only because it happened to be
bundled into report_25's tech compute function for reuse. That data is
radiologist behaviour, not technician — it has been moved to Report 32
("Radiologists Performance", routes/report_32.py), which now computes it
independently with its own self-contained query.

The technician-monitoring section + the flagged-exam acknowledgement feature
are ported verbatim (same queries/logic), just re-homed under report id 35
instead of 25, with a dedicated cache slot (cache_get(35, ...)/cache_put(35,
...)) so this report's cache never collides with report_25's own (25) or its
deferred-background (9925) cache slots.

Unlike report_25, this report does NOT depend on report_template id=25 —
so unlike report_25 (which defers this exact data to a background
/report/25/bg endpoint to keep its much heavier main page fast), this
report computes synchronously in the main GET/POST view. The underlying
queries here are bounded by date range only (no joins against the big
report_template CTE), so there's no other heavy work on this page for it
to compete with — a plain synchronous route is simpler and was preferred
per the task brief.

Flagged-exam acknowledgements are stored in the shared `tech_flag_acknowledgements`
table (db.TechFlagAck), keyed by (accession_number, flag_date) — NOT by
report id. Acks made here are keyed the same way report_25's live page
keys them, so acknowledgement state is naturally shared/consistent between
this page and report_25's existing Technicians tab (no migration needed).

Register in registry.py:
    import routes.report_35
"""
import io
import logging
import statistics as _stats
from datetime import date, datetime

import pandas as pd
from flask import Blueprint, render_template, request, send_file, jsonify, abort
from flask_login import login_required, current_user
from sqlalchemy import text

from db import db, get_etl_cutoff_date
from routes.report_cache import cache_get, cache_put
from routes.insights_engine import run_tech_insights
from utils.site_resolver import default_site

logger = logging.getLogger("report_35")

report_35_bp = Blueprint("report_35", __name__)

_REPORT_ID = 35


def get_technician_tat_data(form_data):
    """
    Main data-fetch for Report 35. Returns a dict:
        { 'tech_data': {...}, 'tech_insights': [...] }

    Ported from report_25.compute_bg_data() — see module docstring for the
    line-range provenance of each section.
    """
    cached = cache_get(_REPORT_ID, form_data)
    if cached is not None:
        return cached

    go_live = get_etl_cutoff_date()
    start = form_data.get("start_date") or (go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01")
    end   = form_data.get("end_date") or date.today().strftime("%Y-%m-%d")

    params = {"start": start, "end": end}
    if form_data.get("mod_enabled") == "on" and form_data.getlist("modality"):
        params["modalities"] = tuple(form_data.getlist("modality"))

    # LAUMC site rule (see report_25.py's get_gold_standard_data for full rationale):
    # reports show RH (main site) only, SJH (satellite) excluded, for now.
    # etl_didb_studies.site_id is never populated; site is resolved via the
    # device instead: storing_ae -> aetitle_modality_map.site_id (RIS-authoritative).
    # default_site() resolves to None (filter skipped) on a non-LAUMC/single-site
    # install rather than zeroing every report.
    rh_site_id = default_site()
    if rh_site_id is not None:
        params["rh_site_id"] = rh_site_id

    # ── Technician monitoring ─────────────────────────────────────────────
    tech_data = {
        'summary': {}, 'by_technician': [], 'by_modality': [],
        'flagged': [], 'never_done': [], 'daily_trend': [],
    }
    _tech_completed_df = pd.DataFrame()
    try:
        _now = datetime.utcnow()

        # hl7_orders carries no storing_ae/site marker of its own — resolve site via
        # the matching PACS study by accession_number, same LAUMC site rule as above.
        _tech_site_join = ""
        if rh_site_id is not None:
            _tech_site_join = (
                "JOIN etl_didb_studies s2 ON s2.accession_number = o.accession_number "
                "JOIN aetitle_modality_map m2 ON UPPER(TRIM(s2.storing_ae)) = UPPER(TRIM(m2.aetitle)) "
                "AND m2.site_id = :rh_site_id"
            )
        tech_rows = db.session.execute(text(f"""
            SELECT
                o.accession_number, o.modality, o.procedure_code, o.done_by,
                o.scheduled_datetime, o.done_at, o.pacs_done_at,
                o.patient_class, o.patient_location,
                COALESCE(p.duration_minutes, 30) AS proc_duration
            FROM hl7_orders o
            LEFT JOIN procedure_duration_map p
                   ON UPPER(TRIM(o.procedure_code)) = UPPER(TRIM(p.procedure_code))
            {_tech_site_join}
            WHERE o.scheduled_datetime IS NOT NULL
              AND o.scheduled_datetime::date BETWEEN :start AND :end
              AND UPPER(TRIM(COALESCE(o.modality, ''))) != 'SCN'
              {"AND UPPER(TRIM(o.modality)) IN :modalities" if "modalities" in params else ""}
            ORDER BY o.modality, o.scheduled_datetime
        """), params).mappings().fetchall()

        if tech_rows:
            tdf = pd.DataFrame(tech_rows)
            tdf['proc_duration']      = pd.to_numeric(tdf['proc_duration'], errors='coerce').fillna(30)
            tdf['scheduled_datetime'] = pd.to_datetime(tdf['scheduled_datetime'])
            tdf['done_at']            = pd.to_datetime(tdf['done_at'],      errors='coerce')
            tdf['pacs_done_at']       = pd.to_datetime(tdf['pacs_done_at'], errors='coerce')
            # Prefer manual done_at; fall back to PACS scanner pacs_done_at
            tdf['effective_done_at']  = tdf['done_at'].fillna(tdf['pacs_done_at'])
            tdf['tat_min']            = (tdf['effective_done_at'] - tdf['scheduled_datetime']).dt.total_seconds() / 60.0
            tdf['pacs_tat_min']       = (tdf['pacs_done_at']      - tdf['scheduled_datetime']).dt.total_seconds() / 60.0

            completed = tdf[tdf['effective_done_at'].notna()].copy()
            pending   = tdf[tdf['effective_done_at'].isna()].copy()
            _tech_completed_df = completed

            # Pre-index ER orders: modality → list of (scheduled_datetime, patient_class, accession)
            # An order is "ER" if accession_number starts with '2XE' (case-insensitive)
            if 'patient_class' not in tdf.columns:
                tdf['patient_class'] = None
            er_rows = tdf[tdf['accession_number'].str.upper().str.startswith('2XE').fillna(False)].copy()
            er_by_modality = {}
            for _, er in er_rows.iterrows():
                er_by_modality.setdefault(str(er['modality'] or '').upper(), []).append(er)

            def _find_concurrent_er(row):
                """Return list of ER accessions whose scheduled_datetime falls inside row's exam window."""
                if pd.isna(row.get('effective_done_at')):
                    return []
                mod   = str(row.get('modality') or '').upper()
                t0    = row['scheduled_datetime']
                t1    = row['effective_done_at']
                acc   = row.get('accession_number')
                found = []
                for er in er_by_modality.get(mod, []):
                    if er['accession_number'] == acc:
                        continue
                    if t0 <= er['scheduled_datetime'] <= t1:
                        found.append({
                            'accession':     str(er['accession_number'] or ''),
                            'patient_class': str(er['patient_class'] or ''),
                        })
                return found

            overlap_accessions = set()
            for mod, grp in completed.groupby('modality'):
                grp = grp.sort_values('scheduled_datetime').reset_index()
                for i in range(len(grp) - 1):
                    cur, nxt = grp.iloc[i], grp.iloc[i + 1]
                    if pd.notna(cur['effective_done_at']) and cur['effective_done_at'] > nxt['scheduled_datetime']:
                        overlap_accessions.add(cur['accession_number'])

            flagged_rows = []
            for _, r in completed.iterrows():
                flags = []
                tat, dur = r['tat_min'], float(r['proc_duration'])
                if pd.isna(tat): continue
                if tat < 0: flags.append('before_scheduled')
                elif tat < dur * 0.5: flags.append('too_early')
                if r['accession_number'] in overlap_accessions: flags.append('overlap')
                if tat > dur * 2: flags.append('too_late')
                pacs_tat      = r.get('pacs_tat_min')
                er_concurrent = _find_concurrent_er(r) if 'too_late' in flags else []
                flagged_rows.append({
                    'accession':      str(r.get('accession_number') or ''),
                    'modality':       str(r.get('modality') or ''),
                    'procedure':      str(r.get('procedure_code') or ''),
                    'technician':     str(r['done_by']) if pd.notna(r.get('done_by')) else '',
                    'patient_class':  str(r.get('patient_class') or ''),
                    'scheduled_at':   r['scheduled_datetime'].strftime('%Y-%m-%d %H:%M'),
                    'done_at':        r['effective_done_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(r.get('effective_done_at')) else None,
                    'tat_min':        round(float(tat), 1),
                    'pacs_done_at':   r['pacs_done_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(r.get('pacs_done_at')) else None,
                    'pacs_tat_min':   round(float(pacs_tat), 1) if pd.notna(pacs_tat) else None,
                    'proc_duration':  int(dur),
                    'flags':          flags,
                    'er_concurrent':  er_concurrent,
                })
            tech_data['flagged'] = sorted([r for r in flagged_rows if r['flags']], key=lambda x: len(x['flags']), reverse=True)

            for _, r in pending.iterrows():
                deadline = r['scheduled_datetime'] + pd.Timedelta(minutes=float(r['proc_duration']))
                if deadline < pd.Timestamp(_now):
                    tech_data['never_done'].append({
                        'accession':    str(r.get('accession_number') or ''),
                        'modality':     str(r.get('modality') or ''),
                        'procedure':    str(r.get('procedure_code') or ''),
                        'scheduled_at': r['scheduled_datetime'].strftime('%Y-%m-%d %H:%M'),
                        'overdue_min':  round((pd.Timestamp(_now) - deadline).total_seconds() / 60, 1),
                    })

            flagged_accessions = {r['accession'] for r in tech_data['flagged']}
            tech_data['summary'] = {
                'total_scheduled':       len(tdf),
                'total_completed':       len(completed),
                'never_done':            len(tech_data['never_done']),
                'flag_before_scheduled': sum(1 for r in tech_data['flagged'] if 'before_scheduled' in r['flags']),
                'flag_too_early':        sum(1 for r in tech_data['flagged'] if 'too_early'        in r['flags']),
                'flag_overlap':          sum(1 for r in tech_data['flagged'] if 'overlap'          in r['flags']),
                'flag_too_late':         sum(1 for r in tech_data['flagged'] if 'too_late'         in r['flags']),
            }

            daily_trend = []
            if len(completed):
                completed = completed.copy()
                completed['_date'] = completed['scheduled_datetime'].dt.date
                for day, gdf in completed.groupby('_date'):
                    tats = gdf['tat_min'].dropna()
                    daily_trend.append({
                        'date':    str(day),
                        'avg_tat': round(float(tats.mean()), 1) if len(tats) else 0,
                        'count':   len(gdf),
                        'flags':   int(gdf['accession_number'].isin(flagged_accessions).sum()),
                    })
                daily_trend.sort(key=lambda x: x['date'])
            tech_data['daily_trend'] = daily_trend

            def _skew_insight(avg, median):
                if avg is None or median is None or median == 0: return None
                ratio = avg / median
                if ratio >= 2.0: return f"Avg is {ratio:.1f}× the median — outliers inflating average."
                if ratio >= 1.5: return f"Avg is {ratio:.1f}× the median — some delayed exams pulling up average."
                return None

            # Dept averages computed first so per-tech delta can reference them
            # TAT > 24h (1440 min) outliers are excluded from dept-level stats
            all_tats = completed['tat_min'].dropna()
            normal_dept_tats = all_tats[all_tats <= 1440]
            dept_avg = dept_median = None
            if len(normal_dept_tats):
                dept_avg    = round(float(normal_dept_tats.mean()),   1)
                dept_median = round(float(normal_dept_tats.median()), 1)
                tech_data['summary']['avg_tat']      = dept_avg
                tech_data['summary']['median_tat']   = dept_median
                tech_data['summary']['dept_insight'] = _skew_insight(dept_avg, dept_median)

            # Per-tech modality breakdown (pure pandas, no extra query)
            _done_with_tech = completed[
                completed['done_by'].notna() & completed['modality'].notna()
            ]
            tech_mod_breakdown = {}
            for (tech, mod), gdf in _done_with_tech.groupby(['done_by', 'modality']):
                tats  = gdf['tat_min'].dropna()
                ptats = gdf['pacs_tat_min'].dropna()
                tech_mod_breakdown.setdefault(str(tech), []).append({
                    'modality':     str(mod),
                    'count':        len(gdf),
                    'avg_tat':      round(float(tats.mean()),  1) if len(tats)  else None,
                    'avg_pacs_tat': round(float(ptats.mean()), 1) if len(ptats) else None,
                })

            for tech, gdf in completed[completed['done_by'].notna()].groupby('done_by'):
                all_tats_tech = gdf['tat_min'].dropna().tolist()
                # Separate outliers (> 24h) before computing stats
                normal_tats  = [v for v in all_tats_tech if v <= 1440]
                outlier_tats = [v for v in all_tats_tech if v >  1440]
                ptats = gdf['pacs_tat_min'].dropna()
                avg         = round(sum(normal_tats) / len(normal_tats), 1)        if normal_tats  else None
                median      = round(_stats.median(normal_tats), 1)                  if normal_tats  else None
                avg_pacs    = round(float(ptats.mean()),   1) if len(ptats) else None
                median_pacs = round(float(ptats.median()), 1) if len(ptats) else None
                flags_count = sum(1 for r in tech_data['flagged'] if r['technician'] == str(tech) and r['flags'])
                flag_rate   = round(flags_count / len(gdf) * 100, 1) if len(gdf) else 0.0
                dept_delta  = round(avg - dept_avg, 1) if avg is not None and dept_avg is not None else None
                mods        = sorted(tech_mod_breakdown.get(str(tech), []), key=lambda x: x['count'], reverse=True)
                top_mod     = mods[0]['modality'] if mods else None
                tech_data['by_technician'].append({
                    'name':            str(tech),
                    'count':           len(gdf),
                    'avg_tat':         avg,
                    'median_tat':      median,
                    'avg_pacs_tat':    avg_pacs,
                    'median_pacs_tat': median_pacs,
                    'flags':           flags_count,
                    'flag_rate':       flag_rate,
                    'dept_delta':      dept_delta,
                    'top_modality':    top_mod,
                    'modalities':      mods,
                    'outlier_count':   len(outlier_tats),
                    'insight':         _skew_insight(avg, median),
                })
            tech_data['by_technician'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)

            for mod, gdf in completed[completed['modality'].notna()].groupby('modality'):
                tats = gdf['tat_min'].dropna()
                avg    = round(float(tats.mean()),   1) if len(tats) else None
                median = round(float(tats.median()), 1) if len(tats) else None
                tech_data['by_modality'].append({
                    'modality': str(mod), 'count': len(gdf),
                    'avg_tat': avg, 'median_tat': median,
                    'insight': _skew_insight(avg, median),
                })
            tech_data['by_modality'].sort(key=lambda x: x['avg_tat'] if x['avg_tat'] is not None else 9999)

    except Exception:
        logger.exception("Failed to build technician monitoring data")
        db.session.rollback()

    # ── Acknowledgements for flagged exams ────────────────────────────────
    try:
        from db import TechFlagAck
        acks = TechFlagAck.query.filter(
            TechFlagAck.flag_date.between(start, end)
        ).all()
        tech_data['ack_map'] = {
            a.accession_number: {
                'by':   a.acknowledged_by_name,
                'at':   a.acknowledged_at.strftime('%Y-%m-%d %H:%M'),
                'note': a.note or '',
            }
            for a in acks
        }
    except Exception:
        db.session.rollback()
        tech_data['ack_map'] = {}

    # ── Insights (technician side only — radiologist insights need report_25's
    # main rad_cards, which this standalone report intentionally has no dependency on) ──
    tech_insights = []
    try:
        if not _tech_completed_df.empty:
            tech_insights = run_tech_insights(_tech_completed_df)
    except Exception:
        logger.exception("Failed to run technician insight signals")

    result = {
        'tech_data':      tech_data,
        'tech_insights':  tech_insights,
    }
    cache_put(_REPORT_ID, form_data, result)
    return result


@report_35_bp.route("/report/35", methods=["GET", "POST"])
@login_required
def report_35():
    run_report = 'start_date' in request.values

    go_live = get_etl_cutoff_date()
    display_start = go_live.strftime("%Y-%m-%d") if go_live else "2024-01-01"
    display_end   = date.today().strftime("%Y-%m-%d")

    filters = {
        "mod_enabled": request.values.get("mod_enabled") == "on",
        "modality":    request.values.getlist("modality"),
    }

    data = None
    if run_report:
        from utils.audit import log_event
        display_start = request.values.get('start_date', display_start)
        display_end   = request.values.get('end_date',   display_end)
        log_event('report_run', category='report', resource_type='report_35',
                  detail={'from': display_start, 'to': display_end})
        data = get_technician_tat_data(request.values)

    return render_template(
        "report_35.html",
        data=data,
        display_start=display_start,
        display_end=display_end,
        run_report=run_report,
        filters=filters,
    )


@report_35_bp.route("/report/35/export", methods=["POST"])
@login_required
def export_report_35():
    """CSV export of the technician monitoring sections. Ported from
    report_25.export_technician_25."""
    from flask import current_app
    from routes.registry import check_license_limit
    ok, msg = check_license_limit(current_app, 'export')
    if not ok:
        return jsonify({"error": msg}), 403

    bg = get_technician_tat_data(request.values)
    tech = bg.get('tech_data', {})

    sections = []
    if tech.get('summary'):
        sections.append(pd.DataFrame([tech['summary']]).assign(_section='Summary'))
    if tech.get('by_technician'):
        sections.append(pd.DataFrame(tech['by_technician']).assign(_section='By Technician'))
    if tech.get('by_modality'):
        sections.append(pd.DataFrame(tech['by_modality']).assign(_section='By Modality'))
    if tech.get('flagged'):
        df_f = pd.DataFrame(tech['flagged'])
        df_f['flags'] = df_f['flags'].apply(lambda x: ', '.join(x))
        sections.append(df_f.assign(_section='Flagged'))
    if tech.get('never_done'):
        sections.append(pd.DataFrame(tech['never_done']).assign(_section='Never Done'))

    if not sections:
        return "No data", 400

    output = io.BytesIO()
    pd.concat(sections, ignore_index=True).to_csv(output, index=False)
    output.seek(0)
    return send_file(output, mimetype='text/csv', as_attachment=True,
                     download_name=f"RAYD_Technician_TAT_{date.today()}.csv")


# ── Flag acknowledgement API ────────────────────────────────────────────
# Same tech_flag_acknowledgements table as report_25's /api/tech/flag/* routes
# (keyed by accession_number + flag_date, not by report id) — routed under a
# distinct path here since report_25's blueprint stays registered until its
# Technicians tab is retired, and Flask won't allow two blueprints to claim
# the same URL rule.

@report_35_bp.route('/report/35/flag/acknowledge', methods=['POST'])
@login_required
def ack_tech_flag_35():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    from db import TechFlagAck
    data      = request.get_json(silent=True) or {}
    accession = (data.get('accession') or '').strip()
    flag_date = (data.get('flag_date') or '').strip()
    flags     = data.get('flags', [])
    note      = (data.get('note') or '').strip() or None
    if not accession or not flag_date:
        return jsonify({'error': 'Missing fields'}), 400
    existing = TechFlagAck.query.filter_by(
        accession_number=accession, flag_date=flag_date
    ).first()
    if existing:
        existing.note                 = note
        existing.acknowledged_by_id   = current_user.id
        existing.acknowledged_by_name = current_user.username
        existing.acknowledged_at      = datetime.utcnow()
    else:
        db.session.add(TechFlagAck(
            accession_number=accession,
            flag_date=flag_date,
            flags=flags,
            note=note,
            acknowledged_by_id=current_user.id,
            acknowledged_by_name=current_user.username,
            acknowledged_at=datetime.utcnow(),
        ))
    db.session.commit()
    return jsonify({'ok': True})


@report_35_bp.route('/report/35/flag/unacknowledge', methods=['POST'])
@login_required
def unack_tech_flag_35():
    if current_user.role not in ('admin', 'viewer', 'viewer2'):
        abort(403)
    from db import TechFlagAck
    data      = request.get_json(silent=True) or {}
    accession = (data.get('accession') or '').strip()
    flag_date = (data.get('flag_date') or '').strip()
    if not accession or not flag_date:
        return jsonify({'error': 'Missing fields'}), 400
    ack = TechFlagAck.query.filter_by(
        accession_number=accession, flag_date=flag_date
    ).first()
    if ack:
        db.session.delete(ack)
        db.session.commit()
    return jsonify({'ok': True})


# ── Self-register ─────────────────────────────────────────────
from routes.report_registry import register_report
register_report(35, report_35_bp, report_35, export_report_35)
