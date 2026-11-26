"""
routes/super_report.py
----------------------
Super Report — dynamic filters from ETL tables, comparison period,
and rule-based plain-language narrative. No LLM API required.

Register in registry.py:
    from routes.super_report import super_report_bp
    app.register_blueprint(super_report_bp)
"""

import logging
import json
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db
from routes.insights_engine import run_dept_insights
from routes.report_cache import cache_get, cache_put
from utils.stats import _pct, _fmt
from utils.site_db import served_site_id  # the site this database serves (item 13a)
from utils.ae_display import ae_display_sql
from utils.radiologist_resolve import rad_alias_join_sql, rad_display_sql, rad_ok_sql

logger = logging.getLogger("SUPER_REPORT")
super_report_bp = Blueprint("super_report", __name__)


# ─────────────────────────────────────────────
#  PAGE ROUTE
# ─────────────────────────────────────────────


@super_report_bp.route("/viewer/super-report-page")
@login_required
def super_report_page():
    # Opens on the report start date (settings, migration 0149), as every report does;
    # it used to open on the last 30 days.
    from db import get_report_start_date
    from utils.roles import can_open_report

    start = get_report_start_date()
    return render_template(
        "super_report.html",
        report_start_date=start.isoformat() if start else "",
        start=start.isoformat() if start else "",
        end=datetime.now().date().isoformat(),
        panel=_panel(),
        # the lead time card links to Report 40 for whoever may open it
        lead_time_report=can_open_report(current_user, 40),
    )


# The Super Report's dropdowns on the shared left panel (_report_filter_panel.html), named
# as super_report() reads them. Their options come from /viewer/super-report/filters, which
# the page loads; each has an "Apply filter" tick (<name>_on) that the page honours when it
# builds the query. Protocol Name and Body Part (Series) are not offered: the query never
# applied them, so picking one changed nothing.
_PANEL_FILTERS = (
    ("modality", "Modality"),
    ("storing_ae", "AE Title"),
    ("study_status", "Study Status"),
    ("procedure_code", "Procedure"),
    ("body_part", "Body Part"),
    ("patient_class", "Patient Class"),
    ("patient_location", "Patient Location"),
    ("sex", "Sex"),
    ("report_status", "Report Status"),
    ("order_status", "Order Status"),
    ("order_modality", "Order Modality"),
    ("signing_physician", "Signing Physician"),
    ("referring_physician", "Referring Physician"),
)


def _panel():
    return [
        {
            "name": name,
            "toggle": f"{name}_on",
            "label": label,
            "values": [],
            "selected": [],
            "enabled": True,
        }
        for name, label in _PANEL_FILTERS
    ]


# ─────────────────────────────────────────────
#  SAVED REPORTS ENDPOINTS
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
#  CHOSEN BLOCKS (item 43, migration 0183)
# ─────────────────────────────────────────────
# One list per person, and one for everyone (settings capability), on the main database
# whichever site is picked (site_db.MAIN_TABLES; raw SQL on db.engine, CLAUDE.md rule 10).


def _layout_rows(user_id):
    with db.engine.connect() as c:
        rows = c.execute(
            text("SELECT scope_key, blocks FROM public.super_report_layouts WHERE scope_key IN (:me, 'everyone')"),
            {"me": f"user:{user_id}"},
        ).fetchall()
    return {k: v for k, v in rows}


@super_report_bp.route("/viewer/super-report/layout", methods=["GET"])
@login_required
def get_layout():
    from utils import super_report_blocks as sr_blocks
    from utils.roles import can

    rows = _layout_rows(current_user.id)
    mine, everyone = rows.get(f"user:{current_user.id}"), rows.get("everyone")
    return jsonify({
        "blocks": [{"key": k, "label": label} for k, label, _ in sr_blocks.BLOCKS],
        "mine": sr_blocks.clean(mine) if mine else None,
        "everyone": sr_blocks.clean(everyone) if everyone else None,
        "can_everyone": can(current_user, "settings"),
    })


@super_report_bp.route("/viewer/super-report/layout", methods=["POST", "DELETE"])
@login_required
def save_layout():
    from utils import super_report_blocks as sr_blocks
    from utils.roles import can

    body = request.get_json(silent=True) or {}
    scope = (body.get("scope") or request.args.get("scope") or "me").strip()
    if scope not in ("me", "everyone"):
        return jsonify({"error": "scope is me or everyone"}), 400
    if scope == "everyone" and not can(current_user, "settings"):
        return jsonify({"error": "Saving for everyone needs the settings capability"}), 403
    key = "everyone" if scope == "everyone" else f"user:{current_user.id}"
    with db.engine.begin() as c:
        if request.method == "DELETE":
            c.execute(text("DELETE FROM public.super_report_layouts WHERE scope_key = :k"), {"k": key})
            return jsonify({"success": True, "scope": scope, "blocks": None})
        raw = body.get("blocks")
        if not isinstance(raw, list) or not any(str(b) in sr_blocks.KEYS for b in raw):
            return jsonify({"error": "Tick at least one block"}), 400
        blocks = sr_blocks.clean(raw)
        c.execute(
            text("""INSERT INTO public.super_report_layouts (scope_key, blocks, updated_by, updated_at)
                    VALUES (:k, CAST(:b AS jsonb), :u, NOW())
                    ON CONFLICT (scope_key) DO UPDATE
                    SET blocks = EXCLUDED.blocks, updated_by = EXCLUDED.updated_by, updated_at = NOW()"""),
            {"k": key, "b": json.dumps(blocks), "u": current_user.id},
        )
    return jsonify({"success": True, "scope": scope, "blocks": blocks})


# ─────────────────────────────────────────────
#  SCHEDULED EMAILS (item 43, migration 0184, utils/super_report_email.py)
# ─────────────────────────────────────────────
# Settings capability, like Report Composer's schedules. In the picked site's database: an SJH
# schedule sends SJH's figures through SJH's mail server.


def _schedules_allowed():
    from utils.roles import can

    return can(current_user, "settings")


@super_report_bp.route("/viewer/super-report/schedules", methods=["GET"])
@login_required
def list_email_schedules():
    from utils import super_report_email as sre
    from utils.worklist_email import allowed_domains, is_ready, mail_config

    if not _schedules_allowed():
        return jsonify({"error": "Scheduled emails need the settings capability"}), 403
    cfg = mail_config()
    return jsonify({
        "schedules": sre.list_schedules(),
        "periods": [{"key": k, "label": v} for k, v in sre.PERIODS.items()],
        "mail_ready": is_ready(cfg),
        "allowed_domains": allowed_domains(cfg),
    })


@super_report_bp.route("/viewer/super-report/schedules", methods=["POST"])
@login_required
def save_email_schedule():
    from utils import super_report_email as sre
    from utils.worklist_email import ScheduleError

    if not _schedules_allowed():
        return jsonify({"error": "Scheduled emails need the settings capability"}), 403
    try:
        schedule_id = sre.save_schedule(request.get_json(silent=True) or {}, current_user.id)
    except ScheduleError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    return jsonify({"success": True, "id": schedule_id})


@super_report_bp.route("/viewer/super-report/schedules/<int:schedule_id>", methods=["DELETE"])
@login_required
def delete_email_schedule(schedule_id):
    from utils import super_report_email as sre

    if not _schedules_allowed():
        return jsonify({"error": "Scheduled emails need the settings capability"}), 403
    sre.delete_schedule(schedule_id)
    return jsonify({"success": True})


@super_report_bp.route("/viewer/super-report/schedules/<int:schedule_id>/send", methods=["POST"])
@login_required
def send_email_schedule_now(schedule_id):
    from utils import super_report_email as sre

    if not _schedules_allowed():
        return jsonify({"error": "Scheduled emails need the settings capability"}), 403
    s = sre.get_schedule(schedule_id)
    if not s:
        return jsonify({"error": "This schedule no longer exists"}), 404
    return jsonify(sre.send_schedule(s, started_by=current_user.id))


@super_report_bp.route("/viewer/super-report/schedules/<int:schedule_id>/runs", methods=["GET"])
@login_required
def email_schedule_runs(schedule_id):
    from utils import super_report_email as sre

    if not _schedules_allowed():
        return jsonify({"error": "Scheduled emails need the settings capability"}), 403
    return jsonify({"runs": sre.list_runs(schedule_id)})


@super_report_bp.route("/viewer/super-report/save", methods=["POST"])
@login_required
def save_super_report():
    """Save current filter preset under a user-defined name."""
    body = request.get_json() or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"success": False, "error": "Name is required"}), 400

    filter_json = body.get("filters", {})

    try:
        # Check for duplicate name for this user
        # Super Report saves have base_report_id = NULL (no backing report_template row)
        existing = db.session.execute(
            text(
                """
            SELECT id FROM saved_reports
            WHERE owner_user_id = :uid
              AND base_report_id IS NULL
              AND name = :name
        """
            ),
            {"uid": current_user.id, "name": name},
        ).fetchone()

        if existing:
            # Update existing
            db.session.execute(
                text(
                    """
                UPDATE saved_reports
                SET filter_json = :fj, updated_at = NOW()
                WHERE id = :id
            """
                ),
                {"fj": json.dumps(filter_json), "id": existing[0]},
            )
            saved_id = existing[0]
        else:
            # Insert new — base_report_id is NULL for Super Report presets
            result = db.session.execute(
                text(
                    """
                INSERT INTO saved_reports (name, owner_user_id, base_report_id, is_public, filter_json)
                VALUES (:name, :uid, NULL, FALSE, :fj)
                RETURNING id
            """
                ),
                {"name": name, "uid": current_user.id, "fj": json.dumps(filter_json)},
            )
            saved_id = result.fetchone()[0]

        db.session.commit()
        return jsonify({"success": True, "id": saved_id, "name": name})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Save report error: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/saved")
@login_required
def list_saved_reports():
    """List all saved Super Report presets for the current user."""
    try:
        rows = (
            db.session.execute(
                text(
                    """
            SELECT id, name, filter_json, created_at, updated_at
            FROM saved_reports
            WHERE owner_user_id = :uid
              AND base_report_id IS NULL
            ORDER BY updated_at DESC
        """
                ),
                {"uid": current_user.id},
            )
            .mappings()
            .fetchall()
        )

        return jsonify(
            [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "filters": (
                        r["filter_json"]
                        if isinstance(r["filter_json"], dict)
                        else json.loads(r["filter_json"] or "{}")
                    ),
                    "updated_at": (
                        r["updated_at"].strftime("%Y-%m-%d %H:%M")
                        if r["updated_at"]
                        else None
                    ),
                }
                for r in rows
            ]
        )

    except Exception as e:
        logger.error(f"List saved reports error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/saved/<int:report_id>", methods=["DELETE"])
@login_required
def delete_saved_report(report_id):
    """Delete a saved preset — only if owned by current user."""
    try:
        result = db.session.execute(
            text(
                """
            DELETE FROM saved_reports
            WHERE id = :id
              AND owner_user_id = :uid
              AND base_report_id IS NULL
        """
            ),
            {"id": report_id, "uid": current_user.id},
        )
        db.session.commit()

        if result.rowcount == 0:
            return jsonify({"success": False, "error": "Not found or not yours"}), 404
        return jsonify({"success": True})

    except Exception as e:
        db.session.rollback()
        logger.error(f"Delete saved report error: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/snapshots")
@login_required
def super_report_snapshots():
    """Return the latest pre-computed snapshot for each of the 3 standard periods."""
    try:
        rows = (
            db.session.execute(
                text(
                    """
            SELECT DISTINCT ON (period_label)
                period_label, period_start::text, period_end::text,
                computed_at, narrative, status
            FROM analytics_snapshots
            ORDER BY period_label, computed_at DESC
        """
                )
            )
            .mappings()
            .fetchall()
        )

        return jsonify(
            [
                {
                    "period_label": r["period_label"],
                    "period_start": r["period_start"],
                    "period_end": r["period_end"],
                    "computed_at": (
                        r["computed_at"].strftime("%d %b %Y %H:%M")
                        if r["computed_at"]
                        else None
                    ),
                    "narrative": r["narrative"] or "",
                    "status": r["status"],
                }
                for r in rows
            ]
        )

    except Exception as e:
        logger.error(f"Snapshots error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/filters")
@login_required
def super_report_filters():
    # 19 SELECT DISTINCT queries below are expensive on the full ETL tables and
    # never change within a TTL window -- cached like everything else in
    # routes/report_cache.py (fixed key, no request params to vary on).
    cached = cache_get("super_report_filters", {})
    if cached is not None:
        return jsonify(cached)

    try:

        def distinct(sql):
            rows = db.session.execute(text(sql)).fetchall()
            return sorted(
                [r[0] for r in rows if r[0] is not None and str(r[0]).strip() != ""]
            )

        data = {
            "modality": distinct(
                "SELECT DISTINCT COALESCE(m.modality, s.study_modality) FROM etl_didb_studies s LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle)) WHERE COALESCE(m.modality, s.study_modality) IS NOT NULL"
            ),
            # Only AE titles the Super Report can count: it keeps a study only when its
            # device is mapped to the default site (RH), so any other AE was offered and
            # always gave 0 (meeting of 2026-11-13). Spelled as stored, since the filter
            # compares the raw storing_ae.
            "storing_ae": distinct(
                """
                SELECT x.ae FROM (SELECT DISTINCT storing_ae AS ae FROM etl_didb_studies
                                  WHERE storing_ae IS NOT NULL) x
                WHERE rayd_served_site_id() IS NULL
                   OR EXISTS (SELECT 1 FROM aetitle_modality_map m
                              WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(x.ae))
                                AND m.site_id = rayd_served_site_id())"""
            ),
            "study_status": distinct(
                "SELECT DISTINCT study_status FROM etl_didb_studies WHERE study_status IS NOT NULL"
            ),
            "report_status": distinct(
                "SELECT DISTINCT report_status FROM etl_didb_studies WHERE report_status IS NOT NULL"
            ),
            "order_status": distinct(
                "SELECT DISTINCT order_status FROM etl_didb_studies WHERE order_status IS NOT NULL"
            ),
            "patient_class": distinct(
                "SELECT DISTINCT patient_class FROM etl_didb_studies WHERE patient_class IS NOT NULL"
            ),
            "patient_location": distinct(
                "SELECT DISTINCT patient_location FROM etl_didb_studies WHERE patient_location IS NOT NULL"
            ),
            "body_part": distinct(
                "SELECT DISTINCT study_body_part FROM etl_didb_studies WHERE study_body_part IS NOT NULL"
            ),
            "procedure_code": distinct(
                "SELECT DISTINCT procedure_code FROM etl_didb_studies WHERE procedure_code IS NOT NULL LIMIT 200"
            ),
            "signing_physician": distinct(
                "SELECT DISTINCT TRIM(CONCAT(signing_physician_first_name,' ',signing_physician_last_name)) FROM etl_didb_studies WHERE signing_physician_last_name IS NOT NULL AND signing_physician_last_name != ''"
            ),
            "referring_physician": distinct(
                "SELECT DISTINCT TRIM(CONCAT(referring_physician_first_name,' ',referring_physician_last_name)) FROM etl_didb_studies WHERE referring_physician_last_name IS NOT NULL AND referring_physician_last_name != ''"
            ),
            "sex": distinct(
                "SELECT DISTINCT sex FROM etl_patient_view WHERE sex IS NOT NULL"
            ),
            "order_control": distinct(
                "SELECT DISTINCT order_control FROM etl_orders WHERE order_control IS NOT NULL"
            ),
            "order_modality": distinct(
                "SELECT DISTINCT modality FROM etl_orders WHERE modality IS NOT NULL"
            ),
            # series modality / body part / protocol and age group are no longer lists here: the
            # query never applied the first three, and nothing on the page offers age group
            # (_request_filters, 2026-11-20). They were the costliest DISTINCTs of this endpoint.
            "ae_modality_map": dict(
                db.session.execute(
                    text(
                        "SELECT aetitle, modality FROM aetitle_modality_map WHERE aetitle IS NOT NULL AND modality IS NOT NULL"
                    )
                ).fetchall()
            ),
            # AE -> its RIS room name (utils.ae_display); AEs with no name are left out
            "ae_station_map": dict(
                db.session.execute(
                    text(
                        f"SELECT aetitle, {ae_display_sql('m', 'NULL')} FROM aetitle_modality_map m "
                        f"WHERE aetitle IS NOT NULL AND {ae_display_sql('m', 'NULL')} IS NOT NULL"
                    )
                ).fetchall()
            ),
            "procedure_name_map": dict(
                db.session.execute(
                    text(
                        "SELECT procedure_code, procedure_name FROM procedure_duration_map WHERE procedure_code IS NOT NULL AND procedure_name IS NOT NULL AND TRIM(procedure_name) != ''"
                    )
                ).fetchall()
            ),
        }
        cache_put("super_report_filters", {}, data)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Filters error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
#  MAIN ROUTE
# ─────────────────────────────────────────────


@super_report_bp.route("/viewer/super-report")
@login_required
def super_report():
    start = request.args.get("start")
    end = request.args.get("end")
    cmp_start = request.args.get("cmp_start")
    cmp_end = request.args.get("cmp_end")
    if not start or not end:
        return jsonify({"error": "start and end date required"}), 400

    # The server validates every date it is given, the comparison window included: the page's
    # updateCmpDates() is the single source of the AUTOMATIC comparison window (the same length,
    # ending the day before start) and sends it, but a hand-written URL or an old preset can
    # send anything. A date that does not parse used to fall through to delta = 30 with the
    # comparison dates left empty, so the previous period ran BETWEEN NULL AND NULL and every
    # change read as zero.
    try:
        d_start = datetime.strptime(start, "%Y-%m-%d")
        d_end = datetime.strptime(end, "%Y-%m-%d")
        if cmp_start and cmp_end:
            d_cmp_start = datetime.strptime(cmp_start, "%Y-%m-%d")
            d_cmp_end = datetime.strptime(cmp_end, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "dates must be YYYY-MM-DD"}), 400
    if d_end < d_start:
        return jsonify({"error": "the end date is before the start date"}), 400
    delta = (d_end - d_start).days + 1
    if not cmp_start or not cmp_end:
        d_cmp_end = d_start - timedelta(days=1)
        d_cmp_start = d_cmp_end - timedelta(days=delta - 1)
        cmp_start = d_cmp_start.strftime("%Y-%m-%d")
        cmp_end = d_cmp_end.strftime("%Y-%m-%d")
    elif d_cmp_end < d_cmp_start:
        return (
            jsonify({"error": "the comparison end date is before its start date"}),
            400,
        )
    cmp_days = (d_cmp_end - d_cmp_start).days + 1

    from utils.audit import log_event

    log_event(
        "report_run",
        category="report",
        resource_type="super_report",
        detail={"from": start, "to": end, "cmp_from": cmp_start, "cmp_to": cmp_end},
    )

    filters = _request_filters()
    # the blocks shown (item 43): only their slow sections are computed, for both periods
    from utils import super_report_blocks as sr_blocks

    blocks = sr_blocks.clean(request.args.getlist("blocks"))
    sections = sr_blocks.sections(blocks)

    # Cache key built post-resolution so an implicit vs. explicit cmp_start/end
    # (auto-computed above when omitted) still lands on the same key, and
    # multi-select values are pre-sorted so UI selection order doesn't cause a
    # spurious cache miss (report_cache's generic key-builder sorts only the
    # outer list, not values nested a level deeper).
    cache_key = {
        "start": start,
        "end": end,
        "cmp_start": cmp_start,
        "cmp_end": cmp_end,
        **{k: (sorted(v) if isinstance(v, list) else v) for k, v in filters.items()},
        "blocks": sorted(blocks),
    }
    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH excluded, for now. Resolved once per request and threaded
    # through both periods rather than re-resolved inside _collect_data (which
    # runs twice per request, current + comparison period).
    rh_site_id = served_site_id()
    cache_key["rh_site_id"] = rh_site_id

    cached = cache_get("super_report", cache_key)
    if cached is not None:
        return jsonify(cached)

    try:
        crn_cfg = dict(
            db.session.execute(
                text(
                    "SELECT key, value FROM settings WHERE key IN ('crn_live_since', 'crn_sms_provider')"
                )
            ).fetchall()
        )
        crn_enabled = bool((crn_cfg.get("crn_live_since") or "").strip())
        crn_dry_run = (crn_cfg.get("crn_sms_provider") or "log") == "log"

        current = _collect_data(start, end, filters, rh_site_id, sections=sections)
        previous = _collect_data(cmp_start, cmp_end, filters, rh_site_id, sections=sections)
        narrative = _generate_narrative(
            current, previous, start, end, cmp_start, cmp_end, cmp_days, blocks=blocks,
            crn_enabled=crn_enabled,
        )
        result = {
            "current": current,
            "previous": previous,
            "narrative": narrative,
            "cmp_start": cmp_start,
            "cmp_end": cmp_end,
            "delta_days": delta,
            "cmp_days": cmp_days,
            "crn_enabled": crn_enabled,
            "crn_dry_run": crn_dry_run,
            "blocks": blocks,
        }
        cache_put("super_report", cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Super report error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@super_report_bp.route("/viewer/super-report/sites")
@login_required
def super_report_sites():
    """RH and SJH side by side, per modality (meeting of 2026-11-13, item 13b: "direct
    comparison only in the Super Report and the Live Department board, per modality"). For
    an account holding two or more sites that have a database (utils/site_db.py): the same
    period and filters run on each site's own database, with the Super Report's own
    definitions (_build_where, _joins, the TAT clock of _collect_data). Anyone else gets no
    sites, and the page shows nothing."""
    from utils import site_db
    from utils.roles import all_sites

    start, end = request.args.get("start"), request.args.get("end")
    if not start or not end:
        return jsonify({"error": "start and end date required"}), 400
    site_ids = sorted(site_db.selectable_site_ids(current_user))
    if len(site_ids) < 2:
        return jsonify({"sites": [], "modalities": [], "totals": {}})
    filters = _request_filters()
    key = {
        "start": start,
        "end": end,
        "sites": site_ids,
        **{k: (sorted(v) if isinstance(v, list) else v) for k, v in filters.items()},
    }
    cached = cache_get("super_report_sites", key)
    if cached is not None:
        return jsonify(cached)

    names = {sid: (code, name) for sid, code, name in all_sites()}
    codes = {sid: names[sid][0] if sid in names else str(sid) for sid in site_ids}
    per_site = {}
    try:
        for sid in site_ids:
            with site_db.use_site(sid):
                per_site[sid] = _site_modalities(start, end, filters, sid)
    except Exception as e:
        db.session.rollback()
        logger.error(f"Super report sites error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

    modalities = {}
    for sid, rows in per_site.items():
        for r in rows:
            modalities.setdefault(r["modality"], {})[codes[sid]] = r
    order = sorted(
        modalities, key=lambda m: -sum(v["studies"] for v in modalities[m].values())
    )
    result = {
        "sites": [
            {
                "id": sid,
                "code": codes[sid],
                "name": names[sid][1] if sid in names else "",
            }
            for sid in site_ids
        ],
        "modalities": [{"modality": m, "by_site": modalities[m]} for m in order],
        "totals": {
            codes[sid]: {
                "studies": sum(r["studies"] for r in rows),
                "patients": sum(r["patients"] for r in rows),
                "gb": round(sum(r["gb"] for r in rows), 2),
            }
            for sid, rows in per_site.items()
        },
    }
    cache_put("super_report_sites", key, result)
    return jsonify(result)


def _site_modalities(start, end, filters, site_id):
    """Per modality, on the database the session reads (use_site): studies, patients,
    reported studies and their median TAT, storage. Run once per site."""
    where, params = _build_where(start, end, filters, site_id)
    mj, pj = _joins()
    tat = (
        "EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) "
        "- s.insert_time)) / 60.0"
    )
    reported = (
        "COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL "
        "AND s.insert_time IS NOT NULL"
    )
    rows = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               COUNT(DISTINCT s.study_db_uid)   AS studies,
               COUNT(DISTINCT s.patient_db_uid) AS patients,
               COUNT(*) FILTER (WHERE {reported}) AS reported,
               ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {tat})
                      FILTER (WHERE {reported}))::numeric, 1) AS median_tat_min
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )
    sp = {"start": start, "end": end}
    sc = "study_date BETWEEN :start AND :end"
    if filters.get("modality"):
        sc += " AND modality = ANY(:modality)"
        sp["modality"] = filters["modality"]
    gb = dict(
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(modality, 'Unknown'), ROUND(SUM(total_gb)::numeric, 2)
        FROM summary_storage_daily WHERE {sc} GROUP BY 1
    """
            ),
            sp,
        ).fetchall()
    )
    out = [
        {
            "modality": r["modality"],
            "studies": int(r["studies"] or 0),
            "patients": int(r["patients"] or 0),
            "reported": int(r["reported"] or 0),
            "median_tat_min": (
                float(r["median_tat_min"]) if r["median_tat_min"] is not None else None
            ),
            "gb": float(gb.pop(r["modality"], 0) or 0),
        }
        for r in rows
    ]
    for modality, value in gb.items():  # storage for a modality with no study counted
        out.append(
            {
                "modality": modality,
                "studies": 0,
                "patients": 0,
                "reported": 0,
                "median_tat_min": None,
                "gb": float(value or 0),
            }
        )
    return out


# ─────────────────────────────────────────────
#  DATA COLLECTION
# ─────────────────────────────────────────────


def _build_where(start, end, filters, rh_site_id=None):
    clauses = [
        "s.study_date BETWEEN :start AND :end",
        "COALESCE(m.modality, s.study_modality, 'Unknown') NOT IN ('SR', 'BMD')",
    ]
    params = {"start": start, "end": end}

    # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
    # site) only, SJH excluded, for now. Same served_site_id()/aetitle_modality_map
    # pattern used throughout (report_25, report_34, viewer_controller, etc.) --
    # `m` (aetitle_modality_map) is already joined into every query built on this
    # WHERE clause. Resolves to None (filter skipped) on a non-LAUMC/single-site
    # install. Note: the orders (etl_orders) and storage (summary_storage_daily)
    # queries in _collect_data below have no site-resolvable join and remain
    # unscoped -- same known, documented gap as etl_orders elsewhere (see
    # viewer_controller.py's daily_briefing()), not fixed here.
    if rh_site_id is not None:
        clauses.append("m.site_id = :rh_site_id")
        params["rh_site_id"] = rh_site_id

    multi = {
        "storing_ae": ("s.storing_ae", "storing_ae"),
        "study_status": ("s.study_status", "study_status"),
        "report_status": ("s.report_status", "report_status"),
        "order_status": ("s.order_status", "order_status"),
        "patient_class": ("s.patient_class", "patient_class"),
        "patient_location": ("s.patient_location", "patient_location"),
        "body_part": ("s.study_body_part", "body_part"),
        "procedure_code": ("s.procedure_code", "procedure_code"),
        "sex": ("p.sex", "sex"),
        # No age_group: the page's age filter is the min / max range below (sr_extra); no
        # dropdown offers age_group, and the two together ANDed (review of 2026-11-20).
    }
    for fk, (col, pk) in multi.items():
        if filters.get(fk):
            clauses.append(f"{col} = ANY(:{pk})")
            params[pk] = filters[fk]

    if filters.get("modality"):
        clauses.append("COALESCE(m.modality, s.study_modality) = ANY(:modality)")
        params["modality"] = filters["modality"]

    if filters.get("signing_physician"):
        clauses.append(
            "TRIM(CONCAT(s.signing_physician_first_name,' ',s.signing_physician_last_name)) = ANY(:signing_physician)"
        )
        params["signing_physician"] = filters["signing_physician"]

    if filters.get("referring_physician"):
        clauses.append(
            "TRIM(CONCAT(s.referring_physician_first_name,' ',s.referring_physician_last_name)) = ANY(:referring_physician)"
        )
        params["referring_physician"] = filters["referring_physician"]

    if filters.get("has_report") == "Yes":
        clauses.append("s.study_has_report = TRUE")
    elif filters.get("has_report") == "No":
        clauses.append("s.study_has_report = FALSE")

    if filters.get("age_min"):
        clauses.append("s.age_at_exam >= :age_min")
        params["age_min"] = float(filters["age_min"])
    if filters.get("age_max"):
        clauses.append("s.age_at_exam <= :age_max")
        params["age_max"] = float(filters["age_max"])

    return " AND ".join(clauses), params


# Per database (each site's database answers for itself, rule 10): once etl_orders.site_id
# carries data it always will, so a yes is kept for the life of the process and the probe runs
# once per process and database, instead of twice per request (both periods). A no is not kept
# (the ETL may fill the column later; until then the probe runs as before), nor a failure. A
# restart asks again.
_ORDERS_SITE_KNOWN = {}


def _orders_have_site():
    """True when etl_orders.site_id exists and is actually populated.

    See the call site: scoping orders to a site is only safe once the ETL has filled
    that column, otherwise the filter matches nothing and the Overview block reports
    zero orders.
    """
    from utils import site_db

    key = site_db.current_site_id()
    if _ORDERS_SITE_KNOWN.get(key):
        return True
    try:
        known = bool(
            db.session.execute(
                text(
                    """
            SELECT EXISTS (SELECT 1 FROM etl_orders WHERE site_id IS NOT NULL)
        """
                )
            ).scalar()
        )
    except Exception:
        db.session.rollback()
        return False
    if known:
        _ORDERS_SITE_KNOWN[key] = True
        logger.info(
            "etl_orders.site_id carries data on this database (site %s): orders scoped to "
            "the site; asked once per process",
            key,
        )
    return known


def stored_filters(saved):
    """A scheduled email's saved filters ({name: [values]}, item 43) in _request_filters' shape,
    read exactly as a page request would be."""
    from werkzeug.datastructures import MultiDict

    args = MultiDict()
    for key, value in (saved or {}).items():
        for v in value if isinstance(value, list) else [value]:
            if v not in (None, ""):
                args.add(str(key), str(v))
    return _request_filters(args)


def _request_filters(args=None):
    """The filters of /viewer/super-report and /viewer/super-report/sites, as named there.
    Only filters a query applies: series_modality, body_part_series and protocol_name were read
    and never applied, age_group was offered by nothing on the page (review of 2026-11-20);
    migration 0180 took them out of the saved presets. order_control and order_modality apply
    to the orders figures. args: request.args, or a scheduled email's (stored_filters)."""
    args = request.args if args is None else args
    filters = {
        k: args.getlist(k)
        for k in [
            "modality",
            "storing_ae",
            "study_status",
            "report_status",
            "order_status",
            "patient_class",
            "patient_location",
            "body_part",
            "procedure_code",
            "signing_physician",
            "referring_physician",
            "sex",
            "order_control",
            "order_modality",
        ]
    }
    filters["has_report"] = args.get("has_report")
    filters["age_min"] = args.get("age_min")
    filters["age_max"] = args.get("age_max")
    return filters


def _joins():
    """The device (m) and patient (p) joins every study query here uses. Why the device join
    is LATERAL ... LIMIT 1: see _collect_data. station_name is the device's label: the RIS
    room name (utils.ae_display). etl_patient_view.patient_db_uid is the PK, so p is 1:1.
    """
    mj = (
        "LEFT JOIN LATERAL ("
        f"SELECT m0.modality, {ae_display_sql('m0', 'NULL')} AS station_name, m0.site_id "
        "FROM aetitle_modality_map m0 "
        "WHERE UPPER(TRIM(m0.aetitle)) = UPPER(TRIM(s.storing_ae)) "
        "ORDER BY m0.id LIMIT 1) m ON TRUE"
    )
    pj = "LEFT JOIN etl_patient_view p ON p.patient_db_uid = s.patient_db_uid"
    return mj, pj


def _collect_data(start, end, filters, rh_site_id=None, include_rollups=True, sections=None):
    """sections: the slow data sections to compute (utils/super_report_blocks.sections of the
    blocks shown); None computes all. Items 42/43 (26 Nov): the comparison period is computed
    with the same sections as the current one, so the analysis compares every figure.
    include_rollups=False still leaves out the CRN summary's detail and the wet reads (kept
    for callers that read only the headline)."""
    def want(section):
        return sections is None or section in sections

    where, params = _build_where(start, end, filters, rh_site_id)
    # LATERAL ... LIMIT 1, not a plain LEFT JOIN. aetitle_modality_map's UNIQUE is on
    # the RAW aetitle while every consumer matches on UPPER(TRIM(...)), so 'ct99' and
    # 'CT99' are both legal rows AND both match the same study -- the fan-out migration
    # 0116 had to repair, and which the RIS import can recreate on any cycle (0116's
    # own closing note).
    #
    # Why it mattered here more than elsewhere: total_studies is COUNT(DISTINCT
    # s.study_db_uid) and is immune, while every other figure feeding the brief is
    # COUNT(*) or SUM() over this join. A duplicated AE inflated the numerators and left
    # the denominator alone, so the narrative printed shares against a smaller total --
    # reproduced with one duplicate row on 20% of studies: modality shares summing to
    # 120%, physician shares 20% high, total_images 20% high, and "Reporting coverage:
    # 115.0% (1,150 of 1,000 studies signed)".
    #
    # LIMIT 1 makes one study exactly one row whether or not duplicates exist, so this
    # holds even after the next RIS import. ORDER BY id keeps the choice deterministic.
    # station_name here is the device's label: the RIS room name (utils.ae_display).
    mj, pj = _joins()

    kpis = (
        db.session.execute(
            text(
                f"""
        SELECT COUNT(DISTINCT s.study_db_uid)     AS total_studies,
               COUNT(DISTINCT s.patient_db_uid)   AS total_patients,
               SUM(s.number_of_study_images)      AS total_images,
               COUNT(DISTINCT s.storing_ae)       AS active_aes,
               COUNT(*) FILTER (WHERE s.study_has_report=TRUE) AS studies_with_report
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """
            ),
            params,
        )
        .mappings()
        .fetchone()
    )

    # orders
    op = {"start": start, "end": end}
    oc = ["o.scheduled_datetime::date BETWEEN :start AND :end"]
    if filters.get("order_modality"):
        oc.append("o.modality = ANY(:order_modality)")
        op["order_modality"] = filters["order_modality"]
    if filters.get("order_control"):
        oc.append("o.order_control = ANY(:order_control)")
        op["order_control"] = filters["order_control"]
    # Site-scope the orders the same way every study figure is scoped. Without this the
    # brief printed "N orders received" (all sites) directly beside "M studies
    # completed" (RH only) in the same Overview block -- on LAUMC the satellite is ~45%
    # of RIS volume, so the two numbers were never comparable and the fulfillment rate
    # was measured against a population half the report never shows.
    #
    # etl_orders.site_id is populated by the orders ETL; the older comment here said
    # orders had "no site-resolvable join", which stopped being true when that column
    # landed. Guarded on the column actually carrying data, because filtering on a
    # column that is NULL everywhere would silently zero the whole section -- if it is
    # unpopulated this leaves the previous (unscoped) behaviour exactly as it was.
    if rh_site_id is not None and _orders_have_site():
        oc.append("o.site_id = :o_site_id")
        op["o_site_id"] = rh_site_id
    orders = (
        db.session.execute(
            text(
                f"""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE has_study=TRUE) AS fulfilled,
               ROUND(COUNT(*) FILTER (WHERE has_study=TRUE)*100.0/NULLIF(COUNT(*),0),1) AS fulfillment_pct
        FROM etl_orders o WHERE {" AND ".join(oc)}
    """
            ),
            op,
        )
        .mappings()
        .fetchone()
    )

    # storage
    sp = {"start": start, "end": end}
    sc = ["study_date BETWEEN :start AND :end"]
    if filters.get("modality"):
        sc.append("modality = ANY(:modality)")
        sp["modality"] = filters["modality"]
    storage = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(SUM(total_gb),0) AS total_gb,
               COALESCE(SUM(study_count),0) AS total_study_count,
               ROUND(COALESCE(SUM(total_gb),0)/NULLIF(COUNT(DISTINCT study_date),0),2) AS avg_gb_per_day
        FROM summary_storage_daily WHERE {" AND ".join(sc)}
    """
            ),
            sp,
        )
        .mappings()
        .fetchone()
    )

    storage_by_mod = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(modality,'N/A') AS modality, ROUND(SUM(total_gb)::numeric,2) AS gb
        FROM summary_storage_daily WHERE {" AND ".join(sc)}
        GROUP BY 1 ORDER BY gb DESC LIMIT 5
    """
            ),
            sp,
        )
        .mappings()
        .fetchall()
    )

    top_mods = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(m.modality,s.study_modality,'N/A') AS modality, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 5
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )

    peak = (
        db.session.execute(
            text(
                f"""
        SELECT s.study_date::text AS peak_day, COUNT(*) AS peak_count
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY s.study_date ORDER BY peak_count DESC LIMIT 1
    """
            ),
            params,
        )
        .mappings()
        .fetchone()
    )

    avg_day = (
        db.session.execute(
            text(
                f"""
        SELECT ROUND(COUNT(*)/NULLIF(COUNT(DISTINCT s.study_date),0)::numeric,1) AS avg_per_day
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """
            ),
            params,
        )
        .mappings()
        .fetchone()
    )

    physicians = (
        db.session.execute(
            text(
                f"""
        SELECT TRIM(CONCAT(s.referring_physician_first_name,' ',s.referring_physician_last_name)) AS physician,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
          AND s.referring_physician_last_name IS NOT NULL AND s.referring_physician_last_name != ''
        GROUP BY 1 ORDER BY cnt DESC LIMIT 30
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )

    # ── Patient-class category mapping (configurable via settings table) ────
    _pc_rows = db.session.execute(
        text(
            "SELECT key, value FROM settings WHERE key IN ('pc_inpatient', 'pc_outpatient', 'pc_emergency')"
        )
    ).fetchall()
    _pc_cfg = {r[0]: r[1] for r in _pc_rows}
    # Each category's classes are a bound array parameter (= ANY, as in _build_where), never
    # quoted into the SQL text. pc_params joins params wherever one of these filters is used.
    pc_params = {}

    def _pc_filter(cfg_key, default):
        vals = [
            v.strip() for v in _pc_cfg.get(cfg_key, default).split(",") if v.strip()
        ]
        if not vals:
            return "FALSE"
        pc_params[cfg_key] = vals
        return f"s.patient_class = ANY(:{cfg_key})"

    inp_filter = _pc_filter("pc_inpatient", "I,IP,INPAT,INPATIENT,INN")
    outp_filter = _pc_filter("pc_outpatient", "O,OP,OUTPAT,OUTPATIENT,AMB,AMBULATORY")

    demo = (
        db.session.execute(
            text(
                f"""
        SELECT COUNT(*) FILTER (WHERE p.sex ILIKE 'M%') AS male,
               COUNT(*) FILTER (WHERE p.sex ILIKE 'F%') AS female,
               COUNT(*) FILTER (WHERE {inp_filter}) AS inpatient,
               COUNT(*) FILTER (WHERE {outp_filter}) AS outpatient,
               ROUND(AVG(s.age_at_exam),1) AS avg_age,
               MIN(s.age_at_exam) AS min_age,
               MAX(s.age_at_exam) AS max_age
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
    """
            ),
            {**params, **pc_params},
        )
        .mappings()
        .fetchone()
    )

    pc_breakdown = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(s.patient_class, 'Unknown') AS class, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )

    er_filter = _pc_filter("pc_emergency", "E,EP,ER,EMERGENCY,URG,URGENT")

    # Busiest AE title by study count in period
    ae_busy_row = (
        db.session.execute(
            text(
                f"""
        SELECT s.storing_ae AS aetitle, m.station_name AS station_name, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where} AND s.storing_ae IS NOT NULL
        GROUP BY s.storing_ae, m.station_name ORDER BY cnt DESC LIMIT 1
    """
            ),
            params,
        )
        .mappings()
        .fetchone()
    )

    # Most idle configured AE (fewest studies in period — includes 0-study AEs)
    _idle_site_clause = (
        "WHERE am.site_id = :rh_site_id" if rh_site_id is not None else ""
    )
    _idle_params = {"start": start, "end": end}
    if rh_site_id is not None:
        _idle_params["rh_site_id"] = rh_site_id
    ae_idle_row = (
        db.session.execute(
            text(
                f"""
        SELECT am.aetitle, {ae_display_sql('am', 'NULL')} AS station_name, COALESCE(sub.cnt, 0) AS cnt
        FROM aetitle_modality_map am
        LEFT JOIN (
            SELECT storing_ae, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date BETWEEN :start AND :end
            GROUP BY storing_ae
        ) sub ON UPPER(TRIM(sub.storing_ae)) = UPPER(TRIM(am.aetitle))
        {_idle_site_clause}
        ORDER BY cnt ASC LIMIT 1
    """
            ),
            _idle_params,
        )
        .mappings()
        .fetchone()
    )

    # rep_final_timestamp (PACS) is sparse/unreliable on this install (operator,
    # 2026-07-27) -- rep_study_last_composed_ts is the PACS field confirmed
    # reliable here and already the standard TAT anchor in report_25.py.
    # Non-ER studies with TAT > P75 on days that also had ER volume (resource contention proxy)
    if "pc_emergency" in pc_params and want("er_delayed"):
        # er_days applies the page's own WHERE (site, modality, patient class and every other
        # filter), like p75 and the count: it used to take any ER study of any site or
        # modality, so with CT on RH picked a day counted for an ER MR or an SJH ER study.
        er_delayed_row = (
            db.session.execute(
                text(
                    f"""
            WITH er_days AS (
                SELECT DISTINCT s.study_date FROM etl_didb_studies s {mj} {pj}
                WHERE {where} AND {er_filter}
            ),
            p75 AS (
                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0
                ) AS val
                FROM etl_didb_studies s {mj} {pj}
                WHERE {where}
                  AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL AND s.insert_time IS NOT NULL
            )
            SELECT COUNT(*) AS cnt
            FROM etl_didb_studies s {mj} {pj}
            JOIN er_days ed ON s.study_date = ed.study_date
            CROSS JOIN p75
            WHERE {where}
              AND NOT ({er_filter})
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL AND s.insert_time IS NOT NULL
              AND EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0 > p75.val
        """
                ),
                {**params, **pc_params},
            )
            .mappings()
            .fetchone()
        )
        er_delayed = int(er_delayed_row.get("cnt") or 0) if er_delayed_row else 0
    else:
        er_delayed = 0

    # ── TAT & reporting (null-safe — columns populated by ETL) ────
    # ER vs non-ER split reuses er_filter (already resolved above from the
    # configurable pc_emergency setting) via FILTER clauses in the same scan,
    # rather than a second round trip.
    _tat_epoch = (
        "EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) "
        "- s.insert_time)) / 60.0"
    )
    tat = (
        db.session.execute(
            text(
                f"""
        SELECT
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch})::numeric, 1) AS median_tat_min,
            COUNT(*) FILTER (WHERE COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS reported_count,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch}) FILTER (WHERE {er_filter})::numeric, 1) AS er_median_tat_min,
            COUNT(*) FILTER (WHERE {er_filter} AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS er_reported_count,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {_tat_epoch}) FILTER (WHERE NOT ({er_filter}))::numeric, 1) AS non_er_median_tat_min,
            COUNT(*) FILTER (WHERE NOT ({er_filter}) AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL) AS non_er_reported_count
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where} AND s.insert_time IS NOT NULL
    """
            ),
            {**params, **pc_params},
        )
        .mappings()
        .fetchone()
    )

    tat_by_mod = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY EXTRACT(EPOCH FROM (COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) - s.insert_time)) / 60.0
               )::numeric, 1) AS median_tat_min,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj}
        WHERE {where} AND :tat_on
          AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
          AND s.insert_time IS NOT NULL
        GROUP BY 1 ORDER BY median_tat_min DESC
    """
            ),
            {**params, "tat_on": want("tat")},
        )
        .mappings()
        .fetchall()
    )

    # Reports whose last author is a resident (PACS "residents" group, utils/pacs_roles): their
    # time counts the preliminary read; a radiologist's approval moves it later (item 42).
    resident_last = 0
    if want("tat"):
        from utils.pacs_roles import role_lookup_cte

        resident_last = int(
            db.session.execute(
                text(
                    f"""
            WITH {role_lookup_cte('role_lookup')}
            SELECT COUNT(DISTINCT s.study_db_uid)
            FROM etl_didb_studies s {mj} {pj}
            WHERE {where}
              AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
              AND EXISTS (SELECT 1 FROM role_lookup r
                          WHERE r.role = 'residents'
                            AND r.login_id = UPPER(SPLIT_PART(BTRIM(s.rep_study_last_composed_by), '@', 1)))
        """
                ),
                params,
            ).scalar()
            or 0
        )

    # ── Daily study volume (time series for charts) ───────────────
    daily_series = (
        db.session.execute(
            text(
                f"""
        SELECT s.study_date::text AS date, COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY s.study_date ORDER BY s.study_date
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )

    # ── Studies per modality (full breakdown) ─────────────────────
    modality_series = (
        db.session.execute(
            text(
                f"""
        SELECT COALESCE(m.modality, s.study_modality, 'Unknown') AS modality,
               COUNT(*) AS cnt
        FROM etl_didb_studies s {mj} {pj} WHERE {where}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 15
    """
            ),
            params,
        )
        .mappings()
        .fetchall()
    )

    # ── Radiologist matrix: reports × modality / AE title / procedure ─
    # rep_final_signed_by/rep_final_timestamp (PACS) are sparse/unreliable on this
    # install (operator, 2026-07-27) -- rep_study_last_composed_by/_ts is the PACS
    # field confirmed reliable here and already the standard anchor in report_25.py.
    _RAD_BASE_SR = (
        "COALESCE(NULLIF(TRIM(CONCAT(s.signing_physician_first_name,' ',"
        "s.signing_physician_last_name)),''),s.rep_study_last_composed_by,"
        "s.rep_final_signed_by)"
    )
    _PAM_SR = rad_alias_join_sql(_RAD_BASE_SR)
    _RAD = rad_display_sql(_RAD_BASE_SR)
    _RAD_OK = rad_ok_sql(
        _RAD_BASE_SR,
        ts_expr="COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp)",
    )

    rad_mod_rows = (
        db.session.execute(
            text(
                f"""
        SELECT {_RAD} AS radiologist,
               COALESCE(m.modality, s.study_modality, 'Unknown') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK} AND :rad_on
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """
            ),
            {**params, "rad_on": want("rad_matrix")},
        )
        .mappings()
        .fetchall()
    )

    rad_ae_rows = (
        db.session.execute(
            text(
                f"""
        SELECT {_RAD} AS radiologist,
               COALESCE(m.station_name, s.storing_ae, 'Unknown') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK} AND :rad_on
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """
            ),
            {**params, "rad_on": want("rad_matrix")},
        )
        .mappings()
        .fetchall()
    )

    rad_proc_rows = (
        db.session.execute(
            text(
                f"""
        WITH top_procs AS (
            SELECT s.procedure_code
            FROM etl_didb_studies s {mj} {pj}
            WHERE {where} AND :rad_on AND COALESCE(s.rep_study_last_composed_ts, s.rep_final_timestamp) IS NOT NULL
              AND s.procedure_code IS NOT NULL AND s.procedure_code != ''
            GROUP BY 1 ORDER BY COUNT(DISTINCT s.study_db_uid) DESC LIMIT 60
        )
        SELECT {_RAD} AS radiologist,
               COALESCE(NULLIF(TRIM(pm.procedure_name),''), s.procedure_code) AS proc,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        LEFT JOIN procedure_duration_map pm ON UPPER(TRIM(s.procedure_code)) = UPPER(TRIM(pm.procedure_code))
        JOIN top_procs tp ON tp.procedure_code = s.procedure_code
        WHERE {where} AND {_RAD_OK} AND :rad_on
        GROUP BY 1, 2 ORDER BY 2, 3 DESC
    """
            ),
            {**params, "rad_on": want("rad_matrix")},
        )
        .mappings()
        .fetchall()
    )

    rad_month_rows = (
        db.session.execute(
            text(
                f"""
        SELECT {_RAD} AS radiologist,
               TO_CHAR(s.study_date, 'YYYY-MM') AS dim,
               COUNT(DISTINCT s.study_db_uid) AS cnt
        FROM etl_didb_studies s {mj} {pj} {_PAM_SR}
        WHERE {where} AND {_RAD_OK} AND :rad_on
        GROUP BY 1, 2 ORDER BY 1, 2
    """
            ),
            {**params, "rad_on": want("rad_matrix")},
        )
        .mappings()
        .fetchall()
    )

    return {
        "kpis": dict(kpis),
        "orders": dict(orders),
        "storage": {**dict(storage), "by_modality": [dict(r) for r in storage_by_mod]},
        "volume": {
            "avg_per_day": dict(avg_day).get("avg_per_day", 0),
            "peak_day": dict(peak).get("peak_day") if peak else None,
            "peak_count": dict(peak).get("peak_count") if peak else 0,
            "top_modalities": [dict(r) for r in top_mods],
        },
        "physicians": [dict(r) for r in physicians],
        "demographics": {**dict(demo), "pc_breakdown": [dict(r) for r in pc_breakdown]},
        "ae_ops": {
            "busiest_ae": (
                (ae_busy_row["station_name"] or ae_busy_row["aetitle"])
                if ae_busy_row
                else None
            ),
            "busiest_cnt": int(ae_busy_row["cnt"] or 0) if ae_busy_row else 0,
            "idle_ae": (
                (ae_idle_row["station_name"] or ae_idle_row["aetitle"])
                if ae_idle_row
                else None
            ),
            "idle_cnt": int(ae_idle_row["cnt"] or 0) if ae_idle_row else 0,
            "er_delayed": er_delayed,
        },
        "tat": {
            "resident_last_author": resident_last,
            "median_tat_min": float(tat.get("median_tat_min") or 0) if tat else 0,
            "reported_count": int(tat.get("reported_count") or 0) if tat else 0,
            "er_median_tat_min": float(tat.get("er_median_tat_min") or 0) if tat else 0,
            "er_reported_count": int(tat.get("er_reported_count") or 0) if tat else 0,
            "non_er_median_tat_min": (
                float(tat.get("non_er_median_tat_min") or 0) if tat else 0
            ),
            "non_er_reported_count": (
                int(tat.get("non_er_reported_count") or 0) if tat else 0
            ),
            "by_modality": [dict(r) for r in tat_by_mod],
        },
        "daily_series": [dict(r) for r in daily_series],
        "modality_series": [dict(r) for r in modality_series],
        "rad_matrix": {
            "by_modality": [dict(r) for r in rad_mod_rows],
            "by_aetitle": [dict(r) for r in rad_ae_rows],
            "by_procedure": [dict(r) for r in rad_proc_rows],
            "by_month": [dict(r) for r in rad_month_rows],
        },
        "crn": (
            (
                _get_crn_summary(start, end)
                if include_rollups
                else {"critical_count": _critical_count(start, end)}
            )
            if want("crn")
            else {}
        ),
        "utilization": (
            _get_utilization_summary(start, end) if want("utilization") else {"available": False}
        ),
        "resident_wetreads": (
            _get_resident_wetreads(where, params, mj, pj)
            if include_rollups and want("resident_wetreads")
            else None
        ),
        "lead_time": (
            _get_lead_time(start, end, filters, full=include_rollups) if want("lead_time") else None
        ),
    }


# The lead time card's tables: the modalities and patient classes with the most appointments.
LEAD_TIME_ROWS = 12


def _get_lead_time(start, end, filters, full=True):
    """Patient schedule lead time (operator 2026-11-21: "add patient lead time to super
    report"), with Report 40's own definitions (utils/schedule_lead_time.py): the RIS exams
    whose appointment falls in the period, from the order's creation on the RIS to the
    appointment, in calendar days; booked after the appointment counted aside; cancelled and
    scheduled more than once. Of the page's filters only Modality applies: the others are the
    PACS study's (its patient class codes, physicians, statuses), which the RIS writes
    differently. full=False (the comparison period): the headline only. None when it cannot be
    worked out (the rest of the page still answers)."""
    from utils import schedule_lead_time as slt

    try:
        d_start = datetime.strptime(start, "%Y-%m-%d").date()
        d_end = datetime.strptime(end, "%Y-%m-%d").date()
        chosen = {"modality": set(filters["modality"])} if filters.get("modality") else None
        _cases, summary = slt.build(d_start, d_end, chosen)
    except Exception:
        logger.exception("lead time not computed")
        db.session.rollback()
        return None
    out = {"kpi": summary["kpi"], "ahead_days": slt.AHEAD_DAYS}
    if full:
        out["buckets"] = summary["buckets"]
        out["by_modality"] = summary["breakdowns"]["modality"]["rows"][:LEAD_TIME_ROWS]
        out["by_class"] = summary["breakdowns"]["patient_class"]["rows"][:LEAD_TIME_ROWS]
        out["modality_filter"] = bool(chosen)
    return out


def _days(v):
    """A lead time in words: 'the same day', '1 day', '2.5 days'."""
    if v is None:
        return "—"
    if v == 0:
        return "the same day"
    return f"{v:g} day" + ("" if v == 1 else "s")


def _get_resident_wetreads(where, params, mj, pj):
    """Item 8 (operator 2026-11-19): residents counted by their wet reads, from the PACS audit
    trail copy (etl_pacs_audit_events, kind 'wetread', the login and second of each one): each
    study once, for the resident whose wet read came first; a resident is a login of the PACS
    "residents" group or marked so (utils/pacs_roles). Only studies that reached PACS while the
    audit copy holds them count (from 1 Oct 2026). Same dates and filters as the rest of the page;
    staff who left read "Former staff" while that switch is on (the audit copy has no rh view).
    """
    from ETL_JOBS.etl_pacs_audit import coverage as audit_coverage
    from utils import staff_status
    from utils.pacs_roles import role_lookup_cte
    from utils.sticky_notes import person_name

    out = {"coverage": None, "modalities": [], "residents": [], "total": 0}
    try:
        audit_from, audit_until = audit_coverage(db.session)
        if audit_from is None:
            return out
        out["coverage"] = {
            "from": audit_from.strftime("%Y-%m-%d"),
            "until": audit_until.strftime("%Y-%m-%d %H:%M"),
        }
        rows = (
            db.session.execute(
                text(
                    f"""
            WITH {role_lookup_cte('role_lookup')},
            studies AS MATERIALIZED (
                SELECT s.study_db_uid, BTRIM(s.study_instance_uid) AS uid,
                       COALESCE(m.modality, s.study_modality, 'Unknown') AS modality
                FROM etl_didb_studies s {mj} {pj}
                WHERE {where} AND s.study_instance_uid IS NOT NULL
                  AND s.insert_time >= :wr_from AND s.insert_time < :wr_until
            ),
            wet AS (
                SELECT DISTINCT ON (st.study_db_uid) st.study_db_uid, st.modality, e.user_name
                FROM studies st
                JOIN etl_pacs_audit_events e ON e.study_instance_uid = st.uid AND e.kind = 'wetread'
                JOIN role_lookup r ON r.login_id = UPPER(SPLIT_PART(e.user_name, '@', 1)) AND r.role = 'residents'
                ORDER BY st.study_db_uid, e.event_at, e.log_dbid
            )
            SELECT w.user_name, alias.canonical_name AS alias,
                   {staff_status.login_sql('w.user_name')} IN {staff_status.left_logins_sql()} AS left_staff,
                   w.modality, COUNT(*) AS cnt
            FROM wet w
            LEFT JOIN physician_alias_map alias ON alias.dismissed = false AND alias.alias = w.user_name
            GROUP BY 1, 2, 3, 4
        """
                ),
                {**params, "wr_from": audit_from, "wr_until": audit_until},
            )
            .mappings()
            .all()
        )
    except Exception:
        logger.exception("resident wet reads not computed")
        db.session.rollback()
        return out
    by_res, mods = {}, {}
    for r in rows:
        name = (
            staff_status.FORMER
            if r["left_staff"]
            else (person_name(r["alias"], r["user_name"]) or r["user_name"])
        )
        res = by_res.setdefault(name, {"name": name, "total": 0, "by_modality": {}})
        res["by_modality"][r["modality"]] = res["by_modality"].get(
            r["modality"], 0
        ) + int(r["cnt"])
        res["total"] += int(r["cnt"])
        mods[r["modality"]] = mods.get(r["modality"], 0) + int(r["cnt"])
    out["modalities"] = sorted(mods, key=lambda m: (-mods[m], m))
    out["residents"] = sorted(by_res.values(), key=lambda r: (-r["total"], r["name"]))
    out["total"] = sum(mods.values())
    return out


def _get_crn_summary(start, end):
    """Critical Result Notification activity for the period.

    Not site-scoped (no established site-resolution join for hl7_oru_reports /
    crn_notifications yet) and not filtered by Super Report's other active
    filters -- a period-only rollup, same simplification as
    _get_utilization_summary below. The CRN numbers are Admin > CRN > Report's
    own summary (utils/crn_admin_ops.report), so the two always agree.
    """
    from utils.crn_admin_ops import report as crn_report

    s = crn_report(start, end)["summary"]
    return {
        "critical_count": _critical_count(start, end),
        "total_notifications": s["total"],
        "acknowledged": s["acknowledged"],
        "overdue": s["overdue"],
        "open": s["open"],
        "median_ack_min": s["median_min"],
    }


def _critical_count(start, end):
    """Reports the NLP worker flagged critical in the period (the CRN card's first line)."""
    n = db.session.execute(
        text(
            """
        SELECT COUNT(*)
        FROM hl7_oru_analysis a
        JOIN hl7_oru_reports r ON r.id = a.report_id
        WHERE a.is_critical = TRUE
          AND COALESCE(r.result_datetime, r.received_at)::date BETWEEN :start AND :end
    """
        ),
        {"start": start, "end": end},
    ).scalar()
    return int(n or 0)


def _get_utilization_summary(start, end):
    """Compact device/modality utilization rollup for the period.

    Reuses report_34's device-utilization matrix verbatim (same std_pps-actuals
    / procedure_duration_map-estimate load logic, same RH-only site filter
    already wired into report_34) rather than re-deriving it -- report_34.py's
    get_device_utilization_data() is the source of truth this ports from, kept
    in sync automatically since it's a direct call, not a copy. Scoped by date
    range only -- Super Report's other active filters (modality, patient
    class, etc.) aren't forwarded, matching report_34's own behavior when none
    of its filter toggles are enabled. Shares report_34's cache (keyed on the
    same start_date/end_date), so a Report 34 view for the same range makes
    this instant.
    """
    from routes.report_34 import utilization_data

    data = utilization_data(start, end)
    if not data:
        return {"available": False}

    matrix = [row for row in data["matrix"] if not row["no_schedule"]]
    avg_util = round(sum(r["avg"] for r in matrix) / len(matrix), 1) if matrix else 0
    ranked = sorted(matrix, key=lambda r: r["avg"], reverse=True)

    return {
        "available": True,
        "avg_utilization": avg_util,
        "high_stress_count": data["summary"]["high_stress_count"],
        "low_util_count": data["summary"]["low_util_count"],
        "no_schedule_count": data["summary"]["no_schedule_count"],
        "busiest": [{"ae": r["ae"], "avg": r["avg"]} for r in ranked[:3]],
        "idlest": (
            [{"ae": r["ae"], "avg": r["avg"]} for r in ranked[-3:][::-1]]
            if len(ranked) >= 6
            else []
        ),
    }


# ─────────────────────────────────────────────
#  RULE-BASED NARRATIVE
# ─────────────────────────────────────────────


def _fp(p):
    """A change, rounded to one decimal by _pct, written as the page's badges write it (pct() in
    static/js/super_report.js): +5% not +5.0%, +0% for a zero of either sign."""
    if p is None:
        return None
    if p == 0:
        p = 0.0
    s = f"{p:.1f}".rstrip("0").rstrip(".")
    return f"{'+' if p >= 0 else ''}{s}%"


def _generate_narrative(cur, prev, start, end, cmp_start, cmp_end, cmp_days, blocks=None,
                        crn_enabled=False):
    """The written analysis (item 42, utils/super_report_analysis.py), in the narrative's section
    format the page draws, and the Clinical Insights signals for their card. Only the blocks
    shown are written about. cmp_days: the length of the comparison window."""
    from utils import super_report_blocks as sr_blocks
    from utils.super_report_analysis import analyse

    blocks = sr_blocks.clean(blocks)
    period = {"start": start, "end": end, "cmp_start": cmp_start, "cmp_end": cmp_end,
              "cmp_days": cmp_days, "crn_enabled": crn_enabled}
    sections = analyse(cur, prev, period, blocks)

    if "insights" in blocks:
        try:
            dept_signals = run_dept_insights(cur, prev)
            severity_order = {"critical": 0, "warning": 1, "info": 2}
            dept_signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 2))
            sections.append({
                "icon": "bi-lightbulb",
                "color": "#fbbf24",
                "title": "Clinical Insights",
                "bullets": [s.get("message", "") for s in dept_signals],
                "signals": dept_signals,
            })
        except Exception as _ins_e:
            logger.warning(f"Clinical insights error: {_ins_e}")
    return sections
