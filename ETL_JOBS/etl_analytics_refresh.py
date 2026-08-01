"""
etl_analytics_refresh.py
────────────────────────────────────────────────────────────────────────────
Phase 4 of the ETL pipeline.
Aggregates image file sizes into summary_storage_daily after each sync.

Join chain (simplified — raw_images has study_db_uid directly):
  etl_didb_studies
    → etl_didb_raw_images  (on study_db_uid)
    → etl_image_locations  (on raw_image_db_uid)  ← image_size_kb lives here

Key column notes from db.py:
  - etl_didb_studies.study_modality  (NOT .modality)
  - etl_didb_raw_images.study_db_uid (direct FK — no need for study_instance_uid)
  - etl_image_locations.image_size_kb
  - summary_storage_daily.modality   (populated from study_modality)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from sqlalchemy import func, distinct, table, column, text, select
from sqlalchemy.dialects.postgresql import insert

# Ensure parent dir (where db.py lives) is on the path when imported from ETL_JOBS/
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from db import (
    db,
    summary_storage_daily,
    etl_didb_studies,
    etl_didb_raw_images,
    etl_image_locations,
    ETLJobLog,
    get_etl_cutoff_date,
)
from utils.site_resolver import default_site

logger = logging.getLogger("ETL_WORKER")

# Lightweight Core reference to aetitle_modality_map.site_id — deliberately NOT added
# to the db.py ORM model (that class is shared everywhere and already has documented
# ORM/DB drift incidents; this file only needs one extra column for a filter). Mirrors
# routes/report_25.py's `m.site_id = :rh_site_id` site-scope pattern. aetitle is UNIQUE
# on the real table (db.py's aetitle_modality_map.aetitle), so this join can't fan out.
_ae_site_map = table("aetitle_modality_map", column("aetitle"), column("site_id"))

_STORAGE_JOB_NAME = "STORAGE_CUMULATIVE_SYNC"
_INCREMENTAL_OVERLAP_MINUTES = 30  # absorbs rows committed while the prior run was still scanning


def _storage_full_rebuild_forced():
    """Manual escape hatch — same RAYD_ETL_* on/off convention as etl_runner.py.
    Forces a full recalculation from go-live, ignoring the watermark."""
    return os.getenv('RAYD_ETL_STORAGE_FULL_REBUILD', '').strip().lower() in ('1', 'true', 'yes')


def _weekly_self_heal_due():
    """
    Incremental mode has one known blind spot: if Oracle corrects image_size_kb or
    file_system on an *existing* etl_image_locations row (not a new row), that UPDATE
    doesn't bump last_update — see etl_image_locations.py's upsert, which deliberately
    excludes last_update from its col_names so the column stays a true "first-seen"
    timestamp (that's what makes it usable as an incremental watermark at all). Such
    in-place corrections are rare (DICOM images are effectively write-once) but this
    file has a documented history of silent, long-lived drift (see the purge-step
    comment below), so a bounded weekly full rebuild self-heals that gap instead of
    letting it accumulate indefinitely.
    """
    return datetime.now().weekday() == 6  # Sunday


def _get_storage_watermark():
    """
    Incremental cutoff = start_time of the last SUCCESSFUL storage rollup, minus an
    overlap buffer. Deliberately using that run's *start* (not end) time re-covers
    anything committed while it was still running, trading a little redundant
    recompute for a hard guarantee against missing rows.
    Returns None when there is no prior success (first run, or every previous run
    failed) — caller does a full rebuild, exactly like before.
    """
    last_success = db.session.execute(text(
        "SELECT MAX(start_time) FROM etl_job_log WHERE job_name = :job AND status = 'SUCCESS'"
    ), {"job": _STORAGE_JOB_NAME}).scalar()
    if last_success is None:
        return None
    return last_success - timedelta(minutes=_INCREMENTAL_OVERLAP_MINUTES)


def refresh_storage_summary():
    """
    Recalculates storage into summary_storage_daily.

    Incremental by default: only studies whose etl_image_locations rows were first
    seen (inserted) since the last successful rollup get re-aggregated — but for
    those studies the FULL group total is recomputed (not just the delta), so late-
    arriving images on old study dates are still always captured correctly, same
    guarantee the old full-rebuild-every-run design had. Falls back to a full
    rebuild from go-live on the first-ever run, when forced via
    RAYD_ETL_STORAGE_FULL_REBUILD=1, or once a week (see _weekly_self_heal_due).
    """
    job_name   = _STORAGE_JOB_NAME
    start_time = datetime.now()
    success    = False

    go_live = get_etl_cutoff_date()
    if not go_live:
        logger.error("❌ [Storage Summary] No go-live date found — skipping.")
        return

    forced_full  = _storage_full_rebuild_forced()
    weekly_full  = (not forced_full) and _weekly_self_heal_due()
    watermark    = None if (forced_full or weekly_full) else _get_storage_watermark()
    full_rebuild = watermark is None

    if full_rebuild:
        reason = ("forced via RAYD_ETL_STORAGE_FULL_REBUILD" if forced_full else
                   "weekly self-heal" if weekly_full else
                   "no prior successful run")
        logger.info(f"📦 [Storage Summary] Full rebuild from go-live {go_live} — {reason}")
    else:
        logger.info(f"📦 [Storage Summary] Incremental — studies with image data added since {watermark}")

    try:
        # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
        # site) only, SJH excluded — same rule routes/report_25.py applies via
        # aetitle_modality_map.site_id (etl_didb_studies.site_id is never actually
        # populated, see utils/site_resolver.py's module docstring). This rollup had
        # never had the rule applied at all. default_site() resolves to None on a
        # non-LAUMC/single-site install (empty `sites` table), so the join/filter
        # below is skipped entirely there — never zeroes out a single-site deployment.
        rh_site_id = default_site()

        # Purge stale rows the site-scope filter below would no longer produce.
        # Found 2026-07-31: this rollup used to run with NO site filter at all
        # (fixed in a later commit, 29b28ff6), so summary_storage_daily accumulated
        # rows for SJH-associated AE titles (and pre-go-live dates) that the current,
        # correctly-scoped query never touches again -- an upsert only adds/updates
        # keys it produces, it never removes keys it no longer produces. Those orphaned
        # rows sat there permanently, and report_29's SUM(total_gb) (no site filter of
        # its own) kept including them: confirmed against production, 626 orphaned SJH
        # rows alone totaled 156TB against a real RH total of 45.6TB -- SJH averaging
        # ~249GB per daily aggregate row, which is physically impossible, versus RH's
        # 159,577 rows at realistic per-row sizes. Delete anything outside the current
        # valid scope before rebuilding it, every run, so this can't recur. Raw SQL
        # (not the ORM query/delete chain) to match how the rest of this codebase does
        # DB writes -- this exact pattern is proven everywhere else, unlike an
        # ORM-level bulk delete which has no precedent in this file to verify against.
        del_result = db.session.execute(text("""
            DELETE FROM summary_storage_daily s
            WHERE s.study_date < :go_live
               OR (
                    :rh_site_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM aetitle_modality_map m
                        WHERE UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
                          AND m.site_id = :rh_site_id
                    )
                )
        """), {"go_live": go_live, "rh_site_id": rh_site_id})
        if del_result.rowcount:
            logger.info(f"🧹 [Storage Summary] Purged {del_result.rowcount} stale/out-of-scope rows.")

        agg_query = (
            db.session.query(
                etl_didb_studies.study_date,
                func.coalesce(etl_didb_studies.storing_ae, 'UNKNOWN').label("storing_ae"),
                func.coalesce(etl_didb_studies.study_modality, 'UNKNOWN').label("modality"),
                func.coalesce(etl_didb_studies.procedure_code, 'UNKNOWN').label("procedure_code"),
                # image_size_kb is misleadingly named -- the raw Oracle column
                # (medistore.didb_image_locations.IMAGE_SIZE) is actually BYTES, not
                # KB. Confirmed 2026-07-31 against the full, freshly-loaded dataset
                # (164M rows): treating it as KB gives a median "image size" of
                # ~200MB and an average of ~336MB, which is physically impossible
                # for a single DICOM image at that volume -- no real modality
                # produces hundred-MB single-frame images as a MEDIAN across
                # millions of images. Treating it as bytes gives a median of ~200KB
                # and average of ~336KB, which matches real DICOM file sizes.
                # This divisor was previously changed from 1_073_741_824 (bytes->GB)
                # to 1_048_576 (KB->GB) based on an earlier, much smaller sample
                # that seemed to confirm "kb" — that change was the actual bug, and
                # explains the storage total jumping ~1024x (0.37TB -> 440TB and
                # climbing to 52,610TB as more data loaded). Reverted to bytes->GB.
                func.round(
                    func.cast(
                        func.coalesce(
                            func.sum(etl_image_locations.image_size_kb), 0
                        ), db.Numeric
                    ) / 1_073_741_824,
                    4,
                ).label("total_gb"),
                func.count(
                    distinct(etl_didb_studies.study_db_uid)
                ).label("study_count"),
            )
            .join(
                etl_didb_raw_images,
                etl_didb_studies.study_db_uid == etl_didb_raw_images.study_db_uid,
            )
            .join(
                etl_image_locations,
                etl_didb_raw_images.raw_image_db_uid == etl_image_locations.raw_image_db_uid,
            )
        )

        if rh_site_id is not None:
            agg_query = agg_query.join(
                _ae_site_map,
                func.upper(func.trim(etl_didb_studies.storing_ae))
                == func.upper(func.trim(_ae_site_map.c.aetitle)),
            ).filter(_ae_site_map.c.site_id == rh_site_id)

        agg_query = (
            agg_query
            .filter(etl_didb_studies.study_date >= go_live)
            .filter(etl_didb_studies.study_modality != 'SR')
        )

        if not full_rebuild:
            # Scope to studies with at least one image row first-seen since the
            # watermark. The SUM above still runs over ALL of that study's images
            # (not just the new ones) once it's in scope, so the recomputed total
            # is exactly right — this only narrows *which* groups get touched, not
            # how their value is computed.
            dirty_studies = (
                select(etl_didb_studies.study_db_uid)
                .select_from(etl_didb_studies)
                .join(etl_didb_raw_images,
                      etl_didb_studies.study_db_uid == etl_didb_raw_images.study_db_uid)
                .join(etl_image_locations,
                      etl_didb_raw_images.raw_image_db_uid == etl_image_locations.raw_image_db_uid)
                .where(etl_image_locations.last_update >= watermark)
                .distinct()
            )
            agg_query = agg_query.filter(etl_didb_studies.study_db_uid.in_(dirty_studies))

        agg_query = agg_query.group_by(
            etl_didb_studies.study_date,
            etl_didb_studies.storing_ae,
            etl_didb_studies.study_modality,
            etl_didb_studies.procedure_code,
        )

        insert_stmt = insert(summary_storage_daily).from_select(
            ["study_date", "storing_ae", "modality", "procedure_code",
             "total_gb", "study_count"],
            agg_query,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["study_date", "storing_ae", "modality", "procedure_code"],
            set_={
                "total_gb":    insert_stmt.excluded.total_gb,
                "study_count": insert_stmt.excluded.study_count,
            },
        )

        result  = db.session.execute(upsert_stmt)
        db.session.commit()
        success = True
        logger.info(f"✅ [Storage Summary] Done — {result.rowcount} rows upserted.")

    except Exception as e:
        db.session.rollback()
        logger.error(f"🛑 [Storage Summary] Failed: {e}", exc_info=True)

    finally:
        try:
            duration = round((datetime.now() - start_time).total_seconds(), 2)
            db.session.add(ETLJobLog(
                job_name         = job_name,
                status           = "SUCCESS" if success else "FAILED",
                start_time       = start_time,
                end_time         = datetime.now(),
                duration_seconds = duration,
                error_message    = None,
            ))
            db.session.commit()
        except Exception as log_e:
            logger.error(f"[Storage Summary] Log write failed: {log_e}")


if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        refresh_storage_summary()
