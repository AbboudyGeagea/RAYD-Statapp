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
from datetime import datetime
from sqlalchemy import func, distinct, table, column
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


def refresh_storage_summary():
    """
    Cumulative rollup: recalculates storage from go-live to today.
    Upserts into summary_storage_daily so newly arrived images on old
    study dates are always captured.
    """
    job_name   = "STORAGE_CUMULATIVE_SYNC"
    start_time = datetime.now()
    success    = False

    go_live = get_etl_cutoff_date()
    if not go_live:
        logger.error("❌ [Storage Summary] No go-live date found — skipping.")
        return

    logger.info(f"📦 [Storage Summary] Rolling up storage from {go_live} ...")

    try:
        # LAUMC site rule (operator instruction, 2026-07-26): reports show RH (main
        # site) only, SJH excluded — same rule routes/report_25.py applies via
        # aetitle_modality_map.site_id (etl_didb_studies.site_id is never actually
        # populated, see utils/site_resolver.py's module docstring). This rollup had
        # never had the rule applied at all. default_site() resolves to None on a
        # non-LAUMC/single-site install (empty `sites` table), so the join/filter
        # below is skipped entirely there — never zeroes out a single-site deployment.
        rh_site_id = default_site()

        agg_query = (
            db.session.query(
                etl_didb_studies.study_date,
                func.coalesce(etl_didb_studies.storing_ae, 'UNKNOWN').label("storing_ae"),
                func.coalesce(etl_didb_studies.study_modality, 'UNKNOWN').label("modality"),
                func.coalesce(etl_didb_studies.procedure_code, 'UNKNOWN').label("procedure_code"),
                func.round(
                    func.cast(
                        func.coalesce(
                            func.sum(etl_image_locations.image_size_kb), 0
                        ), db.Numeric
                    ) / 1_048_576,
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
            .group_by(
                etl_didb_studies.study_date,
                etl_didb_studies.storing_ae,
                etl_didb_studies.study_modality,
                etl_didb_studies.procedure_code,
            )
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
