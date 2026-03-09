"""
etl_storage_summary.py
────────────────────────────────────────────────────────────────────────────
Phase 4 of the ETL pipeline.
Aggregates per-image file sizes into summary_storage_daily after each sync.

Join chain:
  etl_didb_studies
    → etl_didb_raw_images  (on study_instance_uid)
    → etl_image_locations  (on raw_image_db_uid)   ← file_size_kb lives here
    ⇢ aetitle_modality_map (outerjoin, for modality label)

Called from etl_runner.py as the final phase.
Can also be run standalone: python etl_storage_summary.py
"""

import os
import sys
import logging
from datetime import datetime

from sqlalchemy import text, func, distinct
from sqlalchemy.dialects.postgresql import insert

# ── Path injection (needed when run standalone) ───────────────────────────
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
for p in (parent_dir, current_dir):
    if p not in sys.path:
        sys.path.append(p)

from db import (
    db,
    SummaryStorageDaily,
    EtlDidbStudy,
    etl_didb_raw_images,
    etl_image_locations,
    ETLJobLog,
    AETitleModalityMap,
    get_etl_cutoff_date,
)

logger = logging.getLogger("STORAGE_ANALYTICS")


def refresh_storage_summary():
    """
    Cumulative rollup: recalculates storage from go-live to today.
    Upserts into summary_storage_daily — existing rows are overwritten
    so that newly arrived images on old study dates are captured.
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
        # ── Core aggregation query ────────────────────────────────────────
        # NOTE: file_size_kb is on ImageLocation (the physical file record),
        #       not on EtlDidbRawImage (the DICOM object record).
        #       If your schema differs, swap the column source and re-test.
        agg_query = (
            db.session.query(
                EtlDidbStudy.study_date,
                EtlDidbStudy.storing_ae,
                func.coalesce(
                    AETitleModalityMap.modality, "UNKNOWN"
                ).label("modality"),
                EtlDidbStudy.procedure_code,
                # KB → GB  (1 GB = 1,048,576 KB)
                func.round(
                    func.coalesce(func.sum(ImageLocation.image_size_kb), 0)
                    / 1_048_576.0,
                    4,
                ).label("total_gb"),
                func.count(
                    distinct(EtlDidbStudy.study_db_uid)
                ).label("study_count"),
            )
            .join(
                EtlDidbRawImage,
                EtlDidbStudy.study_instance_uid == EtlDidbRawImage.study_instance_uid,
            )
            .join(
                ImageLocation,
                EtlDidbRawImage.raw_image_db_uid == ImageLocation.raw_image_db_uid,
            )
            .outerjoin(
                AETitleModalityMap,
                func.upper(func.trim(EtlDidbStudy.storing_ae))
                == func.upper(func.trim(AETitleModalityMap.aetitle)),
            )
            .filter(EtlDidbStudy.study_date >= go_live)
            .group_by(
                EtlDidbStudy.study_date,
                EtlDidbStudy.storing_ae,
                # Reference the coalesce label by position to avoid repeating it
                text("3"),
                EtlDidbStudy.procedure_code,
            )
        )

        # ── UPSERT into summary_storage_daily ─────────────────────────────
        insert_stmt = insert(SummaryStorageDaily).from_select(
            ["study_date", "storing_ae", "modality", "procedure_code",
             "total_gb", "study_count"],
            agg_query,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="_date_ae_mod_proc_uc",
            set_={
                "total_gb":    insert_stmt.excluded.total_gb,
                "study_count": insert_stmt.excluded.study_count,
            },
        )

        result  = db.session.execute(upsert_stmt)
        db.session.commit()
        success = True

        logger.info(f"✅ [Storage Summary] Done — {result.rowcount} rows upserted (go-live: {go_live}).")

    except Exception as e:
        db.session.rollback()
        logger.error(f"🛑 [Storage Summary] Failed: {e}", exc_info=True)

    finally:
        # ── Write ETL job log ─────────────────────────────────────────────
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


# ── Standalone entry point ────────────────────────────────────────────────
if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        refresh_storage_summary()
