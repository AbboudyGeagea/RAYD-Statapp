import os
import sys
import logging
from datetime import datetime
from sqlalchemy import text, func, distinct
from sqlalchemy.dialects.postgresql import insert

# PATH INJECTION
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
if current_dir not in sys.path:
    sys.path.append(current_dir)

from db import (
    db, 
    SummaryStorageDaily, 
    EtlDidbStudy, 
    ImageLocation, 
    ETLJobLog, 
    AETitleModalityMap,
    get_etl_cutoff_date,
    EtlDidbRawImage   # ✅ REQUIRED FOR CORRECT JOIN
)

logger = logging.getLogger("STORAGE_ANALYTICS")

def refresh_storage_summary():
    """
    Cumulative Rollup: Recalculates storage from Go-Live to today.
    Uses additive logic: if images are added to old studies, 
    the total goes UP, never down.
    """
    job_name = "STORAGE_CUMULATIVE_SYNC"
    start_time = datetime.now()

    go_live = get_etl_cutoff_date()
    if not go_live:
        print("❌ No Go-Live date found. Skipping.")
        return

    try:
        # ✅ FIXED QUERY (Correct Join Chain)
        raw_data_query = db.session.query(
            EtlDidbStudy.study_date,
            EtlDidbStudy.storing_ae,
            func.coalesce(AETitleModalityMap.modality, 'UNKNOWN').label('modality'),
            EtlDidbStudy.procedure_code,
            (func.sum(ImageLocation.image_size_kb) / 1024.0 / 1024.0).label('total_gb'),
            func.count(distinct(EtlDidbStudy.study_db_uid)).label('study_count')
        ).join(
            # Study → Raw Images (via study_instance_uid)
            EtlDidbRawImage,
            EtlDidbStudy.study_instance_uid.cast(text) == EtlDidbRawImage.study_instance_uid.cast(text)
        ).join(
            # Raw Images → Image Locations (via raw_image_db_uid)
            ImageLocation,
            EtlDidbRawImage.raw_image_db_uid.cast(text) == ImageLocation.raw_image_db_uid.cast(text)
        ).outerjoin(
            AETitleModalityMap,
            EtlDidbStudy.storing_ae == AETitleModalityMap.aetitle
        ).filter(
            EtlDidbStudy.study_date >= go_live
        ).group_by(
            EtlDidbStudy.study_date,
            EtlDidbStudy.storing_ae,
            text("modality"),
            EtlDidbStudy.procedure_code
        ).subquery()

        # UPSERT
        stmt = insert(SummaryStorageDaily).from_select(
            ['study_date', 'storing_ae', 'modality', 'procedure_code', 'total_gb', 'study_count'],
            db.session.query(raw_data_query)
        )

        upsert_stmt = stmt.on_conflict_do_update(
            constraint='_date_ae_mod_proc_uc',
            set_={
                'total_gb': stmt.excluded.total_gb,
                'study_count': stmt.excluded.study_count
            }
        )

        db.session.execute(upsert_stmt)
        db.session.commit()

        print(f"✅ Storage Rollup successful from {go_live}")

    except Exception as e:
        db.session.rollback()
        print(f"❌ Storage Rollup Failed: {str(e)}")

    finally:
        try:
            duration = (datetime.now() - start_time).total_seconds()
            new_log = ETLJobLog(
                job_name=job_name,
                status="SUCCESS" if 'upsert_stmt' in locals() else "FAILED",
                start_time=start_time,
                end_time=datetime.now(),
                duration_seconds=round(duration, 2),
                error_message=None
            )
            db.session.add(new_log)
            db.session.commit()
        except Exception as log_e:
            print(f"Log Error: {log_e}")

if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        refresh_storage_summary()
