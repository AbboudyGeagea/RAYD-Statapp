"""
ETL_JOBS/backfill_ae_studies.py
--------------------------------
One-off backfill for an AE that was previously on etl_didb_studies.py's
_EXCLUDED_AE_SQL list and has therefore been silently skipped by every regular
incremental STUDIES_ETL run since that exclusion was added. Removing an AE from
the exclusion list only affects studies going forward -- the normal incremental
query only pulls STUDY_DB_UID > max_uid OR STUDY_DATE within the 10-day lookback,
so historical rows for a newly un-excluded AE need this separate full-history pull.

Run once, inside the app container:
    docker compose exec rayd-app python ETL_JOBS/backfill_ae_studies.py DEFINIUM1

Safe to re-run: the underlying upsert is keyed on study_db_uid, so a repeat run
just re-applies the same rows.

2026-09-04: written to recover 'DEFINIUM1' (a GE Definium general-radiography room
wrongly bundled into the cardiology/vascular-lab exclusion list), which orphaned
~12k hl7_oru_reports rows with no matching PACS study -- surfaced as reports with
blank modality in the ORU tab.
"""
import os
import sys
import logging

# Same bootstrap as daily_analytics.py and etl_runner.py, which this file was
# missing -- so the documented command above died on `from app import create_app`
# with ModuleNotFoundError before reaching a single line of its own logic. Running
# a file directly puts only its OWN directory on sys.path, so ETL_JOBS/ resolved
# (that's where this script lives) but the repo root holding app.py and db.py did
# not. That is why this backfill had never actually run since it was written on
# 2026-09-04: not skipped, broken. HERE is added as well so `python -m
# ETL_JOBS.backfill_ae_studies` works too -- that form puts the ROOT on sys.path
# but not ETL_JOBS/, which is the mirror image of the same failure.
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

logging.basicConfig(level=logging.INFO)


def main():
    if len(sys.argv) != 2:
        print("Usage: python ETL_JOBS/backfill_ae_studies.py <AE_TITLE>")
        sys.exit(1)
    ae = sys.argv[1].strip()

    from app import create_app
    import db as database_module
    from etl_didb_studies import run_studies_etl

    app = create_app()
    with app.app_context():
        go_live = database_module.get_go_live_date() or '2000-01-01'
        engine = database_module.get_pg_engine()
        print(f"[Backfill] Pulling ALL '{ae}' studies since {go_live} ...")
        total, uids = run_studies_etl(
            engine, "PROD_ORACLE", "etl_didb_studies",
            database_module.chunked_upsert, go_live, force_ae=ae,
        )
        print(f"[Backfill] Done — {total:,} '{ae}' studies loaded into etl_didb_studies.")


if __name__ == '__main__':
    main()
