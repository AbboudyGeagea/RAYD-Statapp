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
    argv = sys.argv[1:]
    with_children = '--with-children' in argv
    positional = [a for a in argv if not a.startswith('--')]
    if len(positional) != 1:
        print("Usage: python ETL_JOBS/backfill_ae_studies.py <AE_TITLE> [--with-children]")
        print()
        print("  --with-children  also pull series, raw images and image locations for")
        print("                   the studies recovered, then re-derive study_modality.")
        print("                   Without it only etl_didb_studies is populated, which")
        print("                   is enough for the study-level reports but leaves the")
        print("                   recovered studies invisible to anything counting")
        print("                   images or storage.")
        sys.exit(1)
    ae = positional[0].strip()

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

        if not with_children:
            print(f"[Backfill] Children NOT pulled. These {total:,} studies have no series or "
                  f"image rows, so storage/image analytics will undercount them. "
                  f"Re-run with --with-children to complete them.")
            return

        # `uids` is what run_studies_etl already returns and this script used to
        # discard. Phases 2/3/4 in etl_runner.py take exactly this whitelist, so
        # scoping them to the recovered studies costs one pass over THIS AE's rows
        # instead of the ~166M-row full reload that running those phases via
        # RAYD_ETL_PHASES would trigger (etl_runner._ensure_active_ids rebuilds the
        # whitelist as *every* study in Postgres when Phase 1 is skipped).
        if not uids:
            print("[Backfill] No study IDs returned — nothing to pull children for.")
            return

        from sqlalchemy import text
        from etl_series import run_series_etl
        from etl_didb_raw_images import run_raw_images_etl
        from etl_image_locations import run_images_etl

        # Isolated per step, matching how etl_runner.py treats its own sub-steps: one
        # failing here must not leave the others unattempted, and the studies are
        # already safely loaded by this point either way.
        for label, fn in (
            ("series",          lambda: run_series_etl(
                engine, "PROD_ORACLE", 'etl_didb_serieses',
                database_module.chunked_upsert, uids)),
            ("raw images",      lambda: run_raw_images_etl(
                engine, "PROD_ORACLE", 'etl_didb_raw_images',
                database_module.chunked_upsert, uids)),
            ("image locations", lambda: run_images_etl(
                engine, "PROD_ORACLE", 'etl_image_locations',
                database_module.chunked_upsert, uids)),
        ):
            try:
                print(f"[Backfill] Pulling {label} for {len(uids):,} studies ...")
                fn()
            except Exception as exc:
                logging.error("Backfill sub-step '%s' failed: %s", label, exc, exc_info=True)
                print(f"[Backfill] ⚠️  {label} FAILED — see log. Continuing.")

        # Phase 2b, scoped. study_modality is not in the DIDB_STUDIES query at all; it
        # is derived from the series just loaded, and Phase 7 (storage rollup) and
        # Phase 8 (procedure mapping) both depend on it being set.
        try:
            with engine.begin() as conn:
                res = conn.execute(text("""
                    UPDATE etl_didb_studies s
                    SET study_modality = sub.modality
                    FROM (
                        SELECT study_db_uid,
                               MODE() WITHIN GROUP (ORDER BY modality) AS modality
                        FROM etl_didb_serieses
                        WHERE modality IS NOT NULL AND TRIM(modality) != ''
                          AND study_db_uid = ANY(:uids)
                        GROUP BY study_db_uid
                    ) sub
                    WHERE s.study_db_uid = sub.study_db_uid
                      AND (s.study_modality IS NULL OR s.study_modality != sub.modality)
                """), {"uids": list(uids)})
            print(f"[Backfill] study_modality set on {res.rowcount:,} studies.")
        except Exception as exc:
            logging.error("study_modality backfill failed: %s", exc, exc_info=True)
            print("[Backfill] ⚠️  study_modality backfill FAILED — see log.")

        print(f"[Backfill] Complete for '{ae}'.")


if __name__ == '__main__':
    main()
