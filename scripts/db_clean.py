#!/usr/bin/env python3
"""
scripts/db_clean.py — Wipe all ETL-managed tables and optionally trigger a fresh sync.

Usage (inside the rayd-app container):
    python scripts/db_clean.py           # truncate only
    python scripts/db_clean.py --sync    # truncate then run ETL
    python scripts/db_clean.py --yes     # skip confirmation prompt

Tables cleared:
    etl_didb_studies, etl_patient_view, etl_didb_serieses,
    etl_didb_raw_images, etl_image_locations, etl_orders,
    summary_storage_daily, analytics_snapshots
"""

import os
import sys

# Allow running from repo root or scripts/
_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from sqlalchemy import create_engine, text

RED    = "\033[0;31m"
GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
NC     = "\033[0m"

def info(msg):  print(f"{CYAN}[INFO]{NC}  {msg}")
def ok(msg):    print(f"{GREEN}[OK]{NC}    {msg}")
def warn(msg):  print(f"{YELLOW}[WARN]{NC}  {msg}")
def error(msg): print(f"{RED}[ERROR]{NC} {msg}"); sys.exit(1)


ETL_TABLES = [
    "etl_didb_studies",
    "etl_patient_view",
    "etl_didb_serieses",
    "etl_didb_raw_images",
    "etl_image_locations",
    "etl_orders",
    "summary_storage_daily",
    "analytics_snapshots",
]


def truncate_etl_tables(engine):
    table_list = ", ".join(ETL_TABLES)
    with engine.begin() as conn:
        info(f"Truncating: {table_list}")
        conn.execute(text(
            f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE"
        ))
    ok("All ETL tables cleared.")


def run_etl_sync():
    info("Triggering ETL sync via app context...")
    try:
        from app import create_app
        from ETL_JOBS.etl_runner import execute_sync
        app = create_app()
        with app.app_context():
            execute_sync(app)
        ok("ETL sync complete.")
    except Exception as e:
        error(f"ETL sync failed: {e}")


def main():
    auto_yes = "--yes" in sys.argv or "-y" in sys.argv
    run_sync  = "--sync" in sys.argv

    uri = os.getenv("SQLALCHEMY_DATABASE_URI")
    if not uri:
        error("SQLALCHEMY_DATABASE_URI is not set.")

    print()
    print("=" * 54)
    print("       RAYD — ETL Clean DB")
    print("=" * 54)
    print()
    warn("This will TRUNCATE all ETL data tables.")
    warn("Tables: " + ", ".join(ETL_TABLES))
    if run_sync:
        info("A full ETL sync will run immediately after.")
    print()

    if not auto_yes:
        try:
            answer = input("Type 'yes' to continue: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            error("Aborted.")
        if answer != "yes":
            error("Aborted.")

    engine = create_engine(uri)
    truncate_etl_tables(engine)

    if run_sync:
        run_etl_sync()

    print()
    print("=" * 54)
    print(f"{GREEN}  Done.{NC}")
    if not run_sync:
        print(f"  Run ETL: docker compose exec rayd-app python app.py -m")
    print("=" * 54)
    print()


if __name__ == "__main__":
    main()
