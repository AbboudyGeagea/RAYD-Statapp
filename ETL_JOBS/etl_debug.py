"""
etl_debug.py  —  run this directly on the server to find the crash:
  cd /home/stats/StatsApp/ETL_JOBS
  python etl_debug.py
"""
import sys, os
print(f"Python: {sys.version}")
print(f"CWD: {os.getcwd()}")

steps = []

def try_import(label, fn):
    try:
        fn()
        print(f"  ✅  {label}")
    except Exception as e:
        print(f"  ❌  {label}  →  {type(e).__name__}: {e}")
        sys.exit(1)   # stop at first failure so the error is clear

# ── Path setup ───────────────────────────────────────────────────────────
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
for p in (parent_dir, current_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

print(f"\nsys.path entries:")
for p in sys.path[:6]:
    print(f"  {p}")

print("\n── Imports ─────────────────────────────────────────────────────────")

try_import("sqlalchemy.text",        lambda: __import__('sqlalchemy'))
try_import("db module",              lambda: __import__('db'))
try_import("etl_settings / ETL_GEAR",lambda: __import__('etl_settings'))

try_import("etl_didb_studies",       lambda: __import__('etl_didb_studies'))
try_import("etl_orders",             lambda: __import__('etl_orders'))
try_import("etl_series",             lambda: __import__('etl_series'))
try_import("etl_didb_raw_images",    lambda: __import__('etl_didb_raw_images'))
try_import("etl_patients_view",      lambda: __import__('etl_patients_view'))
try_import("etl_image_locations",    lambda: __import__('etl_image_locations'))
try_import("etl_analytics_refresh",    lambda: __import__('etl_analytics_refresh'))

print("\n── DB connection ───────────────────────────────────────────────────")
try_import("OracleConnector import", lambda: None)  # already done above

try:
    import db as database_module
    from sqlalchemy import create_engine, text
    uri = os.getenv('SQLALCHEMY_DATABASE_URI', 'postgresql://etl_user:SecureCrynBabe@localhost:5432/etl_db')
    engine = create_engine(uri)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
    print(f"  ✅  PostgreSQL connection → {result}")
except Exception as e:
    print(f"  ❌  PostgreSQL connection → {type(e).__name__}: {e}")

try:
    from etl_settings import ETL_GEAR
    print(f"\n── ETL_GEAR ────────────────────────────────────────────────────────")
    for k, v in ETL_GEAR.items():
        print(f"  {k}: {v}")
except Exception as e:
    print(f"  ❌  ETL_GEAR read → {e}")

print("\n── etl_runner import ───────────────────────────────────────────────")
try_import("etl_runner itself",      lambda: __import__('etl_runner'))

print("\n✅ All imports OK — crash is happening at runtime, not import time.")
print("   Try running execute_sync() manually and check for the error below:\n")
try:
    from etl_runner import execute_sync
    execute_sync()
except Exception as e:
    import traceback
    print(f"❌ execute_sync() crashed:\n")
    traceback.print_exc()
