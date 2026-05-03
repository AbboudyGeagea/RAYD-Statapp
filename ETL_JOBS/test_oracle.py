import os
import oracledb
import traceback

# ------------------------------
# CONFIGURATION — read from environment, never hardcode credentials
# ------------------------------
USER = os.environ.get("ORACLE_TEST_USER", "sys")
PASSWORD = os.environ.get("ORACLE_TEST_PASSWORD", "")
HOST = os.environ.get("ORACLE_TEST_HOST", "")
PORT = int(os.environ.get("ORACLE_TEST_PORT", "1521"))
SID = os.environ.get("ORACLE_TEST_SID", "")

# ------------------------------
# TEST CONNECTION
# ------------------------------
try:
    print(f"🔌 Attempting Oracle connection to {HOST}:{PORT}/{SID} as SYSDBA…")
    
    conn = oracledb.connect(
        user=USER,
        password=PASSWORD,
        dsn=f"{HOST}:{PORT}/{SID}",
        mode=oracledb.SYSDBA
    )
    
    print("✅ Connected successfully!")

    # Quick test query
    cursor = conn.cursor()
    cursor.execute("SELECT sysdate FROM dual")
    result = cursor.fetchone()
    print(f"⏰ Oracle sysdate: {result[0]}")

    cursor.close()
    conn.close()

except Exception as e:
    print("❌ Connection failed!")
    print(traceback.format_exc())
