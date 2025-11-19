import oracledb
import traceback

# ------------------------------
# CONFIGURATION
# ------------------------------
USER = "sys"
PASSWORD = "a1d2m7i4"        # replace with your real password
HOST = "10.10.11.50"
PORT = 1521
SID = "mst1"

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
