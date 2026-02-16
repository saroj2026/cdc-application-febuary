"""
Restart backend, then: drop target table, reset pipeline2 full load state,
delete connectors, start pipeline2. Verifies auto-create + full load + CDC.
"""
import requests
import time
import pyodbc
import psycopg2
import subprocess
import sys
import os

API_URL = "http://localhost:8000/api/v1"
KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
PIPELINE2_ID = "acc52bb1-e5e5-4a4f-aa6a-7aed85b254ed"

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

def wait_for_backend(timeout=60):
    for i in range(timeout):
        try:
            r = requests.get(f"{API_URL}/health", timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False

print("="*70)
print("RESTART BACKEND AND RUN PIPELINE2 WITH NEW CODE")
print("="*70)

# Step 1: Kill existing backend on port 8000 (Windows)
print("\n1. Stopping any backend on port 8000...")
try:
    subprocess.run(
        ["powershell", "-Command", "Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"],
        capture_output=True, timeout=10, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    time.sleep(2)
except Exception as e:
    print(f"   (optional) {e}")

# Step 2: Start backend in background
print("\n2. Starting backend...")
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
proc = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "ingestion.api:app", "--host", "0.0.0.0", "--port", "8000"],
    cwd=backend_dir,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
)
print("   Backend process started (PID %s)" % proc.pid)

print("\n3. Waiting for backend to be ready...")
if not wait_for_backend(90):
    print("   ERROR: Backend did not become ready")
    proc.kill()
    sys.exit(1)
print("   Backend is ready")

# Step 4: Drop dbo.employees in SQL Server
print("\n4. Dropping dbo.employees in SQL Server...")
try:
    sql_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
    ) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    sql_cur.execute("IF OBJECT_ID('dbo.employees', 'U') IS NOT NULL DROP TABLE dbo.employees")
    sql_conn.commit()
    sql_conn.close()
    print("   Dropped (or table did not exist)")
except Exception as e:
    print("   Error: %s" % e)

# Step 5: Reset pipeline2 full load state in PostgreSQL
print("\n5. Resetting pipeline2 full load state in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        UPDATE public.pipelines
        SET full_load_status = 'NOT_STARTED', full_load_completed_at = NULL, full_load_lsn = NULL
        WHERE name = 'pipeline2'
    """)
    pg_conn.commit()
    pg_conn.close()
    print("   Reset full_load_status to NOT_STARTED")
except Exception as e:
    print("   Error: %s" % e)

# Step 6: Delete Kafka Connect connectors for pipeline2
print("\n6. Deleting Kafka Connect connectors for pipeline2...")
for conn_name in ["sink-pipeline2-mssql-dbo", "cdc-pipeline2-pg-public"]:
    try:
        r = requests.delete("%s/connectors/%s" % (KAFKA_CONNECT_URL, conn_name), timeout=15)
        if r.status_code in [200, 204]:
            print("   Deleted %s" % conn_name)
        else:
            print("   %s: %s" % (conn_name, r.status_code))
    except Exception as e:
        print("   %s: %s" % (conn_name, e))
time.sleep(5)

# Step 7: Start pipeline2 (auto-create + full load + CDC)
print("\n7. Starting pipeline2 (auto-create -> full load -> CDC)...")
try:
    r = requests.post("%s/pipelines/%s/start" % (API_URL, PIPELINE2_ID), timeout=120)
    if r.status_code == 200:
        result = r.json()
        print("   Started successfully")
        print("   Full load: %s" % result.get("full_load", {}).get("success"))
        print("   Debezium: %s" % result.get("debezium_connector", {}).get("success"))
        print("   Sink: %s" % result.get("sink_connector", {}).get("success"))
    else:
        print("   Error: %s %s" % (r.status_code, r.text[:300]))
except Exception as e:
    print("   Error: %s" % e)

print("\n8. Waiting 45 seconds for full load and CDC setup...")
time.sleep(45)

# Step 9: Verify target table and data
print("\n9. Verifying target table (row_id + CDC columns) and data...")
try:
    sql_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
    ) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    
    sql_cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    columns = sql_cur.fetchall()
    if not columns:
        print("   Table dbo.employees not found")
    else:
        col_names = [c[0] for c in columns]
        has_row_id = "row_id" in col_names
        has_op = "__op" in col_names
        has_ts = "__source_ts_ms" in col_names
        has_deleted = "__deleted" in col_names
        print("   Columns: %s" % ", ".join(col_names))
        print("   row_id: %s" % ("YES" if has_row_id else "NO"))
        print("   __op, __source_ts_ms, __deleted: %s" % ("YES" if (has_op and has_ts and has_deleted) else "NO"))
        
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees")
        total = sql_cur.fetchone()[0]
        print("   Total rows: %s" % total)
        
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees WHERE __op = 'r'")
        full_load_rows = sql_cur.fetchone()[0]
        print("   Rows with __op='r' (full load): %s" % full_load_rows)
        
        if total > 0:
            sql_cur.execute("SELECT TOP 3 row_id, id, name, __op, __source_ts_ms FROM dbo.employees")
            print("   Sample rows:")
            for row in sql_cur.fetchall():
                print("     row_id=%s id=%s name=%s __op=%s ts=%s" % (row[0], row[1], row[2], row[3], row[4]))
    
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

print("\n" + "="*70)
print("Done. Backend is still running (PID %s)." % proc.pid)
print("="*70)
