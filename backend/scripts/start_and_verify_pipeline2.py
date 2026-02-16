"""
Start pipeline2 and verify automatic flow:
auto-create schema -> full load -> CDC
"""
import requests
import json
import time
import pyodbc

API_URL = "http://localhost:8000/api/v1"
PIPELINE_ID = "acc52bb1-e5e5-4a4f-aa6a-7aed85b254ed"  # pipeline2

print("="*70)
print("STARTING PIPELINE2 - Testing Automatic Flow")
print("="*70)

# Step 1: Start pipeline2
print("\n1. Starting pipeline2...")
print("   This will automatically: auto-create schema -> full load -> CDC")
try:
    r = requests.post(f"{API_URL}/pipelines/{PIPELINE_ID}/start", timeout=120)
    if r.status_code == 200:
        result = r.json()
        print(f"   Started successfully!")
        print(f"   Full load: {result.get('full_load', {}).get('success', 'N/A')}")
        print(f"   Debezium: {result.get('debezium_connector', {}).get('success', 'N/A')}")
        print(f"   Sink: {result.get('sink_connector', {}).get('success', 'N/A')}")
    else:
        print(f"   Error: {r.status_code}")
        print(f"   {r.text[:500]}")
        exit(1)
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 2: Wait for full load to complete
print("\n2. Waiting 30 seconds for full load and CDC setup...")
time.sleep(30)

# Step 3: Verify target table was auto-created with CDC columns
print("\n3. Verifying target table schema...")
SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

sql_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_HOST},{SQL_PORT};DATABASE={SQL_DB};"
    f"UID={SQL_USER};PWD={SQL_PASS};"
    f"Encrypt=no;TrustServerCertificate=yes"
)

try:
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    
    sql_cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    columns = sql_cur.fetchall()
    if columns:
        print("   Target table columns (auto-created):")
        has_row_id = False
        has_op = False
        has_ts = False
        has_deleted = False
        
        for col in columns:
            print(f"     {col[0]:25s} {col[1]}")
            if col[0] == 'row_id':
                has_row_id = True
            if col[0] == '__op':
                has_op = True
            if col[0] == '__source_ts_ms':
                has_ts = True
            if col[0] == '__deleted':
                has_deleted = True
        
        print("\n   Verification:")
        print(f"     Surrogate key (row_id): {'YES' if has_row_id else 'NO'}")
        print(f"     CDC metadata (__op): {'YES' if has_op else 'NO'}")
        print(f"     CDC metadata (__source_ts_ms): {'YES' if has_ts else 'NO'}")
        print(f"     CDC metadata (__deleted): {'YES' if has_deleted else 'NO'}")
        
        if has_row_id and has_op and has_ts and has_deleted:
            print("\n   SUCCESS! Table auto-created with correct SCD2 schema!")
        else:
            print("\n   WARNING: Some CDC columns are missing")
    else:
        print("   Table not found - auto-create might have failed")
    
    # Check full load data
    sql_cur.execute("SELECT COUNT(*) FROM dbo.employees WHERE __op = 'r'")
    full_load_count = sql_cur.fetchone()[0]
    print(f"\n4. Full load data:")
    print(f"   Rows with __op='r': {full_load_count}")
    
    if full_load_count > 0:
        print("   SUCCESS! Full load completed with __op='r'")
        sql_cur.execute("SELECT TOP 3 row_id, id, first_name, __op, __source_ts_ms FROM dbo.employees WHERE __op = 'r'")
        print("\n   Sample full load rows:")
        for row in sql_cur.fetchall():
            print(f"     row_id={row[0]}, id={row[1]}, name={row[2]}, __op={row[3]}, ts={row[4]}")
    
    sql_conn.close()

except Exception as e:
    print(f"   Error: {e}")

# Step 4: Check Kafka Connect connectors
print("\n5. Checking Kafka Connect connectors...")
try:
    r = requests.get("http://72.61.233.209:8083/connectors", timeout=10)
    connectors = r.json()
    
    pipeline2_connectors = [c for c in connectors if 'pipeline-2' in c or 'pipeline2' in c]
    if pipeline2_connectors:
        print(f"   Found {len(pipeline2_connectors)} connector(s) for pipeline2:")
        for conn_name in pipeline2_connectors:
            r = requests.get(f"http://72.61.233.209:8083/connectors/{conn_name}/status", timeout=10)
            status = r.json()
            state = status.get('connector', {}).get('state', 'UNKNOWN')
            task_state = status.get('tasks', [{}])[0].get('state', 'UNKNOWN') if status.get('tasks') else 'NO TASKS'
            print(f"     {conn_name}: {state} / Task: {task_state}")
    else:
        print("   No pipeline2 connectors found yet")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "="*70)
print("AUTO-CREATE -> FULL LOAD -> CDC flow test complete!")
print("Next: Test CDC by running INSERT/UPDATE/DELETE in PostgreSQL")
print("="*70)
