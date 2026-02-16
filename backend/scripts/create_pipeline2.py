"""
Create and start pipeline2 to test automatic flow:
auto-create schema → full load → CDC

Pipeline2: PostgreSQL public.employees → SQL Server dbo.employees
"""
import requests
import json
import time
import pyodbc

API_URL = "http://localhost:8000/api/v1"
KAFKA_CONNECT_URL = "http://72.61.233.209:8083"

# Connection IDs (need to get these from the backend)
# For now, we'll use the connection details directly

print("="*70)
print("CREATING PIPELINE2 - Testing Automatic Flow")
print("="*70)

# Step 1: Get or create connections
print("\n1. Checking connections...")
try:
    # List connections
    r = requests.get(f"{API_URL}/connections", timeout=10)
    connections = r.json()
    
    # Find PostgreSQL and SQL Server connections
    pg_conn = None
    mssql_conn = None
    
    for conn in connections:
        if conn.get("database_type") == "postgresql" and conn.get("database") == "cdctest":
            pg_conn = conn
            print(f"   Found PostgreSQL connection: {conn['id']}")
        elif conn.get("database_type") in ["sqlserver", "mssql"] and conn.get("database") == "cdctest":
            mssql_conn = conn
            print(f"   Found SQL Server connection: {conn['id']}")
    
    if not pg_conn or not mssql_conn:
        print("   Connections not found. Creating them...")
        # Create connections if they don't exist
        # (Skip for now - assume they exist from pipeline-1)
        print("   ERROR: Connections not found. Run backend setup first.")
        exit(1)
    
except Exception as e:
    print(f"   Error: {e}")
    print("   Backend might not be running. Start backend: python -m uvicorn ingestion.api:app --reload")
    exit(1)

# Step 2: Create pipeline2
print("\n2. Creating pipeline2...")
pipeline_data = {
    "name": "pipeline2",
    "source_connection_id": pg_conn["id"],
    "target_connection_id": mssql_conn["id"],
    "source_database": "cdctest",
    "source_schema": "public",
    "source_tables": ["employees"],
    "target_database": "cdctest",
    "target_schema": "dbo",
    "target_tables": ["employees"],
    "mode": "full_load_and_cdc",
    "enable_full_load": True,
    "auto_create_target": True
}

try:
    r = requests.post(f"{API_URL}/pipelines", json=pipeline_data, timeout=30)
    if r.status_code in [200, 201]:
        pipeline = r.json()
        pipeline_id = pipeline.get("id")
        print(f"   Created pipeline2: {pipeline_id}")
    else:
        print(f"   Error: {r.status_code}")
        print(f"   {r.text[:500]}")
        exit(1)
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 3: Start pipeline2
print("\n3. Starting pipeline2...")
print("   This will automatically: auto-create schema → full load → CDC")
try:
    r = requests.post(f"{API_URL}/pipelines/{pipeline_id}/start", timeout=120)
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

# Step 4: Wait for full load to complete
print("\n4. Waiting 30 seconds for full load and CDC setup...")
time.sleep(30)

# Step 5: Verify target table was auto-created with CDC columns
print("\n5. Verifying target table schema...")
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
        print(f"     Surrogate key (row_id): {'✓' if has_row_id else '✗'}")
        print(f"     CDC metadata (__op): {'✓' if has_op else '✗'}")
        print(f"     CDC metadata (__source_ts_ms): {'✓' if has_ts else '✗'}")
        print(f"     CDC metadata (__deleted): {'✓' if has_deleted else '✗'}")
        
        if has_row_id and has_op and has_ts and has_deleted:
            print("\n   SUCCESS! Table auto-created with correct SCD2 schema!")
        else:
            print("\n   WARNING: Some CDC columns are missing")
    else:
        print("   Table not found - auto-create might have failed")
    
    # Check full load data
    sql_cur.execute("SELECT COUNT(*) FROM dbo.employees WHERE __op = 'r'")
    full_load_count = sql_cur.fetchone()[0]
    print(f"\n6. Full load data:")
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

print("\n" + "="*70)
print("AUTO-CREATE → FULL LOAD → CDC flow test complete!")
print("Next: Test CDC by running INSERT/UPDATE/DELETE in PostgreSQL")
print("="*70)
