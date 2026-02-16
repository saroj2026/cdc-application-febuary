"""
Create pipeline-6: AS400 (existing connection) -> SQL Server (cdctest dbo.MYTABLE).

- Source: use existing AS400 connection id f1bbe13a-a3e7-4a86-9ff5-4e6a95f8557b (public.connections).
  Same flow as discussed: Debezium initial snapshot = full load, then CDC (no ODBC on backend; we use Db2 connector).
- Target SQL Server: SA / Sql@12345, db cdctest, table dbo.MYTABLE.
"""
import requests
import time

API_URL = "http://localhost:8000/api/v1"

# Use existing AS400 connection (public.connections)
AS400_CONNECTION_ID = "2ac747c6-2e03-4ff7-954d-0f239e519193"

# AS400 credentials: database QGPL, host pub400.com, port 9471
AS400_HOST = "pub400.com"
AS400_PORT = 9471
AS400_DATABASE = "QGPL"
AS400_USERNAME = "segmetriq"
AS400_PASSWORD = "segmetriq123"

# Source table from AS400 (library SEGMETRIQ1)
SOURCE_SCHEMA = "SEGMETRIQ1"
SOURCE_TABLE = "MYTABLE"

# SQL Server target (same as pipeline-5)
SQL_HOST = "72.61.233.209"
SQL_PORT = 1433
SQL_DB = "cdctest"
SQL_USER = "SA"
SQL_PASS = "Sql@12345"
SQL_SCHEMA = "dbo"
SQL_TABLE = "MYTABLE"

print("=" * 70)
print("CREATE PIPELINE-6: AS400 (existing conn) -> SQL Server [Debezium snapshot = full load + CDC]")
print("=" * 70)

# 1. Get existing connections
print("\n1. Getting connections...")
r = requests.get(f"{API_URL}/connections", timeout=10)
if r.status_code != 200:
    print("   Failed to get connections:", r.status_code)
    exit(1)
conns = r.json()

# 2. Use existing AS400 connection and ensure credentials (QGPL, pub400.com:9471)
source_conn = next((c for c in conns if c.get("id") == AS400_CONNECTION_ID), None)
if not source_conn:
    print(f"   AS400 connection {AS400_CONNECTION_ID} not found in public.connections")
    exit(1)
source_id = source_conn["id"]
print(f"   Using AS400 connection: {source_id[:8]}... ({source_conn.get('name', '')})")

# Ensure AS400 connection has correct credentials (database=QGPL, host=pub400.com, port=9471)
update_payload = {
    "name": source_conn.get("name") or "AS400 pub400",
    "connection_type": source_conn.get("connection_type") or "source",
    "database_type": source_conn.get("database_type") or "as400",
    "host": AS400_HOST,
    "port": AS400_PORT,
    "database": AS400_DATABASE,
    "username": AS400_USERNAME,
    "password": AS400_PASSWORD,
    "schema": source_conn.get("schema"),
    "additional_config": source_conn.get("additional_config") or {},
}
up = requests.put(f"{API_URL}/connections/{source_id}", json=update_payload, timeout=10)
if up.status_code == 200:
    print("   Updated AS400 connection: database=QGPL, host=pub400.com, port=9471")
else:
    print("   Warning: could not update AS400 connection:", up.status_code, up.text[:200])

# 3. Find or create SQL Server connection (cdctest, SA)
mssql_conn = next(
    (c for c in conns if (c.get("database_type") or "").lower() in ["sqlserver", "mssql"]
    and (c.get("database") or "").lower() == SQL_DB.lower()
    and (c.get("username") or "").upper() == SQL_USER.upper()),
    None,
)
if mssql_conn:
    print(f"   Using existing SQL Server connection: {mssql_conn.get('id', '')[:8]}...")
    mssql_id = mssql_conn["id"]
else:
    print("   Creating SQL Server connection...")
    payload = {
        "name": "SQL Server cdctest",
        "connection_type": "target",
        "database_type": "sqlserver",
        "host": SQL_HOST,
        "port": SQL_PORT,
        "database": SQL_DB,
        "username": SQL_USER,
        "password": SQL_PASS,
        "schema": SQL_SCHEMA,
        "additional_config": {"trust_server_certificate": True},
    }
    r = requests.post(f"{API_URL}/connections", json=payload, timeout=15)
    if r.status_code not in [200, 201]:
        print("   Failed to create SQL Server connection:", r.status_code, r.text[:500])
        exit(1)
    mssql_conn = r.json()
    mssql_id = mssql_conn["id"]
    print(f"   Created SQL Server connection: {mssql_id[:8]}...")

# 4. Get or create pipeline-6 and ensure it uses AS400 connection 2ac747c6
print("\n2. Getting or creating pipeline-6...")
r = requests.get(f"{API_URL}/pipelines", timeout=10)
if r.status_code != 200:
    print("   Failed to get pipelines:", r.status_code)
    exit(1)
data = r.json()
pipelines = data if isinstance(data, list) else data.get("pipelines", data)
existing = next((p for p in pipelines if (p.get("name") or "").strip().lower() == "pipeline-6"), None)
if existing:
    pipeline_id = existing["id"]
    print(f"   Using existing pipeline-6: {pipeline_id}")
    # Ensure pipeline uses AS400 connection 2ac747c6 (and correct tables); stop first if running
    status = (existing.get("status") or "").upper()
    if status in ("RUNNING", "STARTING"):
        print("   Stopping pipeline-6 so we can update it...")
        stop_r = requests.post(f"{API_URL}/pipelines/{pipeline_id}/stop", timeout=30)
        if stop_r.status_code == 200:
            time.sleep(2)
        else:
            print("   Warning: stop returned", stop_r.status_code)
    update_payload = {
        "source_connection_id": source_id,
        "source_schema": SOURCE_SCHEMA,
        "source_tables": [SOURCE_TABLE],
        "target_table_mapping": {SOURCE_TABLE: SQL_TABLE},
        "mode": "full_load_and_cdc",
    }
    patch_r = requests.put(f"{API_URL}/pipelines/{pipeline_id}", json=update_payload, timeout=15)
    if patch_r.status_code == 200:
        print("   Updated pipeline-6 to use AS400 connection", source_id[:8] + "...", SOURCE_SCHEMA + "." + SOURCE_TABLE)
    else:
        print("   Warning: PUT pipeline returned", patch_r.status_code, patch_r.text[:200])
else:
    payload = {
        "name": "pipeline-6",
        "source_connection_id": source_id,
        "target_connection_id": mssql_id,
        "source_database": source_conn.get("database") or "QGPL",
        "source_schema": source_conn.get("schema") or SOURCE_SCHEMA,
        "source_tables": [SOURCE_TABLE],
        "target_database": SQL_DB,
        "target_schema": SQL_SCHEMA,
        "target_tables": [SQL_TABLE],
        "mode": "full_load_and_cdc",
        "enable_full_load": True,
        "auto_create_target": True,
    }
    r = requests.post(f"{API_URL}/pipelines", json=payload, timeout=30)
    if r.status_code not in [200, 201]:
        print("   Error creating pipeline:", r.status_code, r.text[:500])
        exit(1)
    pipeline = r.json()
    pipeline_id = pipeline["id"]
    print(f"   Created pipeline-6: {pipeline_id}")

# 5. Start pipeline-6 (full load + CDC)
print("\n3. Starting pipeline-6 (full load + CDC)...")
r = requests.post(f"{API_URL}/pipelines/{pipeline_id}/start", timeout=180)
if r.status_code != 200:
    print("   Error starting pipeline:", r.status_code, r.text[:800])
    if r.status_code == 500 and "500 error responses" in (r.text or ""):
        print("   Tip: Restart the backend and run this script again (Kafka Connect 500 is no longer retried).")
    exit(1)
res = r.json()
print("   Full load:", res.get("full_load", {}).get("success"))
print("   Debezium:", res.get("debezium_connector", {}).get("success"))
print("   Sink:", res.get("sink_connector", {}).get("success"))
if res.get("full_load", {}).get("message"):
    print("   Full load message:", res["full_load"]["message"][:200])
if res.get("debezium_connector", {}).get("message"):
    print("   Debezium message:", res["debezium_connector"]["message"][:200])

print("\n4. Pipeline-6 created and started.")
print("   Source: AS400 connection", source_id[:8] + "...", SOURCE_SCHEMA + "." + SOURCE_TABLE, "(Debezium snapshot = full load + CDC)")
print("   Target: SQL Server", SQL_SCHEMA + "." + SQL_TABLE)
print("=" * 70)
