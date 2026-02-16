"""
Fix sink connector configuration - the issue is with the field name prefix.

Debezium ExtractNewRecordState SMT adds __ prefix to custom fields.
So when we set add.fields=op:_op, it becomes ___op (triple underscore).

We need to either:
1. Use no prefix in add.fields (op:op, source.ts_ms:source_ts_ms) 
   and update SQL Server table columns to match
2. OR adjust the add.fields to account for the __ prefix

Let's use option 1 - simplest field names without extra underscores.
"""
import json
import requests
import pyodbc

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
SINK_CONNECTOR = "sink-pipeline-1-mssql-dbo"
SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("STEP 1: Delete existing sink connector...")
try:
    r = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}", timeout=30)
    print(f"  Deleted: {r.status_code}")
except Exception as e:
    print(f"  Error: {e}")

print("\nSTEP 2: Recreate SQL Server table with simple column names...")
sql_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_HOST},{SQL_PORT};DATABASE={SQL_DB};"
    f"UID={SQL_USER};PWD={SQL_PASS};"
    f"Encrypt=no;TrustServerCertificate=yes"
)

try:
    sql_conn = pyodbc.connect(sql_conn_str, autocommit=True)
    sql_cur = sql_conn.cursor()
    
    sql_cur.execute("IF OBJECT_ID('dbo.department', 'U') IS NOT NULL DROP TABLE dbo.department;")
    print("  Table dropped")
    
    # Create with simple column names to match Debezium output
    sql_cur.execute("""
        CREATE TABLE dbo.department (
            row_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            id           INT NOT NULL,
            name         NVARCHAR(255) NULL,
            location     NVARCHAR(255) NULL,
            __source_ts_ms BIGINT NULL,
            __op         NVARCHAR(10) NULL,
            __deleted    NVARCHAR(10) NULL
        );
    """)
    print("  Table created with columns: row_id (PK), id, name, location, __source_ts_ms, __op, __deleted")
    
    sql_conn.close()
except Exception as e:
    print(f"  Error: {e}")
    exit(1)

print("\nSTEP 3: Create sink connector with corrected field mapping...")
# The key is add.fields format: source_field:target_field
# Debezium adds __ prefix, so we need: op,source.ts_ms (no rename)
config = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": f"jdbc:sqlserver://{SQL_HOST}:{SQL_PORT};databaseName={SQL_DB};encrypt=false;trustServerCertificate=true",
    "connection.user": SQL_USER,
    "connection.password": SQL_PASS,
    "topics": "pipeline-1.public.department",
    "table.name.format": "department",
    "insert.mode": "insert",
    "pk.mode": "none",
    "auto.create": "false",
    "auto.evolve": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.add.fields": "op,source.ts_ms",  # No rename, use defaults
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "consumer.override.auto.offset.reset": "earliest",
}

try:
    r = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        json={"name": SINK_CONNECTOR, "config": config},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    print(f"  Response: {r.status_code}")
    if r.status_code >= 400:
        print(f"  Error: {r.text[:500]}")
    else:
        print("  Connector created successfully")
except Exception as e:
    print(f"  Error: {e}")
    exit(1)

print("\nSTEP 4: Wait 10 seconds for connector to start...")
import time
time.sleep(10)

print("\nSTEP 5: Check connector status...")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/status", timeout=15)
    status = r.json()
    print(f"  Connector: {status['connector']['state']}")
    if status['tasks']:
        task_state = status['tasks'][0]['state']
        print(f"  Task: {task_state}")
        if task_state == 'FAILED':
            print(f"\n  Error trace:\n{status['tasks'][0].get('trace', '')[:800]}")
        elif task_state == 'RUNNING':
            print("\n  SUCCESS! Connector is running.")
            print("\n  Now delete the consumer group in Kafka UI:")
            print("    1. Go to: http://72.61.233.209:8080/ui/clusters/local/consumers")
            print("    2. Find: connect-sink-pipeline-1-mssql-dbo")
            print("    3. Delete it")
            print("    4. Restart connector: python scripts/restart_connector.py")
            print("    5. Check results: python scripts/check_cdc_results.py")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "="*70)
