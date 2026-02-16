"""
FINAL FIX - Create table with TRIPLE underscores to match Debezium output.

The issue: Debezium ExtractNewRecordState SMT produces fields with ___
(triple underscore) for custom metadata fields when using add.fields.

Root cause: When add.fields=op,source.ts_ms is specified, Debezium adds
a __ prefix by default, BUT the field name itself starts with _, resulting
in ___ (triple underscore) in the output:
  - op → __op → ___op (triple!)
  - source.ts_ms → __source_ts_ms → ___source_ts_ms (triple!)

Solution: Create SQL Server table with ___op, ___source_ts_ms, ___deleted
"""
import requests
import pyodbc
import psycopg2
import json
import time

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
DEBEZIUM_CONNECTOR = "cdc-pipeline-1-pg-public"
SINK_CONNECTOR = "sink-pipeline-1-mssql-dbo"
PIPELINE_NAME = "pipeline-1"

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("=" * 70)
print("FINAL FIX: Creating table with TRIPLE underscores")
print("=" * 70)

# Step 1: Delete sink connector
print("\n1. Deleting sink connector...")
try:
    r = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}", timeout=30)
    print(f"   Deleted: {r.status_code}")
except Exception as e:
    print(f"   Error: {e}")

# Step 2: Recreate table with TRIPLE underscores
print("\n2. Recreating SQL Server table...")
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
    print("   Table dropped")
    
    # Create with TRIPLE underscores to match Debezium output!
    sql_cur.execute("""
        CREATE TABLE dbo.department (
            row_id         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            id             INT NOT NULL,
            name           NVARCHAR(255) NULL,
            location       NVARCHAR(255) NULL,
            ___source_ts_ms BIGINT NULL,
            ___op          NVARCHAR(10) NULL,
            ___deleted     NVARCHAR(10) NULL
        );
    """)
    print("   Table created: row_id, id, name, location, ___source_ts_ms, ___op, ___deleted")
    
    # Full load
    print("\n3. Full load from PostgreSQL...")
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT id, name, location FROM department ORDER BY id")
    rows = pg_cur.fetchall()
    print(f"   Found {len(rows)} rows")
    
    ts_ms = int(time.time() * 1000)
    for row in rows:
        sql_cur.execute(
            "INSERT INTO dbo.department (id, name, location, ___source_ts_ms, ___op) VALUES (?, ?, ?, ?, ?)",
            (row[0], row[1], row[2], ts_ms, 'r')
        )
    print(f"   Inserted {len(rows)} rows with ___op='r'")
    
    pg_conn.close()
    sql_conn.close()

except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 3: Recreate sink connector
print("\n4. Recreating sink connector...")
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
    "transforms.unwrap.add.fields": "op,source.ts_ms",
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
    print(f"   Created: {r.status_code}")
    if r.status_code >= 400:
        print(f"   Error: {r.text[:500]}")
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 4: Wait and check status
print("\n5. Waiting 15 seconds for connector to start...")
time.sleep(15)

try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/status", timeout=15)
    status = r.json()
    print(f"   Connector: {status['connector']['state']}")
    if status['tasks']:
        print(f"   Task: {status['tasks'][0]['state']}")
except:
    pass

# Step 5: Update PostgreSQL pipelines
print("\n6. Updating public.pipelines...")
try:
    debezium_config = {}
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{DEBEZIUM_CONNECTOR}/config", timeout=15)
    if r.status_code == 200:
        debezium_config = r.json()
    
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        UPDATE pipelines
        SET debezium_connector_name = %s,
            sink_connector_name = %s,
            kafka_topics = %s,
            debezium_config = %s,
            sink_config = %s,
            status = 'RUNNING',
            cdc_status = 'RUNNING'
        WHERE name = %s
    """, (
        DEBEZIUM_CONNECTOR,
        SINK_CONNECTOR,
        json.dumps(["pipeline-1.public.department"]),
        json.dumps(debezium_config),
        json.dumps(config),
        PIPELINE_NAME,
    ))
    print(f"   Updated {pg_cur.rowcount} row(s)")
    pg_conn.commit()
    pg_conn.close()
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "=" * 70)
print("DONE! Wait 30 seconds, then run: python scripts/check_cdc_results_v3.py")
print("=" * 70)
