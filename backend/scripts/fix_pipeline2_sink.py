"""
Fix pipeline2 sink connector config to use correct SCD2 settings.
"""
import requests
import json
import time
import pyodbc

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
SINK_CONNECTOR = "sink-pipeline2-mssql-dbo"

print("="*70)
print("FIXING PIPELINE2 SINK CONNECTOR CONFIG")
print("="*70)

# Step 1: Get current config
print("\n1. Getting current sink connector config...")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/config", timeout=10)
    current_config = r.json()
    print(f"   Current config retrieved")
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 2: Update config with correct SCD2 settings
print("\n2. Updating config with SCD2 settings...")
updated_config = current_config.copy()

# Fix the critical settings
updated_config["auto.evolve"] = "false"  # Was "true"
updated_config["transforms.unwrap.delete.handling.mode"] = "rewrite"  # Was "none"
updated_config["consumer.override.auto.offset.reset"] = "earliest"  # Add this

print(f"   Changes:")
print(f"     auto.evolve: {current_config.get('auto.evolve')} -> {updated_config['auto.evolve']}")
print(f"     delete.handling.mode: {current_config.get('transforms.unwrap.delete.handling.mode')} -> {updated_config['transforms.unwrap.delete.handling.mode']}")
print(f"     auto.offset.reset: {current_config.get('consumer.override.auto.offset.reset', 'NOT SET')} -> {updated_config['consumer.override.auto.offset.reset']}")

# Step 3: Delete and recreate connector with new config
print("\n3. Deleting sink connector...")
try:
    r = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}", timeout=30)
    if r.status_code in [200, 204]:
        print(f"   Deleted successfully")
    else:
        print(f"   Delete error: {r.status_code} - {r.text[:200]}")
except Exception as e:
    print(f"   Error: {e}")

print("\n4. Waiting 5 seconds...")
time.sleep(5)

print("\n5. Recreating sink connector with updated config...")
try:
    payload = {
        "name": SINK_CONNECTOR,
        "config": updated_config
    }
    r = requests.post(f"{KAFKA_CONNECT_URL}/connectors", json=payload, timeout=30)
    if r.status_code in [200, 201]:
        print(f"   Created successfully")
    else:
        print(f"   Create error: {r.status_code}")
        print(f"   {r.text[:500]}")
        exit(1)
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

print("\n6. Waiting 15 seconds for connector to consume messages...")
time.sleep(15)

# Step 7: Check if table was auto-created
print("\n7. Checking if target table was auto-created...")
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
        print("   SUCCESS! Table auto-created by sink connector:")
        has_op = False
        has_ts = False
        has_deleted = False
        
        for col in columns:
            print(f"     {col[0]:25s} {col[1]}")
            if col[0] == '__op':
                has_op = True
            if col[0] == '__source_ts_ms':
                has_ts = True
            if col[0] == '__deleted':
                has_deleted = True
        
        print(f"\n   CDC metadata columns:")
        print(f"     __op: {'YES' if has_op else 'NO'}")
        print(f"     __source_ts_ms: {'YES' if has_ts else 'NO'}")
        print(f"     __deleted: {'YES' if has_deleted else 'NO'}")
        
        # Check row count
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees")
        count = sql_cur.fetchone()[0]
        print(f"\n   Row count: {count}")
        
        if count > 0:
            sql_cur.execute("SELECT TOP 3 id, first_name, __op, __source_ts_ms FROM dbo.employees")
            print("\n   Sample rows:")
            for row in sql_cur.fetchall():
                print(f"     id={row[0]}, name={row[1]}, __op={row[2]}, ts={row[3]}")
    else:
        print("   Table not found yet - waiting for first message...")
    
    sql_conn.close()

except Exception as e:
    print(f"   Error: {e}")

print("\n" + "="*70)
print("Sink connector fixed! Table should be auto-created on first CDC event.")
print("Next: Test CDC by running INSERT/UPDATE/DELETE in PostgreSQL")
print("="*70)
