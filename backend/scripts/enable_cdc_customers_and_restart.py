"""
Enable CDC on dbo.customers in SQL Server cdctest, then restart pipeline-4 source connector.
Requires SA or db_owner. After this, INSERT/UPDATE/DELETE on dbo.customers will appear in topics.
"""
import pyodbc
import requests
import time

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"
KAFKA_CONNECT = "http://72.61.233.209:8083"
CONNECTOR = "cdc-pipeline-4-mssql-dbo"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)

print("="*70)
print("Enable CDC on dbo.customers and restart connector")
print("="*70)

# Check if table already has CDC
conn = pyodbc.connect(conn_str)
cur = conn.cursor()
cur.execute("SELECT object_id FROM sys.tables WHERE name = 'customers' AND schema_id = SCHEMA_ID('dbo')")
tbl = cur.fetchone()
if not tbl:
    print("Table dbo.customers not found.")
    conn.close()
    exit(1)
customers_object_id = tbl[0]

cur.execute("SELECT object_id FROM cdc.change_tables WHERE source_object_id = ?", (customers_object_id,))
already = cur.fetchone()
if already:
    print("CDC already enabled on dbo.customers.")
else:
    print("Enabling CDC on dbo.customers...")
    try:
        # Enable CDC on table; @role_name = NULL means no access control
        cur.execute("""
            EXEC sys.sp_cdc_enable_table
                @source_schema = N'dbo',
                @source_name   = N'customers',
                @role_name     = NULL,
                @supports_net_changes = 1
        """)
        conn.commit()
        print("CDC enabled on dbo.customers.")
    except Exception as e:
        print("Error enabling CDC: %s" % e)
        conn.close()
        exit(1)
conn.close()

# Restart source connector so it picks up CDC
print("Restarting source connector...")
r = requests.post("%s/connectors/%s/restart" % (KAFKA_CONNECT, CONNECTOR), timeout=15)
if r.status_code in (200, 204):
    print("Connector restart requested.")
else:
    print("Restart response: %s %s" % (r.status_code, r.text))

time.sleep(10)
r = requests.get("%s/connectors/%s/status" % (KAFKA_CONNECT, CONNECTOR), timeout=15)
d = r.json()
print("Connector: %s, Task: %s" % (d.get("connector", {}).get("state"), d.get("tasks", [{}])[0].get("state") if d.get("tasks") else "N/A"))
print("\nDone. New INSERT/UPDATE/DELETE on dbo.customers will now appear in the topic and in PostgreSQL.")
