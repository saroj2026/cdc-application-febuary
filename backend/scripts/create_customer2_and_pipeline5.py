"""
1. Create dbo.customer2 in SQL Server (cdctest, SA, Sql@12345) with sample data
2. Enable CDC on dbo.customer2
3. Create pipeline-5: SQL Server dbo.customer2 -> PostgreSQL public.customer2
4. Start pipeline-5 (full load + CDC)
5. Verify public.customer2 in PostgreSQL
"""
import pyodbc
import psycopg2
import requests
import time

API_URL = "http://localhost:8000/api/v1"
SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"
PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

print("="*70)
print("CREATE CUSTOMER2 + PIPELINE-5 (SQL Server -> PostgreSQL)")
print("="*70)

# 1. Create dbo.customer2 in SQL Server
print("\n1. Creating dbo.customer2 in SQL Server...")
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
conn = pyodbc.connect(conn_str)
cur = conn.cursor()

cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='customer2'")
if cur.fetchone():
    print("   dbo.customer2 already exists")
else:
    cur.execute("""
        CREATE TABLE dbo.customer2 (
            customer_id INT PRIMARY KEY,
            first_name NVARCHAR(100),
            last_name NVARCHAR(100),
            email NVARCHAR(255),
            phone NVARCHAR(50),
            balance DECIMAL(10,2) DEFAULT 0,
            registration_date DATETIME2 NULL,
            is_premium BIT DEFAULT 0
        )
    """)
    cur.execute("""
        INSERT INTO dbo.customer2 (customer_id, first_name, last_name, email, phone, balance, is_premium)
        VALUES (1,'Alice','Two','alice2@example.com','111',10.0,0),
               (2,'Bob','Two','bob2@example.com','222',20.0,1)
    """)
    conn.commit()
    print("   Created dbo.customer2 with 2 rows")

# 2. Enable CDC on dbo.customer2
print("\n2. Enabling CDC on dbo.customer2...")
cur.execute("SELECT object_id FROM sys.tables WHERE name = 'customer2' AND schema_id = SCHEMA_ID('dbo')")
tbl = cur.fetchone()
if tbl:
    cur.execute("SELECT object_id FROM cdc.change_tables WHERE source_object_id = ?", (tbl[0],))
    if cur.fetchone():
        print("   CDC already enabled on dbo.customer2")
    else:
        try:
            cur.execute("""
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name   = N'customer2',
                    @role_name     = NULL,
                    @supports_net_changes = 1
            """)
            conn.commit()
            print("   CDC enabled on dbo.customer2")
        except Exception as e:
            print("   Error enabling CDC:", e)
conn.close()

# 3. Get connections
print("\n3. Getting connections...")
r = requests.get("%s/connections" % API_URL, timeout=10)
if r.status_code != 200:
    print("   Failed to get connections:", r.status_code)
    exit(1)
conns = r.json()
mssql = next((c for c in conns if c.get("database_type") in ["sqlserver","mssql"] and (c.get("database")=="cdctest" or c.get("database")=="cdc_test")), None)
pg = next((c for c in conns if c.get("database_type")=="postgresql" and c.get("database")=="cdctest"), None)
if not mssql or not pg:
    print("   Missing connection. MSSQL:", bool(mssql), "PG:", bool(pg))
    exit(1)
print("   SQL Server:", mssql["id"][:8], "...  PostgreSQL:", pg["id"][:8], "...")

# 4. Create pipeline-5
print("\n4. Creating pipeline-5...")
payload = {
    "name": "pipeline-5",
    "source_connection_id": mssql["id"],
    "target_connection_id": pg["id"],
    "source_database": "cdctest",
    "source_schema": "dbo",
    "source_tables": ["customer2"],
    "target_database": "cdctest",
    "target_schema": "public",
    "target_tables": ["customer2"],
    "mode": "full_load_and_cdc",
    "enable_full_load": True,
    "auto_create_target": True,
}
r = requests.post("%s/pipelines" % API_URL, json=payload, timeout=30)
if r.status_code not in [200, 201]:
    print("   Error:", r.status_code, r.text[:400])
    exit(1)
pipeline = r.json()
pipeline_id = pipeline["id"]
print("   Created pipeline-5:", pipeline_id)

# 5. Start pipeline-5
print("\n5. Starting pipeline-5 (full load + CDC)...")
r = requests.post("%s/pipelines/%s/start" % (API_URL, pipeline_id), timeout=120)
if r.status_code != 200:
    print("   Error:", r.status_code, r.text[:500])
    exit(1)
res = r.json()
print("   Full load:", res.get("full_load", {}).get("success"))
print("   Debezium:", res.get("debezium_connector", {}).get("success"))
print("   Sink:", res.get("sink_connector", {}).get("success"))

print("\n6. Waiting 40 seconds for full load and CDC...")
time.sleep(40)

# 7. Verify public.customer2 in PostgreSQL
print("\n7. Verifying public.customer2 in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='customer2'
        ORDER BY ordinal_position
    """)
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        print("   Table public.customer2 not found")
    else:
        print("   Columns:", ", ".join(cols))
        print("   row_id:", "YES" if "row_id" in cols else "NO")
        print("   __op, __source_ts_ms, __deleted:", "YES" if all(c in cols for c in ["__op","__source_ts_ms","__deleted"]) else "NO")
        cur.execute("SELECT COUNT(*) FROM public.customer2")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.customer2 WHERE __op = 'r'")
        full_load = cur.fetchone()[0]
        print("   Total rows:", total)
        print("   Rows __op='r':", full_load)
        if total > 0:
            cur.execute("SELECT row_id, customer_id, first_name, __op, __source_ts_ms FROM public.customer2 ORDER BY row_id LIMIT 5")
            print("   Sample:")
            for row in cur.fetchall():
                print("     row_id=%s customer_id=%s first_name=%s __op=%s ts=%s" % (row[0], row[1], row[2], row[3], row[4]))
    pg_conn.close()
except Exception as e:
    print("   Error:", e)

print("\n" + "="*70)
print("Pipeline-5: SQL Server dbo.customer2 -> PostgreSQL public.customer2")
print("="*70)
