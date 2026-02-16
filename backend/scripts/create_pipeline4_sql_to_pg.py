"""
Create pipeline-4: SQL Server (dbo.customers) -> PostgreSQL (public.customers).
Source: SQL Server cdctest, SA, Sql@12345, table dbo.customers
Target: PostgreSQL cdctest, cdc_user, cdc_pass, table public.customers
Mode: full load + CDC
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
print("PIPELINE-4: SQL Server dbo.customers -> PostgreSQL public.customers")
print("="*70)

# 1. Ensure dbo.customers exists in SQL Server
print("\n1. Checking/creating dbo.customers in SQL Server...")
sql_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
try:
    conn = pyodbc.connect(sql_conn_str)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='customers'")
    exists = cur.fetchone()[0]
    if exists:
        cur.execute("SELECT COUNT(*) FROM dbo.customers")
        n = cur.fetchone()[0]
        print("   dbo.customers exists, row count: %s" % n)
    else:
        cur.execute("""
            CREATE TABLE dbo.customers (
                id INT PRIMARY KEY,
                customer_name NVARCHAR(255),
                email NVARCHAR(255),
                country NVARCHAR(100)
            )
        """)
        cur.execute("""
            INSERT INTO dbo.customers (id, customer_name, email, country)
            VALUES (1,'Acme Corp','acme@example.com','USA'),
                   (2,'Beta Inc','beta@example.com','UK'),
                   (3,'Gamma LLC','gamma@example.com','India')
        """)
        conn.commit()
        print("   Created dbo.customers with 3 rows")
    conn.close()
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

# 2. Get connections
print("\n2. Getting connections...")
try:
    r = requests.get("%s/connections" % API_URL, timeout=10)
    conns = r.json()
    mssql = next((c for c in conns if c.get("database_type") in ["sqlserver","mssql"] and (c.get("database")=="cdctest" or c.get("database")=="cdc_test")), None)
    pg = next((c for c in conns if c.get("database_type")=="postgresql" and c.get("database")=="cdctest"), None)
    if not mssql or not pg:
        print("   Missing connection. MSSQL: %s, PG: %s" % (bool(mssql), bool(pg)))
        exit(1)
    print("   SQL Server: %s" % mssql["id"])
    print("   PostgreSQL: %s" % pg["id"])
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

# 3. Create or get pipeline-4
print("\n3. Creating or getting pipeline-4...")
try:
    r = requests.get("%s/pipelines" % API_URL, timeout=10)
    pipelines = r.json() if r.status_code == 200 else []
    existing = next((p for p in pipelines if p.get("name") == "pipeline-4"), None)
    if existing:
        pipeline_id = existing["id"]
        print("   Using existing pipeline-4: %s" % pipeline_id)
        # Reset full load state so start will run full load again
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
        cur = pg_conn.cursor()
        cur.execute("""
            UPDATE public.pipelines
            SET full_load_status = 'NOT_STARTED', full_load_completed_at = NULL, full_load_lsn = NULL
            WHERE id = %s
        """, (pipeline_id,))
        pg_conn.commit()
        pg_conn.close()
        print("   Reset full load state")
    else:
        payload = {
            "name": "pipeline-4",
            "source_connection_id": mssql["id"],
            "target_connection_id": pg["id"],
            "source_database": "cdctest",
            "source_schema": "dbo",
            "source_tables": ["customers"],
            "target_database": "cdctest",
            "target_schema": "public",
            "target_tables": ["customers"],
            "mode": "full_load_and_cdc",
            "enable_full_load": True,
            "auto_create_target": True,
        }
        r = requests.post("%s/pipelines" % API_URL, json=payload, timeout=30)
        if r.status_code not in [200, 201]:
            print("   Error: %s %s" % (r.status_code, r.text[:400]))
            exit(1)
        pipeline = r.json()
        pipeline_id = pipeline["id"]
        print("   Created pipeline-4: %s" % pipeline_id)
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

# 3b. Delete existing Kafka connectors for pipeline-4 so they are recreated
print("\n3b. Deleting existing Kafka connectors for pipeline-4...")
for name in ["sink-pipeline-4-pg-public", "cdc-pipeline-4-mssql-dbo"]:
    try:
        r = requests.delete("http://72.61.233.209:8083/connectors/%s" % name, timeout=10)
        if r.status_code in [200, 204]:
            print("   Deleted %s" % name)
    except Exception as e:
        print("   %s: %s" % (name, e))
time.sleep(3)

# 4. Start pipeline-4 (full load + CDC)
print("\n4. Starting pipeline-4 (full load + CDC)...")
try:
    r = requests.post("%s/pipelines/%s/start" % (API_URL, pipeline_id), timeout=120)
    if r.status_code != 200:
        print("   Error: %s %s" % (r.status_code, r.text[:500]))
        exit(1)
    res = r.json()
    print("   Full load: %s" % res.get("full_load", {}).get("success"))
    print("   Debezium: %s" % res.get("debezium_connector", {}).get("success"))
    print("   Sink: %s" % res.get("sink_connector", {}).get("success"))
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

print("\n5. Waiting 35 seconds for full load and CDC...")
time.sleep(35)

# 6. Verify public.customers in PostgreSQL
print("\n6. Verifying public.customers in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='customers'
        ORDER BY ordinal_position
    """)
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        print("   Table public.customers not found")
    else:
        print("   Columns: %s" % ", ".join(cols))
        print("   row_id: %s" % ("YES" if "row_id" in cols else "NO"))
        print("   __op, __source_ts_ms, __deleted: %s" % ("YES" if all(c in cols for c in ["__op","__source_ts_ms","__deleted"]) else "NO"))
        cur.execute("SELECT COUNT(*) FROM public.customers")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.customers WHERE __op = 'r'")
        full_load = cur.fetchone()[0]
        print("   Total rows: %s" % total)
        print("   Rows __op='r': %s" % full_load)
        if total > 0:
            cur.execute("SELECT row_id, customer_id, first_name, __op, __source_ts_ms FROM public.customers ORDER BY row_id LIMIT 5")
            print("   Sample:")
            for row in cur.fetchall():
                print("     row_id=%s customer_id=%s first_name=%s __op=%s ts=%s" % (row[0], row[1], row[2], row[3], row[4]))
    pg_conn.close()
except Exception as e:
    print("   Error: %s" % e)

print("\n" + "="*70)
print("Pipeline-4: SQL Server dbo.customers -> PostgreSQL public.customers (full load + CDC)")
print("="*70)
