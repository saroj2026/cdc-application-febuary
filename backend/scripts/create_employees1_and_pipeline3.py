"""
1. Create public.employees1 in PostgreSQL (same structure as employees)
2. Create pipeline3 via API: source public.employees1 -> target dbo.employees1
3. Start pipeline3 (auto-create, full load, CDC)
4. Verify
"""
import psycopg2
import requests
import time
import pyodbc

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"
API_URL = "http://localhost:8000/api/v1"
SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("="*70)
print("CREATE EMPLOYEES1 + PIPELINE3")
print("="*70)

# --- 1. Create employees1 in PostgreSQL ---
print("\n1. Creating public.employees1 in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema='public' AND table_name='employees1'
    """)
    if pg_cur.fetchone()[0] > 0:
        print("   employees1 already exists, skipping create")
    else:
        pg_cur.execute("""
            CREATE TABLE public.employees1 (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                department_id INT
            )
        """)
        pg_cur.execute("""
            INSERT INTO public.employees1 (name, email, department_id)
            VALUES
                ('Alice One', 'alice.one@example.com', 1),
                ('Bob One', 'bob.one@example.com', 1),
                ('Carol One', 'carol.one@example.com', 2),
                ('Dave One', 'dave.one@example.com', 2),
                ('Eve One', 'eve.one@example.com', 3)
        """)
        pg_conn.commit()
        print("   Created employees1 with 5 rows")
    pg_cur.execute("SELECT COUNT(*) FROM public.employees1")
    print("   Row count: %s" % pg_cur.fetchone()[0])
    pg_conn.close()
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

# --- 2. Get connections and create pipeline3 ---
print("\n2. Getting connections...")
try:
    r = requests.get("%s/connections" % API_URL, timeout=10)
    connections = r.json()
    pg_conn = next((c for c in connections if c.get("database_type") == "postgresql" and c.get("database") == "cdctest"), None)
    mssql_conn = next((c for c in connections if c.get("database_type") in ["sqlserver", "mssql"] and (c.get("database") == "cdctest" or c.get("database") == "cdc_test")), None)
    if not pg_conn or not mssql_conn:
        print("   ERROR: PostgreSQL or SQL Server connection not found")
        exit(1)
    print("   PostgreSQL: %s" % pg_conn["id"])
    print("   SQL Server: %s" % mssql_conn["id"])
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

print("\n3. Creating pipeline3...")
pipeline_data = {
    "name": "pipeline3",
    "source_connection_id": pg_conn["id"],
    "target_connection_id": mssql_conn["id"],
    "source_database": "cdctest",
    "source_schema": "public",
    "source_tables": ["employees1"],
    "target_database": "cdctest",
    "target_schema": "dbo",
    "target_tables": ["employees1"],
    "mode": "full_load_and_cdc",
    "enable_full_load": True,
    "auto_create_target": True,
}
try:
    r = requests.post("%s/pipelines" % API_URL, json=pipeline_data, timeout=30)
    if r.status_code not in [200, 201]:
        print("   Error: %s %s" % (r.status_code, r.text[:400]))
        exit(1)
    pipeline = r.json()
    pipeline_id = pipeline["id"]
    print("   Created pipeline3: %s" % pipeline_id)
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

# --- 4. Start pipeline3 ---
print("\n4. Starting pipeline3 (auto-create -> full load -> CDC)...")
try:
    r = requests.post("%s/pipelines/%s/start" % (API_URL, pipeline_id), timeout=120)
    if r.status_code != 200:
        print("   Error: %s %s" % (r.status_code, r.text[:400]))
        exit(1)
    result = r.json()
    print("   Full load: %s" % result.get("full_load", {}).get("success"))
    print("   Debezium: %s" % result.get("debezium_connector", {}).get("success"))
    print("   Sink: %s" % result.get("sink_connector", {}).get("success"))
except Exception as e:
    print("   Error: %s" % e)
    exit(1)

print("\n5. Waiting 30 seconds for full load and CDC...")
time.sleep(30)

# --- 6. Verify target ---
print("\n6. Verifying dbo.employees1 in SQL Server...")
try:
    sql_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
    ) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
    sql_conn = pyodbc.connect(sql_conn_str)
    cur = sql_conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'employees1' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        print("   Table dbo.employees1 not found")
    else:
        print("   Columns: %s" % ", ".join(cols))
        print("   row_id: %s" % ("YES" if "row_id" in cols else "NO"))
        cur.execute("SELECT COUNT(*) FROM dbo.employees1")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dbo.employees1 WHERE __op = 'r'")
        full_load = cur.fetchone()[0]
        print("   Total rows: %s" % total)
        print("   Rows __op='r': %s" % full_load)
        if total > 0:
            cur.execute("SELECT TOP 3 row_id, id, name, __op, __source_ts_ms FROM dbo.employees1")
            print("   Sample:")
            for row in cur.fetchall():
                print("     row_id=%s id=%s name=%s __op=%s ts=%s" % (row[0], row[1], row[2], row[3], row[4]))
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

print("\n" + "="*70)
print("Done. Pipeline3: public.employees1 -> dbo.employees1")
print("="*70)
