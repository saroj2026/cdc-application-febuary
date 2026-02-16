"""
Drop target table, reset pipeline2 full load state, delete connectors,
start pipeline2. Run after backend is restarted.
"""
import requests
import time
import pyodbc
import psycopg2
import sys
import os

API_URL = "http://localhost:8000/api/v1"
KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
PIPELINE2_ID = "acc52bb1-e5e5-4a4f-aa6a-7aed85b254ed"

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("="*70)
print("RUN PIPELINE2 WITH NEW CODE (backend already restarted)")
print("="*70)

# Wait for backend
print("\n1. Waiting for backend...")
for i in range(30):
    try:
        r = requests.get("http://localhost:8000/health", timeout=2)
        if r.status_code == 200:
            print("   Backend ready")
            break
    except Exception:
        pass
    time.sleep(1)
else:
    print("   ERROR: Backend not ready")
    sys.exit(1)

# Drop dbo.employees
print("\n2. Dropping dbo.employees in SQL Server...")
try:
    sql_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
    ) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    sql_cur.execute("IF OBJECT_ID('dbo.employees', 'U') IS NOT NULL DROP TABLE dbo.employees")
    sql_conn.commit()
    sql_conn.close()
    print("   Done")
except Exception as e:
    print("   Error: %s" % e)

# Reset pipeline2 full load state
print("\n3. Resetting pipeline2 full load state in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        UPDATE public.pipelines
        SET full_load_status = 'NOT_STARTED', full_load_completed_at = NULL, full_load_lsn = NULL
        WHERE name = 'pipeline2'
    """)
    pg_conn.commit()
    pg_conn.close()
    print("   Done")
except Exception as e:
    print("   Error: %s" % e)

# Delete Kafka connectors
print("\n4. Deleting Kafka Connect connectors for pipeline2...")
for conn_name in ["sink-pipeline2-mssql-dbo", "cdc-pipeline2-pg-public"]:
    try:
        r = requests.delete("%s/connectors/%s" % (KAFKA_CONNECT_URL, conn_name), timeout=15)
        print("   %s: %s" % (conn_name, "deleted" if r.status_code in [200, 204] else r.status_code))
    except Exception as e:
        print("   %s: %s" % (conn_name, e))
time.sleep(5)

# Start pipeline2
print("\n5. Starting pipeline2 (auto-create -> full load -> CDC)...")
try:
    r = requests.post("%s/pipelines/%s/start" % (API_URL, PIPELINE2_ID), timeout=120)
    if r.status_code == 200:
        result = r.json()
        print("   Started successfully")
        print("   Full load: %s" % result.get("full_load", {}).get("success"))
        print("   Debezium: %s" % result.get("debezium_connector", {}).get("success"))
        print("   Sink: %s" % result.get("sink_connector", {}).get("success"))
    else:
        print("   Error: %s %s" % (r.status_code, r.text[:400]))
except Exception as e:
    print("   Error: %s" % e)
    sys.exit(1)

print("\n6. Waiting 25 seconds for full load and CDC...")
time.sleep(25)

# Verify
print("\n7. Verifying target table and data...")
try:
    sql_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
    ) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    sql_cur.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    col_names = [r[0] for r in sql_cur.fetchall()]
    if not col_names:
        print("   Table dbo.employees not found")
    else:
        has_row_id = "row_id" in col_names
        has_op = "__op" in col_names
        print("   Columns: %s" % ", ".join(col_names))
        print("   row_id: %s" % ("YES" if has_row_id else "NO"))
        print("   __op, __source_ts_ms, __deleted: %s" % ("YES" if ("__op" in col_names and "__source_ts_ms" in col_names and "__deleted" in col_names) else "NO"))
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees")
        total = sql_cur.fetchone()[0]
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees WHERE __op = 'r'")
        full_load = sql_cur.fetchone()[0]
        print("   Total rows: %s" % total)
        print("   Rows __op='r': %s" % full_load)
        if total > 0:
            sql_cur.execute("SELECT TOP 3 row_id, id, name, __op, __source_ts_ms FROM dbo.employees")
            print("   Sample:")
            for row in sql_cur.fetchall():
                print("     row_id=%s id=%s name=%s __op=%s ts=%s" % (row[0], row[1], row[2], row[3], row[4]))
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

print("\n" + "="*70)
