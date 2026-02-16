"""
Trigger CDC events for pipeline2 (INSERT, UPDATE, DELETE in PostgreSQL)
and verify they appear in SQL Server with __op and __deleted.
"""
import psycopg2
import pyodbc
import time

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"
SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("="*70)
print("PIPELINE2 CDC EVENTS TEST")
print("="*70)

# 1. Current state in target
print("\n1. Current state in SQL Server (before CDC test)...")
sql_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
try:
    sql_conn = pyodbc.connect(sql_conn_str)
    cur = sql_conn.cursor()
    cur.execute("SELECT __op, COUNT(*) FROM dbo.employees GROUP BY __op ORDER BY __op")
    for row in cur.fetchall():
        print("   __op=%s count=%s" % (row[0], row[1]))
    cur.execute("SELECT COUNT(*) FROM dbo.employees")
    print("   Total rows: %s" % cur.fetchone()[0])
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

# 2. INSERT in PostgreSQL
print("\n2. INSERT in PostgreSQL (id=998, name='CDC Insert Test')...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("DELETE FROM public.employees WHERE id IN (998, 997)")
    pg_conn.commit()
    pg_cur.execute(
        "INSERT INTO public.employees (id, name, email, department_id) VALUES (998, 'CDC Insert Test', 'insert@test.com', 1)"
    )
    pg_conn.commit()
    pg_conn.close()
    print("   Done")
except Exception as e:
    print("   Error: %s" % e)

time.sleep(15)

# 3. UPDATE in PostgreSQL
print("\n3. UPDATE in PostgreSQL (id=998 -> name='CDC Update Test')...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("UPDATE public.employees SET name = 'CDC Update Test' WHERE id = 998")
    pg_conn.commit()
    pg_conn.close()
    print("   Done")
except Exception as e:
    print("   Error: %s" % e)

time.sleep(15)

# 4. DELETE in PostgreSQL
print("\n4. DELETE in PostgreSQL (id=998)...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("DELETE FROM public.employees WHERE id = 998")
    pg_conn.commit()
    pg_conn.close()
    print("   Done")
except Exception as e:
    print("   Error: %s" % e)

time.sleep(15)

# 5. Verify CDC events in SQL Server
print("\n5. CDC events in SQL Server (id=998)...")
try:
    sql_conn = pyodbc.connect(sql_conn_str)
    cur = sql_conn.cursor()
    cur.execute(
        "SELECT row_id, id, name, __op, __source_ts_ms, __deleted FROM dbo.employees WHERE id = 998 ORDER BY row_id"
    )
    rows = cur.fetchall()
    if not rows:
        print("   No rows for id=998 (CDC may not have synced yet)")
    else:
        for row in rows:
            print("   row_id=%s id=%s name=%s __op=%s ts=%s __deleted=%s" % (row[0], row[1], row[2], row[3], row[4], row[5]))
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

# 6. Summary by __op
print("\n6. Summary by __op in target...")
try:
    sql_conn = pyodbc.connect(sql_conn_str)
    cur = sql_conn.cursor()
    cur.execute("SELECT __op, __deleted, COUNT(*) FROM dbo.employees GROUP BY __op, __deleted ORDER BY __op, __deleted")
    for row in cur.fetchall():
        print("   __op=%s __deleted=%s count=%s" % (row[0], row[1], row[2]))
    cur.execute("SELECT COUNT(*) FROM dbo.employees")
    print("   Total rows: %s" % cur.fetchone()[0])
    sql_conn.close()
except Exception as e:
    print("   Error: %s" % e)

print("\n" + "="*70)
