"""
Insert one row into SQL Server dbo.customers (with correct columns), then verify in topic/target.
Columns from full load: customer_id, first_name, last_name, email, phone, balance, registration_date, is_premium
"""
import pyodbc
import time
import requests
import psycopg2

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"
PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

# Get max customer_id and next id
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)
conn = pyodbc.connect(conn_str)
cur = conn.cursor()
cur.execute("SELECT ISNULL(MAX(customer_id),0)+1 FROM dbo.customers")
next_id = cur.fetchone()[0]
# Insert with all 8 columns: customer_id, first_name, last_name, email, phone, balance, registration_date, is_premium
cur.execute("""
    INSERT INTO dbo.customers (customer_id, first_name, last_name, email, phone, balance, registration_date, is_premium)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", (next_id, 'dev', 'patra', 'dev@gmail.com', '483', 6.0, None, None))
conn.commit()
print("Inserted row: customer_id=%s, first_name=dev, last_name=patra" % next_id)
conn.close()

print("Waiting 20 seconds for CDC to stream to topic and sink...")
time.sleep(20)

# Check PostgreSQL target
pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
cur = pg_conn.cursor()
cur.execute("SELECT row_id, customer_id, first_name, last_name, __op FROM public.customers WHERE first_name='dev' AND last_name='patra' ORDER BY row_id DESC LIMIT 3")
rows = cur.fetchall()
pg_conn.close()
if rows:
    print("Found in PostgreSQL public.customers:")
    for r in rows:
        print("  row_id=%s customer_id=%s first_name=%s last_name=%s __op=%s" % r)
else:
    print("Not yet in PostgreSQL. Check Kafka topic pipeline-4.dbo.customers and sink connector status.")
