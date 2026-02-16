"""
Trigger CDC events for pipeline2 and verify SCD2 behavior.
"""
import psycopg2
import pyodbc
import time

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

print("="*70)
print("TESTING PIPELINE2 CDC WITH SCD2")
print("="*70)

# Step 1: Trigger INSERT in PostgreSQL
print("\n1. Inserting new employee (id=999) in PostgreSQL...")
try:
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pg_cur = pg_conn.cursor()
    
    # Delete if exists
    pg_cur.execute("DELETE FROM public.employees WHERE id = 999")
    pg_conn.commit()
    
    # Insert new record
    pg_cur.execute("""
        INSERT INTO public.employees (id, name, email, department_id)
        VALUES (999, 'CDC Test', 'cdc.test@example.com', 1)
    """)
    pg_conn.commit()
    print("   Inserted: id=999, name=CDC Test")
    
    pg_conn.close()
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

# Step 2: Wait for CDC to process
print("\n2. Waiting 20 seconds for CDC to process...")
time.sleep(20)

# Step 3: Check SQL Server target table
print("\n3. Checking SQL Server target table...")

sql_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_HOST},{SQL_PORT};DATABASE={SQL_DB};"
    f"UID={SQL_USER};PWD={SQL_PASS};"
    f"Encrypt=no;TrustServerCertificate=yes"
)

try:
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    
    # Check if table exists
    sql_cur.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = 'dbo'
    """)
    
    exists = sql_cur.fetchone()[0]
    
    if exists:
        print("   SUCCESS! Table auto-created by sink connector!")
        
        # Get columns
        sql_cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = 'dbo'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = sql_cur.fetchall()
        print(f"\n   Table schema ({len(columns)} columns):")
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
        
        print(f"\n   CDC metadata verification:")
        print(f"     __op: {'YES' if has_op else 'NO'}")
        print(f"     __source_ts_ms: {'YES' if has_ts else 'NO'}")
        print(f"     __deleted: {'YES' if has_deleted else 'NO'}")
        
        # Check for our test record
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees WHERE id = 999")
        count = sql_cur.fetchone()[0]
        print(f"\n   Test record (id=999) count: {count}")
        
        if count > 0:
            sql_cur.execute("SELECT id, name, __op, __source_ts_ms, __deleted FROM dbo.employees WHERE id = 999")
            print("\n   Test record details:")
            for row in sql_cur.fetchall():
                print(f"     id={row[0]}, name={row[1]}, __op={row[2]}, ts={row[3]}, __deleted={row[4]}")
        
        # Get total row count
        sql_cur.execute("SELECT COUNT(*) FROM dbo.employees")
        total_count = sql_cur.fetchone()[0]
        print(f"\n   Total rows in target: {total_count}")
        
    else:
        print("   Table still not created - CDC event might not have been processed yet")
    
    sql_conn.close()

except Exception as e:
    print(f"   Error: {e}")

print("\n" + "="*70)
print("If table was created, proceed with UPDATE and DELETE tests")
print("="*70)
