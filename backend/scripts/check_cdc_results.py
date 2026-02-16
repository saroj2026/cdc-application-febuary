"""
Check CDC results in SQL Server after consumer group reset.
Shows full load rows and CDC test rows with history.
"""
import pyodbc

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

sql_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_HOST},{SQL_PORT};DATABASE={SQL_DB};"
    f"UID={SQL_USER};PWD={SQL_PASS};"
    f"Encrypt=no;TrustServerCertificate=yes"
)

print("="*70)
print("CDC RESULTS - SQL Server dbo.department")
print("="*70)

try:
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cur = sql_conn.cursor()
    
    # Total rows
    sql_cur.execute("SELECT COUNT(*) FROM dbo.department")
    total = sql_cur.fetchone()[0]
    print(f"\nTotal rows: {total}")
    
    # Full load rows
    sql_cur.execute("SELECT COUNT(*) FROM dbo.department WHERE __op = 'r'")
    count_r = sql_cur.fetchone()[0]
    print(f"Full load rows (__op='r'): {count_r}")
    
    # CDC rows by operation
    sql_cur.execute("SELECT __op, COUNT(*) FROM dbo.department WHERE __op IS NOT NULL AND __op != 'r' GROUP BY __op")
    for row in sql_cur.fetchall():
        op_map = {'c': 'CREATE', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
        print(f"CDC {op_map.get(row[0], row[0])} rows (__op='{row[0]}'): {row[1]}")
    
    # Test rows (id=99999)
    print("\n" + "-"*70)
    print("CDC Test Rows (id=99999) - Should show INSERT, UPDATE, DELETE history:")
    print("-"*70)
    sql_cur.execute("""
        SELECT row_id, id, name, location, __source_ts_ms, __op, __deleted 
        FROM dbo.department 
        WHERE id = 99999 
        ORDER BY row_id
    """)
    rows = sql_cur.fetchall()
    
    if len(rows) == 0:
        print("  No rows found for id=99999")
        print("\n  TROUBLESHOOTING:")
        print("  1. Check if consumer group was deleted")
        print("  2. Check sink connector status: http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status")
        print("  3. Check Kafka topic has events: http://72.61.233.209:8080/ -> Topics -> pipeline-1.public.department")
    else:
        for r in rows:
            op_str = r[5] or 'NULL'
            deleted_str = 'Yes' if r[6] else 'No'
            print(f"  row_id={r[0]:5d} | id={r[1]:5d} | name={r[2]:20s} | __op={op_str:3s} | __deleted={deleted_str}")
        
        print("\n" + "="*70)
        print("EXPECTED HISTORY (SCD2-style):")
        print("="*70)
        print("  Row 1: _op='c'  -> INSERT (CDC_INSERT_TEST)")
        print("  Row 2: _op='u'  -> UPDATE (CDC_UPDATE_TEST) - keeps old row + adds new")
        print("  Row 3: _op='d'  -> DELETE with __deleted=True")
        
        if len(rows) == 3:
            print("\n✓ SUCCESS: All 3 CDC operations captured with history!")
        else:
            print(f"\n⚠ WARNING: Expected 3 rows, found {len(rows)}")
    
    # Sample of other rows
    print("\n" + "-"*70)
    print("Sample of full load rows (first 5):")
    print("-"*70)
    sql_cur.execute("SELECT TOP 5 row_id, id, name, __op FROM dbo.department WHERE __op = 'r' ORDER BY row_id")
    for r in sql_cur.fetchall():
        print(f"  row_id={r[0]:5d} | id={r[1]:5d} | name={r[2]:20s} | __op={r[3]}")
    
    sql_conn.close()

except Exception as e:
    print(f"Error: {e}")

print("\n" + "="*70)
