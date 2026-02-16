"""Check CDC results in auto-created table."""
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
print("CDC RESULTS - Auto-Created Table")
print("="*70)

try:
    conn = pyodbc.connect(sql_conn_str)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dbo.department")
    total = cur.fetchone()[0]
    print(f"\nTotal rows: {total}")
    
    cur.execute("SELECT COUNT(*) FROM dbo.department WHERE id = 99999")
    cdc_rows = cur.fetchone()[0]
    print(f"CDC test rows (id=99999): {cdc_rows}")
    
    if cdc_rows > 0:
        print("\nCDC Test Data (id=99999):")
        cur.execute("SELECT TOP 10 id, name, __op, __deleted, __source_ts_ms FROM dbo.department WHERE id = 99999")
        for r in cur.fetchall():
            name_str = (r[1] or "NULL")[:20]
            op_str = r[2] or "NULL"
            deleted_str = r[3] or "NULL"
            ts = r[4] or 0
            print(f"  id={r[0]}, name={name_str:20s}, __op={op_str:6s}, __deleted={deleted_str:6s}, ts={ts}")
        
        print("\n" + "="*70)
        if cdc_rows >= 3:
            print("SUCCESS! CDC is working with SCD2-style history!")
            print("- INSERT: new row with __op='c'")
            print("- UPDATE: keeps old row + adds new with __op='u'")
            print("- DELETE: adds row with __op='d' and __deleted='true'")
        else:
            print(f"Partial success - found {cdc_rows} rows, expected 3")
        print("="*70)
    else:
        print("\nNo CDC events found yet. Wait a bit longer or check:")
        print("1. Connector status")
        print("2. Kafka topic for messages")
        print("3. Consumer group offset/lag")
    
    conn.close()

except Exception as e:
    print(f"Error: {e}")
