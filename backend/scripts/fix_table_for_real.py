"""
Fix the department table column names to match what the sink connector expects.
The connector is producing ___source_ts_ms and ___op (triple underscore),
but our table has single underscores.
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

print("Fixing column names to match connector output...")
print("=" * 60)

try:
    sql_conn = pyodbc.connect(sql_conn_str, autocommit=True)
    sql_cur = sql_conn.cursor()
    
    # Check current columns
    sql_cur.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'department' 
        ORDER BY ORDINAL_POSITION
    """)
    print("Current columns:")
    for row in sql_cur.fetchall():
        print(f"  {row[0]}")
    
    print("\nRenaming columns...")
    
    # Rename columns to match triple underscore format
    try:
        sql_cur.execute("EXEC sp_rename 'dbo.department._source_ts_ms', '___source_ts_ms', 'COLUMN'")
        print("  ✓ Renamed _source_ts_ms to ___source_ts_ms")
    except Exception as e:
        print(f"  _source_ts_ms rename: {e}")
    
    try:
        sql_cur.execute("EXEC sp_rename 'dbo.department._op', '___op', 'COLUMN'")
        print("  ✓ Renamed _op to ___op")
    except Exception as e:
        print(f"  _op rename: {e}")
    
    try:
        sql_cur.execute("EXEC sp_rename 'dbo.department.__deleted', '___deleted', 'COLUMN'")
        print("  ✓ Renamed __deleted to ___deleted")
    except Exception as e:
        print(f"  __deleted rename: {e}")
    
    # Check new columns
    print("\nNew columns:")
    sql_cur.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'department' 
        ORDER BY ORDINAL_POSITION
    """)
    for row in sql_cur.fetchall():
        print(f"  {row[0]}")
    
    print("\n" + "=" * 60)
    print("✓ Column names fixed!")
    print("\nNext steps:")
    print("1. Restart sink connector in Kafka UI")
    print("2. Wait 15 seconds")
    print("3. Run: python scripts/check_cdc_results.py")
    
    sql_conn.close()

except Exception as e:
    print(f"Error: {e}")
