"""Find the employees table in SQL Server."""
import pyodbc

SQL_HOST, SQL_PORT = "72.61.233.209", 1433
SQL_USER, SQL_PASS = "SA", "Sql@12345"

# Try different database names
databases_to_check = ["cdctest", "cdc_test", "master"]

for db_name in databases_to_check:
    print(f"\nChecking database: {db_name}")
    try:
        sql_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_HOST},{SQL_PORT};DATABASE={db_name};"
            f"UID={SQL_USER};PWD={SQL_PASS};"
            f"Encrypt=no;TrustServerCertificate=yes"
        )
        
        sql_conn = pyodbc.connect(sql_conn_str)
        sql_cur = sql_conn.cursor()
        
        # Check for employees table in any schema
        sql_cur.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'employees'
        """)
        
        tables = sql_cur.fetchall()
        if tables:
            for schema, table in tables:
                print(f"  FOUND: [{schema}].[{table}]")
                
                # Get columns
                sql_cur.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'employees' AND TABLE_SCHEMA = '{schema}'
                    ORDER BY ORDINAL_POSITION
                """)
                cols = sql_cur.fetchall()
                print(f"    Columns ({len(cols)}):")
                for col_name, col_type in cols:
                    print(f"      {col_name:25s} {col_type}")
                
                # Check row count
                sql_cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
                count = sql_cur.fetchone()[0]
                print(f"    Row count: {count}")
        else:
            print("  No employees table found")
        
        sql_conn.close()
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "="*70)
print("Also checking what database the sink connector is configured for...")
print("="*70)

import requests
try:
    r = requests.get("http://72.61.233.209:8083/connectors/sink-pipeline2-mssql-dbo/config", timeout=10)
    config = r.json()
    print(f"\nSink connector config:")
    print(f"  connection.url: {config.get('connection.url', 'N/A')}")
    print(f"  table.name.format: {config.get('table.name.format', 'N/A')}")
    print(f"  auto.create: {config.get('auto.create', 'N/A')}")
    print(f"  auto.evolve: {config.get('auto.evolve', 'N/A')}")
    print(f"  insert.mode: {config.get('insert.mode', 'N/A')}")
    print(f"  pk.mode: {config.get('pk.mode', 'N/A')}")
except Exception as e:
    print(f"Error: {e}")
