"""
Check if SQL Server CDC is enabled on database cdctest and table dbo.customers.
If not, print SQL to run in SSMS to enable CDC. Debezium only sees changes when CDC is enabled.
"""
import pyodbc

SQL_HOST, SQL_PORT, SQL_DB = "72.61.233.209", 1433, "cdctest"
SQL_USER, SQL_PASS = "SA", "Sql@12345"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=no;TrustServerCertificate=yes"
) % (SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASS)

print("="*70)
print("SQL Server CDC check for dbo.customers (pipeline-4)")
print("="*70)

conn = pyodbc.connect(conn_str)
cur = conn.cursor()

# 1. Is CDC enabled on database?
cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = ?", (SQL_DB,))
row = cur.fetchone()
db_cdc = row[0] if row else False
print("\n1. Database '%s' CDC enabled: %s" % (SQL_DB, db_cdc))

# 2. Is dbo.customers captured by CDC?
table_cdc = False
if db_cdc:
    try:
        cur.execute("""
            SELECT 1 FROM cdc.change_tables
            WHERE source_schema_name = 'dbo' AND source_table_name = 'customers'
        """)
        table_cdc = cur.fetchone() is not None
    except Exception as e:
        print("   (cdc.change_tables check failed: %s)" % e)
print("2. Table dbo.customers in CDC: %s" % table_cdc)

conn.close()

if not db_cdc or not table_cdc:
    print("\n" + "="*70)
    print("CDC is NOT fully enabled. Run the following in SQL Server (SSMS) as SA:")
    print("="*70)
    print("USE cdctest;")
    if not db_cdc:
        print("GO")
        print("EXEC sys.sp_cdc_enable_db;")
        print("GO")
    if not table_cdc:
        print("""
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'customers',
  @role_name     = NULL,
  @supports_net_changes = 1;
GO
""")
    print("Then restart the source connector so it picks up CDC:")
    print('  POST http://72.61.233.209:8083/connectors/cdc-pipeline-4-mssql-dbo/restart')
    print("="*70)
else:
    print("\nCDC is enabled. If inserts still don't appear in topics, check connector logs or restart the connector.")
