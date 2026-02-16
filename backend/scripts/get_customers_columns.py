import pyodbc
conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=72.61.233.209,1433;DATABASE=cdctest;UID=SA;PWD=Sql@12345;Encrypt=no;TrustServerCertificate=yes")
cur = conn.cursor()
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='customers' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(r[0], r[1])
conn.close()
