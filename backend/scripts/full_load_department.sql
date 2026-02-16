-- Full load: Copy initial data from PostgreSQL to SQL Server
-- This is a template - adjust connection details as needed
-- You can use SSMS with linked server or run via psycopg2/pyodbc

-- If you have a linked server to PostgreSQL:
-- INSERT INTO dbo.department (id, name, location, _source_ts_ms, _op)
-- SELECT id, name, location, CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT), 'r'
-- FROM [POSTGRES_LINKED_SERVER].cdctest.public.department;

-- Or use the Python script full_load_department.py to copy data
-- It queries PostgreSQL and inserts into SQL Server with _op='r' (read/initial load)
