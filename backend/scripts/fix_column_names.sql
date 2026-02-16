-- Fix column names to match Debezium output
-- The SMT is producing ___source_ts_ms and ___op (triple underscore)
-- but our table has _source_ts_ms and _op (single underscore)

USE cdctest;
GO

-- Option 1: Rename existing columns (preserves data)
EXEC sp_rename 'dbo.department._source_ts_ms', '___source_ts_ms', 'COLUMN';
EXEC sp_rename 'dbo.department._op', '___op', 'COLUMN';
EXEC sp_rename 'dbo.department.__deleted', '___deleted', 'COLUMN';
GO

SELECT TOP 5 * FROM dbo.department;
GO
