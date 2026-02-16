-- Complete setup for pipeline-1 with SCD2-style history
-- Run this in SSMS against SQL Server (72.61.233.209, cdctest database)

USE cdctest;
GO

-- Step 1: Drop existing table
IF OBJECT_ID('dbo.department', 'U') IS NOT NULL
    DROP TABLE dbo.department;
GO

-- Step 2: Create table with surrogate key and metadata columns
CREATE TABLE dbo.department (
    row_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    id           INT NOT NULL,
    name         NVARCHAR(255) NULL,
    location     NVARCHAR(255) NULL,
    _source_ts_ms BIGINT NULL,
    _op          NVARCHAR(10) NULL,
    __deleted    BIT NULL
);
GO

PRINT 'Table dbo.department created with row_id IDENTITY PRIMARY KEY';
GO
