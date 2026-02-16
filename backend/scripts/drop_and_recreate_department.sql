-- Run this in SSMS against 72.61.233.209, database cdctest (as sa).
-- Recreates dbo.department with surrogate key for SCD2-style history (multiple rows per id).

USE cdctest;
GO

IF OBJECT_ID('dbo.department', 'U') IS NOT NULL
    DROP TABLE dbo.department;
GO

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
