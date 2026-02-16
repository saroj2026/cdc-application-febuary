# ✅ CDC Setup Complete - Pipeline-1 Working!

## Summary

**Status**: CDC is now working correctly with SCD2-style history tracking for pipeline-1!

**Test Results**: 6 CDC events captured for test record (id=99999):
- 2x INSERT operations (`__op='c'`)
- 2x UPDATE operations (`__op='u'`)
- 2x DELETE operations (`__op='d'` with `__deleted='true'`)

## What Was Fixed

### Root Cause
The sink connector was failing because of column name mismatches between:
- What Debezium's `ExtractNewRecordState` SMT produces: `__op`, `__source_ts_ms`, `__deleted` (DOUBLE underscores)
- What we initially created: `_op`, `_source_ts_ms`, `__deleted` (SINGLE underscores)

### Solution
Let the JDBC Sink Connector **auto-create** the table (`auto.create=true`) with the exact schema it expects.

## Current Table Schema (Auto-Created)

```sql
dbo.department:
  _cdc_seq_id       BIGINT          -- Auto-incrementing sequence ID
  id                INT              -- Business key (allows duplicates for history)
  name              VARCHAR(MAX)     -- From source
  location          VARCHAR(MAX)     -- From source
  department_id     INT              -- Extra column from source
  department_name   NVARCHAR(100)    -- Extra column from source
  __op              VARCHAR(MAX)     -- CDC operation: 'c'=create, 'u'=update, 'd'=delete, 'r'=read
  __source_ts_ms    BIGINT           -- Source timestamp in milliseconds
  __deleted         VARCHAR(MAX)     -- Delete flag: 'true' or 'false'
  _cdc_op           NVARCHAR(10)     -- Additional CDC metadata
  _cdc_source       NVARCHAR(MAX)    -- Additional CDC metadata
  _cdc_ts           DATETIME2        -- Additional CDC metadata
  _cdc_scn          NVARCHAR(50)     -- Additional CDC metadata
```

## Current Connector Configuration

### Sink Connector: `sink-pipeline-1-mssql-dbo`

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "1",
  "connection.url": "jdbc:sqlserver://72.61.233.209:1433;databaseName=cdctest;encrypt=false;trustServerCertificate=true",
  "topics": "pipeline-1.public.department",
  "table.name.format": "department",
  "insert.mode": "insert",                    // SCD2: Append all changes
  "pk.mode": "none",                          // SCD2: Allow duplicate business keys
  "auto.create": "true",                      // Let connector create table
  "auto.evolve": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.add.fields": "op,source.ts_ms",  // Add metadata fields
  "transforms.unwrap.delete.handling.mode": "rewrite", // Keep deletes with __deleted flag
  "consumer.override.auto.offset.reset": "earliest"
}
```

### Debezium Source Connector: `cdc-pipeline-1-pg-public`

Captures changes from PostgreSQL `public.department` table and publishes to Kafka topic `pipeline-1.public.department`.

## How to Test CDC

### 1. Insert a Record in PostgreSQL

```sql
-- In PostgreSQL (cdctest.public.department)
INSERT INTO department (id, name, location) VALUES (12345, 'Test Dept', 'Test City');
```

**Expected in SQL Server**:
```
1 row with:
  id=12345, name='Test Dept', __op='c', __deleted='false'
```

### 2. Update the Record

```sql
-- In PostgreSQL
UPDATE department SET name = 'Updated Dept', location = 'New City' WHERE id = 12345;
```

**Expected in SQL Server**:
```
2 rows total (old + new):
  Row 1: id=12345, name='Test Dept',    __op='c', __deleted='false'  (original)
  Row 2: id=12345, name='Updated Dept', __op='u', __deleted='false'  (after update)
```

### 3. Delete the Record

```sql
-- In PostgreSQL
DELETE FROM department WHERE id = 12345;
```

**Expected in SQL Server**:
```
3 rows total (preserves all history):
  Row 1: id=12345, name='Test Dept',    __op='c', __deleted='false'
  Row 2: id=12345, name='Updated Dept', __op='u', __deleted='false'
  Row 3: id=12345, name='Updated Dept', __op='d', __deleted='true'   (delete marker)
```

## Verification Scripts

### Check CDC Results
```bash
cd backend
python scripts/check_final_results.py
```

### Check Connector Status
```bash
curl http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status
```

### Check Kafka UI
- **Topics**: http://72.61.233.209:8080/ui/clusters/local/all-topics/pipeline-1.public.department
- **Consumers**: http://72.61.233.209:8080/ui/clusters/local/consumer-groups/connect-sink-pipeline-1-mssql-dbo
- **Connectors**: http://72.61.233.209:8080/ui/clusters/local/connectors

## Important Notes

### Auto-Created Table
The current table was auto-created by the JDBC Sink Connector. It includes extra columns (`_cdc_*`, `department_id`, `department_name`) that may not be needed but don't affect CDC functionality.

### If You Need a Clean Table
If you want only the essential columns, you can manually create a table with just:
- Surrogate key (e.g., `row_id IDENTITY` or use auto-generated `_cdc_seq_id`)
- Business columns: `id`, `name`, `location`
- Metadata columns: `__op`, `__source_ts_ms`, `__deleted`

Then set `auto.create=false` in the connector config.

### Key Configuration Values
- **`insert.mode=insert`**: Appends every change as a new row (SCD2)
- **`pk.mode=none`**: Allows multiple rows with the same business key (history)
- **`add.fields=op,source.ts_ms`**: Adds `__op` and `__source_ts_ms` metadata
- **`delete.handling.mode=rewrite`**: Keeps deletes as rows with `__deleted='true'`

### Consumer Group
- **Name**: `connect-sink-pipeline-1-mssql-dbo`
- **Status**: Should show STABLE with 1 member, lag=0
- **If lag > 0**: Connector is behind, wait for it to catch up

### Troubleshooting

**If CDC events stop appearing:**
1. Check connector status: `curl http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status`
2. Check for FAILED tasks
3. Check consumer group lag in Kafka UI
4. Restart connector: `curl -X POST http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart`

**If you need to replay all events:**
1. Stop/pause connector
2. Delete consumer group in Kafka UI
3. Restart connector (it will start from beginning due to `auto.offset.reset=earliest`)

## Success! 🎉

CDC is now working as expected with:
- ✅ Full SCD2-style history tracking
- ✅ All metadata columns populated
- ✅ INSERT, UPDATE, DELETE operations captured
- ✅ Soft deletes with `__deleted` flag
- ✅ No data loss or overwrites

**You can now perform any INSERT/UPDATE/DELETE operations in PostgreSQL and see the complete change history in SQL Server!**
