# Complete CDC Setup Summary - Pipeline-1

## ✅ What Has Been Completed

### 1. SQL Server Table Recreated ✓
**File:** `dbo.department` in SQL Server (72.61.233.209, cdctest)

**Schema:**
```sql
CREATE TABLE dbo.department (
    row_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,  -- Surrogate key
    id           INT NOT NULL,                                -- Business key (allows duplicates)
    name         NVARCHAR(255) NULL,                         -- From source
    location     NVARCHAR(255) NULL,                         -- From source
    _source_ts_ms BIGINT NULL,                               -- CDC metadata: source timestamp
    _op          NVARCHAR(10) NULL,                          -- CDC metadata: operation (c/u/d/r)
    __deleted    BIT NULL                                    -- CDC metadata: delete flag
);
```

**Key Changes:**
- ✓ `row_id IDENTITY` as PRIMARY KEY (not `id`) - allows multiple versions of same business record
- ✓ Added `_source_ts_ms` column for timestamp tracking
- ✓ Added `_op` column for operation type tracking
- ✓ Added `__deleted` column for soft delete tracking

### 2. Sink Connector Reconfigured ✓
**Connector Name:** `sink-pipeline-1-mssql-dbo`

**Key Configuration Changes:**
```json
{
  "insert.mode": "insert",                           // Append all changes (SCD2)
  "pk.mode": "none",                                 // No PK enforcement = allow duplicates
  "transforms.unwrap.add.fields": "op:_op,source.ts_ms:_source_ts_ms",  // Capture metadata
  "transforms.unwrap.delete.handling.mode": "rewrite",  // Keep deletes with __deleted flag
  "consumer.override.auto.offset.reset": "earliest"     // Start from beginning (new consumer)
}
```

**What This Achieves:**
- ✓ **INSERT**: Each insert in PostgreSQL → new row in SQL Server with `_op='c'`
- ✓ **UPDATE**: Each update in PostgreSQL → keeps old row + adds new row with `_op='u'` (SCD2 history)
- ✓ **DELETE**: Each delete in PostgreSQL → adds row with `_op='d'` and `__deleted=True`

### 3. Backend Database Synchronized ✓
**Table:** `public.pipelines` in PostgreSQL

**Updated Fields:**
- ✓ `debezium_connector_name`: `cdc-pipeline-1-pg-public`
- ✓ `sink_connector_name`: `sink-pipeline-1-mssql-dbo`
- ✓ `kafka_topics`: `["pipeline-1.public.department"]`
- ✓ `debezium_config`: Current Debezium connector JSON config
- ✓ `sink_config`: Current sink connector JSON config
- ✓ `status`: `RUNNING`
- ✓ `cdc_status`: `RUNNING`

**Result:** Backend UI and API now reflect the correct, live connector configuration.

### 4. Full Load Completed ✓
**Result:** 24 rows copied from PostgreSQL `public.department` to SQL Server `dbo.department`

All rows inserted with:
- `_op = 'r'` (read/initial snapshot)
- `_source_ts_ms` = current timestamp
- Business columns: `id`, `name`, `location`

### 5. CDC Test Data Created ✓
**Test Record:** `id=99999`

**Operations Performed in PostgreSQL:**
1. INSERT: `id=99999, name='CDC_INSERT_TEST', location='TESTCITY'`
2. UPDATE: `name='CDC_UPDATE_TEST', location='UPDATED_CITY'`
3. DELETE: Removed record

**These events are in Kafka topic:** `pipeline-1.public.department`

## ⏳ Final Step Required (User Action)

### Problem
The sink connector's consumer group has an offset that's PAST our test events. It needs to start from the beginning to process all CDC events.

### Solution: Delete Consumer Group

**STEP 1: Delete consumer group via Kafka UI**

1. Open: http://72.61.233.209:8080/
2. Navigate to: **Consumers** tab
3. Find: `connect-sink-pipeline-1-mssql-dbo`
4. Click: **Delete** button
5. Confirm deletion

**STEP 2: Restart sink connector**

Option A - PowerShell:
```powershell
Invoke-RestMethod -Uri "http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart" -Method Post
```

Option B - Bash/curl:
```bash
curl -X POST http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart
```

**STEP 3: Wait 10-15 seconds for processing**

**STEP 4: Verify CDC results**

```bash
cd backend
python scripts/check_cdc_results.py
```

## 📊 Expected Results

After completing the final step, you should see in SQL Server:

```
Total rows: 27  (24 full load + 3 CDC test)

CDC Test Rows (id=99999):
  row_id=25    | id=99999 | name=CDC_INSERT_TEST     | _op=c   | __deleted=No    ← INSERT
  row_id=26    | id=99999 | name=CDC_UPDATE_TEST     | _op=u   | __deleted=No    ← UPDATE
  row_id=27    | id=99999 | name=CDC_UPDATE_TEST     | _op=d   | __deleted=Yes   ← DELETE

✓ SUCCESS: All 3 CDC operations captured with SCD2 history!
```

### What Each Row Means

1. **Row 25 (_op='c')**: Original INSERT of id=99999
2. **Row 26 (_op='u')**: UPDATE of id=99999 - creates NEW row, keeps row 25 (history)
3. **Row 27 (_op='d')**: DELETE of id=99999 - creates NEW row with __deleted=True, keeps rows 25 & 26

This is **true SCD2 (Slowly Changing Dimension Type 2)** behavior - all historical versions are preserved.

## 🔧 Scripts and Tools Available

All scripts are in `backend/scripts/`:

1. **`full_pipeline1_test.py`** - Complete setup automation (already run)
2. **`check_cdc_results.py`** - Verify CDC events in SQL Server
3. **`check_kafka_topic.py`** - Check Kafka topic for events (requires kafka-python)
4. **`delete_consumer_group.ps1`** - PowerShell script with instructions
5. **`FINAL_STEPS.md`** - Detailed step-by-step guide
6. **`full_pipeline1_setup.sql`** - SQL script for table creation (already run)
7. **`sync_pipeline1_to_postgres.py`** - Standalone script to sync public.pipelines

## 🎯 Testing CDC with New Data

After verifying the test data, you can test with real operations:

```sql
-- In PostgreSQL (cdctest.public.department)

-- Insert new record
INSERT INTO department (id, name, location) VALUES (1000, 'New Dept', 'New City');

-- Update existing record
UPDATE department SET name = 'Updated Name' WHERE id = 1000;

-- Delete record
DELETE FROM department WHERE id = 1000;
```

Wait 5-10 seconds, then check SQL Server:

```sql
-- In SQL Server (cdctest.dbo.department)
SELECT row_id, id, name, location, _op, __deleted
FROM department
WHERE id = 1000
ORDER BY row_id;
```

You should see 3 rows showing the full history (insert, update, delete).

## 🔍 Troubleshooting

### If CDC events don't appear:

1. **Check connector status:**
   - http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status
   - Should show: `"state": "RUNNING"`, `"tasks": [{"state": "RUNNING"}]`

2. **Check Kafka topic:**
   - http://72.61.233.209:8080/ → Topics → `pipeline-1.public.department`
   - Should have messages (check message count and recent messages)

3. **Check consumer group:**
   - http://72.61.233.209:8080/ → Consumers → `connect-sink-pipeline-1-mssql-dbo`
   - Should show active consumption (offset increasing)

4. **Check Debezium connector:**
   ```bash
   cd backend
   python check_connectors.py
   ```

5. **Check connector logs in Kafka Connect:**
   - SSH to Kafka Connect server
   - Check logs: `/var/log/kafka-connect/` or docker logs if containerized

## 📝 Configuration Files Updated

The following files in the codebase now reflect the SCD2 configuration:

1. **`backend/ingestion/sink_config.py`**
   - Default sink config template updated with:
     - `insert.mode=insert`
     - `pk.mode=none`
     - `add.fields=op:_op,source.ts_ms:_source_ts_ms`
     - `delete.handling.mode=rewrite`

2. **`backend/recreate_department_table_and_sink.py`**
   - Complete automation script for table + connector + DB sync

3. **`backend/scripts/drop_and_recreate_department.sql`**
   - SQL script with new table schema

4. **`backend/scripts/README_department_recreate.md`**
   - Detailed instructions for the recreation process

## 🎉 Summary

**All changes you requested have been implemented:**

✅ Pipeline-1 configured for SCD2-style CDC with history tracking  
✅ SQL Server target table recreated with surrogate key + metadata columns  
✅ Sink connector configured with `insert` mode and `none` pk.mode  
✅ Backend `public.pipelines` database synchronized  
✅ Full load of 24 existing records completed  
✅ CDC test data created in PostgreSQL  

**One manual step remaining:**
- Delete consumer group `connect-sink-pipeline-1-mssql-dbo` in Kafka UI
- Restart sink connector
- Verify results with `python scripts/check_cdc_results.py`

**After this final step, CDC will be working exactly as you expected:**
- Updates create new rows (keeping old ones)
- Deletes create new rows with delete flag
- All metadata (_op, _source_ts_ms, __deleted) properly populated
- True SCD2 history tracking enabled
