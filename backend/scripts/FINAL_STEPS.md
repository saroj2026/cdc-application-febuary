# Final Steps to Complete CDC Setup for Pipeline-1

## What We've Done So Far ✓

1. ✓ Recreated `dbo.department` in SQL Server with:
   - `row_id IDENTITY` as PRIMARY KEY (surrogate key)
   - `id, name, location` (business columns from source)
   - `_source_ts_ms, _op, __deleted` (metadata columns)

2. ✓ Recreated sink connector with SCD2-style config:
   - `insert.mode=insert` (append all changes, don't update)
   - `pk.mode=none` (allow multiple rows with same business key)
   - `transforms.unwrap.add.fields=op:_op,source.ts_ms:_source_ts_ms` (capture metadata)
   - `transforms.unwrap.delete.handling.mode=rewrite` (capture deletes with __deleted flag)

3. ✓ Updated `public.pipelines` in PostgreSQL with current connector config

4. ✓ Full load: Copied 24 existing rows from PostgreSQL to SQL Server with `_op='r'`

5. ✓ CDC test: Inserted, updated, and deleted test record (id=99999) in PostgreSQL

## What's Remaining

The CDC events (id=99999) haven't appeared in SQL Server yet because the sink connector's **consumer group offset** is pointing to a position AFTER our test events.

### Solution: Reset Consumer Group to Start from Beginning

**OPTION 1: Via Kafka UI (Recommended)**

1. Open Kafka UI: http://72.61.233.209:8080/
2. Navigate to **Consumers** tab
3. Find consumer group: `connect-sink-pipeline-1-mssql-dbo`
4. Click **Delete** button
5. Confirm deletion

**OPTION 2: Via Kafka CLI (if you have access to Kafka server)**

```bash
kafka-consumer-groups.sh --bootstrap-server 72.61.233.209:9092 \
    --delete --group connect-sink-pipeline-1-mssql-dbo
```

### After Deleting Consumer Group

**Step 1: Restart the sink connector**

Run in PowerShell from backend directory:

```powershell
Invoke-RestMethod -Uri "http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart" -Method Post
```

Or from backend directory:

```bash
curl -X POST http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart
```

**Step 2: Wait 10-15 seconds for processing**

**Step 3: Verify CDC results**

```bash
python scripts/check_cdc_results.py
```

## Expected Results

After consumer group deletion and connector restart, you should see in SQL Server:

```
CDC Test Rows (id=99999):
  row_id=25    | id=99999 | name=CDC_INSERT_TEST     | _op=c   | __deleted=No
  row_id=26    | id=99999 | name=CDC_UPDATE_TEST     | _op=u   | __deleted=No
  row_id=27    | id=99999 | name=CDC_UPDATE_TEST     | _op=d   | __deleted=Yes
```

This proves:
- ✓ **INSERT**: New row with `_op='c'` (create)
- ✓ **UPDATE**: Keeps original row + adds new row with `_op='u'` (update) - SCD2 history
- ✓ **DELETE**: Adds row with `_op='d'` (delete) and `__deleted=True`

## Future CDC Testing

To test CDC with real data (not test id=99999):

1. Insert/update/delete rows in PostgreSQL: `cdctest.public.department`
2. Wait a few seconds
3. Check SQL Server: `cdctest.dbo.department`
4. Verify that:
   - Each INSERT creates 1 row with `_op='c'`
   - Each UPDATE creates 1 NEW row with `_op='u'` (old row remains)
   - Each DELETE creates 1 NEW row with `_op='d'` and `__deleted=True`

## Troubleshooting

If CDC events still don't appear:

1. **Check connector status:**
   ```bash
   curl http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status
   ```
   - Should show `state: RUNNING`
   - Check for task failures

2. **Check Kafka topic has events:**
   - Go to http://72.61.233.209:8080/
   - Topics → `pipeline-1.public.department`
   - Verify messages exist (should have INSERT/UPDATE/DELETE for id=99999)

3. **Check consumer group offset:**
   - Go to http://72.61.233.209:8080/
   - Consumers → `connect-sink-pipeline-1-mssql-dbo`
   - Verify it's consuming (offset should be increasing)

4. **Check connector logs:**
   ```bash
   curl http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status
   ```
   Look for errors in task trace

## Summary

All infrastructure is now set up correctly for SCD2-style CDC:
- ✓ SQL Server table with surrogate key
- ✓ Sink connector with insert mode + metadata capture
- ✓ PostgreSQL pipelines table in sync
- ✓ Full load complete

Just need to **delete the consumer group** to replay CDC events from the beginning!
