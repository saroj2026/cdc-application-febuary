# Quick Guide: Final Step to Complete CDC Setup

## Current Status ✅

✅ **SQL Server Table:** Recreated with surrogate key + metadata columns (24 rows loaded with _op='r')  
✅ **Sink Connector:** Configured for SCD2 with insert mode, pk.mode=none, metadata capture  
✅ **PostgreSQL DB:** public.pipelines synchronized with current config  
✅ **Test Data:** INSERT/UPDATE/DELETE performed on id=99999 in PostgreSQL (events in Kafka)

## What's Missing ⏳

CDC test events (id=99999) haven't appeared in SQL Server because the consumer group offset is past them.

## Final Step (2 minutes) 🎯

### Option 1: Via Kafka UI (Easiest)

1. **Open Kafka UI:** http://72.61.233.209:8080/
2. **Go to Consumers tab**
3. **Find consumer group:** `connect-sink-pipeline-1-mssql-dbo`
4. **Click Delete** and confirm
5. **Restart connector:**
   ```powershell
   Invoke-RestMethod -Uri "http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart" -Method Post
   ```
6. **Wait 15 seconds**, then verify:
   ```bash
   cd backend
   python scripts/check_cdc_results.py
   ```

### Option 2: Via Kafka CLI

```bash
# On Kafka server
kafka-consumer-groups.sh --bootstrap-server 72.61.233.209:9092 \
    --delete --group connect-sink-pipeline-1-mssql-dbo

# Then restart connector
curl -X POST http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart
```

## Expected Result 🎉

After deleting consumer group and restarting, you should see:

```
CDC Test Rows (id=99999):
  row_id=25 | id=99999 | name=CDC_INSERT_TEST  | _op=c | __deleted=No   ← INSERT
  row_id=26 | id=99999 | name=CDC_UPDATE_TEST  | _op=u | __deleted=No   ← UPDATE (keeps old + adds new)
  row_id=27 | id=99999 | name=CDC_UPDATE_TEST  | _op=d | __deleted=Yes  ← DELETE (soft delete)

✓ SUCCESS: All 3 CDC operations captured with SCD2 history!
```

## If You Need Help

- **Detailed instructions:** See `COMPLETE_SETUP_SUMMARY.md`
- **Troubleshooting:** See `FINAL_STEPS.md`
- **Check connector status:** http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/status
- **Check Kafka topic:** http://72.61.233.209:8080/ → Topics → pipeline-1.public.department

---

**Everything else is done. Just delete the consumer group and CDC will work as expected!** 🚀
