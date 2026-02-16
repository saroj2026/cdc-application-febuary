# Pipeline-5: 5000 records in Kafka but not in Postgres

## What happened

- **Single insert** (e.g. `INSERT INTO customer2 VALUES(7,'hou','ba',...)`) → **reached Postgres** ✓  
- **Bulk 5000 inserts** (CTE with `ROW_NUMBER` → `INSERT INTO dbo.customer2 SELECT ...`) → **in SQL Server and in Kafka topic** ✓ but **not in Postgres** ✗  

The sink connector’s consumer group has **already committed its offset** past those 5000 messages (or never read them), so it will not consume them again. The data is in the topic; the sink just needs to be forced to re-read from the beginning.

## Why only ~16 rows appear (not 5009)

The sink is **consuming** the topic but **failing to insert** most records. With `errors.tolerance=all` it skips failed records and continues, so you only see the first N that succeed (e.g. 16). To find the real error: **check the Kafka Connect worker logs** on the server where Connect runs (e.g. 72.61.233.209). Look for the sink task `sink-pipeline-5-pg-public` and any JDBC/PostgreSQL exception (e.g. type mismatch, NOT NULL violation, or duplicate key). Fix the schema or data so those records can be inserted.

## Fix: reset sink consumer offset to earliest

Do this **on a host that can reach the Kafka broker** (e.g. the same server where Kafka Connect runs).

### Option A: Run the Python script there

```bash
cd backend
export KAFKA_BOOTSTRAP_SERVERS=72.61.233.209:9092   # if needed
python scripts/reset_pipeline5_sink_offset.py
```

The script pauses the sink, resets the consumer group `connect-sink-pipeline-5-pg-public` for topic `pipeline-5.cdctest.dbo.customer2` to earliest, then resumes the sink.

### Option B: Reset offset with Kafka tools, then resume connector

1. **Pause the sink** (so it stops consuming):

   ```bash
   curl -X PUT http://72.61.233.209:8083/connectors/sink-pipeline-5-pg-public/pause
   ```

2. **Reset the consumer group offset** (on the server where Kafka is installed):

   ```bash
   kafka-consumer-groups.sh --bootstrap-server 72.61.233.209:9092 \
     --group connect-sink-pipeline-5-pg-public \
     --topic pipeline-5.cdctest.dbo.customer2 \
     --reset-offsets --to-earliest --execute
   ```

3. **Resume the sink**:

   ```bash
   curl -X PUT http://72.61.233.209:8083/connectors/sink-pipeline-5-pg-public/resume
   ```

After that, the sink will re-consume from the start of the topic and write all messages (including the 5000) to `public.customer2`. Check with:

```sql
SELECT COUNT(*) FROM public.customer2;
```

You should see the single row(s) plus the 5000.
