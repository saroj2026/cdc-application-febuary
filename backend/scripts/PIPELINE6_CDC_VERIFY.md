# Pipeline-6: Seeing CDC After Full Load

Full load is done via Debezium initial snapshot. To **see CDC** (change data capture) working:

---

## 1. What Must Be in Place

### AS400: Journaling

- CDC on AS400 uses **journaling**. Debezium (As400RpcConnector) reads from the journal.
- **Journaling must be enabled** for the library/table you replicate:
  - Library: `SEGMETRIQ1`
  - Table: `MYTABLE`
- On IBM i, ensure:
  - A journal exists (e.g. in `QSYS` or your journal library).
  - The library `SEGMETRIQ1` (or at least the table `SEGMETRIQ1.MYTABLE`) is attached to that journal.
- Backend config uses `database.journal.library` (default `QSYS`). If your journal is in another library, set `additional_config.journal_library` on the AS400 connection.

### Connectors

- **Debezium** (`pipeline-6-as400-SEGMETRIQ1`): RUNNING — reads journal, publishes to Kafka topic `pipeline-6.SEGMETRIQ1.MYTABLE`.
- **Sink** (`sink-pipeline-6-mssql-dbo`): RUNNING — consumes that topic, writes to SQL Server `cdctest.dbo.MYTABLE`.

Check status:

- Kafka Connect: `GET http://72.61.233.209:8083/connectors/pipeline-6-as400-SEGMETRIQ1/status`
- Sink: `GET http://72.61.233.209:8083/connectors/sink-pipeline-6-mssql-dbo/status`

---

## 2. How to See CDC

### Option A: Verify in the target (SQL Server)

1. On **AS400**, change data in `SEGMETRIQ1.MYTABLE`:
   - INSERT a row, or  
   - UPDATE a row, or  
   - DELETE a row.
2. In **SQL Server**, query `cdctest.dbo.MYTABLE` and confirm the new/changed/deleted row appears (and that deletes show as you expect, e.g. `__deleted=true` if using SCD2-style).

If you see the change in SQL Server, CDC is working end-to-end.

### Option B: Replication events in the app

- The app can log CDC events to the DB when the **CDC Event Logger** is subscribed to the pipeline’s Kafka topic.
- Pipeline-6 must be **RUNNING** and have **Kafka topics** set in the DB (e.g. `["pipeline-6.SEGMETRIQ1.MYTABLE"]`). On startup, the backend loads running pipelines’ topics and subscribes the event logger.
- Then call:
  - `GET /api/v1/monitoring/events?pipeline_id=<pipeline-6-uuid>`
  - or `GET /api/v1/monitoring/replication-events?pipeline_id=<pipeline-6-uuid>`
- You should see insert/update/delete events for that pipeline (from `pipeline_runs` / run_metadata).

---

## 3. If No CDC Events Appear

1. **Confirm journaling** on AS400 for `SEGMETRIQ1` / `SEGMETRIQ1.MYTABLE` and that the journal library matches `database.journal.library` (or `additional_config.journal_library`).
2. **Check Debezium connector/task** status and logs for errors (e.g. journal not found, permission, or offset).
3. **Check sink connector/task** status and logs for consume/write errors.
4. **Trigger a clear change** on AS400 (one INSERT or UPDATE) and wait a short time (e.g. 30–60 seconds), then re-check SQL Server and (if used) replication-events API.

---

## Duplicate rows (two source connectors)

If you see each change **twice** in SQL Server, you had **two** Debezium source connectors publishing to the **same** topic:

- **cdc-pipeline-6-as400-segmetriq1** — backend-managed (keep)
- **pipeline-6-as400-SEGMETRIQ1** — from test script (delete)

Delete the duplicate:

```bash
cd backend
python scripts/delete_duplicate_pipeline6_source.py
```

Then only one source will write to the topic and duplicates will stop. You may want to clean existing duplicate rows in `cdctest.dbo.MYTABLE` (e.g. keep one row per id and delete the extra).

---

## Summary

| Requirement              | Purpose                                      |
|--------------------------|----------------------------------------------|
| Journaling on AS400      | Debezium can read changes from the journal  |
| **One** Debezium connector RUNNING | Publishes changes to Kafka (two = duplicates) |
| Sink connector RUNNING   | Writes Kafka events to SQL Server           |
| Change on AS400          | Produces an event to capture                 |
| Check target or events API | Confirms CDC end-to-end                  |
