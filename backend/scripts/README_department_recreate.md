# Recreate dbo.department and restart pipeline-1 (SCD2-style)

Use this when you want a clean table with **surrogate key** (`row_id IDENTITY`) so multiple rows per `id` are allowed (history: a, b, c; update a→o adds a new row with _op=u; delete b adds a row with _op=d).

**Important:** After recreating the sink (or table), the script also **updates `public.pipelines` in PostgreSQL** so the pipeline row has the correct `sink_connector_name`, `sink_config`, `kafka_topics`, and `debezium_config`. That way the UI, API, and `start_pipeline` all stay in sync.

## Option A: Run everything from your machine (recommended)

1. **Run the Python script** (from `backend` dir; needs network to SQL Server and Kafka Connect):
   ```bash
   cd backend
   python recreate_department_table_and_sink.py
   ```
   This will: delete the sink connector → drop and recreate `dbo.department` with `row_id` PK → recreate the sink connector.

2. **Optional – replay all events from the beginning**  
   In Kafka UI: **Consumers** → select **connect-sink-pipeline-1-mssql-dbo** → **Delete consumer group**.  
   Then restart the sink connector (Kafka Connect UI or `POST .../connectors/sink-pipeline-1-mssql-dbo/restart`).

3. **Start pipeline-1** (if not already running)  
   Use your API or UI to start pipeline-1. The Debezium source should already be running; the sink is recreated by step 1.

---

## Option B: Run SQL in SSMS, then connector only

If the script cannot connect to SQL Server from where you run it (e.g. ODBC/encryption issues):

1. **In SSMS** (connected to `72.61.233.209`, database `cdctest`), run:
   ```
   scripts/drop_and_recreate_department.sql
   ```
   (Or copy the contents of `scripts/drop_and_recreate_department.sql` into a new query and execute.)

2. **Recreate only the sink connector** (from `backend` dir):
   ```bash
   python recreate_department_table_and_sink.py --connector-only
   ```
   This skips the table step and only deletes + recreates the sink connector.

3. **Optional** – delete consumer group **connect-sink-pipeline-1-mssql-dbo** in Kafka UI for full replay, then restart the sink connector.

4. **Start pipeline-1** via API (if not already running):
   ```http
   POST http://localhost:8000/api/v1/pipelines/{pipeline_id}/start
   ```
   Or use the pipeline ID for the pipeline named "pipeline-1" from your DB/UI.

---

## Table schema after recreate

| Column         | Type           | Description                    |
|----------------|----------------|--------------------------------|
| row_id         | BIGINT IDENTITY| Primary key (surrogate)        |
| id             | INT            | Business key from source       |
| name           | NVARCHAR(255)  |                                |
| location       | NVARCHAR(255)  |                                |
| _source_ts_ms  | BIGINT         | Event timestamp from Debezium |
| _op            | NVARCHAR(10)   | c / u / d / r                  |
| __deleted      | BIT            | True for delete events         |

Sink is configured with `insert.mode=insert` and `pk.mode=none`, so every CDC event inserts a new row (no overwrites).

---

## Sync only public.pipelines (connectors already recreated)

If you already recreated the sink connector (or table) and only need to update **public.pipelines** in PostgreSQL so the UI/API and `start_pipeline` use the correct config:

```bash
cd backend
python scripts/sync_pipeline1_to_postgres.py
```

This fetches the current `debezium_config` and `sink_config` from Kafka Connect and updates the pipeline row for `pipeline-1`.
