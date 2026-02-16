# Why pipeline CDC status is ERROR (e.g. pipeline-feb-11)

## What we found for pipeline-feb-11

- **Full load**: COMPLETED (data was copied Postgres → SQL Server).
- **CDC status**: ERROR.
- **Debezium connector name** and **Sink connector name** are **not stored** in `public.pipelines`.

So the failure happened **during CDC setup**, before the Debezium connector was successfully created and its name saved. The connectors `cdc-pipeline-feb-11-pg-public` and `sink-pipeline-feb-11-mssql-dbo` do **not** exist on Kafka Connect (404).

So the error occurred at **Debezium connector creation**: the backend called Kafka Connect to create the Postgres source connector, Kafka Connect returned an error (e.g. 400 with a message), the backend raised an exception, set `status=ERROR` and `cdc_status=ERROR`, and never created the connector.

## How CDC status becomes ERROR (code)

In `cdc_manager.py`, `cdc_status` is set to **ERROR** when:

1. **Full load fails** – `FullLoadError` (e.g. transfer or connection error).
2. **Any exception during CDC setup**, after full load:
   - **Debezium connector creation** – Kafka Connect returns 4xx/5xx or invalid config.
   - **Debezium connector never reaches RUNNING** – e.g. task stays FAILED (replication slot, `wal_level`, connection, plugin not loaded).
   - **Topic discovery** – no topics (e.g. Debezium didn’t create them yet or wrong names).
   - **Sink connector creation** – Kafka Connect error (config, JDBC driver, plugin).
   - **Sink connector never reaches RUNNING** – e.g. SQL Server connection or table/schema/type issue.

The **exact** message is only in backend logs (and possibly in the API response when you start the pipeline). It is **not** stored in `public.pipelines`.

## How to find the real cause

1. **Start the pipeline again and read the error**
   - Use the UI “Start” or `POST /api/v1/pipelines/{id}/start`.
   - If it fails, the response body often contains the exception message (e.g. the Kafka Connect error).

2. **Check backend logs**
   - When the pipeline was first started, the backend logs the exception and often the Kafka Connect response (e.g. “Debezium connector creation failed: …”).

3. **Run the diagnostic script**
   - From backend dir:  
     `python scripts/diagnose_pipeline_cdc_error.py pipeline-feb-11`
   - It prints pipeline row and, if connector names exist, Kafka Connect status and **task trace** (the real error). If connector names are missing (as for pipeline-feb-11), it tries inferred names and lists common causes.

## Common causes (Postgres → SQL Server)

**Debezium (Postgres source) – creation or task FAILED**

- **Plugin not installed** – Kafka Connect workers don’t have `debezium-connector-postgresql`. Install the plugin and restart Connect.
- **Postgres `wal_level`** – must be `logical`.  
  `ALTER SYSTEM SET wal_level = logical;` then restart Postgres.
- **Replication slot** – Debezium creates it; if it already exists with wrong config or is stuck, drop it and retry (or use a different slot name in connector config).
- **Connection / auth** – host, port, user, password, database in connector config must be correct and reachable from Connect workers; user needs `REPLICATION` and appropriate permissions.

**Sink (SQL Server) – creation or task FAILED**

- **JDBC plugin/driver** – `confluentinc-kafka-connect-jdbc` and SQL Server JDBC driver on Connect workers.
- **Connection** – JDBC URL, user, password; network/firewall from Connect to SQL Server.
- **Table/schema** – target table/schema must exist (or `auto.create` allowed); type mapping (e.g. Postgres types → SQL Server) must be valid.

## Optional: store last error on the pipeline

To make future diagnosis easier, you could add a column (e.g. `last_cdc_error` or `status_message`) to `public.pipelines` and set it in `cdc_manager` whenever you set `cdc_status = ERROR` (from the exception message or Kafka Connect response). Then the UI or a small query could show why CDC failed without re-running start or digging logs.
