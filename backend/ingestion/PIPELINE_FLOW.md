# Default Pipeline Flow (Any Source → Any Target)

From now on, whenever you create and start a pipeline with **any source and any target**, the system follows this order.

**Supported targets (change target in UI/API only):** SQL Server, PostgreSQL, Oracle, S3, Snowflake.  
**Supported sources:** PostgreSQL, SQL Server, Oracle, DB2/AS400.  
To add a **new** target once, see [ADDING_A_TARGET.md](ADDING_A_TARGET.md) (4 places only).

## 1. Auto-create schema

- **Step 0**: Create target schema and tables.
- For **SQL Server** targets: tables are created with:
  - **Surrogate key**: `row_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY`
  - **Source columns**: same as source table
  - **CDC metadata**: `__op`, `__source_ts_ms`, `__deleted` (SCD2-style history)
- For **PostgreSQL** targets: same SCD2-style — surrogate key `row_id BIGSERIAL PRIMARY KEY` + source columns + `__op`, `__source_ts_ms`, `__deleted`
- Other targets (Snowflake, S3): schema/tables from source schema as before.

## 2. Full load

- **Step 1**: Copy existing data from source to target.
- For **SQL Server** and **PostgreSQL** targets: each row is inserted with `__op='r'`, `__source_ts_ms=<timestamp>`, `__deleted=NULL` (initial read/snapshot).
- Full load runs only if mode is `full_load_and_cdc` or `full_load_only` and has not already completed.

## 3. CDC (Change Data Capture)

- **Step 2**: Create Debezium source connector and JDBC Sink connector; stream changes.
- **SQL Server** and **PostgreSQL** sinks are configured for SCD2-style history:
  - `insert.mode=insert`, `pk.mode=none` (append every change as a new row)
  - Debezium unwrap: `add.fields=op,source.ts_ms` → `__op`, `__source_ts_ms` in target
  - `delete.handling.mode=rewrite` (deletes as rows with `__deleted=true`)
  - `consumer.override.auto.offset.reset=earliest`, `auto.evolve=false`

## Summary

| Step | Action |
|------|--------|
| **0** | Auto-create target schema and tables (with row_id + CDC metadata for SQL Server & PostgreSQL) |
| **1** | Full load: copy source data to target (with `__op='r'` for SQL Server & PostgreSQL) |
| **2** | CDC: Debezium source + JDBC Sink stream changes (SCD2 history for SQL Server & PostgreSQL) |

This flow is the default for all pipelines when you start them via the API or UI.
