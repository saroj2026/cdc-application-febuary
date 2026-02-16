# Adding a New Target (or "Just Change the Target")

If **sources** (PostgreSQL, SQL Server, Oracle, DB2/AS400) are already supported, then **changing only the target** in the UI/API should work as long as that target is supported. No source changes needed.

---

## Why AS400/DB2 full load worked before but not on another machine

- **Full load** runs in the **Python backend** (this app). It uses **AS400Connector (pyodbc)** to connect **from the backend machine to AS400**. So the **machine where this backend runs** must have the **IBM i Access ODBC driver** installed.
- The connectors you see in Kafka Connect (Debezium Db2, Oracle, JDBC Sink, Snowflake, S3, etc.) run inside **Kafka Connect (Java)**. They are used for **CDC (streaming)** and for connection tests; they **do not** run the Python full load step.
- So: **AS400 → Oracle full load + CDC worked before** because that backend machine had the ODBC driver. On a machine **without** the driver, full load fails with "No IBM i Access ODBC Driver found"; CDC can still work if you use source type **db2** (Debezium Db2 connector).
- **Fix (option A):** Install **IBM i Access (ODBC driver)** on the machine where this backend runs — then AS400/DB2 → any target full load + CDC works like before.
- **Fix (option B):** Use **source database_type = "db2"** (not "as400"). For **db2** we skip backend full load and use **Debezium initial snapshot** so Kafka Connect (Db2 connector) does the initial load. No ODBC on the backend needed. Flow: Step 0 and Step 1 skipped; Step 2 creates Debezium (snapshot.mode=initial) + Sink; target tables are created by the Sink (auto.create) when the snapshot arrives. So **db2 source + SQL Server (or any JDBC target) = full load + CDC works** without ODBC.

## Supported Today (target = where data lands)

| Target        | Full load | CDC sink | Files that define it |
|---------------|-----------|----------|------------------------|
| **SQL Server** | ✓         | ✓        | sink_config, cdc_manager, transfer |
| **PostgreSQL** | ✓         | ✓        | sink_config, cdc_manager, transfer |
| **Oracle**     | ✓         | ✓        | sink_config, cdc_manager, transfer |
| S3             | ✓         | ✓        | sink_config, cdc_manager (special path) |
| Snowflake      | ✓         | ✓        | sink_config, cdc_manager (special path) |

Sources that work with these targets: PostgreSQL, SQL Server, Oracle, AS400/DB2 (with type mappings per pair).

---

## To Add a New Target (one-time, 4 places)

Then "just change the target" in the UI will work. Touch only these 4 spots:

### 1. Sink config (CDC writes to target)

**File:** `sink_config.py`

- In `generate_sink_config`: add `elif database_type == "newtarget":` and call `_generate_newtarget_sink_config(...)`.
- Add `_generate_newtarget_sink_config(...)` (JDBC URL, user/password, `insert.mode`, `pk.mode=none`, unwrap, `add.fields=op,source.ts_ms`, `delete.handling.mode=rewrite`, etc.).
- In `generate_connector_name`: add `elif database_type == "newtarget": db_short = "newtarget"`.

### 2. Full-load target connector

**File:** `cdc_manager.py`

- In `_run_full_load`, after the S3/Snowflake blocks, add:
  `elif target_connection.database_type == "newtarget":`
  - Build `target_config` from connection (same pattern as PostgreSQL/SQL Server).
  - `target_connector = NewTargetConnector(target_config)`.

### 3. Create table + insert batch (full load)

**File:** `transfer.py`

- Import the new connector (e.g. `NewTargetConnector`).
- In `create_target_table` / `transfer_schema`: add `elif isinstance(self.target, NewTargetConnector): self._create_newtarget_table(...)`.
- Implement `_create_newtarget_table(...)` (DDL with `row_id` + `__op`, `__source_ts_ms`, `__deleted`).
- In `_insert_batch`: add `elif isinstance(self.target, NewTargetConnector): self._insert_batch_newtarget(...)`.
- Implement `_insert_batch_newtarget(...)` (append `__op='r'`, `__source_ts_ms`, `__deleted=NULL`).

### 4. Type mappings (if source types differ from target)

**File:** `transfer.py`

- Add `SOURCE_TO_NEWTARGET_TYPE_MAP` (e.g. `POSTGRESQL_TO_NEWTARGET_TYPE_MAP`).
- In `_get_type_mapping`: add `elif isinstance(self.source, XConnector) and isinstance(self.target, NewTargetConnector): return self.SOURCE_TO_NEWTARGET_TYPE_MAP.get(...)` for each source you care about.

---

## Why it felt like it "took so long"

- Previously, targets were added one-by-one and the **same 4 places** were updated each time, but there was no single checklist.
- So each new target looked like a big change instead of: "add in these 4 places, then it’s just changing the target."

From here on: **add a new target once using the 4 steps above; after that, changing the target is only a config/UI change** (choose the target connection and run the pipeline).
