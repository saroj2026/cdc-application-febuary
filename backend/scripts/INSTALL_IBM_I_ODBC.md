# AS400/DB2: jt400.jar vs ODBC — what we use where

We have **two** different AS400/DB2 paths in the codebase:

| What | Where it runs | What it needs | Used for |
|------|----------------|---------------|----------|
| **jt400.jar** (IBM Toolbox for Java) | **Kafka Connect** (Java) | jt400.jar in Connect plugin/lib path | **CDC only** — when using Debezium **As400RpcConnector** (`io.debezium.connector.db2as400.As400RpcConnector`) |
| **IBM i Access ODBC driver** | **Python backend** (this app) | ODBC driver on the machine where the backend runs | **Full load + auto-create schema** — when using **AS400Connector** (pyodbc) |

- **jt400.jar** is for the **Debezium AS400 connector** inside Kafka Connect. It does **not** help the Python backend; the backend never uses a JAR.
- **ODBC driver** is for the backend’s **AS400Connector** (pyodbc) so it can connect to AS400 for schema + full load.

Your current Kafka Connect has **Db2Connector** (`io.debezium.connector.db2.Db2Connector`) but **not** As400RpcConnector. So we use source type **"db2"** and skip backend full load (Debezium initial snapshot does the initial load) — no jt400 and no ODBC needed on the backend.

If you **did** use “as400 jar” (jt400) before, it was in **Kafka Connect** for the Debezium AS400 connector (CDC). Full load + schema would still have needed the **ODBC driver on the backend** (or a DB2 source with Debezium snapshot as we do now).

---

# Install IBM i Access ODBC Driver (for AS400/DB2 full load on backend)

The backend **cannot** install this for you — it is **proprietary IBM software** and must be downloaded from IBM and installed on the **machine where the backend runs**.

## 1. Download

- **IBM i Access Client Solutions** (includes ODBC driver):  
  https://www.ibm.com/support/pages/ibm-i-access-client-solutions  
  or  
  https://www.ibm.com/resources/mrs/assets?source=swg-ia  

- You may need an **IBM.com user ID** to download.

## 2. Install on Windows (backend server)

1. Extract the downloaded `.zip`.
2. Open the **Windows_Application** folder.
3. Run:
   - **64-bit:** `install_acs_64` (or the 64-bit installer .exe)
   - **32-bit:** `install_acs_32`
4. Choose “current user” or “all users” and complete the installer.
5. **Restart the backend** (or the whole machine if the installer asks).

## 3. Verify

On the **same machine** where the backend runs:

```powershell
cd backend
python scripts/check_ibm_i_odbc.py
```

If the script reports “IBM i / AS400 ODBC driver(s) found”, the backend can use AS400 for **auto-create schema** and **full load** (e.g. AS400 → Oracle or AS400 → SQL Server).

## 4. Alternative (no ODBC)

If you cannot install the driver, use **source `database_type = "db2"`** (not `as400`). Then the backend skips schema + full load and uses **Debezium initial snapshot**; no ODBC is required on the backend.

---

## 5. jt400.jar (Kafka Connect only — for Debezium As400RpcConnector)

If you want to use the **Debezium AS400 connector** (`io.debezium.connector.db2as400.As400RpcConnector`) in Kafka Connect (instead of the standard Db2 connector), you need **jt400.jar** (IBM Toolbox for Java) on the **Kafka Connect** server:

1. Get **jt400.jar**: Maven `com.ibm.as400:jt400` or download from IBM.
2. Put it in Kafka Connect’s **plugin directory** (same place as the Debezium AS400 connector) or in Connect’s **lib** directory.
3. Restart Kafka Connect.

This only affects **CDC** when source type is **"as400"** and Connect has the As400RpcConnector plugin. It does **not** help the Python backend full load (that still needs ODBC on the backend, or use source type **"db2"** so we skip backend full load).
