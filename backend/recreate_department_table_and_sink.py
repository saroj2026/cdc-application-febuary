"""
Recreate dbo.department with surrogate key (row_id IDENTITY) for SCD2-style history,
restart the pipeline-1 sink connector, and sync public.pipelines in PostgreSQL.

Run from backend dir:
  python recreate_department_table_and_sink.py           # full: delete connector, drop/create table, create connector, update DB
  python recreate_department_table_and_sink.py --connector-only   # after running SQL in SSMS: only create connector + update DB
"""
import argparse
import json
import pyodbc
import psycopg2
import requests

# Config - adjust if needed
SQL_SERVER = "72.61.233.209"
SQL_PORT = 1433
SQL_DATABASE = "cdctest"
SQL_USER = "SA"
SQL_PASSWORD = "Sql@12345"
KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
SINK_CONNECTOR_NAME = "sink-pipeline-1-mssql-dbo"
DEBEZIUM_CONNECTOR_NAME = "cdc-pipeline-1-pg-public"
PIPELINE_NAME = "pipeline-1"
# PostgreSQL (public.pipelines)
PG_HOST = "72.61.233.209"
PG_PORT = 5432
PG_DATABASE = "cdctest"
PG_USER = "cdc_user"
PG_PASSWORD = "cdc_pass"


def main():
    parser = argparse.ArgumentParser(description="Recreate department table and sink connector")
    parser.add_argument("--connector-only", action="store_true", help="Only recreate sink connector (run SQL in SSMS first)")
    args = parser.parse_args()

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER},{SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=no;TrustServerCertificate=yes"
    )

    # 1. Delete sink connector first (so no writes during table drop)
    print("1. Deleting sink connector...")
    try:
        r = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR_NAME}", timeout=30)
        if r.status_code in (200, 204):
            print("   Sink connector deleted.")
        else:
            print(f"   Response: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        print(f"   Error: {e}")

    if not args.connector_only:
        # 2. Drop and recreate dbo.department with row_id as PK
        print("2. Dropping and recreating dbo.department...")
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            cur = conn.cursor()

            cur.execute("IF OBJECT_ID('dbo.department', 'U') IS NOT NULL DROP TABLE dbo.department;")
            print("   Table dropped (if existed).")

            cur.execute("""
            CREATE TABLE dbo.department (
                row_id     BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                id         INT NOT NULL,
                name       NVARCHAR(255) NULL,
                location   NVARCHAR(255) NULL,
                _source_ts_ms BIGINT NULL,
                _op        NVARCHAR(10) NULL,
                __deleted  BIT NULL
            );
            """)
            print("   Table created with row_id IDENTITY PRIMARY KEY, id/name/location/_source_ts_ms/_op/__deleted.")

            conn.close()
        except Exception as e:
            print(f"   Error: {e}")
            print("   Run scripts/drop_and_recreate_department.sql in SSMS, then run with --connector-only")
            return
    else:
        print("2. Skipping table (--connector-only). Ensure you ran scripts/drop_and_recreate_department.sql in SSMS.")

    # 3. Recreate sink connector (insert mode, pk.mode=none for history)
    print("3. Recreating sink connector...")
    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": f"jdbc:sqlserver://{SQL_SERVER}:{SQL_PORT};databaseName={SQL_DATABASE};encrypt=false;trustServerCertificate=true",
        "connection.user": SQL_USER,
        "connection.password": SQL_PASSWORD,
        "topics": "pipeline-1.public.department",
        "table.name.format": "department",
        "insert.mode": "insert",
        "pk.mode": "none",
        "auto.create": "false",
        "auto.evolve": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "true",
        "transforms.unwrap.add.fields": "op:_op,source.ts_ms:_source_ts_ms",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "consumer.override.auto.offset.reset": "earliest",
    }
    try:
        r = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json={"name": SINK_CONNECTOR_NAME, "config": config},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        if r.status_code in (200, 201):
            print("   Sink connector created.")
        else:
            print(f"   Error: {r.status_code} - {r.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")
        return

    # 4. Update public.pipelines in PostgreSQL so UI/API and start_pipeline use correct config
    print("4. Updating public.pipelines in PostgreSQL...")
    kafka_topics = ["pipeline-1.public.department"]
    debezium_config = {}
    try:
        r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{DEBEZIUM_CONNECTOR_NAME}/config", timeout=15)
        if r.status_code == 200:
            debezium_config = r.json()
    except Exception as e:
        print(f"   (Could not fetch debezium_config: {e})")
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        pg_cur = pg_conn.cursor()
        pg_cur.execute("""
            UPDATE pipelines
            SET debezium_connector_name = %s,
                sink_connector_name = %s,
                kafka_topics = %s,
                debezium_config = %s,
                sink_config = %s,
                status = 'RUNNING',
                cdc_status = 'RUNNING'
            WHERE name = %s
        """, (
            DEBEZIUM_CONNECTOR_NAME,
            SINK_CONNECTOR_NAME,
            json.dumps(kafka_topics),
            json.dumps(debezium_config),
            json.dumps(config),
            PIPELINE_NAME,
        ))
        updated = pg_cur.rowcount
        pg_conn.commit()
        pg_conn.close()
        if updated:
            print("   Pipeline row updated (debezium_connector_name, sink_connector_name, kafka_topics, sink_config, status, cdc_status).")
        else:
            print("   No row matched name=%s (pipelines table unchanged)." % PIPELINE_NAME)
    except Exception as e:
        print(f"   Error updating pipelines: {e}")

    print("\nDone. Pipeline-1 sink is running and public.pipelines is in sync.")
    print("To replay all events from the beginning: in Kafka UI (Consumers) delete consumer group")
    print("'connect-sink-pipeline-1-mssql-dbo', then restart the sink connector.")


if __name__ == "__main__":
    main()
