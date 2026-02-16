"""
Diagnose why a pipeline has CDC status ERROR.
Fetches the pipeline from public.pipelines (Postgres), then checks Kafka Connect
connector status and task traces for the Debezium and Sink connectors.

Usage (from backend dir):
  python scripts/diagnose_pipeline_cdc_error.py pipeline-feb-11
  python scripts/diagnose_pipeline_cdc_error.py  <pipeline_id>
"""
import os
import sys
import json
import requests

# Postgres (backend DB)
PG_HOST = os.getenv("PG_HOST", "72.61.233.209")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DATABASE", "cdctest")
PG_USER = os.getenv("PG_USER", "cdc_user")
PG_PASS = os.getenv("PG_PASSWORD", "cdc_pass")

KAFKA_CONNECT = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")


def get_pipeline_by_name_or_id(name_or_id: str):
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()
    # Try by name first, then by id
    cur.execute(
        """
        SELECT id, name, status, full_load_status, cdc_status,
               debezium_connector_name, sink_connector_name,
               source_schema, target_schema, source_tables, kafka_topics
        FROM public.pipelines
        WHERE name = %s OR id::text = %s
        LIMIT 1
        """,
        (name_or_id, name_or_id),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "name": row[1],
        "status": row[2],
        "full_load_status": row[3],
        "cdc_status": row[4],
        "debezium_connector_name": row[5],
        "sink_connector_name": row[6],
        "source_schema": row[7],
        "target_schema": row[8],
        "source_tables": row[9],
        "kafka_topics": row[10],
    }


def get_connector_status(connector_name: str):
    if not connector_name:
        return None
    try:
        r = requests.get(
            f"{KAFKA_CONNECT}/connectors/{connector_name}/status",
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None
        return {"_error": r.status_code, "_text": r.text[:500]}
    except Exception as e:
        return {"_error": str(e)}


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/diagnose_pipeline_cdc_error.py <pipeline_name_or_id>")
        print("Example: python scripts/diagnose_pipeline_cdc_error.py pipeline-feb-11")
        sys.exit(1)
    name_or_id = sys.argv[1].strip()

    print("Fetching pipeline from public.pipelines...")
    pipeline = get_pipeline_by_name_or_id(name_or_id)
    if not pipeline:
        print(f"No pipeline found with name or id: {name_or_id}")
        sys.exit(1)

    print(f"Pipeline: {pipeline['name']} (id={pipeline['id']})")
    print(f"  status={pipeline['status']}, full_load_status={pipeline['full_load_status']}, cdc_status={pipeline['cdc_status']}")
    print(f"  Debezium connector: {pipeline['debezium_connector_name'] or '(not set)'}")
    print(f"  Sink connector:    {pipeline['sink_connector_name'] or '(not set)'}")
    print()

    # If connectors were never set, CDC failed during Debezium create or before saving names.
    # Try inferred names in case the connector exists but pipeline row was not updated.
    if not pipeline["debezium_connector_name"] or not pipeline["sink_connector_name"]:
        src_schema = (pipeline.get("source_schema") or "public").lower().replace(" ", "_")
        tgt_schema = (pipeline.get("target_schema") or "dbo").lower().replace(" ", "_")
        name_slug = pipeline["name"].lower().replace(" ", "_")
        inferred_debezium = f"cdc-{name_slug}-pg-{src_schema}"
        inferred_sink = f"sink-{name_slug}-mssql-{tgt_schema}"
        if not pipeline["debezium_connector_name"]:
            pipeline["debezium_connector_name"] = inferred_debezium
            print(f"Debezium connector name not in DB; checking inferred: {inferred_debezium}")
        if not pipeline["sink_connector_name"]:
            pipeline["sink_connector_name"] = inferred_sink
            print(f"Sink connector name not in DB; checking inferred: {inferred_sink}")
        print()

    for label, connector_name in [
        ("Debezium (source)", pipeline["debezium_connector_name"]),
        ("Sink", pipeline["sink_connector_name"]),
    ]:
        if not connector_name:
            continue
        print(f"--- {label}: {connector_name} ---")
        status = get_connector_status(connector_name)
        if status is None:
            print("  Connector not found (404). It may have been deleted or never created.")
            print()
            continue
        if isinstance(status.get("_error"), str):
            print(f"  Error fetching status: {status['_error']}")
            print()
            continue
        if "_error" in status:
            print(f"  HTTP error: {status.get('_error')} {status.get('_text', '')[:200]}")
            print()
            continue

        conn_state = status.get("connector", {}).get("state", "?")
        print(f"  Connector state: {conn_state}")
        tasks = status.get("tasks", [])
        for i, task in enumerate(tasks):
            task_state = task.get("state", "?")
            trace = task.get("trace", "").strip()
            print(f"  Task {i} state: {task_state}")
            if trace:
                print(f"  Task {i} trace (error):")
                for line in trace.split("\n")[:30]:
                    print(f"    {line}")
                if trace.count("\n") >= 30:
                    print("    ...")
        print()

    print("Common causes for Postgres -> SQL Server CDC error:")
    print("  - Debezium: replication slot missing, wal_level not 'logical', connection/auth to Postgres failed")
    print("  - Debezium: plugin not installed on Connect workers (debezium-connector-postgresql)")
    print("  - Sink: JDBC driver or connection to SQL Server failed; table/schema mismatch; type mapping")
    print("  - Sink: Connector plugin not installed (confluentinc-kafka-connect-jdbc)")


if __name__ == "__main__":
    main()
