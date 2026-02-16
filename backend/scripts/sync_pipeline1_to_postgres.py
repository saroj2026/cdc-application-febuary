"""
Sync pipeline-1 connector names and config from Kafka Connect to public.pipelines in PostgreSQL.
Run this when you recreated the sink (or table) manually and need the DB row to match.

Run from backend dir: python scripts/sync_pipeline1_to_postgres.py
"""
import json
import psycopg2
import requests

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
DEBEZIUM_CONNECTOR_NAME = "cdc-pipeline-1-pg-public"
SINK_CONNECTOR_NAME = "sink-pipeline-1-mssql-dbo"
PIPELINE_NAME = "pipeline-1"
PG_HOST, PG_PORT, PG_DATABASE = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASSWORD = "cdc_user", "cdc_pass"


def main():
    # Fetch current configs from Kafka Connect
    debezium_config = {}
    sink_config = {}
    kafka_topics = ["pipeline-1.public.department"]
    try:
        r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{DEBEZIUM_CONNECTOR_NAME}/config", timeout=15)
        if r.status_code == 200:
            debezium_config = r.json()
        r2 = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{DEBEZIUM_CONNECTOR_NAME}/topics", timeout=15)
        if r2.status_code == 200:
            topics_data = r2.json()
            kafka_topics = topics_data.get(DEBEZIUM_CONNECTOR_NAME, {}).get("topics", [])
            kafka_topics = [t for t in kafka_topics if "." in t and t != PIPELINE_NAME]
        r3 = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR_NAME}/config", timeout=15)
        if r3.status_code == 200:
            sink_config = r3.json()
    except Exception as e:
        print("Error fetching from Kafka Connect:", e)
        return

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
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
        json.dumps(sink_config),
        PIPELINE_NAME,
    ))
    n = cur.rowcount
    conn.commit()
    conn.close()
    print("Updated %d row(s) in public.pipelines for name=%s." % (n, PIPELINE_NAME))


if __name__ == "__main__":
    main()
