import psycopg2
import requests
import json
from ingestion.sink_config import SinkConfigGenerator
from ingestion.connection_service import ConnectionService

# Get pipeline and connections
conn = psycopg2.connect(host='72.61.233.209', port=5432, database='cdctest', user='cdc_user', password='cdc_pass')
cur = conn.cursor()

# Get pipeline
cur.execute("""
    SELECT p.id, p.name, p.source_connection_id, p.target_connection_id,
           p.source_database, p.target_database, p.target_schema,
           sc.database_type as source_type, tc.database_type as target_type
    FROM pipelines p
    JOIN connections sc ON p.source_connection_id = sc.id
    JOIN connections tc ON p.target_connection_id = tc.id
    WHERE p.name = 'pipeline-1'
""")
pipeline_row = cur.fetchone()

if not pipeline_row:
    print("Pipeline not found")
    exit(1)

pipeline_id, pipeline_name, source_conn_id, target_conn_id, source_db, target_db, target_schema, source_type, target_type = pipeline_row

print(f"Pipeline: {pipeline_name}")
print(f"Source: {source_type}, Target: {target_type}")

# Get target connection details
cur.execute("""
    SELECT id, name, database_type, host, port, database, schema, username, password, additional_config
    FROM connections WHERE id = %s
""", (target_conn_id,))
target_conn_row = cur.fetchone()
target_cols = [desc[0] for desc in cur.description]

# Build target connection dict
target_connection_dict = {}
for i, col in enumerate(target_cols):
    target_connection_dict[col] = target_conn_row[i]

print(f"\nTarget Connection: {target_connection_dict['name']}")

# Get Debezium connector name and topics
debezium_connector_name = "cdc-pipeline-1-pg-public"
topics_response = requests.get(f'http://72.61.233.209:8083/connectors/{debezium_connector_name}/topics')
if topics_response.status_code == 200:
    topics_data = topics_response.json()
    kafka_topics = topics_data.get(debezium_connector_name, {}).get('topics', [])
    # Filter out schema change topic
    kafka_topics = [t for t in kafka_topics if '.' in t and t != pipeline_name]
    print(f"Kafka topics: {kafka_topics}")
else:
    print("Could not get topics")
    kafka_topics = ["pipeline-1.public.department"]

# Get Debezium config
debezium_config_response = requests.get(f'http://72.61.233.209:8083/connectors/{debezium_connector_name}/config')
if debezium_config_response.status_code == 200:
    debezium_config = debezium_config_response.json()
    print(f"Debezium config retrieved")
else:
    debezium_config = {}

# Create Connection object for sink config
from ingestion.models import Connection
target_connection = Connection(
    id=target_connection_dict['id'],
    name=target_connection_dict['name'],
    connection_type='target',  # Doesn't matter for sink config
    database_type=target_connection_dict['database_type'],
    host=target_connection_dict['host'],
    port=target_connection_dict['port'],
    database=target_connection_dict['database'],
    schema=target_connection_dict.get('schema'),
    username=target_connection_dict['username'],
    password=target_connection_dict['password'],
    additional_config=target_connection_dict.get('additional_config') or {}
)

# Generate sink connector name and config
sink_connector_name = SinkConfigGenerator.generate_connector_name(
    pipeline_name=pipeline_name,
    database_type=target_type,
    schema=target_schema or target_connection.schema or "public"
)

print(f"\nSink connector name: {sink_connector_name}")

sink_config = SinkConfigGenerator.generate_sink_config(
    connector_name=sink_connector_name,
    target_connection=target_connection,
    target_database=target_db or target_connection.database,
    target_schema=target_schema or target_connection.schema or "public",
    kafka_topics=kafka_topics
)

print(f"Sink config generated")

# Create sink connector
print(f"\nCreating sink connector...")
try:
    create_response = requests.post(
        f'http://72.61.233.209:8083/connectors',
        headers={'Content-Type': 'application/json'},
        json={
            "name": sink_connector_name,
            "config": sink_config
        }
    )
    if create_response.status_code in [201, 409]:
        print(f"Sink connector created/updated: {sink_connector_name}")
    else:
        print(f"Error creating sink connector: {create_response.status_code} - {create_response.text}")
except Exception as e:
    print(f"Error: {e}")

# Update pipeline in database
print(f"\nUpdating pipeline in database...")
cur.execute("""
    UPDATE pipelines 
    SET debezium_connector_name = %s,
        sink_connector_name = %s,
        kafka_topics = %s,
        debezium_config = %s,
        sink_config = %s,
        cdc_status = 'RUNNING',
        status = 'RUNNING'
    WHERE id = %s
""", (
    debezium_connector_name,
    sink_connector_name,
    json.dumps(kafka_topics),
    json.dumps(debezium_config),
    json.dumps(sink_config),
    pipeline_id
))
conn.commit()
print("Pipeline updated in database")

# Check if data is being written to SQL Server
print(f"\nChecking SQL Server for data...")
import pyodbc
try:
    sql_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={target_connection.host},{target_connection.port};"
        f"DATABASE={target_connection.database};"
        f"UID={target_connection.username};"
        f"PWD={target_connection.password}"
    )
    sql_cur = sql_conn.cursor()
    sql_cur.execute("SELECT COUNT(*) FROM department")
    count = sql_cur.fetchone()[0]
    print(f"Rows in SQL Server department table: {count}")
    sql_conn.close()
except Exception as e:
    print(f"Could not check SQL Server: {e}")

conn.close()
print("\nDone!")

