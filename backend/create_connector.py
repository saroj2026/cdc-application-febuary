import requests
import json
import time

# Delete any existing connectors first
connectors = requests.get("http://72.61.233.209:8083/connectors").json()
print(f"Current connectors: {connectors}")

for conn in connectors:
    try:
        requests.delete(f"http://72.61.233.209:8083/connectors/{conn}")
        print(f"Deleted connector: {conn}")
    except Exception as e:
        print(f"Failed to delete {conn}: {e}")

time.sleep(2)

# Create new connector
config = {
    "name": "cdc-pipeline-1-pg-public",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "72.61.233.209",
        "database.port": "5432",
        "database.user": "cdc_user",
        "database.password": "cdc_pass",
        "database.dbname": "cdctest",
        "topic.prefix": "pipeline-1",
        "table.include.list": "public.department",
        "plugin.name": "pgoutput",
        "slot.name": "pipeline_1_slot",
        "publication.name": "pipeline_1_pub",
        "publication.autocreate.mode": "disabled",
        "snapshot.mode": "initial",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true"
    }
}

print(f"Creating connector with config: {json.dumps(config, indent=2)}")

response = requests.post(
    "http://72.61.233.209:8083/connectors",
    headers={"Content-Type": "application/json"},
    json=config
)

print(f"Response status: {response.status_code}")
print(f"Response: {response.text}")

# Wait and check status
time.sleep(10)

status = requests.get("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/status").json()
print(f"\nConnector status: {json.dumps(status, indent=2)}")

# Check topics
topics_response = requests.get("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/topics").json()
print(f"\nConnector topics: {json.dumps(topics_response, indent=2)}")


