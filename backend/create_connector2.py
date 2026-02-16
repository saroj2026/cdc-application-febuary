import requests
import json
import time

# Delete any existing connectors
try:
    requests.delete("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public")
    print("Deleted existing connector")
    time.sleep(3)
except:
    pass

# Create new connector with additional fields
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
        "database.server.name": "pipeline1",  # Legacy field that might be required
        "topic.prefix": "pipeline-1",
        "schema.include.list": "public",
        "table.include.list": "public.department",
        "plugin.name": "pgoutput",
        "slot.name": "pipeline_1_slot",
        "publication.name": "pipeline_1_pub",
        "publication.autocreate.mode": "disabled",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",
        "include.schema.changes": "false"
    }
}

print(f"Creating connector...")

response = requests.post(
    "http://72.61.233.209:8083/connectors",
    headers={"Content-Type": "application/json"},
    json=config
)

print(f"Response status: {response.status_code}")
if response.status_code != 201:
    print(f"Error: {response.text}")
else:
    print("Connector created successfully")

# Wait longer
print("Waiting 20 seconds for task to start...")
time.sleep(20)

# Check status
status = requests.get("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/status").json()
print(f"\nConnector status:")
print(json.dumps(status, indent=2))

# Check task 0
try:
    task = requests.get("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/tasks/0/status")
    if task.status_code == 200:
        print(f"\nTask 0 status: {task.json()}")
    else:
        print(f"\nTask 0 not found: {task.text}")
except Exception as e:
    print(f"Error checking task: {e}")

# Check topics in Kafka
topics = requests.get("http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/topics").json()
print(f"\nTopics: {json.dumps(topics, indent=2)}")


