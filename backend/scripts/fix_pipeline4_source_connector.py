"""Fix pipeline-4 source connector: set schema history Kafka bootstrap to reachable host and restart."""
import requests

KAFKA_CONNECT = "http://72.61.233.209:8083"
CONNECTOR = "cdc-pipeline-4-mssql-dbo"
BOOTSTRAP_SERVERS = "72.61.233.209:9092"

# Get current config
r = requests.get(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/config", timeout=15)
if r.status_code != 200:
    print("Failed to get config:", r.status_code, r.text)
    exit(1)
config = r.json()
print("Current schema.history.internal.kafka.bootstrap.servers:", config.get("schema.history.internal.kafka.bootstrap.servers"))

# Update to reachable Kafka
config["schema.history.internal.kafka.bootstrap.servers"] = BOOTSTRAP_SERVERS

# Put updated config
r = requests.put(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/config", json=config, timeout=15)
if r.status_code not in (200, 201):
    print("Failed to update config:", r.status_code, r.text)
    exit(1)
print("Config updated.")

# Restart connector
r = requests.post(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/restart", timeout=15)
if r.status_code not in (200, 204):
    print("Restart response:", r.status_code, r.text)
else:
    print("Connector restart requested. Check status in a few seconds.")

# Check status after short wait
import time
time.sleep(8)
r = requests.get(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/status", timeout=15)
d = r.json()
print("Connector state:", d.get("connector", {}).get("state"))
for t in d.get("tasks", []):
    print("Task 0 state:", t.get("state"), "-", (t.get("trace") or "")[:200])
