"""Get pipeline-4 source connector status and task error."""
import requests
import json

KAFKA_CONNECT = "http://72.61.233.209:8083"
CONNECTOR = "cdc-pipeline-4-mssql-dbo"

r = requests.get(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/status", timeout=15)
print("Status response:", r.status_code)
data = r.json()
print(json.dumps(data, indent=2))

if data.get("tasks"):
    for i, t in enumerate(data["tasks"]):
        print(f"\nTask {i}: state={t.get('state')}, trace={t.get('trace', '')[:1500] if t.get('trace') else 'N/A'}")
