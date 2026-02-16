"""Restart the sink connector after fixing table schema."""
import requests
import time

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
SINK_CONNECTOR = "sink-pipeline-1-mssql-dbo"

print("Restarting sink connector...")
try:
    r = requests.post(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/restart", timeout=30)
    print(f"Response: {r.status_code}")
    if r.status_code >= 400:
        print(r.text)
    else:
        print("✓ Connector restarted")
except Exception as e:
    print(f"Error: {e}")

print("\nWaiting 10 seconds for connector to process events...")
time.sleep(10)

print("\nChecking connector status...")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/status", timeout=15)
    status = r.json()
    print(f"Connector state: {status['connector']['state']}")
    if status['tasks']:
        print(f"Task state: {status['tasks'][0]['state']}")
        if status['tasks'][0]['state'] == 'FAILED':
            print(f"\nError trace:\n{status['tasks'][0].get('trace', 'No trace available')[:500]}")
except Exception as e:
    print(f"Error: {e}")
