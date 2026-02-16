"""Diagnose why sink connector is not consuming from Kafka."""
import requests
import json

KAFKA_CONNECT_URL = "http://72.61.233.209:8083"
SINK_CONNECTOR = "sink-pipeline-1-mssql-dbo"

print("="*70)
print("DIAGNOSIS: Why is sink connector not consuming?")
print("="*70)

# 1. Check connector status
print("\n1. Connector Status:")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/status", timeout=15)
    status = r.json()
    print(f"   Connector: {status['connector']['state']}")
    if status['tasks']:
        print(f"   Task: {status['tasks'][0]['state']}")
        if status['tasks'][0]['state'] == 'FAILED':
            print(f"   Error: {status['tasks'][0].get('trace', '')[:300]}")
    else:
        print("   NO TASKS!")
except Exception as e:
    print(f"   Error: {e}")

# 2. Check connector config
print("\n2. Connector Config (consumer settings):")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/config", timeout=15)
    config = r.json()
    print(f"   topics: {config.get('topics', 'NOT SET')}")
    print(f"   tasks.max: {config.get('tasks.max', 'NOT SET')}")
    print(f"   consumer.override.auto.offset.reset: {config.get('consumer.override.auto.offset.reset', 'NOT SET')}")
except Exception as e:
    print(f"   Error: {e}")

# 3. List all connectors
print("\n3. All connectors:")
try:
    r = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=15)
    connectors = r.json()
    for conn in connectors:
        print(f"   - {conn}")
except Exception as e:
    print(f"   Error: {e}")

# 4. Check if connector needs restart
print("\n4. Recommendation:")
print("   Try restarting the connector to force it to consume from the beginning:")
print(f"   curl -X POST {KAFKA_CONNECT_URL}/connectors/{SINK_CONNECTOR}/restart")

print("\n" + "="*70)
