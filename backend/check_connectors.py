import requests
import json

# Check all connectors in Kafka Connect
connectors = requests.get('http://72.61.233.209:8083/connectors').json()
print(f'All connectors ({len(connectors)}): {connectors}')

# Check each connector status
for conn_name in connectors:
    status = requests.get(f'http://72.61.233.209:8083/connectors/{conn_name}/status').json()
    conn_type = status.get('type', 'unknown')
    conn_state = status.get('connector', {}).get('state', 'unknown')
    tasks = status.get('tasks', [])
    print(f'\n{conn_name}:')
    print(f'  Type: {conn_type}')
    print(f'  State: {conn_state}')
    print(f'  Tasks: {len(tasks)}')
    if tasks:
        for task in tasks:
            print(f'    Task {task.get("id")}: {task.get("state")}')


