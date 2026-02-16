import requests
import json
import os

connector_name = "cdc-pipeline-feb-11-pg-public"
connect_url = "http://72.61.233.209:8083"

try:
    print(f"Checking status for {connector_name}...")
    response = requests.get(f"{connect_url}/connectors/{connector_name}/status")
    if response.status_code == 200:
        status = response.json()
        print(json.dumps(status, indent=2))
        
        # Check for failed tasks and print trace
        tasks = status.get("tasks", [])
        for task in tasks:
            if task.get("state") == "FAILED":
                print(f"\nTask {task.get('id')} FAILED:")
                print(task.get("trace"))
    else:
        print(f"Failed to get status: {response.status_code} - {response.text}")

except Exception as e:
    print(f"Error: {e}")
