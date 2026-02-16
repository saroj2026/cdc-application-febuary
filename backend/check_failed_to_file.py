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
        
        with open("failed_trace.txt", "w", encoding="utf-8") as f:
            f.write(json.dumps(status, indent=2))
            
            # Check for failed tasks
            tasks = status.get("tasks", [])
            for task in tasks:
                if task.get("state") == "FAILED":
                    f.write(f"\n\nTask {task.get('id')} FAILED:\n")
                    f.write(task.get("trace"))
        print("Logged trace to failed_trace.txt")
    else:
        print(f"Failed to get status: {response.status_code} - {response.text}")

except Exception as e:
    print(f"Error: {e}")
