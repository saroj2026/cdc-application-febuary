import requests
import json

try:
    response = requests.get('http://72.61.233.209:8083/connectors')
    connectors = response.json()
    print("Connectors found:", len(connectors))
    for c in connectors:
        if 'pipeline-feb-11' in c:
            print(f"FOUND: {c}")
            status = requests.get(f'http://72.61.233.209:8083/connectors/{c}/status').json()
            print(f"STATUS: {status}")
except Exception as e:
    print(f"Error: {e}")
