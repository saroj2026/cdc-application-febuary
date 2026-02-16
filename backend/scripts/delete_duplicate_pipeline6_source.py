"""Delete the duplicate AS400 source connector for pipeline-6.

Root cause of duplicates: Two source connectors were publishing to the same topic
  pipeline-6.SEGMETRIQ1.MY.TABLE:
  - cdc-pipeline-6-as400-segmetriq1  (backend-managed, keep this one)
  - pipeline-6-as400-SEGMETRIQ1       (created by test script, delete this one)

The sink consumes the topic once but each change was written twice -> duplicate rows in SQL Server.
"""
import os
import requests

KAFKA_CONNECT = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
# Connector created by test_pipeline6_connector_create.py - same topic as backend connector
DUPLICATE_CONNECTOR = "pipeline-6-as400-SEGMETRIQ1"

def main():
    print(f"Deleting duplicate source connector: {DUPLICATE_CONNECTOR}")
    r = requests.delete(f"{KAFKA_CONNECT}/connectors/{DUPLICATE_CONNECTOR}", timeout=15)
    if r.status_code == 204:
        print("Deleted. Only cdc-pipeline-6-as400-segmetriq1 will publish to the topic now.")
    elif r.status_code == 404:
        print("Connector already gone (404).")
    else:
        print(f"Unexpected: {r.status_code} {r.text[:300]}")

if __name__ == "__main__":
    main()
