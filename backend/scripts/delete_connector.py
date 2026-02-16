"""Script to delete a Kafka Connect connector."""

import sys
import os
import requests

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def delete_connector(connector_name: str, kafka_connect_url: str = "http://72.61.233.209:8083"):
    """Delete a Kafka Connect connector.
    
    Args:
        connector_name: Name of the connector to delete
        kafka_connect_url: Kafka Connect REST API URL
    """
    url = f"{kafka_connect_url.rstrip('/')}/connectors/{connector_name}"
    
    try:
        response = requests.delete(url, timeout=10)
        if response.status_code == 204:
            print(f"✅ Successfully deleted connector: {connector_name}")
            return True
        elif response.status_code == 404:
            print(f"⚠️  Connector not found: {connector_name}")
            return False
        else:
            print(f"❌ Failed to delete connector: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error deleting connector: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python delete_connector.py <connector_name> [kafka_connect_url]")
        print("Example: python delete_connector.py cdc-pipeline-feb-11-pg-public")
        sys.exit(1)
    
    connector_name = sys.argv[1]
    kafka_connect_url = sys.argv[2] if len(sys.argv) > 2 else "http://72.61.233.209:8083"
    
    delete_connector(connector_name, kafka_connect_url)

