import requests
import json

# Get current config
current_config = requests.get('http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/config').json()

# Fix the config - use ExtractNewRecordState instead of ExtractField
new_config = current_config.copy()

# Remove the broken transforms
del new_config['transforms']
del new_config['transforms.extractPayload.type']
del new_config['transforms.extractPayload.field']
del new_config['transforms.extractAfter.type']
del new_config['transforms.extractAfter.field']

# Use Debezium's ExtractNewRecordState transform which properly handles Debezium envelope
new_config['transforms'] = 'unwrap'
new_config['transforms.unwrap.type'] = 'io.debezium.transforms.ExtractNewRecordState'
new_config['transforms.unwrap.drop.tombstones'] = 'false'
new_config['transforms.unwrap.delete.handling.mode'] = 'none'
new_config['transforms.unwrap.add.fields'] = 'op,source.ts_ms'

# Update the connector
print("Updating sink connector config...")
response = requests.put(
    f'http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/config',
    headers={'Content-Type': 'application/json'},
    json=new_config
)

if response.status_code == 200:
    print("✓ Sink connector config updated successfully")
    print("\nNew config:")
    print(json.dumps(new_config, indent=2))
else:
    print(f"✗ Error updating config: {response.status_code}")
    print(response.text)

# Restart the connector
print("\nRestarting connector...")
restart_response = requests.post(f'http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart')
if restart_response.status_code in [200, 202, 204]:
    print("✓ Connector restart initiated")
else:
    print(f"✗ Error restarting: {restart_response.status_code}")


