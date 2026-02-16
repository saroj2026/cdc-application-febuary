import requests
import json

# Get current config
current_config = requests.get('http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/config').json()

print("Current table.name.format:", current_config.get('table.name.format'))

# Fix table.name.format - should be just 'department', not 'dbo.department'
# The JDBC connector interprets schema.table as database.table, causing the error
current_config['table.name.format'] = 'department'

print("New table.name.format:", current_config['table.name.format'])

print("\nUpdating sink connector config...")
response = requests.put(
    'http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/config',
    headers={'Content-Type': 'application/json'},
    json=current_config
)

if response.status_code == 200:
    print("✓ Config updated successfully")
else:
    print(f"✗ Error: {response.status_code}")
    print(response.text)

# Restart connector
print("\nRestarting connector...")
restart_resp = requests.post('http://72.61.233.209:8083/connectors/sink-pipeline-1-mssql-dbo/restart')
if restart_resp.status_code in [200, 202, 204]:
    print("✓ Restart initiated")
else:
    print(f"Restart response: {restart_resp.status_code}")


