import requests
import json

# Get all available connector plugins
plugins = requests.get('http://72.61.233.209:8083/connector-plugins').json()
print('Available plugins:')
for p in plugins:
    print(f"  - {p['class']} (type: {p['type']}, version: {p.get('version', 'N/A')})")

# Get worker info
info = requests.get('http://72.61.233.209:8083/').json()
print(f'\nKafka Connect info: {json.dumps(info, indent=2)}')

# Try to create a simple file connector to test if tasks start
print('\n--- Testing with FileStreamSourceConnector ---')
# First check if it exists
file_plugin = [p for p in plugins if 'FileStreamSource' in p['class']]
if file_plugin:
    print(f'FileStreamSourceConnector available: {file_plugin}')
else:
    print('FileStreamSourceConnector not available')


