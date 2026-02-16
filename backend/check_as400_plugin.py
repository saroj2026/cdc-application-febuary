import requests
import json

try:
    r = requests.get('http://72.61.233.209:8083/connector-plugins', timeout=10)
    plugins = r.json()
    
    # Check for As400RpcConnector
    as400_plugins = [p for p in plugins if 'as400' in p.get('class', '').lower() or 'db2as400' in p.get('class', '').lower()]
    
    if as400_plugins:
        print('SUCCESS: AS400 Connector Found!')
        for p in as400_plugins:
            print(f'  - {p["class"]}')
            print(f'    Version: {p.get("version", "unknown")}')
    else:
        print('WARNING: AS400 Connector NOT found in Kafka Connect')
        print('\nAvailable Debezium connectors:')
        debezium = [p for p in plugins if 'debezium' in p.get('class', '').lower()]
        for p in debezium[:5]:
            print(f'  - {p["class"]}')
        
        print('\nTo use As400RpcConnector, you need to install:')
        print('  debezium-connector-ibmi plugin in Kafka Connect')
        
except Exception as e:
    print(f'ERROR checking Kafka Connect: {e}')
