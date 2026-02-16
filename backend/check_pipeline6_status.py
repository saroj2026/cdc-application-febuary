import requests
import json

try:
    r = requests.get('http://localhost:8000/api/v1/pipelines/8dc8df67-b081-42f1-a4f2-a932e529ac70', timeout=5)
    p = r.json()
    
    print('=' * 60)
    print('PIPELINE-6 STATUS')
    print('=' * 60)
    print(f'Name: {p.get("name")}')
    print(f'Status: {p.get("status")}')
    print(f'Full load status: {p.get("full_load_status")}')
    print(f'CDC status: {p.get("cdc_status")}')
    print(f'Mode: {p.get("mode")}')
    print(f'Auto-create target: {p.get("auto_create_target")}')
    print(f'\nSource connection: {p.get("source_connection_id")[:8]}...')
    print(f'Source database: {p.get("source_database")}')
    print(f'Source schema: {p.get("source_schema")}')
    print(f'Source tables: {p.get("source_tables")}')
    print(f'\nTarget connection: {p.get("target_connection_id")[:8]}...')
    print(f'Target database: {p.get("target_database")}')
    print(f'Target schema: {p.get("target_schema")}')
    
    if p.get('debezium_connector_name'):
        print(f'\nDebezium connector: {p.get("debezium_connector_name")}')
    if p.get('sink_connector_name'):
        print(f'Sink connector: {p.get("sink_connector_name")}')
    
except Exception as e:
    print(f'ERROR: {e}')
