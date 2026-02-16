import psycopg2
import requests
import json
import time

# Check PostgreSQL replication slot
conn = psycopg2.connect(host='72.61.233.209', port=5432, database='cdctest', user='cdc_user', password='cdc_pass')
cur = conn.cursor()
cur.execute("SELECT slot_name, slot_type, active FROM pg_replication_slots WHERE slot_name LIKE '%pipeline%'")
slots = cur.fetchall()
print(f'Replication slots: {slots}')
conn.close()

# Wait a bit for snapshot to complete
print('Waiting 30 seconds for snapshot...')
time.sleep(30)

# Check connector status
status = requests.get('http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/status').json()
print(f'Connector status: {json.dumps(status, indent=2)}')

# Check topics
topics = requests.get('http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/topics').json()
print(f'Topics: {json.dumps(topics, indent=2)}')


