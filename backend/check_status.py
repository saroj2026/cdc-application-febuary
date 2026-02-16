import requests
import json
import psycopg2
import time

# Wait a bit more
print('Waiting 15 seconds...')
time.sleep(15)

# Check connector status again
status = requests.get('http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/status').json()
print(f'Connector status: {json.dumps(status, indent=2)}')

# Check PostgreSQL for replication slot
conn = psycopg2.connect(
    host='72.61.233.209',
    port=5432,
    database='cdctest',
    user='cdc_user',
    password='cdc_pass'
)
cur = conn.cursor()
cur.execute("SELECT slot_name, slot_type, active FROM pg_replication_slots WHERE slot_name LIKE '%pipeline%'")
slots = cur.fetchall()
print(f'Pipeline slots: {slots}')
conn.close()

# Check if task exists
try:
    task_status = requests.get('http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/tasks/0/status').json()
    print(f'Task 0 status: {json.dumps(task_status, indent=2)}')
except Exception as e:
    print(f'Task 0 error: {e}')

# Check topics
topics = requests.get('http://72.61.233.209:8083/connectors/cdc-pipeline-1-pg-public/topics').json()
print(f'Topics: {json.dumps(topics, indent=2)}')


