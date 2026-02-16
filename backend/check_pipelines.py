import psycopg2
import json

conn = psycopg2.connect(host='72.61.233.209', port=5432, database='cdctest', user='cdc_user', password='cdc_pass')
cur = conn.cursor()

# Check pipeline table structure
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = 'pipelines'
    ORDER BY ordinal_position
""")
columns = cur.fetchall()
print('Pipeline table columns:')
for col in columns:
    print(f'  {col[0]}: {col[1]}')

# Check actual data in pipelines table
cur.execute("""
    SELECT id, name, debezium_connector_name, sink_connector_name, 
           kafka_topics, debezium_config, sink_config, cdc_status, status
    FROM pipelines 
    ORDER BY created_at DESC 
    LIMIT 5
""")
rows = cur.fetchall()
colnames = [desc[0] for desc in cur.description]
print(f'\nFound {len(rows)} pipelines:')
for row in rows:
    print(f'\nPipeline: {row[1]}')
    for i, col in enumerate(colnames):
        if col in ['debezium_connector_name', 'sink_connector_name', 'kafka_topics', 'debezium_config', 'sink_config', 'cdc_status', 'status']:
            print(f'  {col}: {row[i]}')

conn.close()


