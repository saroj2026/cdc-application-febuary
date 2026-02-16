import psycopg2

conn = psycopg2.connect(
    host='72.61.233.209',
    port=5432,
    database='cdctest',
    user='cdc_user',
    password='cdc_pass'
)
conn.autocommit = True
cur = conn.cursor()

# Drop old slots that might conflict
slots_to_drop = ['pipeline_1_slot', 'testconn2_slot', 'testconn_slot']
for slot in slots_to_drop:
    try:
        cur.execute(f"SELECT pg_drop_replication_slot('{slot}')")
        print(f'Dropped slot: {slot}')
    except Exception as e:
        print(f'Slot {slot} does not exist or already dropped: {e}')

# Create publication for department table if it doesn't exist
pub_name = 'pipeline_1_pub'
try:
    cur.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
    print(f'Dropped publication: {pub_name}')
except Exception as e:
    print(f'Could not drop publication: {e}')

try:
    cur.execute(f"CREATE PUBLICATION {pub_name} FOR TABLE public.department")
    print(f'Created publication: {pub_name}')
except Exception as e:
    print(f'Publication error: {e}')

# Verify
cur.execute("SELECT pubname FROM pg_publication WHERE pubname = 'pipeline_1_pub'")
result = cur.fetchone()
print(f'Publication exists: {result is not None}')

cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'pipeline_1_slot'")
result = cur.fetchone()
print(f'Slot exists: {result is not None}')

conn.close()
print('Setup completed')


