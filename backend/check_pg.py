import psycopg2

# Connect as cdc_user first to check current state
conn = psycopg2.connect(
    host='72.61.233.209',
    port=5432,
    database='cdctest',
    user='cdc_user',
    password='cdc_pass'
)
conn.autocommit = True
cur = conn.cursor()

# Check if user has replication role
cur.execute("SELECT rolreplication FROM pg_roles WHERE rolname = 'cdc_user'")
result = cur.fetchone()
print(f'cdc_user has REPLICATION: {result[0] if result else "unknown"}')

# Check wal_level
cur.execute('SHOW wal_level')
result = cur.fetchone()
print(f'wal_level: {result[0]}')

# Check replication slots
cur.execute('SELECT slot_name, slot_type, active FROM pg_replication_slots')
slots = cur.fetchall()
print(f'Replication slots: {slots}')

# Check publications
cur.execute('SELECT pubname FROM pg_publication')
pubs = cur.fetchall()
print(f'Publications: {pubs}')

# Check if department table exists
cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'department')")
result = cur.fetchone()
print(f'department table exists: {result[0]}')

# Check current permissions on department table
cur.execute("""
    SELECT privilege_type 
    FROM information_schema.role_table_grants 
    WHERE table_name = 'department' AND grantee = 'cdc_user'
""")
perms = cur.fetchall()
print(f'cdc_user permissions on department: {[p[0] for p in perms]}')

conn.close()
print('Connection test completed')


