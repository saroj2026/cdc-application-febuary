"""Check pipelines table schema and pipeline2 data."""
import psycopg2

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    # Get column names
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'pipelines' AND table_schema = 'public' 
        ORDER BY ordinal_position
    """)
    
    cols = [row[0] for row in cur.fetchall()]
    print("Columns in public.pipelines:")
    for col in cols:
        print(f"  - {col}")
    
    # Get pipeline2 data
    print("\nPipeline2 data:")
    cur.execute("SELECT * FROM public.pipelines WHERE name = 'pipeline2'")
    rows = cur.fetchall()
    
    if rows:
        row = rows[0]
        for i, col in enumerate(cols):
            print(f"  {col}: {row[i]}")
    else:
        print("  Pipeline2 not found")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
