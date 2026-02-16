"""Check pipeline2 status in PostgreSQL public.pipelines."""
import psycopg2
import json

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            id, name, status, mode, 
            full_load_completed_at, cdc_enabled, error_message
        FROM public.pipelines 
        WHERE name = 'pipeline2'
    """)
    
    row = cur.fetchone()
    if row:
        print("Pipeline2 status in public.pipelines:")
        print(f"  ID: {row[0]}")
        print(f"  Name: {row[1]}")
        print(f"  Status: {row[2]}")
        print(f"  Mode: {row[3]}")
        print(f"  Full load completed at: {row[4]}")
        print(f"  CDC enabled: {row[5]}")
        print(f"  Error message: {row[6]}")
    else:
        print("Pipeline2 not found in public.pipelines")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")

print("\n" + "="*70)
print("Checking if backend auto-created the table during full load...")
print("="*70)

# Check backend transfer.py to see if it's supposed to create the table
print("\nThe backend should have:")
print("1. Created dbo.employees table with row_id + CDC metadata columns")
print("2. Inserted data from public.employees with __op='r'")
print("\nLet's check if the transfer.py changes are in place...")
