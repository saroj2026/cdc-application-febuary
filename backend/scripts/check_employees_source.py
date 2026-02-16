"""Check if employees table exists in source PostgreSQL."""
import psycopg2

PG_HOST, PG_PORT, PG_DB = "72.61.233.209", 5432, "cdctest"
PG_USER, PG_PASS = "cdc_user", "cdc_pass"

try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='employees'
    """)
    exists = cur.fetchone()[0]
    print(f"employees table exists: {exists > 0}")
    
    if exists > 0:
        cur.execute("SELECT COUNT(*) FROM public.employees")
        count = cur.fetchone()[0]
        print(f"Row count: {count}")
        
        cur.execute("SELECT * FROM public.employees LIMIT 3")
        print("\nSample rows:")
        for row in cur.fetchall():
            print(f"  {row}")
    else:
        print("\nemployees table does NOT exist in source.")
        print("Creating sample employees table...")
        cur.execute("""
            CREATE TABLE public.employees (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                department_id INT,
                hire_date DATE
            )
        """)
        cur.execute("""
            INSERT INTO public.employees (first_name, last_name, email, department_id, hire_date)
            VALUES 
                ('John', 'Doe', 'john.doe@example.com', 1, '2020-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 2, '2019-03-20'),
                ('Bob', 'Johnson', 'bob.j@example.com', 3, '2021-06-10')
        """)
        conn.commit()
        print("Created employees table with 3 sample rows")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
