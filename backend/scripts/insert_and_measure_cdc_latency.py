"""
Insert N rows into SQL Server dbo.customer2, then measure how long until they
appear in Postgres public.customer2 (pipeline-5 CDC). Use 1000 or 10000 to see
how time scales.

Usage (from backend dir):
  python scripts/insert_and_measure_cdc_latency.py 100
  python scripts/insert_and_measure_cdc_latency.py 1000
  python scripts/insert_and_measure_cdc_latency.py 10000
"""
import sys
import time
import pyodbc
import psycopg2

# Same connection string format as get_customers_columns.py, create_customer2_and_pipeline5.py, etc.
SQL_SERVER_CONN = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=72.61.233.209,1433;DATABASE=cdctest;UID=SA;PWD=Sql@12345;Encrypt=no;TrustServerCertificate=yes"
PG_CONN = "host=72.61.233.209 port=5432 dbname=cdctest user=cdc_user password=cdc_pass"


def get_pg_count():
    with psycopg2.connect(PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.customer2")
            return cur.fetchone()[0]


def insert_sql_server(n: int):
    sql = f"""
DECLARE @start_id INT;
SELECT @start_id = ISNULL(MAX(customer_id), 0) FROM dbo.customer2;
;WITH numbers AS (
    SELECT TOP ({n})
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS num
    FROM sys.objects a
    CROSS JOIN sys.objects b
)
INSERT INTO dbo.customer2
(customer_id, first_name, last_name, email, phone, balance, registration_date, is_premium)
SELECT
    @start_id + num,
    CONCAT('First', num),
    CONCAT('Last', num),
    CONCAT('user', @start_id + num, '@example.com'),
    CONCAT('9', RIGHT('000000000' + CAST(num AS VARCHAR), 9)),
    ROUND(RAND(CHECKSUM(NEWID())) * 1000, 2),
    DATEADD(DAY, -num, GETDATE()),
    CASE WHEN num % 2 = 0 THEN 1 ELSE 0 END
FROM numbers;
"""
    with pyodbc.connect(SQL_SERVER_CONN) as conn:
        conn.execute(sql)
        conn.commit()


def main():
    n = 1000
    if len(sys.argv) >= 2:
        try:
            n = int(sys.argv[1])
        except ValueError:
            n = 1000
    if n < 1 or n > 100000:
        n = 1000

    print(f"Target: insert {n} rows into SQL Server dbo.customer2, then measure time until Postgres public.customer2 count increases by {n}.")
    print()

    # Initial Postgres count
    try:
        count_before = get_pg_count()
    except Exception as e:
        print(f"Postgres error: {e}")
        sys.exit(1)
    print(f"Postgres count before insert: {count_before}")
    expected_after = count_before + n

    # Insert into SQL Server and record time
    print(f"Inserting {n} rows into SQL Server...")
    t0 = time.perf_counter()
    try:
        insert_sql_server(n)
    except Exception as e:
        print(f"SQL Server insert error: {e}")
        sys.exit(1)
    t_insert = time.perf_counter() - t0
    print(f"Insert completed in {t_insert:.2f} s.")
    print(f"Waiting for Postgres count to reach {expected_after}...")

    # Poll Postgres until count reaches expected or timeout (e.g. 120 s)
    t_start_wait = time.perf_counter()
    timeout = 120
    poll_interval = 2
    last_count = count_before
    while True:
        elapsed = time.perf_counter() - t_start_wait
        if elapsed >= timeout:
            print(f"Timeout after {timeout} s. Last count: {last_count}, expected: {expected_after}")
            break
        time.sleep(poll_interval)
        try:
            last_count = get_pg_count()
        except Exception as e:
            print(f"Postgres poll error: {e}")
            continue
        if last_count >= expected_after:
            t_done = time.perf_counter() - t_start_wait
            print(f"Postgres count reached {last_count} in {t_done:.2f} s after insert.")
            print()
            print(f"--- Result: {n} rows took ~{t_done:.1f} s to appear in Postgres (end-to-end CDC latency).")
            break
        print(f"  {elapsed:.0f} s: count = {last_count} (need {expected_after})")


if __name__ == "__main__":
    main()
