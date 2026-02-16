
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    print("--- LATEST EVENT DATE ---")
    result = conn.execute(text("SELECT started_at FROM pipeline_runs WHERE run_type = 'CDC' ORDER BY started_at DESC LIMIT 1"))
    row = result.fetchone()
    if row:
        print(f"LATEST_DATE: {row[0]}")
    else:
        print("No CDC events found")

    print("\n--- SAMPLE EVENTS (LAST 5) ---")
    result = conn.execute(text("SELECT id, started_at, pipeline_id, run_metadata FROM pipeline_runs WHERE run_type = 'CDC' ORDER BY started_at DESC LIMIT 5"))
    for row in result:
        print(f"ID: {row[0]}, Date: {row[1]}, Pipeline: {row[2]}, Metadata: {row[3]}")
