
from sqlalchemy import create_engine, text
import json

DATABASE_URL = "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    print("--- PIPELINE INFO ---")
    result = conn.execute(text("SELECT id, name, status, kafka_topics FROM pipelines WHERE name = 'pipeline-feb-11'"))
    row = result.fetchone()
    if row:
        print(f"ID: {row[0]}")
        print(f"Name: {row[1]}")
        print(f"Status: {row[2]}")
        print(f"Kafka Topics: {row[3]}")
    else:
        print("Pipeline 'pipeline-feb-11' not found")

    print("\n--- CDC EVENTS SAMPLE ---")
    result = conn.execute(text("SELECT id, run_metadata FROM pipeline_runs WHERE pipeline_id = 'b8fa80ef-dd00-4635-82f5-6ea499c646cf' AND run_type = 'CDC' LIMIT 5"))
    for row in result:
        print(f"ID: {row[0]}, Metadata: {row[1]}")

    print("\n--- CDC EVENTS COUNT ---")
    result = conn.execute(text("SELECT count(*) FROM pipeline_runs WHERE pipeline_id = 'b8fa80ef-dd00-4635-82f5-6ea499c646cf' AND run_type = 'CDC'"))
    print(f"Total CDC events: {result.scalar()}")
