
import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import sys
import os
# Add backend to path to import models and session
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from ingestion.database.session import SessionLocal, DATABASE_URL
from ingestion.database.models_db import PipelineModel, PipelineRunModel

print(f"Connecting to: {DATABASE_URL}")
session = SessionLocal()

try:
    print("--- Pipelines ---")
    pipelines = session.query(PipelineModel).all()
    for p in pipelines:
        print(f"ID: {p.id}, Name: {p.name}, Status: {p.status}, Topics: {p.kafka_topics}")

    print("\n--- Recent CDC Events in Database ---")
    events = session.query(PipelineRunModel).filter(PipelineRunModel.run_type == 'CDC').order_by(PipelineRunModel.started_at.desc()).limit(10).all()
    print(f"Total CDC events in DB: {session.query(PipelineRunModel).filter(PipelineRunModel.run_type == 'CDC').count()}")
    for e in events:
        event_type = e.run_metadata.get('event_type') if e.run_metadata else 'unknown'
        print(f"ID: {e.id}, Date: {e.started_at}, PipelineID: {e.pipeline_id}, EventType: {event_type}")

    print("\n--- Event Type Counts (All time) ---")
    from sqlalchemy import func
    event_counts = session.query(
        text("run_metadata->>'event_type' as et"), 
        func.count(PipelineRunModel.id)
    ).filter(PipelineRunModel.run_type == 'CDC').group_by(text("run_metadata->>'event_type'")).all()
    for et, count in event_counts:
        print(f"Type: {et}, Count: {count}")

finally:
    session.close()
