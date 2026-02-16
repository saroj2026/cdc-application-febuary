"""Script to check if CDC events are being logged to the database."""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineRunModel, PipelineModel
from sqlalchemy import func, desc
from datetime import datetime, timedelta

def check_events():
    """Check CDC events in the database."""
    db = SessionLocal()
    try:
        # Get all pipelines
        pipelines = db.query(PipelineModel).filter(
            PipelineModel.deleted_at.is_(None)
        ).all()
        
        print("=" * 80)
        print("CDC Events Diagnostic Report")
        print("=" * 80)
        print()
        
        # Check total CDC events
        total_cdc_events = db.query(func.count(PipelineRunModel.id)).filter(
            PipelineRunModel.run_type == 'CDC'
        ).scalar() or 0
        
        print(f"Total CDC events in database (run_type='CDC'): {total_cdc_events}")
        print()
        
        # Check events by pipeline
        for pipeline in pipelines:
            pipeline_id = pipeline.id
            pipeline_name = pipeline.name
            
            # Count CDC events for this pipeline
            event_count = db.query(func.count(PipelineRunModel.id)).filter(
                PipelineRunModel.pipeline_id == pipeline_id,
                PipelineRunModel.run_type == 'CDC'
            ).scalar() or 0
            
            # Get event type breakdown
            events = db.query(PipelineRunModel).filter(
                PipelineRunModel.pipeline_id == pipeline_id,
                PipelineRunModel.run_type == 'CDC'
            ).order_by(desc(PipelineRunModel.started_at)).limit(10).all()
            
            event_types = {}
            for event in events:
                if event.run_metadata and isinstance(event.run_metadata, dict):
                    et = event.run_metadata.get('event_type', 'unknown')
                    event_types[et] = event_types.get(et, 0) + 1
            
            print(f"Pipeline: {pipeline_name} (ID: {pipeline_id})")
            print(f"  Status: {pipeline.status}")
            print(f"  CDC Status: {pipeline.cdc_status}")
            print(f"  Kafka Topics: {pipeline.kafka_topics}")
            print(f"  CDC Events Count: {event_count}")
            if event_types:
                print(f"  Event Types: {event_types}")
            else:
                print(f"  Event Types: None (no events found)")
            print()
        
        # Check recent events (last 24 hours)
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_events = db.query(PipelineRunModel).filter(
            PipelineRunModel.run_type == 'CDC',
            PipelineRunModel.started_at >= yesterday
        ).order_by(desc(PipelineRunModel.started_at)).limit(20).all()
        
        print("=" * 80)
        print(f"Recent CDC Events (last 24 hours): {len(recent_events)}")
        print("=" * 80)
        for event in recent_events[:10]:
            event_type = 'unknown'
            table_name = 'unknown'
            if event.run_metadata and isinstance(event.run_metadata, dict):
                event_type = event.run_metadata.get('event_type', 'unknown')
                table_name = event.run_metadata.get('table_name', 'unknown')
            
            print(f"  {event.started_at} | Pipeline: {event.pipeline_id} | Type: {event_type} | Table: {table_name} | Status: {event.status}")
        
        print()
        print("=" * 80)
        print("Event Logger Status Check")
        print("=" * 80)
        print("Check event logger status via API: GET /api/v1/monitoring/event-logger-status")
        print()
        
    finally:
        db.close()

if __name__ == "__main__":
    check_events()

