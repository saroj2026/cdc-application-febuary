"""Script to force consume events from Kafka topics and log them to database."""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.cdc_event_logger import get_event_logger, initialize_event_logger
from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, PipelineStatus as DBPipelineStatus
import time

def force_consume_events():
    """Force the event logger to consume all events from Kafka topics."""
    print("=" * 80)
    print("Force Consume Events from Kafka")
    print("=" * 80)
    print()
    
    # Get event logger
    event_logger = get_event_logger()
    
    if not event_logger:
        print("Event logger not initialized. Initializing...")
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092")
        event_logger = initialize_event_logger(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            db_session_factory=SessionLocal
        )
    
    if not event_logger:
        print("❌ Failed to initialize event logger")
        return
    
    # Get all running pipelines with topics
    db = SessionLocal()
    try:
        running_pipelines = db.query(PipelineModel).filter(
            PipelineModel.status == DBPipelineStatus.RUNNING,
            PipelineModel.kafka_topics.isnot(None),
            PipelineModel.deleted_at.is_(None)
        ).all()
        
        topics = []
        topic_mapping = {}
        
        for pipeline in running_pipelines:
            if pipeline.kafka_topics:
                for topic in pipeline.kafka_topics:
                    topics.append(topic)
                    topic_mapping[topic] = pipeline.id
                    print(f"Found topic: {topic} for pipeline: {pipeline.name} ({pipeline.id})")
        
        if not topics:
            print("❌ No topics found for running pipelines")
            return
        
        print()
        print(f"Subscribing to {len(topics)} topics...")
        print(f"Topics: {topics}")
        print()
        
        # Restart event logger with topics
        if event_logger._running:
            print("Stopping current event logger...")
            event_logger.stop()
            time.sleep(2)
        
        # Start with topics
        event_logger.start(topics=topics, pipeline_mapping=topic_mapping)
        print("✅ Event logger started")
        print()
        print("Consuming events for 30 seconds...")
        print("(Check backend logs for 'Committed X CDC events to database')")
        print()
        
        # Wait for events to be consumed
        time.sleep(30)
        
        # Check status
        is_running = event_logger._running if hasattr(event_logger, '_running') else False
        subscribed_topics = list(event_logger._subscribed_topics) if hasattr(event_logger, '_subscribed_topics') else []
        
        print()
        print("=" * 80)
        print("Event Logger Status")
        print("=" * 80)
        print(f"Running: {is_running}")
        print(f"Subscribed Topics: {len(subscribed_topics)}")
        print(f"Topics: {subscribed_topics}")
        print()
        print("Check backend logs for event consumption details.")
        
    finally:
        db.close()

if __name__ == "__main__":
    force_consume_events()

