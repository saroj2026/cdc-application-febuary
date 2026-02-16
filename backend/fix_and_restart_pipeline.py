import requests
import json
from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, PipelineStatus, CDCStatus
from ingestion.cdc_manager import CDCManager
from ingestion.database import get_db
from ingestion.cdc_manager import CDCManager, set_db_session_factory
import os
import time

# Configuration
connector_name = "cdc-pipeline-feb-11-pg-public"
connect_url = "http://72.61.233.209:8083"
pipeline_id = "b8fa80ef-dd00-4635-82f5-6ea499c646cf"

# 1. DELETE existing failed connector
print(f"Deleting connector {connector_name}...")
try:
    response = requests.delete(f"{connect_url}/connectors/{connector_name}")
    if response.status_code in [204, 200, 404]:
        print(f"Connector deleted (status {response.status_code})")
    else:
        print(f"Failed to delete connector: {response.status_code} - {response.text}")
except Exception as e:
    print(f"Error deleting connector: {e}")

# 2. RESET Pipeline Status in DB
print("Resetting pipeline status in DB...")
try:
    db = SessionLocal()
    pipeline = db.query(PipelineModel).filter(PipelineModel.id == pipeline_id).first()
    if pipeline:
        pipeline.status = PipelineStatus.STOPPED
        pipeline.cdc_status = CDCStatus.NOT_STARTED
        pipeline.debezium_config = None # Clear config to force regeneration
        db.commit()
        print("Pipeline status reset.")
    else:
        print("Pipeline not found in DB.")
    db.close()
except Exception as e:
    print(f"Error resetting DB: {e}")

# 3. START Pipeline
print("Starting pipeline...")
try:
    # Setup manager
    kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
    manager = CDCManager(kafka_connect_url=kafka_connect_url)
    set_db_session_factory(get_db)
    
    # Load and start
    pipeline = manager._load_pipeline_from_db(pipeline_id)
    if pipeline:
        manager.pipeline_store[pipeline_id] = pipeline
        result = manager.start_pipeline(pipeline_id)
        print("Pipeline start result:", result)
    else:
        print("Failed to load pipeline for start.")
except Exception as e:
    print(f"Error starting pipeline: {e}")
