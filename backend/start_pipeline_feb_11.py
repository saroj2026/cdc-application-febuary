from ingestion.cdc_manager import CDCManager
from ingestion.database import get_db
from ingestion.cdc_manager import CDCManager, set_db_session_factory
import os
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)

# Initialize components same as api.py
kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
manager = CDCManager(kafka_connect_url=kafka_connect_url)
set_db_session_factory(get_db)

pipeline_id = "b8fa80ef-dd00-4635-82f5-6ea499c646cf" 

try:
    print(f"Attempting to start pipeline {pipeline_id}...")
    
    # Load pipeline into manager's store
    pipeline = manager._load_pipeline_from_db(pipeline_id)
    if pipeline:
        print(f"Loaded pipeline: {pipeline.name}")
        manager.pipeline_store[pipeline_id] = pipeline
        
        # Start the pipeline
        result = manager.start_pipeline(pipeline_id)
        print(f"Start result: {result}")
    else:
        print("Pipeline not found in DB.")
except Exception as e:
    print(f"Error starting pipeline: {e}")
