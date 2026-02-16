
import logging
from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_pipeline():
    session = SessionLocal()
    try:
        # Check pipeline-5 (using name or looking for it)
        # Assuming ID might be related to "pipeline-5" but usually they are UUIDs
        # Search by name "pipeline-5"
        pipeline = session.query(PipelineModel).filter(PipelineModel.name == "pipeline-5").first()
        
        if pipeline:
            logger.info(f"Found pipeline: {pipeline.name} ({pipeline.id})")
            logger.info(f"Status: {pipeline.status}")
            logger.info(f"CDC Status: {pipeline.cdc_status}")
            logger.info(f"Kafka Topics: {pipeline.kafka_topics}")
            logger.info(f"Debezium Connector: {pipeline.debezium_connector_name}")
        else:
            logger.info("Pipeline 'pipeline-5' not found by name.")
            
            # List all pipelines
            pipelines = session.query(PipelineModel).all()
            for p in pipelines:
                logger.info(f"Available: {p.name} ({p.status}) - Topics: {p.kafka_topics}")

    finally:
        session.close()

if __name__ == "__main__":
    check_pipeline()
