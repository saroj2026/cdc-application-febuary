"""Script to force fix CDC by deleting connector and clearing errors."""

import sys
import os
import requests

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.database.session import get_db
from ingestion.database.models_db import PipelineModel
from ingestion.kafka_connect_client import KafkaConnectClient

def force_fix_cdc(pipeline_id: str, kafka_connect_url: str = "http://72.61.233.209:8083"):
    """Force fix CDC by deleting connector and clearing errors.
    
    Args:
        pipeline_id: Pipeline ID
        kafka_connect_url: Kafka Connect REST API URL
    """
    db = next(get_db())
    
    try:
        # Get pipeline from database
        pipeline = db.query(PipelineModel).filter(
            PipelineModel.id == pipeline_id,
            PipelineModel.deleted_at.is_(None)
        ).first()
        
        if not pipeline:
            print(f"❌ Pipeline not found: {pipeline_id}")
            return False
        
        print(f"📋 Pipeline: {pipeline.name} ({pipeline_id})")
        print(f"   Current CDC status: {pipeline.cdc_status}")
        print(f"   Debezium connector: {pipeline.debezium_connector_name}")
        
        # Delete connector if it exists
        if pipeline.debezium_connector_name:
            kafka_client = KafkaConnectClient(kafka_connect_url)
            try:
                # Check if connector exists
                status = kafka_client.get_connector_status(pipeline.debezium_connector_name)
                if status:
                    print(f"🗑️  Deleting connector: {pipeline.debezium_connector_name}")
                    kafka_client.delete_connector(pipeline.debezium_connector_name)
                    print(f"✅ Deleted connector: {pipeline.debezium_connector_name}")
                else:
                    print(f"ℹ️  Connector doesn't exist: {pipeline.debezium_connector_name}")
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 404:
                    print(f"ℹ️  Connector doesn't exist: {pipeline.debezium_connector_name}")
                else:
                    print(f"⚠️  Could not delete connector: {e}")
            except Exception as e:
                print(f"⚠️  Error checking/deleting connector: {e}")
        
        # Clear error from debezium_config
        if pipeline.debezium_config and '_last_error' in pipeline.debezium_config:
            print(f"🧹 Clearing error from debezium_config...")
            error_config = pipeline.debezium_config.copy()
            if '_last_error' in error_config:
                del error_config['_last_error']
            pipeline.debezium_config = error_config
            print(f"✅ Cleared error from debezium_config")
        
        # Reset CDC status to NOT_STARTED
        from ingestion.database.models_db import CDCStatus
        pipeline.cdc_status = CDCStatus.NOT_STARTED
        pipeline.debezium_connector_name = None
        
        db.commit()
        print(f"✅ Reset CDC status to NOT_STARTED")
        print(f"✅ Pipeline is ready to be restarted")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python force_fix_cdc.py <pipeline_id> [kafka_connect_url]")
        print("Example: python force_fix_cdc.py b8fa80ef-dd00-4635-82f5-6ea499c646cf")
        sys.exit(1)
    
    pipeline_id = sys.argv[1]
    kafka_connect_url = sys.argv[2] if len(sys.argv) > 2 else "http://72.61.233.209:8083"
    
    force_fix_cdc(pipeline_id, kafka_connect_url)

