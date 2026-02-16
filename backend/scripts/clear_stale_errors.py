"""Clear stale error messages with old Kafka Connect URL (port 8080) from database."""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, PipelineRunModel
import json

def clear_stale_errors():
    """Clear stale error messages containing old URL (8080) from database."""
    db = SessionLocal()
    try:
        # Clear errors from PipelineRunModel
        runs_updated = 0
        # Get all runs for the specific pipeline first
        pipeline_id = "b8fa80ef-dd00-4635-82f5-6ea499c646cf"  # pipeline-feb-11
        runs = db.query(PipelineRunModel).filter(
            PipelineRunModel.pipeline_id == pipeline_id,
            PipelineRunModel.error_message.isnot(None),
            PipelineRunModel.error_message != ''
        ).all()
        
        print(f"Found {len(runs)} runs with error messages for pipeline {pipeline_id}")
        
        for run in runs:
            error_msg = str(run.error_message)
            if ":8080" in error_msg and ":8083" not in error_msg:
                print(f"Clearing stale error from run {run.id} (started: {run.started_at}): {error_msg[:100]}...")
                run.error_message = None
                runs_updated += 1
            else:
                print(f"Keeping run {run.id} error (doesn't contain old URL): {error_msg[:80]}...")
        
        # Clear errors from PipelineModel.debezium_config
        pipelines_updated = 0
        pipelines = db.query(PipelineModel).filter(
            PipelineModel.debezium_config.isnot(None)
        ).all()
        
        for pipeline in pipelines:
            updated = False
            if pipeline.debezium_config and isinstance(pipeline.debezium_config, dict):
                if '_last_error' in pipeline.debezium_config:
                    error = pipeline.debezium_config.get('_last_error')
                    if isinstance(error, dict):
                        error_msg = error.get('message', '')
                        if ":8080" in str(error_msg) and ":8083" not in str(error_msg):
                            print(f"Clearing stale error from pipeline {pipeline.name} (debezium_config): {str(error_msg)[:100]}...")
                            del pipeline.debezium_config['_last_error']
                            updated = True
            
            if pipeline.sink_config and isinstance(pipeline.sink_config, dict):
                if '_last_error' in pipeline.sink_config:
                    error = pipeline.sink_config.get('_last_error')
                    if isinstance(error, dict):
                        error_msg = error.get('message', '')
                        if ":8080" in str(error_msg) and ":8083" not in str(error_msg):
                            print(f"Clearing stale error from pipeline {pipeline.name} (sink_config): {str(error_msg)[:100]}...")
                            del pipeline.sink_config['_last_error']
                            updated = True
            
            if updated:
                pipelines_updated += 1
        
        db.commit()
        print(f"\n✅ Cleared stale errors:")
        print(f"   - {runs_updated} pipeline runs")
        print(f"   - {pipelines_updated} pipelines")
        print(f"\nThe old error messages with port 8080 have been removed from the database.")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error clearing stale errors: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    clear_stale_errors()

