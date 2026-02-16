from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, PipelineStatus, CDCStatus
import datetime

pipeline_name = "pipeline-feb-11"
connector_name = "cdc-pipeline-feb-11-pg-public"

db = SessionLocal()
try:
    pipeline = db.query(PipelineModel).filter(PipelineModel.name == pipeline_name).first()
    if pipeline:
        print(f"Updating pipeline {pipeline.name} status...")
        pipeline.status = PipelineStatus.RUNNING
        pipeline.cdc_status = CDCStatus.RUNNING
        pipeline.debezium_connector_name = connector_name
        # Also ensure sink connector name is set if known, but for now just fix source
        # pipeline.sink_connector_name = ... (leave as is)
        pipeline.updated_at = datetime.datetime.utcnow()
        db.commit()
        print("Pipeline status set to RUNNING.")
    else:
        print("Pipeline not found.")
except Exception as e:
    print(f"Error updating DB: {e}")
finally:
    db.close()
