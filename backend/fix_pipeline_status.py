from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, PipelineStatus, CDCStatus

try:
    db = SessionLocal()
    # Find pipeline by name (using like just in case, but specific name is safer if we know it)
    pipeline = db.query(PipelineModel).filter(PipelineModel.name == 'pipeline-feb-11').first()
    
    if pipeline:
        print(f"Found pipeline: {pipeline.name} (ID: {pipeline.id})")
        print(f"Current Status: {pipeline.status}")
        
        # Reset status
        pipeline.status = PipelineStatus.STOPPED
        pipeline.cdc_status = CDCStatus.NOT_STARTED
        
        # Clear specific config to force regeneration with new logic
        # We need to ensure the config is regenerated to use the sanitized slot name
        if pipeline.debezium_config:
            # We can either clear it entirely, or just remove slot.name if we want to be surgical
            # But clearing it is safer to ensure full regeneration
            print(f"Clearing existing Debezium config (was: {pipeline.debezium_config.get('slot.name', 'N/A')})")
            pipeline.debezium_config = None
            
        db.commit()
        print("Pipeline status reset to STOPPED and Debezium config cleared.")
        print("Please restart the pipeline from the UI.")
    else:
        print("Pipeline 'pipeline-feb-11' not found.")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
