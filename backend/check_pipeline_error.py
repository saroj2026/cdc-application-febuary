from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel
import json

try:
    db = SessionLocal()
    pipelines = db.query(PipelineModel).filter(PipelineModel.name.like('%feb-11%')).all()
    if pipelines:
        for p in pipelines:
             config = p.debezium_config or {}
             if '_last_error' in config:
                 print("LAST ERROR FOUND:")
                 print(json.dumps(config['_last_error'], indent=2))
             else:
                 print("No _last_error in debezium_config")
    else:
        print("No pipelines found.")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
