"""Force clear stale error from database."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel

pipeline_id = "b8fa80ef-dd00-4635-82f5-6ea499c646cf"

db = SessionLocal()
try:
    p = db.query(PipelineModel).filter(PipelineModel.id == pipeline_id).first()
    if p:
        print(f"Before: {p.debezium_config.get('_last_error') if p.debezium_config else None}")
        if p.debezium_config and '_last_error' in p.debezium_config:
            # Create a new dict without _last_error
            new_config = {k: v for k, v in p.debezium_config.items() if k != '_last_error'}
            p.debezium_config = new_config
            db.commit()
            db.refresh(p)  # Refresh to get updated data
            print(f"After: {p.debezium_config.get('_last_error') if p.debezium_config else None}")
            print("✅ Cleared from database")
        else:
            print("No _last_error found in debezium_config")
    else:
        print("Pipeline not found")
finally:
    db.close()

