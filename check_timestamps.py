
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineRunModel

session = SessionLocal()
try:
    runs = session.query(PipelineRunModel).filter(PipelineRunModel.run_type == 'CDC').all()
    for r in runs:
        print(f"ID: {r.id}, Topic: {r.run_metadata.get('topic')}, Time: {r.started_at}")
finally:
    session.close()
