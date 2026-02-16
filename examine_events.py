
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineRunModel

session = SessionLocal()
try:
    runs = session.query(PipelineRunModel).filter(PipelineRunModel.run_type == 'CDC').all()
    print(f"Total CDC runs: {len(runs)}")
    for r in runs:
        print(f"ID: {r.id}")
        print(f"  Pipeline ID: {r.pipeline_id}")
        print(f"  Status: {r.status}")
        print(f"  Metadata: {r.run_metadata}")
        print("-" * 20)
finally:
    session.close()
