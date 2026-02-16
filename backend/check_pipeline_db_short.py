from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel

try:
    db = SessionLocal()
    pipelines = db.query(PipelineModel).filter(PipelineModel.name.like('%feb-11%')).all()
    if pipelines:
        for p in pipelines:
            print(f"Pipeline: {p.name}")
            print(f"ID: {p.id}")
            print(f"Status: {p.status}")
            print(f"Debezium Name: {p.debezium_connector_name}")
            print(f"Sink Name: {p.sink_connector_name}")
            print(f"Topics: {p.kafka_topics}")
    else:
        print("No pipelines found.")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
