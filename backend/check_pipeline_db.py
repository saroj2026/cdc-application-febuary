from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel

try:
    db = SessionLocal()
    pipelines = db.query(PipelineModel).filter(PipelineModel.name.like('%feb-11%')).all()
    if pipelines:
        for p in pipelines:
            print(f"--- Pipeline {p.name} ---")
            print(f"Pipeline ID: {p.id}")
            print(f"Status: {p.status}")
            print(f"Debezium Connector Name: {p.debezium_connector_name}")
            print(f"Debezium Config: {p.debezium_config}")
            print(f"Sink Connector Name: {p.sink_connector_name}")
            print(f"Sink Config: {p.sink_config}")
            print(f"Kafka Topics: {p.kafka_topics}")
    else:
        print("No pipelines found with 'feb-11' in name.")
        # List all to be sure
        all_p = db.query(PipelineModel).limit(5).all()
        print("First 5 pipelines:")
        for p in all_p:
            print(f"- {p.name}")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
