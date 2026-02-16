import requests
import json
import time
from ingestion.debezium_config import DebeziumConfigGenerator

# Configuration
connect_url = "http://72.61.233.209:8083"
pipeline_name = "pipeline-feb-11"
schema = "public"
connector_name = "cdc-pipeline-feb-11-pg-public"

# Mock connection for config generation
class MockConnection:
    host = "72.61.233.209" # Using actual IP
    port = 5432
    username = "cdc_user" # Assuming user
    password = "cdc_pass" # Assuming pass, will be replaced by user actual checked pass if needed, but Debezium needs actual pass.
    # We must use actual credentials from the database to be successful!
    # But for this script I'll use placeholders and let the user fill if they fail, 
    # OR better: load from DB.
    database_type = "postgresql"
    schema = "public"
    database = "cdctest"
    additional_config = {}

# Load actual connection from DB to get password
from ingestion.database.session import SessionLocal
from ingestion.database.models_db import PipelineModel, ConnectionModel

db = SessionLocal()
pipeline = db.query(PipelineModel).filter(PipelineModel.name == pipeline_name).first()
if not pipeline:
    print("Pipeline not found!")
    exit(1)

source_conn = db.query(ConnectionModel).filter(ConnectionModel.id == pipeline.source_connection_id).first()
db.close()

if not source_conn:
    print("Source connection not found!")
    exit(1)

print(f"Loaded connection: {source_conn.host}:{source_conn.port} ({source_conn.username})")

# Generate Config
print("Generating config...")
debezium_config = DebeziumConfigGenerator.generate_source_config(
    pipeline_name=pipeline_name,
    source_connection=source_conn, # Pass actual connection model object (wrapper needed?)
    # DebeziumConfigGenerator expects 'Connection' object from models.py, NOT sqlalchemy model
    # So we need to wrap it.
    source_database=pipeline.source_database,
    source_schema=pipeline.source_schema or "public",
    source_tables=pipeline.source_tables,
    full_load_lsn=None,
    snapshot_mode="initial"
)

# Convert Config to 'Connection' class structure expected by Generator?
# Wait, I passed SqlAlchemy model to generate_source_config?
# DebeziumConfigGenerator expects `ingestion.models.Connection`
# The SqlAlchemy model `ConnectionModel` has similar fields but `to_dict` might be missing or different.
# Let's map it quickly.
from ingestion.models import Connection
connection_obj = Connection(
    id=source_conn.id,
    name=source_conn.name,
    connection_type=str(source_conn.connection_type),
    database_type=source_conn.database_type.value if hasattr(source_conn.database_type, 'value') else str(source_conn.database_type),
    host=source_conn.host,
    port=source_conn.port,
    database=source_conn.database,
    username=source_conn.username,
    password=source_conn.password,
    schema=source_conn.schema,
    additional_config=source_conn.additional_config
)

debezium_config = DebeziumConfigGenerator.generate_source_config(
    pipeline_name=pipeline_name,
    source_connection=connection_obj,
    source_database=pipeline.source_database,
    source_schema=pipeline.source_schema or "public",
    source_tables=pipeline.source_tables,
    full_load_lsn=None,
    snapshot_mode="initial"
)

print(f"Generated Config for {connector_name}:")
print(f"Slot: {debezium_config.get('slot.name')}")
print(f"Pub: {debezium_config.get('publication.name')}")

# Add Connector Name to config for validation call if needed (but create uses payload)
payload = {
    "name": connector_name,
    "config": debezium_config
}

# 1. DELETE
print(f"Deleting {connector_name}...")
requests.delete(f"{connect_url}/connectors/{connector_name}")
time.sleep(2)

# 2. CREATE
print(f"Creating {connector_name}...")
resp = requests.post(
    f"{connect_url}/connectors",
    json=payload,
    headers={"Content-Type": "application/json"}
)

if resp.status_code in [200, 201]:
    print("Creation SUCCESS!")
    print(json.dumps(resp.json(), indent=2))
else:
    print(f"Creation FAILED: {resp.status_code}")
    print(resp.text)
    exit(1)

# 3. POLL STATUS
print("Waiting for status...")
for i in range(10):
    time.sleep(2)
    resp = requests.get(f"{connect_url}/connectors/{connector_name}/status")
    if resp.status_code == 200:
        status = resp.json()
        state = status.get('connector', {}).get('state')
        print(f"State: {state}")
        if state == "RUNNING":
            print(" connector is RUNNING!")
            break
        elif state == "FAILED":
            print(" connector FAILED!")
            print(json.dumps(status, indent=2))
            break
    else:
        print(f"Status check failed: {resp.status_code}")
