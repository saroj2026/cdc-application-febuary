from ingestion.debezium_config import DebeziumConfigGenerator
from ingestion.models import Connection
import json

# Simulate dependencies
class MockConnection:
    host = "localhost"
    port = 5432
    username = "test_user"
    password = "password"
    database_type = "postgresql"
    schema = "public"
    database = "test_db"
    additional_config = {}

mock_conn = MockConnection()

try:
    print("Testing config generation...")
    config = DebeziumConfigGenerator.generate_source_config(
        pipeline_name="pipeline-feb-11",
        source_connection=mock_conn,
        source_database="test_db",
        source_schema="public",
        source_tables=["dpt"],
        full_load_lsn=None,
        snapshot_mode="initial"
    )
    
    print("\nGenerated Config:")
    print(json.dumps(config, indent=2))
    
    slot_name = config.get("slot.name")
    pub_name = config.get("publication.name")
    
    print(f"\nSlot Name: {slot_name}")
    print(f"Publication Name: {pub_name}")
    
    if "-" in slot_name or "-" in pub_name:
        print("FAIL: Hyphens still present!")
    else:
        print("Pass: Names are sanitized.")

except Exception as e:
    print(f"Error: {e}")
