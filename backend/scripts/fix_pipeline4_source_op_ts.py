"""
Fix pipeline-4 source connector: use real __op (c/u/d) and __source_ts_ms from envelope.
Remove static addOp transform; add add.fields=op,source.ts_ms to unwrap.
"""
import requests

KAFKA_CONNECT = "http://72.61.233.209:8083"
CONNECTOR = "cdc-pipeline-4-mssql-dbo"

r = requests.get(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/config", timeout=15)
if r.status_code != 200:
    print("Failed to get config:", r.status_code)
    exit(1)
config = r.json()

# Remove static addOp (was forcing __op='c')
config.pop("transforms.addOp.type", None)
config.pop("transforms.addOp.static.field", None)
config.pop("transforms.addOp.static.value", None)
# Single transform: unwrap only
config["transforms"] = "unwrap"
# Add envelope fields so __op and __source_ts_ms come from Debezium
config["transforms.unwrap.add.fields"] = "op,source.ts_ms"

r = requests.put(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/config", json=config, timeout=15)
if r.status_code not in (200, 201):
    print("Failed to update config:", r.status_code, r.text)
    exit(1)
print("Updated source connector: removed static __op='c', added add.fields=op,source.ts_ms")

r = requests.post(f"{KAFKA_CONNECT}/connectors/{CONNECTOR}/restart", timeout=15)
print("Restart:", r.status_code)
print("Done. New CDC events will have correct __op (c/u/d) and __source_ts_ms. Existing rows in target are unchanged.")
