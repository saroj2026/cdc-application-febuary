"""
Fix pipeline-4 sink connector to consume from the correct topic.
Source (Debezium SQL Server) writes to: pipeline-4.cdctest.dbo.customers
Sink must consume from that topic, not pipeline-4.dbo.customers.
"""
import requests

KAFKA_CONNECT = "http://72.61.233.209:8083"
SINK_CONNECTOR = "sink-pipeline-4-pg-public"
CORRECT_TOPIC = "pipeline-4.cdctest.dbo.customers"

r = requests.get(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/config", timeout=15)
if r.status_code != 200:
    print("Failed to get sink config:", r.status_code)
    exit(1)
config = r.json()
current_topics = config.get("topics", "")
print("Current sink topics:", current_topics)

if CORRECT_TOPIC in current_topics and current_topics.strip() == CORRECT_TOPIC:
    print("Sink already configured for correct topic.")
    exit(0)

config["topics"] = CORRECT_TOPIC
r = requests.put(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/config", json=config, timeout=15)
if r.status_code not in (200, 201):
    print("Failed to update config:", r.status_code, r.text)
    exit(1)
print("Updated sink config: topics =", CORRECT_TOPIC)

r = requests.post(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/restart", timeout=15)
print("Sink restart:", r.status_code)
print("Done. Sink will now consume from", CORRECT_TOPIC)
