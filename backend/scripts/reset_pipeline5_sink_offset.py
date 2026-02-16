"""
Reset pipeline-5 sink connector so it re-reads all messages from Kafka topic.

Scenario: Single insert (id 7) reached Postgres. Bulk 5000 inserts are in Kafka
topic pipeline-5.cdctest.dbo.customer2 but the sink committed offset past them,
so it never wrote them to PostgreSQL.

Fix: Pause sink -> reset consumer group offset to earliest -> resume sink.
Run from a host that can reach the Kafka broker (e.g. same server as Connect).
"""
import os
import sys
import time
import requests
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092")
KAFKA_CONNECT = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
SINK_CONNECTOR = "sink-pipeline-5-pg-public"
TOPIC = "pipeline-5.cdctest.dbo.customer2"
# Kafka Connect uses group id "connect-{connector-name}" for sink connectors
CONSUMER_GROUP = f"connect-{SINK_CONNECTOR}"


def main():
    print("1. Pausing sink connector so we can reset offset...")
    r = requests.put(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/pause", timeout=15)
    if r.status_code not in (200, 202, 204):
        print("   Failed to pause:", r.status_code, r.text[:200])
        return
    print("   Paused (or accepted).")
    time.sleep(3)

    print("2. Resetting consumer group offset to earliest...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
        )
        parts = consumer.partitions_for_topic(TOPIC)
        if not parts:
            print("   Topic not found or empty:", TOPIC)
            consumer.close()
            resume_connector()
            return
        partitions = [TopicPartition(TOPIC, p) for p in parts]
        consumer.assign(partitions)
        consumer.seek_to_beginning(*partitions)
        consumer.commit()
        consumer.close()
        print("   Offset reset to earliest for group", CONSUMER_GROUP)
    except Exception as e:
        if "NoBrokersAvailable" in str(type(e).__name__) or "NoBrokersAvailable" in str(e):
            print("   Kafka broker not reachable from this host.")
            print()
            print("   On the server where Kafka runs, reset offset then resume:")
            print(f"   kafka-consumer-groups.sh --bootstrap-server {KAFKA_BOOTSTRAP} \\")
            print(f"     --group {CONSUMER_GROUP} --topic {TOPIC} \\")
            print("     --reset-offsets --to-earliest --execute")
            print(f"   curl -X PUT {KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/resume")
        else:
            print("   Error:", e)
        resume_connector()
        sys.exit(1)

    print("3. Restarting sink connector...")
    resume_connector()
    print("   Done. Sink will re-consume from start and write to PostgreSQL.")
    print("   Check: SELECT COUNT(*) FROM public.customer2;")


def resume_connector():
    r = requests.put(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/resume", timeout=15)
    if r.status_code not in (200, 202, 204):
        print("   Resume failed:", r.status_code, r.text[:200])


if __name__ == "__main__":
    main()
