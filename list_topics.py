
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "72.61.233.209:9092"

def list_topics():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            request_timeout_ms=5000
        )
        topics = consumer.topics()
        print(f"Total topics: {len(topics)}")
        for t in sorted(topics):
            print(f" - {t}")
        consumer.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_topics()
