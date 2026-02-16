
import json
import time
from kafka import KafkaConsumer, TopicPartition

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "72.61.233.209:9092"

def check_topics():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            request_timeout_ms=10000
        )
        
        topics = consumer.topics()
        print(f"Total topics found: {len(topics)}")
        
        # Filter for CDC related topics (usually start with pipeline-)
        cdc_topics = [t for t in topics if t.startswith('pipeline-')]
        print(f"CDC topics found: {len(cdc_topics)}")
        
        for topic in cdc_topics:
            print(f"\nChecking topic: {topic}")
            try:
                # Get partitions
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    print("  No partitions found")
                    continue
                
                for p in partitions:
                    tp = TopicPartition(topic, p)
                    # Get end offset
                    end_offsets = consumer.end_offsets([tp])
                    # Get beginning offset
                    beginning_offsets = consumer.beginning_offsets([tp])
                    
                    start = beginning_offsets[tp]
                    end = end_offsets[tp]
                    print(f"  Partition {p}: range {start} - {end} (count: {end - start})")
                    
                    if end > start:
                        # Peek at the last message
                        temp_consumer = KafkaConsumer(
                            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                            auto_offset_reset='earliest',
                            enable_auto_commit=False,
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                            consumer_timeout_ms=2000
                        )
                        temp_consumer.assign([tp])
                        temp_consumer.seek(tp, max(start, end - 1))
                        
                        msgs = next(temp_consumer, None)
                        if msgs:
                            val = msgs.value
                            if val:
                                payload = val.get('payload', val) if isinstance(val, dict) else val
                                op = payload.get('op') if isinstance(payload, dict) else 'unknown'
                                print(f"    Latest message op: {op}")
                                # Print keys of payload to see structure
                                if isinstance(payload, dict):
                                    print(f"    Payload keys: {list(payload.keys())}")
                        temp_consumer.close()
            except Exception as e:
                print(f"  Error checking topic {topic}: {e}")
        
        consumer.close()
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")

if __name__ == "__main__":
    check_topics()
