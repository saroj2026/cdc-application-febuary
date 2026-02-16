
import json
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP_SERVERS = "72.61.233.209:9092"

def fast_check():
    topics = [
        'pipeline-7.cdctest.dbo.Orders',
        'pipeline-4.cdctest.dbo.customers',
        'pipeline-8.cdctest.dbo.Orders'
    ]
    
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        request_timeout_ms=5000
    )
    
    for topic in topics:
        print(f"\nTopic: {topic}")
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                print("  No partitions")
                continue
            
            for p in partitions:
                tp = TopicPartition(topic, p)
                try:
                    # Get end offset - this is the most important
                    end = consumer.end_offsets([tp])[tp]
                    start = consumer.beginning_offsets([tp])[tp]
                    print(f"  Partition {p}: range {start}-{end} (count: {end-start})")
                except Exception as e:
                    print(f"  Error getting offsets for {p}: {e}")
        except Exception as e:
            print(f"  Error: {e}")
    consumer.close()

if __name__ == "__main__":
    fast_check()
