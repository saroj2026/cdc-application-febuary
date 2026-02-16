
import json
import socket
import time
from kafka import KafkaConsumer, TopicPartition

# Monkeypatch socket.getaddrinfo to resolve 'kafka' to '72.61.233.209'
original_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(host, port, *args, **kwargs):
    if host == 'kafka':
        return original_getaddrinfo('72.61.233.209', port, *args, **kwargs)
    return original_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = patched_getaddrinfo

KAFKA_BOOTSTRAP_SERVERS = "72.61.233.209:9092"

def peek_messages():
    topic = 'pipeline-7.cdctest.dbo.Orders'
    print(f"Peeking into topic: {topic}")
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            consumer_timeout_ms=5000
        )
        
        # Manually assign partition 0
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        
        # Seek to beginning
        consumer.seek_to_beginning()
        
        count = 0
        for message in consumer:
            print(f"Message {count} at offset {message.offset}:")
            val = message.value
            print(f"  Raw value: {json.dumps(val, indent=2)}")
            if val:
                payload = val.get('payload', val) if isinstance(val, dict) else val
                if isinstance(payload, dict):
                    print(f"  Op: {payload.get('op')}")
                    print(f"  Source: {payload.get('source', {}).get('table')}")
                else:
                    print(f"  Value: {val}")
            print("-" * 20)
            count += 1
            if count >= 2: # Just check 2 messages
                break
        
        if count == 0:
            print("No messages found or timed out")
            
        consumer.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    peek_messages()
