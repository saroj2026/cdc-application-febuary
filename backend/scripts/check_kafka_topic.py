"""
Check if CDC events exist in Kafka topic.
Uses kafka-python library to consume last N messages from the topic.
"""
try:
    from kafka import KafkaConsumer
    import json
    
    KAFKA_BOOTSTRAP = "72.61.233.209:9092"
    TOPIC = "pipeline-1.public.department"
    
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print("="*70)
    
    # Create consumer starting from beginning
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    messages = []
    for msg in consumer:
        messages.append({
            'offset': msg.offset,
            'partition': msg.partition,
            'value': msg.value
        })
    
    print(f"\nTotal messages in topic: {len(messages)}")
    
    # Look for id=99999 events
    print("\n" + "="*70)
    print("Looking for CDC test events (id=99999)...")
    print("="*70)
    
    found_99999 = []
    for msg in messages:
        try:
            if msg['value'] and '99999' in msg['value']:
                data = json.loads(msg['value'])
                found_99999.append({
                    'offset': msg['offset'],
                    'data': data
                })
        except:
            pass
    
    if found_99999:
        print(f"\nFound {len(found_99999)} messages for id=99999:")
        for m in found_99999:
            payload = m['data'].get('payload', {})
            op = payload.get('op', 'N/A')
            after = payload.get('after', {})
            print(f"\n  Offset: {m['offset']}")
            print(f"  Operation: {op}")
            print(f"  After: {after}")
    else:
        print("\n  No messages found for id=99999")
        print("\n  POSSIBLE ISSUES:")
        print("  1. Debezium connector not capturing changes from PostgreSQL")
        print("  2. WAL not configured correctly in PostgreSQL")
        print("  3. Replication slot issue")
        print("\n  Run: python check_connectors.py to check Debezium status")
    
    consumer.close()
    
except ImportError:
    print("kafka-python library not installed.")
    print("\nInstall with: pip install kafka-python")
    print("\nAlternatively, check Kafka UI manually:")
    print("  1. Go to: http://72.61.233.209:8080/")
    print("  2. Click Topics")
    print("  3. Click 'pipeline-1.public.department'")
    print("  4. Check if messages exist (especially for id=99999)")
    
except Exception as e:
    print(f"Error: {e}")
    print("\nUse Kafka UI to check manually:")
    print("  http://72.61.233.209:8080/ -> Topics -> pipeline-1.public.department")
