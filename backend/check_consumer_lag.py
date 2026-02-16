
import os
import sys
import logging
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

import socket

# Monkey patch getaddrinfo to resolve 'kafka' to the bootstrap server IP
_orig_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host == 'kafka':
        # Resolve 'kafka' to the visible IP
        return _orig_getaddrinfo('72.61.233.209', port, family, type, proto, flags)
    return _orig_getaddrinfo(host, port, family, type, proto, flags)

socket.getaddrinfo = _patched_getaddrinfo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_consumer_lag():
    """Check Kafka consumer lag for all consumer groups."""
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092")
    logger.info(f"Connecting to Kafka at {bootstrap_servers}")

    try:
        # Create Admin Client to list groups
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        consumer_groups = admin_client.list_consumer_groups()
        
        logger.info(f"Found {len(consumer_groups)} consumer groups")
        
        # Create Consumer to fetch offsets
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        
        results = []
        
        for group in consumer_groups:
            group_id = group[0]
            if group_id == "connect-cluster": # Skip connect internal group usually
                continue
                
            logger.info(f"Checking group: {group_id}")
            
            try:
                # Get committed offsets for the group
                # Note: list_consumer_group_offsets returns {TopicPartition: OffsetAndMetadata}
                group_offsets = admin_client.list_consumer_group_offsets(group_id)
                
                for tp, offset_meta in group_offsets.items():
                    if not offset_meta:
                        continue
                        
                    committed_offset = offset_meta.offset
                    
                    # Get end offset (High Watermark)
                    # end_offsets returns {TopicPartition: int}
                    end_offsets = consumer.end_offsets([tp])
                    end_offset = end_offsets.get(tp, 0)
                    
                    lag = max(0, end_offset - committed_offset)
                    
                    if lag > 0:
                        logger.warning(f"Group {group_id} - Topic {tp.topic} - Partition {tp.partition}: Lag={lag}")
                    
                    results.append({
                        "group_id": group_id,
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "committed_offset": committed_offset,
                        "end_offset": end_offset,
                        "lag": lag
                    })
                    
            except Exception as e:
                logger.error(f"Error checking group {group_id}: {e}")
                
        # Print summary
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Failed to check consumer lag: {e}")
        
if __name__ == "__main__":
    check_consumer_lag()
