"""CDC Event Logger - Consumes Kafka messages and logs CDC events to database.

This service listens to Kafka topics and logs individual CDC events (insert/update/delete)
to the pipeline_runs table for monitoring purposes.
"""

import json
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from contextlib import contextmanager
import socket

logger = logging.getLogger(__name__)

# CRITICAL: Monkeypatch socket.getaddrinfo to resolve 'kafka' hostname to the bootstrap IP
# This is a workaround for misconfigured Kafka brokers that advertise internal hostnames
# to external clients.
_original_getaddrinfo = socket.getaddrinfo

def _patched_getaddrinfo(host, port, *args, **kwargs):
    if host == 'kafka' or host == 'kafka.default.svc.cluster.local':
        # Try to use the bootstrap IP if available, otherwise use a common default or let it fail
        # In this environment, we know the bootstrap IP is 72.61.233.209
        try:
            return _original_getaddrinfo('72.61.233.209', port, *args, **kwargs)
        except:
            return _original_getaddrinfo(host, port, *args, **kwargs)
    return _original_getaddrinfo(host, port, *args, **kwargs)

# Apply the patch globally
socket.getaddrinfo = _patched_getaddrinfo
logger.info("✅ Applied DNS patch for 'kafka' hostname resolution")

# Import Kafka consumer
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    logger.warning("kafka-python not installed. CDC event logging will be disabled.")
    KAFKA_AVAILABLE = False
    KafkaConsumer = None
    KafkaError = Exception
    NoBrokersAvailable = Exception


class CDCEventLogger:
    """Logs CDC events from Kafka topics to the database.
    
    This class creates a Kafka consumer that listens to CDC topics and logs
    individual insert/update/delete events to the pipeline_runs table.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = "72.61.233.209:9092",
        consumer_group_id: str = "cdc-event-logger",
        db_session_factory = None,
        max_batch_size: int = 50,
        batch_timeout_seconds: float = 1.0
    ):
        """Initialize the CDC Event Logger.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            consumer_group_id: Kafka consumer group ID
            db_session_factory: SQLAlchemy session factory function
            max_batch_size: Maximum events to batch before committing
            batch_timeout_seconds: Max time to wait before committing a batch
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.consumer_group_id = consumer_group_id
        self.db_session_factory = db_session_factory
        self.max_batch_size = max_batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._subscribed_topics: Set[str] = set()
        self._pipeline_topic_mapping: Dict[str, str] = {}  # topic -> pipeline_id
        self._lock = threading.Lock()
        
    def start(self, topics: Optional[List[str]] = None, pipeline_mapping: Optional[Dict[str, str]] = None):
        """Start the event logger in a background thread.
        
        Args:
            topics: List of Kafka topics to subscribe to
            pipeline_mapping: Mapping of topic names to pipeline IDs
        """
        if not KAFKA_AVAILABLE:
            logger.warning("Kafka library not available. CDC event logging disabled.")
            return False
            
        if self._running:
            logger.warning("CDC Event Logger is already running")
            return True
            
        if topics:
            self._subscribed_topics = set(topics)
        if pipeline_mapping:
            self._pipeline_topic_mapping = pipeline_mapping
            
        self._running = True
        self._thread = threading.Thread(target=self._run_consumer, daemon=True)
        self._thread.start()
        logger.info(f"CDC Event Logger started for topics: {self._subscribed_topics}")
        return True
        
    def stop(self):
        """Stop the event logger."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        if self._consumer:
            try:
                self._consumer.close()
            except Exception as e:
                logger.warning(f"Error closing Kafka consumer: {e}")
        logger.info("CDC Event Logger stopped")
        
    def add_topic(self, topic: str, pipeline_id: str):
        """Add a topic to the subscription list.
        
        Args:
            topic: Kafka topic name
            pipeline_id: Associated pipeline ID
        """
        # Ensure pipeline_id is string
        pipeline_id_str = str(pipeline_id)
        
        with self._lock:
            self._subscribed_topics.add(topic)
            self._pipeline_topic_mapping[topic] = pipeline_id_str
            logger.info(f"✅ Added topic mapping: {topic} → {pipeline_id_str} (total topics: {len(self._subscribed_topics)})")
            
        # If consumer is running, update subscription
        if self._consumer and self._running:
            try:
                self._consumer.subscribe(list(self._subscribed_topics))
                logger.info(f"✅ Updated consumer subscription to include topic {topic} (total: {len(self._subscribed_topics)})")
            except Exception as e:
                logger.warning(f"Failed to update subscription with new topic {topic}: {e}")
        elif not self._running:
            # If not running, start it with the new topics
            logger.info(f"Event logger not running, starting it with topics including {topic}")
            self.start()
                
    def remove_topic(self, topic: str):
        """Remove a topic from the subscription list.
        
        Args:
            topic: Kafka topic name to remove
        """
        with self._lock:
            self._subscribed_topics.discard(topic)
            self._pipeline_topic_mapping.pop(topic, None)
            
        # If consumer is running, update subscription
        if self._consumer and self._running and self._subscribed_topics:
            try:
                self._consumer.subscribe(list(self._subscribed_topics))
                logger.info(f"Removed topic {topic} from CDC event logger")
            except Exception as e:
                logger.warning(f"Failed to update subscription after removing topic {topic}: {e}")
    
    def _run_consumer(self):
        """Main consumer loop (runs in background thread)."""
        retry_count = 0
        max_retries = 5
        
        while self._running and retry_count < max_retries:
            try:
                # Create consumer
                # Use persistent consumer group to track offsets across restarts
                # This ensures we don't miss events and can resume from last position
                self._consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_bootstrap_servers.split(','),
                    group_id=self.consumer_group_id,  # Use persistent group ID to track offsets
                    auto_offset_reset='earliest',
  # Start from earliest if no offset exists, then use committed offsets
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                    key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                    consumer_timeout_ms=1000,  # 1 second timeout for poll
                    max_poll_records=100,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info(f"Kafka consumer created for group: {self.consumer_group_id} (subscribed to: {list(self._subscribed_topics) if self._subscribed_topics else 'none'})")
                
                # Subscribe to topics
                with self._lock:
                    topics = list(self._subscribed_topics)
                    
                if topics:
                    self._consumer.subscribe(topics)
                    logger.info(f"CDC Event Logger subscribed to topics: {topics}")
                else:
                    logger.info("CDC Event Logger waiting for topics to be added...")
                    
                retry_count = 0  # Reset retry count on successful connection
                
                # Main processing loop
                self._process_messages()
                
            except NoBrokersAvailable as e:
                retry_count += 1
                wait_time = min(30, 2 ** retry_count)  # Exponential backoff, max 30 seconds
                logger.warning(f"Kafka brokers not available (attempt {retry_count}/{max_retries}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                
            except Exception as e:
                retry_count += 1
                wait_time = min(30, 2 ** retry_count)
                logger.error(f"CDC Event Logger error (attempt {retry_count}/{max_retries}): {e}", exc_info=True)
                time.sleep(wait_time)
                
        if retry_count >= max_retries:
            logger.error("CDC Event Logger stopped after max retries")
            
    def _process_messages(self):
        """Process messages from Kafka."""
        batch = []
        batch_start_time = time.time()
        
        while self._running:
            try:
                # Check if topics need to be updated
                with self._lock:
                    current_topics = list(self._subscribed_topics)
                    
                if current_topics and self._consumer.subscription() != set(current_topics):
                    self._consumer.subscribe(current_topics)
                    logger.info(f"Updated subscription to: {current_topics}")
                    
                # Poll for messages
                message_batch = self._consumer.poll(timeout_ms=1000)
                
                total_messages = sum(len(msgs) for msgs in message_batch.values()) if message_batch else 0
                if total_messages > 0:
                    logger.info(f"📥 Received {total_messages} messages from Kafka")
                
                for topic_partition, messages in message_batch.items():
                    topic_name = topic_partition.topic
                    partition = topic_partition.partition
                    logger.info(f"Processing {len(messages)} messages from topic {topic_name}, partition {partition}")
                    
                    parsed_count = 0
                    failed_count = 0
                    
                    for message in messages:
                        event = self._parse_debezium_message(message)
                        if event:
                            batch.append(event)
                            parsed_count += 1
                            event_type = event.get('run_metadata', {}).get('event_type', 'unknown')
                            table_name = event.get('run_metadata', {}).get('table_name', 'unknown')
                            logger.debug(f"✅ Parsed CDC event: {event_type} for table {table_name}")
                        else:
                            failed_count += 1
                            logger.warning(f"⚠️  Failed to parse message {message.offset} from topic {topic_name}")
                    
                    if parsed_count > 0 or failed_count > 0:
                        logger.info(f"Topic {topic_name}: {parsed_count} parsed, {failed_count} failed out of {len(messages)} messages")
                            
                # Commit batch if size or time threshold reached
                current_time = time.time()
                if batch and (len(batch) >= self.max_batch_size or 
                              current_time - batch_start_time >= self.batch_timeout_seconds):
                    self._commit_batch(batch)
                    self._consumer.commit()
                    batch = []
                    batch_start_time = current_time
                    
            except Exception as e:
                logger.error(f"Error processing Kafka messages: {e}", exc_info=True)
                # Commit any pending events before continuing
                if batch:
                    try:
                        self._commit_batch(batch)
                        self._consumer.commit()
                    except Exception as commit_error:
                        logger.error(f"Failed to commit batch after error: {commit_error}")
                    batch = []
                    batch_start_time = time.time()
                time.sleep(1)  # Brief pause before retrying
                
        # Final commit
        if batch:
            self._commit_batch(batch)
            try:
                self._consumer.commit()
            except Exception:
                pass
                
    def _parse_debezium_message(self, message) -> Optional[Dict[str, Any]]:
        """Parse a Debezium message and extract CDC event information.
        
        Args:
            message: Kafka message
            
        Returns:
            Parsed event dict or None if not a valid CDC event
        """
        try:
            topic = message.topic
            value = message.value
            
            if not value:
                logger.debug(f"Message from topic {topic} has no value")
                return None
            
            # Log raw message structure for debugging
            if isinstance(value, dict):
                logger.debug(f"Message from topic {topic} has keys: {list(value.keys())}")
            else:
                logger.debug(f"Message from topic {topic} value type: {type(value)}")
                
            # Get pipeline ID from topic mapping
            pipeline_id = self._pipeline_topic_mapping.get(topic)
            if not pipeline_id:
                # Try to find pipeline by topic prefix or partial match
                for mapped_topic, pid in self._pipeline_topic_mapping.items():
                    # Try exact match first
                    if topic == mapped_topic:
                        pipeline_id = pid
                        break
                    # Try prefix match
                    topic_parts = topic.split('.')
                    mapped_parts = mapped_topic.split('.')
                    if len(topic_parts) > 0 and len(mapped_parts) > 0:
                        if topic_parts[0] == mapped_parts[0] or topic.startswith(mapped_parts[0]):
                            pipeline_id = pid
                            logger.info(f"Matched topic {topic} to pipeline {pid} via prefix {mapped_parts[0]}")
                            break
                        
            if not pipeline_id:
                logger.warning(f"⚠️  No pipeline mapping found for topic {topic}. Available mappings: {list(self._pipeline_topic_mapping.keys())}")
                
                # CRITICAL FIX: Try to lookup pipeline_id from database using topic name
                # This is a fallback if topic mapping wasn't set up correctly
                try:
                    if self.db_session_factory:
                        from ingestion.database.models_db import PipelineModel
                        from sqlalchemy import text
                        db = self.db_session_factory()
                        try:
                            # CRITICAL FIX: Query pipeline by kafka_topics JSONB array containing this topic
                            # PostgreSQL JSONB array query: check if topic exists in the array
                            # Works for both JSON and JSONB columns
                            # CRITICAL FIX: Query pipeline by kafka_topics JSON/JSONB array containing this topic
                            # Try PostgreSQL JSONB operator first (if using JSONB column)
                            pipeline = None
                            try:
                                # PostgreSQL JSONB contains operator: @> checks if array contains value
                                topic_json = json.dumps([topic])
                                pipeline = db.query(PipelineModel).filter(
                                    PipelineModel.deleted_at.is_(None),
                                    text("kafka_topics::jsonb @> :topic_json").bindparam(topic_json=topic_json)
                                ).first()
                            except Exception as jsonb_error:
                                # Fallback: Python-side filtering (works for JSON and JSONB)
                                logger.debug(f"JSONB query failed, using Python-side filtering: {jsonb_error}")
                            
                            # Fallback: If JSONB query doesn't work or returned None, try Python-side filtering
                            if not pipeline:
                                all_pipelines = db.query(PipelineModel).filter(
                                    PipelineModel.deleted_at.is_(None),
                                    PipelineModel.kafka_topics.isnot(None)
                                ).all()
                                
                                for p in all_pipelines:
                                    if p.kafka_topics:
                                        # Handle both list and JSON formats
                                        topics_list = p.kafka_topics if isinstance(p.kafka_topics, list) else []
                                        if topic in topics_list:
                                            pipeline = p
                                            break
                            
                            if pipeline:
                                pipeline_id = str(pipeline.id)
                                logger.info(f"✅ Found pipeline_id {pipeline_id} for topic {topic} via database lookup")
                                # Cache it for future messages
                                self._pipeline_topic_mapping[topic] = pipeline_id
                            else:
                                logger.error(f"❌ CRITICAL: No pipeline found in database for topic {topic}. Event will be skipped!")
                                logger.error(f"   This means events from this topic will NOT be saved to database.")
                                logger.error(f"   Available topics in database:")
                                # Log available topics for debugging
                                try:
                                    all_pipelines = db.query(PipelineModel).filter(
                                        PipelineModel.deleted_at.is_(None),
                                        PipelineModel.kafka_topics.isnot(None)
                                    ).all()
                                    for p in all_pipelines:
                                        if p.kafka_topics:
                                            topics_list = p.kafka_topics if isinstance(p.kafka_topics, list) else []
                                            logger.error(f"   Pipeline {p.id}: {topics_list}")
                                except:
                                    pass
                                logger.error(f"   Please ensure topics are registered with Event Logger when pipeline starts.")
                                return None
                        finally:
                            db.close()
                    else:
                        logger.error(f"❌ CRITICAL: No database session factory available. Cannot lookup pipeline_id for topic {topic}")
                        return None
                except Exception as db_error:
                    logger.error(f"❌ Error looking up pipeline_id from database for topic {topic}: {db_error}")
                    return None
                
            # Parse Debezium message format
            # Debezium messages have structure: {"schema": ..., "payload": {"before": ..., "after": ..., "source": ..., "op": ...}}
            # Handle both wrapped and unwrapped formats
            if isinstance(value, dict):
                payload = value.get('payload', value)
            else:
                # If value is not a dict, try to parse as JSON string
                try:
                    if isinstance(value, str):
                        value = json.loads(value)
                    payload = value.get('payload', value) if isinstance(value, dict) else value
                except (json.JSONDecodeError, AttributeError):
                    logger.warning(f"Could not parse message value from topic {topic}: {type(value)}")
                    return None
            
            # Get operation type - check multiple possible locations
            op = None
            if isinstance(payload, dict):
                op = payload.get('op') or payload.get('operation') or payload.get('__op')
                # Also check if it's nested in source
                if not op and 'source' in payload:
                    source = payload.get('source', {})
                    if isinstance(source, dict):
                        op = source.get('op') or source.get('operation')
            
            if not op:
                logger.debug(f"Message from topic {topic} has no 'op' field. Payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'not a dict'}")
                return None
                
            # CRITICAL: Map Debezium operation codes to normalized event types
            # Debezium emits: c (create), u (update), d (delete), r (read/snapshot)
            # We normalize to: insert, update, delete (frontend expects these)
            op_mapping = {
                'c': 'insert',   # Create → insert
                'r': 'insert',   # Read (snapshot) → insert
                'u': 'update',   # Update → update
                'd': 'delete',   # Delete → delete
                't': 'truncate'  # Truncate (some connectors) → truncate
            }
            
            # Handle alternative op field names in simplified Debezium format
            if not op:
                op = payload.get('__op')
            
            event_type = op_mapping.get(op)
            if not event_type:
                logger.warning(f"Unknown Debezium operation code: {op}. Message will be skipped.")
                return None
            
            # Log the mapping for debugging
            logger.debug(f"Mapped Debezium op '{op}' to event_type '{event_type}'")
                
            # Extract metadata - handle nested source structure
            source = {}
            if isinstance(payload, dict):
                source = payload.get('source', {})
                if not isinstance(source, dict):
                    source = {}
            
            table_name = source.get('table') or source.get('table_name') or 'unknown'
            schema_name = source.get('schema') or source.get('schema_name') or 'unknown'
            database_name = source.get('db') or source.get('database') or source.get('database_name') or 'unknown'
            
            # Fallback: Extract table/schema/database from topic name if missing
            # Typical Debezium topic format: server.schema.table or server.database.schema.table
            if table_name == 'unknown' and topic:
                parts = topic.split('.')
                if len(parts) >= 3:
                    table_name = parts[-1]
                    schema_name = parts[-2]
                    database_name = parts[-3] if len(parts) >= 4 else 'unknown'
                elif len(parts) == 2:
                    table_name = parts[1]
                    schema_name = parts[0]
            
            # Get timestamp - check multiple locations
            ts_ms = None
            if isinstance(source, dict):
                ts_ms = source.get('ts_ms') or source.get('timestamp_ms')
            if not ts_ms and isinstance(payload, dict):
                ts_ms = payload.get('ts_ms') or payload.get('timestamp_ms') or payload.get('timestamp') or payload.get('__source_ts_ms')
            
            # Convert timestamp to datetime
            if ts_ms:
                try:
                    if isinstance(ts_ms, (int, float)):
                        event_time = datetime.utcfromtimestamp(ts_ms / 1000)
                    elif isinstance(ts_ms, str):
                        # Try to parse ISO format
                        event_time = datetime.fromisoformat(ts_ms.replace('Z', '+00:00'))
                    else:
                        event_time = datetime.utcnow()
                except (ValueError, TypeError, OSError):
                    event_time = datetime.utcnow()
            else:
                event_time = datetime.utcnow()
            
            # Create event record
            event = {
                'id': str(uuid.uuid4()),
                'pipeline_id': pipeline_id,
                'run_type': 'CDC',
                'event_type': event_type,
                'status': 'completed',
                'started_at': event_time,
                'completed_at': event_time,
                'rows_processed': 1,
                'errors_count': 0,
                'run_metadata': {
                    'event_type': event_type,
                    'operation': op,
                    'table_name': table_name,
                    'schema_name': schema_name,
                    'database_name': database_name,
                    'topic': topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'source_ts_ms': ts_ms
                }
            }
            
            return event
            
        except Exception as e:
            logger.debug(f"Failed to parse Debezium message: {e}")
            return None
            
    def _commit_batch(self, events: List[Dict[str, Any]]):
        """Commit a batch of events to the database.
        
        Args:
            events: List of event dictionaries
        """
        if not events:
            return
            
        if not self.db_session_factory:
            logger.warning("No database session factory configured, cannot commit events")
            return
            
        # CRITICAL: Validate events before committing
        valid_events = []
        invalid_events = []
        for event in events:
            if not event.get('pipeline_id'):
                invalid_events.append(event)
                topic = event.get('run_metadata', {}).get('topic', 'unknown') if isinstance(event.get('run_metadata'), dict) else 'unknown'
                event_id = event.get('id', 'unknown')
                logger.error(f"❌ Event missing pipeline_id! Topic: {topic}, Event ID: {event_id}")
            else:
                valid_events.append(event)
        
        if invalid_events:
            logger.error(f"❌ CRITICAL: {len(invalid_events)} events have no pipeline_id and will be skipped!")
            logger.error(f"   This usually means topic → pipeline_id mapping is not set up correctly.")
            logger.error(f"   Please check that topics are registered with Event Logger when pipeline starts.")
            
        if not valid_events:
            logger.warning("No valid events to commit (all events missing pipeline_id)")
            return
            
        try:
            from ingestion.database.models_db import PipelineRunModel
            
            session = self.db_session_factory()
            try:
                # Group events by pipeline_id for logging
                pipeline_counts = {}
                for event in valid_events:
                    pid = event['pipeline_id']
                    pipeline_counts[pid] = pipeline_counts.get(pid, 0) + 1
                
                for event in valid_events:
                    run = PipelineRunModel(
                        id=event['id'],
                        pipeline_id=event['pipeline_id'],
                        run_type=event['run_type'],
                        status=event['status'],
                        started_at=event['started_at'],
                        completed_at=event['completed_at'],
                        rows_processed=event['rows_processed'],
                        errors_count=event['errors_count'],
                        run_metadata=event['run_metadata']
                    )
                    session.add(run)
                    
                session.commit()
                logger.info(f"✅ Committed {len(valid_events)} CDC events to database (PipelineRunModel)")
                logger.info(f"   Events by pipeline: {pipeline_counts}")
                
                # Log event type breakdown
                event_counts = {}
                pipeline_counts = {}
                for event in events:
                    et = event['run_metadata'].get('event_type', 'unknown')
                    event_counts[et] = event_counts.get(et, 0) + 1
                    pid = event.get('pipeline_id', 'unknown')
                    pipeline_counts[pid] = pipeline_counts.get(pid, 0) + 1
                logger.info(f"Event breakdown by type: {event_counts}")
                logger.info(f"Event breakdown by pipeline: {pipeline_counts}")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to commit events to database: {e}", exc_info=True)
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Database error in CDC event logger: {e}", exc_info=True)


# Global event logger instance
_event_logger: Optional[CDCEventLogger] = None


def get_event_logger() -> Optional[CDCEventLogger]:
    """Get the global CDC event logger instance."""
    return _event_logger


def initialize_event_logger(
    kafka_bootstrap_servers: str = "72.61.233.209:9092",
    db_session_factory = None
) -> Optional[CDCEventLogger]:
    """Initialize and start the global CDC event logger.
    
    Args:
        kafka_bootstrap_servers: Kafka bootstrap servers
        db_session_factory: SQLAlchemy session factory
        
    Returns:
        CDCEventLogger instance or None if Kafka not available
    """
    global _event_logger
    
    if not KAFKA_AVAILABLE:
        logger.warning("Kafka library not available. CDC event logging disabled.")
        return None
        
    if _event_logger is not None:
        logger.info("CDC Event Logger already initialized")
        return _event_logger
        
    _event_logger = CDCEventLogger(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        db_session_factory=db_session_factory
    )
    
    return _event_logger


def shutdown_event_logger():
    """Shutdown the global CDC event logger."""
    global _event_logger
    
    if _event_logger:
        _event_logger.stop()
        _event_logger = None
        logger.info("CDC Event Logger shutdown complete")


