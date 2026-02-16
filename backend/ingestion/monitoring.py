
import os
import logging
import time
import socket
from datetime import datetime
from typing import Any, Dict, List, Optional

# Monkey patch getaddrinfo for Kafka hostname resolution
_orig_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host == 'kafka':
        return _orig_getaddrinfo('72.61.233.209', port, family, type, proto, flags)
    return _orig_getaddrinfo(host, port, family, type, proto, flags)
socket.getaddrinfo = _patched_getaddrinfo

try:
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    KAFKA_PYTHON_AVAILABLE = False

from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.models import Pipeline

logger = logging.getLogger(__name__)


class CDCMonitor:
    """Monitor CDC pipeline health and metrics."""
    
    def __init__(self, kafka_connect_client: KafkaConnectClient):
        """Initialize CDC monitor.
        
        Args:
            kafka_connect_client: Kafka Connect client instance
        """
        self.kafka_client = kafka_connect_client
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092")

    def get_consumer_lag(self, pipeline: Pipeline) -> Dict[str, Any]:
        """Get consumer lag for pipeline topics.
        
        Args:
            pipeline: Pipeline object
            
        Returns:
            Dictionary with lag metrics per consumer group/topic
        """
        if not KAFKA_PYTHON_AVAILABLE:
            return {"error": "kafka-python not installed"}
            
        # Initialize clients lazily or per request to avoid connection issues on init
        admin_client = None
        consumer = None
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
            
            # Identify relevant consumer groups
            # 1. CDC Event Logger (global but we filter by pipeline topics)
            # 2. Sink Connector (specific to pipeline)
            relevant_groups = []
            
            # Add event logger group
            relevant_groups.append("cdc-event-logger")

            # Add sink connector group name directly
            # Sink connector group usually follows pattern: connect-{connector-name}
            if pipeline.sink_connector_name:
                relevant_groups.append(f"connect-{pipeline.sink_connector_name}")
            
            results = {}
            pipeline_topics = set(pipeline.kafka_topics) if pipeline.kafka_topics else set()
            
            # If no topics, return empty
            if not pipeline_topics:
                return {}
            
            for group_id in relevant_groups:
                try:
                    # List consumer group offsets
                    try:
                        group_offsets = admin_client.list_consumer_group_offsets(group_id)
                    except Exception as e:
                        logger.debug(f"Group {group_id} not found or error: {e}")
                        continue
                        
                    total_lag = 0
                    has_lag = False
                    
                    for tp, offset_meta in group_offsets.items():
                        if not offset_meta:
                            continue
                            
                        # Filter by pipeline topics
                        if tp.topic not in pipeline_topics:
                            continue
                            
                        committed_offset = offset_meta.offset
                        
                        try:
                            end_offsets = consumer.end_offsets([tp])
                            end_offset = end_offsets.get(tp, 0)
                            lag = max(0, end_offset - committed_offset)
                        except:
                            lag = 0
                        
                        total_lag += lag
                        
                        # Store detailed lag if significant
                        if lag > 0:
                            has_lag = True
                            if "details" not in results:
                                results["details"] = []
                            results["details"].append({
                                "group": group_id,
                                "topic": tp.topic,
                                "partition": tp.partition,
                                "lag": lag
                            })
                            
                    results[group_id] = total_lag
                    
                except Exception as e:
                    logger.debug(f"Could not check group {group_id}: {e}")
                    results[group_id] = -1 # Indicates error/unknown
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to check consumer lag: {e}")
            return {"error": str(e)}
        finally:
            try:
                if admin_client: admin_client.close()
                if consumer: consumer.close()
            except:
                pass
    
    def check_connector_health(
        self,
        connector_name: str
    ) -> Dict[str, Any]:
        """Check connector health.
        
        Args:
            connector_name: Connector name
            
        Returns:
            Health status dictionary
        """
        try:
            status = self.kafka_client.get_connector_status(connector_name)
            if status is None:
                # Connector doesn't exist
                return {
                    "connector_state": "NOT_FOUND",
                    "tasks": [],
                    "failed_tasks": []
                }
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])
            
            failed_tasks = [
                task for task in tasks
                if task.get("state") == "FAILED"
            ]
            
            health = {
                "connector_name": connector_name,
                "status": "healthy" if connector_state == "RUNNING" and not failed_tasks else "unhealthy",
                "connector_state": connector_state,
                "total_tasks": len(tasks),
                "failed_tasks": len(failed_tasks),
                "task_details": tasks,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if failed_tasks:
                health["errors"] = [
                    task.get("trace", "Unknown error")
                    for task in failed_tasks
                ]
            
            return health
            
        except Exception as e:
            logger.error(f"Failed to check connector health: {e}")
            return {
                "connector_name": connector_name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def check_pipeline_health(
        self,
        pipeline: Pipeline
    ) -> Dict[str, Any]:
        """Check overall pipeline health.
        
        Args:
            pipeline: Pipeline object
            
        Returns:
            Pipeline health status
        """
        health = {
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.name,
            "overall_status": "unknown",
            "components": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Check Debezium connector
        if pipeline.debezium_connector_name:
            debezium_health = self.check_connector_health(
                pipeline.debezium_connector_name
            )
            health["components"]["debezium"] = debezium_health
        
        # Check Sink connector
        if pipeline.sink_connector_name:
            sink_health = self.check_connector_health(
                pipeline.sink_connector_name
            )
            health["components"]["sink"] = sink_health
        
        # Determine overall status
        component_statuses = [
            comp.get("status", "unknown")
            for comp in health["components"].values()
        ]
        
        if all(status == "healthy" for status in component_statuses):
            health["overall_status"] = "healthy"
        elif any(status == "unhealthy" for status in component_statuses):
            health["overall_status"] = "unhealthy"
        elif any(status == "error" for status in component_statuses):
            health["overall_status"] = "error"
        
        return health
    
    def get_pipeline_metrics(
        self,
        pipeline: Pipeline
    ) -> Dict[str, Any]:
        """Get pipeline metrics.
        
        Args:
            pipeline: Pipeline object
            
        Returns:
            Metrics dictionary
        """
        metrics = {
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.name,
            "status": pipeline.status,
            "full_load_status": pipeline.full_load_status,
            "cdc_status": pipeline.cdc_status,
            "kafka_topics_count": len(pipeline.kafka_topics),
            "tables_count": len(pipeline.source_tables),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add connector metrics
        if pipeline.debezium_connector_name:
            try:
                dbz_status = self.kafka_client.get_connector_status(
                    pipeline.debezium_connector_name
                )
                if dbz_status:
                    metrics["debezium_connector_state"] = dbz_status.get("connector", {}).get("state")
            except Exception:
                pass
        
        # Check consumer lag
        try:
             consumer_lag = self.get_consumer_lag(pipeline)
             metrics["consumer_lag"] = consumer_lag
        except Exception as e:
             logger.debug(f"Failed to get consumer lag: {e}")

        
        if pipeline.sink_connector_name:
            try:
                sink_status = self.kafka_client.get_connector_status(
                    pipeline.sink_connector_name
                )
                if sink_status:
                    metrics["sink_connector_state"] = sink_status.get("connector", {}).get("state")
            except Exception:
                pass
        
        return metrics




