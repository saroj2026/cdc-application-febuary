"""Health check functions for CDC pipeline components."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from datetime import datetime

from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.connectors.base_connector import BaseConnector
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector

logger = logging.getLogger(__name__)


def check_database_health(connector: BaseConnector) -> Dict[str, Any]:
    """Check database connectivity and health.
    
    Args:
        connector: Database connector instance
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {}
    }
    
    try:
        # Test connection
        conn = connector.connect()
        
        # Test query
        if isinstance(connector, PostgreSQLConnector):
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        elif isinstance(connector, SQLServerConnector):
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        
        conn.close()
        
        health["status"] = "healthy"
        health["details"]["connection"] = "ok"
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        health["details"]["connection"] = "failed"
        logger.error(f"Database health check failed: {e}")
    
    return health


def check_kafka_connect_health(kafka_client: KafkaConnectClient) -> Dict[str, Any]:
    """Check Kafka Connect service health.
    
    Args:
        kafka_client: Kafka Connect client instance
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {}
    }
    
    try:
        # Try to list connectors (lightweight operation)
        connectors = kafka_client.list_connectors()
        
        health["status"] = "healthy"
        health["details"]["connection"] = "ok"
        health["details"]["connectors_count"] = len(connectors)
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        health["details"]["connection"] = "failed"
        logger.error(f"Kafka Connect health check failed: {e}")
    
    return health


def check_connector_health(
    kafka_client: KafkaConnectClient,
    connector_name: str
) -> Dict[str, Any]:
    """Check specific connector health.
    
    Args:
        kafka_client: Kafka Connect client instance
        connector_name: Name of the connector
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "connector_name": connector_name,
        "details": {}
    }
    
    try:
        # Get connector status
        status = kafka_client.get_connector_status(connector_name)
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        tasks = status.get("tasks", [])
        
        # Check if connector is running
        if connector_state == "RUNNING":
            # Check task states
            task_states = [task.get("state", "UNKNOWN") for task in tasks]
            failed_tasks = [i for i, state in enumerate(task_states) if state == "FAILED"]
            
            if failed_tasks:
                health["status"] = "degraded"
                health["details"]["connector_state"] = connector_state
                health["details"]["failed_tasks"] = failed_tasks
                health["details"]["task_states"] = task_states
            else:
                health["status"] = "healthy"
                health["details"]["connector_state"] = connector_state
                health["details"]["task_states"] = task_states
        else:
            health["status"] = "unhealthy"
            health["details"]["connector_state"] = connector_state
            health["details"]["task_states"] = [task.get("state", "UNKNOWN") for task in tasks]
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        logger.error(f"Connector health check failed for {connector_name}: {e}")
    
    return health


def check_pipeline_health(
    pipeline_id: str,
    source_connector: Optional[BaseConnector] = None,
    target_connector: Optional[BaseConnector] = None,
    kafka_client: Optional[KafkaConnectClient] = None,
    debezium_connector_name: Optional[str] = None,
    sink_connector_name: Optional[str] = None
) -> Dict[str, Any]:
    """Comprehensive health check for a pipeline.
    
    Args:
        pipeline_id: Pipeline ID
        source_connector: Source database connector
        target_connector: Target database connector
        kafka_client: Kafka Connect client
        debezium_connector_name: Debezium connector name
        sink_connector_name: Sink connector name
        
    Returns:
        Dictionary with comprehensive health check results
    """
    health = {
        "pipeline_id": pipeline_id,
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Check source database
    if source_connector:
        health["components"]["source_database"] = check_database_health(source_connector)
    
    # Check target database
    if target_connector:
        health["components"]["target_database"] = check_database_health(target_connector)
    
    # Check Kafka Connect
    if kafka_client:
        health["components"]["kafka_connect"] = check_kafka_connect_health(kafka_client)
    
    # Check Debezium connector
    if kafka_client and debezium_connector_name:
        health["components"]["debezium_connector"] = check_connector_health(
            kafka_client, debezium_connector_name
        )
    
    # Check Sink connector
    if kafka_client and sink_connector_name:
        health["components"]["sink_connector"] = check_connector_health(
            kafka_client, sink_connector_name
        )
    
    # Determine overall status
    component_statuses = [
        comp.get("status", "unknown")
        for comp in health["components"].values()
    ]
    
    if "unhealthy" in component_statuses:
        health["status"] = "unhealthy"
    elif "degraded" in component_statuses:
        health["status"] = "degraded"
    elif all(status == "healthy" for status in component_statuses if status != "unknown"):
        health["status"] = "healthy"
    else:
        health["status"] = "unknown"
    
    return health


from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from datetime import datetime

from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.connectors.base_connector import BaseConnector
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector

logger = logging.getLogger(__name__)


def check_database_health(connector: BaseConnector) -> Dict[str, Any]:
    """Check database connectivity and health.
    
    Args:
        connector: Database connector instance
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {}
    }
    
    try:
        # Test connection
        conn = connector.connect()
        
        # Test query
        if isinstance(connector, PostgreSQLConnector):
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        elif isinstance(connector, SQLServerConnector):
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        
        conn.close()
        
        health["status"] = "healthy"
        health["details"]["connection"] = "ok"
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        health["details"]["connection"] = "failed"
        logger.error(f"Database health check failed: {e}")
    
    return health


def check_kafka_connect_health(kafka_client: KafkaConnectClient) -> Dict[str, Any]:
    """Check Kafka Connect service health.
    
    Args:
        kafka_client: Kafka Connect client instance
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {}
    }
    
    try:
        # Try to list connectors (lightweight operation)
        connectors = kafka_client.list_connectors()
        
        health["status"] = "healthy"
        health["details"]["connection"] = "ok"
        health["details"]["connectors_count"] = len(connectors)
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        health["details"]["connection"] = "failed"
        logger.error(f"Kafka Connect health check failed: {e}")
    
    return health


def check_connector_health(
    kafka_client: KafkaConnectClient,
    connector_name: str
) -> Dict[str, Any]:
    """Check specific connector health.
    
    Args:
        kafka_client: Kafka Connect client instance
        connector_name: Name of the connector
        
    Returns:
        Dictionary with health check results
    """
    health = {
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "connector_name": connector_name,
        "details": {}
    }
    
    try:
        # Get connector status
        status = kafka_client.get_connector_status(connector_name)
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        tasks = status.get("tasks", [])
        
        # Check if connector is running
        if connector_state == "RUNNING":
            # Check task states
            task_states = [task.get("state", "UNKNOWN") for task in tasks]
            failed_tasks = [i for i, state in enumerate(task_states) if state == "FAILED"]
            
            if failed_tasks:
                health["status"] = "degraded"
                health["details"]["connector_state"] = connector_state
                health["details"]["failed_tasks"] = failed_tasks
                health["details"]["task_states"] = task_states
            else:
                health["status"] = "healthy"
                health["details"]["connector_state"] = connector_state
                health["details"]["task_states"] = task_states
        else:
            health["status"] = "unhealthy"
            health["details"]["connector_state"] = connector_state
            health["details"]["task_states"] = [task.get("state", "UNKNOWN") for task in tasks]
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["details"]["error"] = str(e)
        logger.error(f"Connector health check failed for {connector_name}: {e}")
    
    return health


def check_pipeline_health(
    pipeline_id: str,
    source_connector: Optional[BaseConnector] = None,
    target_connector: Optional[BaseConnector] = None,
    kafka_client: Optional[KafkaConnectClient] = None,
    debezium_connector_name: Optional[str] = None,
    sink_connector_name: Optional[str] = None
) -> Dict[str, Any]:
    """Comprehensive health check for a pipeline.
    
    Args:
        pipeline_id: Pipeline ID
        source_connector: Source database connector
        target_connector: Target database connector
        kafka_client: Kafka Connect client
        debezium_connector_name: Debezium connector name
        sink_connector_name: Sink connector name
        
    Returns:
        Dictionary with comprehensive health check results
    """
    health = {
        "pipeline_id": pipeline_id,
        "status": "unknown",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Check source database
    if source_connector:
        health["components"]["source_database"] = check_database_health(source_connector)
    
    # Check target database
    if target_connector:
        health["components"]["target_database"] = check_database_health(target_connector)
    
    # Check Kafka Connect
    if kafka_client:
        health["components"]["kafka_connect"] = check_kafka_connect_health(kafka_client)
    
    # Check Debezium connector
    if kafka_client and debezium_connector_name:
        health["components"]["debezium_connector"] = check_connector_health(
            kafka_client, debezium_connector_name
        )
    
    # Check Sink connector
    if kafka_client and sink_connector_name:
        health["components"]["sink_connector"] = check_connector_health(
            kafka_client, sink_connector_name
        )
    
    # Determine overall status
    component_statuses = [
        comp.get("status", "unknown")
        for comp in health["components"].values()
    ]
    
    if "unhealthy" in component_statuses:
        health["status"] = "unhealthy"
    elif "degraded" in component_statuses:
        health["status"] = "degraded"
    elif all(status == "healthy" for status in component_statuses if status != "unknown"):
        health["status"] = "healthy"
    else:
        health["status"] = "unknown"
    
    return health

