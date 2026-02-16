"""CDC Health Monitor with Auto-Recovery for Replication Slot Lag."""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session

import psycopg2
from psycopg2.extras import RealDictCursor
import requests

from ingestion.database.models_db import PipelineModel, ConnectionModel
from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.connection_service import ConnectionService
from ingestion.models import Pipeline, PipelineStatus, CDCStatus

logger = logging.getLogger(__name__)


class CDCHealthMonitor:
    """Monitor CDC pipeline health and automatically recover from stuck states."""
    
    # Configuration thresholds
    LAG_WARNING_THRESHOLD_KB = 100  # 100 KB - warning level
    LAG_CRITICAL_THRESHOLD_KB = 500  # 500 KB - critical, trigger recovery
    LAG_MAX_THRESHOLD_KB = 1000  # 1 MB - maximum before forced recovery
    
    # Recovery settings
    MAX_RECOVERY_ATTEMPTS = 3
    RECOVERY_COOLDOWN_SECONDS = 300  # 5 minutes between recovery attempts
    
    def __init__(
        self,
        kafka_client: KafkaConnectClient,
        connection_service: ConnectionService,
        db_session: Session,
        kafka_connect_url: str = "http://localhost:8083"
    ):
        """Initialize CDC Health Monitor.
        
        Args:
            kafka_client: Kafka Connect client
            connection_service: Connection service
            db_session: Database session
            kafka_connect_url: Kafka Connect URL
        """
        self.kafka_client = kafka_client
        self.connection_service = connection_service
        self.db_session = db_session
        self.kafka_connect_url = kafka_connect_url
        self.recovery_attempts: Dict[str, Dict[str, Any]] = {}  # Track recovery attempts per pipeline
    
    def check_replication_slot_lag(
        self,
        pipeline: Pipeline,
        source_connection: ConnectionModel
    ) -> Dict[str, Any]:
        """Check replication slot lag for a pipeline.
        
        Args:
            pipeline: Pipeline object
            source_connection: Source connection model
            
        Returns:
            Lag information dictionary
        """
        try:
            if source_connection.database_type != "postgresql":
                return {
                    "lag_bytes": 0,
                    "lag_kb": 0,
                    "status": "not_applicable",
                    "error": "Not a PostgreSQL source"
                }
            
            # Skip if connection test has failed - don't spam logs with auth errors
            if source_connection.last_test_status == "failed":
                return {
                    "lag_bytes": 0,
                    "lag_kb": 0,
                    "status": "connection_failed",
                    "error": "Connection test failed - skipping lag check"
                }
            
            # Get slot name from Debezium connector config
            slot_name = None
            if pipeline.debezium_connector_name:
                try:
                    config = self.kafka_client.get_connector_config(
                        pipeline.debezium_connector_name
                    )
                    slot_name = config.get("slot.name")
                except Exception as e:
                    logger.warning(f"Could not get connector config: {e}")
            
            if not slot_name:
                # Try to infer slot name from pipeline name
                slot_name = f"{pipeline.name.lower().replace('-', '_')}_slot"
            
            # Connect to PostgreSQL and check slot lag
            try:
                conn = psycopg2.connect(
                    host=source_connection.host,
                    port=source_connection.port,
                    database=source_connection.database,
                    user=source_connection.username,
                    password=source_connection.password,
                    connect_timeout=5  # 5 second timeout
                )
            except psycopg2.OperationalError as e:
                error_msg = str(e)
                if "password authentication failed" in error_msg:
                    # Only log at debug level to reduce noise - this is expected for pipelines with incorrect credentials
                    logger.debug(f"Authentication failed for {source_connection.host}:{source_connection.port}/{source_connection.database} - check credentials")
                    return {
                        "lag_bytes": 0,
                        "lag_kb": 0,
                        "status": "auth_failed",
                        "error": "Database authentication failed - check connection credentials"
                    }
                elif "could not connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                    logger.debug(f"Could not connect to {source_connection.host}:{source_connection.port} - server may be down")
                    return {
                        "lag_bytes": 0,
                        "lag_kb": 0,
                        "status": "connection_failed",
                        "error": f"Could not connect to database server"
                    }
                else:
                    logger.debug(f"Database connection error: {e}")
                    return {
                        "lag_bytes": 0,
                        "lag_kb": 0,
                        "status": "connection_error",
                        "error": f"Database connection error: {str(e)[:100]}"
                    }
            
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                cursor.execute('''
                    SELECT 
                        slot_name,
                        confirmed_flush_lsn AS cdc_lsn,
                        pg_current_wal_lsn() AS current_lsn,
                        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
                        active
                    FROM pg_replication_slots
                    WHERE slot_name = %s
                ''', (slot_name,))
                
                slot = cursor.fetchone()
                cursor.close()
            finally:
                conn.close()
            
            if not slot:
                return {
                    "lag_bytes": 0,
                    "lag_kb": 0,
                    "status": "slot_not_found",
                    "error": f"Replication slot '{slot_name}' not found"
                }
            
            lag_bytes = slot['lag_bytes'] or 0
            lag_kb = lag_bytes / 1024
            
            # Determine status
            if lag_kb < self.LAG_WARNING_THRESHOLD_KB:
                status = "healthy"
            elif lag_kb < self.LAG_CRITICAL_THRESHOLD_KB:
                status = "warning"
            elif lag_kb < self.LAG_MAX_THRESHOLD_KB:
                status = "critical"
            else:
                status = "stuck"
            
            return {
                "slot_name": slot['slot_name'],
                "cdc_lsn": str(slot['cdc_lsn']) if slot['cdc_lsn'] else None,
                "current_lsn": str(slot['current_lsn']) if slot['current_lsn'] else None,
                "lag_bytes": lag_bytes,
                "lag_kb": lag_kb,
                "status": status,
                "active": slot['active'],
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking replication slot lag: {e}", exc_info=True)
            return {
                "lag_bytes": 0,
                "lag_kb": 0,
                "status": "error",
                "error": str(e)
            }
    
    def check_connector_health(
        self,
        connector_name: str
    ) -> Dict[str, Any]:
        """Check connector health status.
        
        Args:
            connector_name: Connector name
            
        Returns:
            Health status dictionary
        """
        try:
            status = self.kafka_client.get_connector_status(connector_name)
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])
            
            running_tasks = [t for t in tasks if t.get("state") == "RUNNING"]
            failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
            
            return {
                "connector_state": connector_state,
                "total_tasks": len(tasks),
                "running_tasks": len(running_tasks),
                "failed_tasks": len(failed_tasks),
                "is_healthy": connector_state == "RUNNING" and len(failed_tasks) == 0,
                "errors": [t.get("trace", "") for t in failed_tasks] if failed_tasks else []
            }
        except Exception as e:
            logger.error(f"Error checking connector health: {e}")
            return {
                "connector_state": "ERROR",
                "is_healthy": False,
                "error": str(e)
            }
    
    def recover_stuck_cdc(
        self,
        pipeline: Pipeline,
        source_connection: ConnectionModel,
        lag_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recover from stuck CDC by restarting connector or recreating slot.
        
        Args:
            pipeline: Pipeline object
            source_connection: Source connection model
            lag_info: Lag information from check_replication_slot_lag
            
        Returns:
            Recovery result dictionary
        """
        pipeline_id = pipeline.id
        connector_name = pipeline.debezium_connector_name
        
        if not connector_name:
            return {
                "success": False,
                "error": "No Debezium connector name found"
            }
        
        # Check recovery cooldown
        if pipeline_id in self.recovery_attempts:
            last_attempt = self.recovery_attempts[pipeline_id]
            time_since_last = (datetime.utcnow() - last_attempt["timestamp"]).total_seconds()
            
            if time_since_last < self.RECOVERY_COOLDOWN_SECONDS:
                remaining = self.RECOVERY_COOLDOWN_SECONDS - time_since_last
                return {
                    "success": False,
                    "error": f"Recovery cooldown active. Wait {remaining:.0f} seconds.",
                    "cooldown_remaining": remaining
                }
            
            if last_attempt["attempts"] >= self.MAX_RECOVERY_ATTEMPTS:
                return {
                    "success": False,
                    "error": f"Max recovery attempts ({self.MAX_RECOVERY_ATTEMPTS}) reached. Manual intervention required."
                }
        
        try:
            lag_kb = lag_info.get("lag_kb", 0)
            slot_name = lag_info.get("slot_name")
            
            logger.warning(
                f"Attempting CDC recovery for pipeline {pipeline.name} "
                f"(lag: {lag_kb:.2f} KB, slot: {slot_name})"
            )
            
            # Strategy 1: Restart connector (for moderate lag)
            if lag_kb < self.LAG_MAX_THRESHOLD_KB:
                result = self._restart_connector(connector_name)
                if result["success"]:
                    self._record_recovery_attempt(pipeline_id, "restart_connector", True)
                    return result
            
            # Strategy 2: Recreate replication slot (for severe lag)
            if lag_kb >= self.LAG_CRITICAL_THRESHOLD_KB and slot_name:
                result = self._recreate_replication_slot(
                    connector_name,
                    slot_name,
                    source_connection
                )
                if result["success"]:
                    self._record_recovery_attempt(pipeline_id, "recreate_slot", True)
                    return result
            
            # If all strategies failed
            self._record_recovery_attempt(pipeline_id, "failed", False)
            return {
                "success": False,
                "error": "All recovery strategies failed"
            }
            
        except Exception as e:
            logger.error(f"Error during CDC recovery: {e}", exc_info=True)
            self._record_recovery_attempt(pipeline_id, "error", False)
            return {
                "success": False,
                "error": str(e)
            }
    
    def _restart_connector(self, connector_name: str) -> Dict[str, Any]:
        """Restart a connector by pausing and resuming.
        
        Args:
            connector_name: Connector name
            
        Returns:
            Result dictionary
        """
        try:
            # Pause connector
            pause_url = f"{self.kafka_connect_url}/connectors/{connector_name}/pause"
            response = requests.put(pause_url, timeout=10)
            if response.status_code not in [200, 202, 204]:
                return {
                    "success": False,
                    "error": f"Failed to pause connector: {response.status_code}"
                }
            
            # Wait a moment
            time.sleep(2)
            
            # Resume connector
            resume_url = f"{self.kafka_connect_url}/connectors/{connector_name}/resume"
            response = requests.put(resume_url, timeout=10)
            if response.status_code not in [200, 202, 204]:
                return {
                    "success": False,
                    "error": f"Failed to resume connector: {response.status_code}"
                }
            
            logger.info(f"Successfully restarted connector: {connector_name}")
            return {
                "success": True,
                "method": "restart_connector",
                "message": f"Connector {connector_name} restarted successfully"
            }
            
        except Exception as e:
            logger.error(f"Error restarting connector: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _recreate_replication_slot(
        self,
        connector_name: str,
        slot_name: str,
        source_connection: ConnectionModel
    ) -> Dict[str, Any]:
        """Recreate replication slot by deleting and restarting connector.
        
        Args:
            connector_name: Debezium connector name
            slot_name: Replication slot name
            source_connection: Source connection model
            
        Returns:
            Result dictionary
        """
        try:
            # Step 1: Pause connector
            pause_url = f"{self.kafka_connect_url}/connectors/{connector_name}/pause"
            response = requests.put(pause_url, timeout=10)
            if response.status_code not in [200, 202, 204]:
                return {
                    "success": False,
                    "error": f"Failed to pause connector: {response.status_code}"
                }
            
            time.sleep(2)
            
            # Step 2: Delete replication slot
            try:
                conn = psycopg2.connect(
                    host=source_connection.host,
                    port=source_connection.port,
                    database=source_connection.database,
                    user=source_connection.username,
                    password=source_connection.password
                )
                cursor = conn.cursor()
                cursor.execute(f"SELECT pg_drop_replication_slot(%s)", (slot_name,))
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"Deleted replication slot: {slot_name}")
            except psycopg2.Error as e:
                if "does not exist" not in str(e):
                    logger.warning(f"Could not delete slot (may not exist): {e}")
            
            time.sleep(2)
            
            # Step 3: Resume connector (will recreate slot automatically)
            resume_url = f"{self.kafka_connect_url}/connectors/{connector_name}/resume"
            response = requests.put(resume_url, timeout=10)
            if response.status_code not in [200, 202, 204]:
                return {
                    "success": False,
                    "error": f"Failed to resume connector: {response.status_code}"
                }
            
            logger.info(f"Successfully recreated replication slot: {slot_name}")
            return {
                "success": True,
                "method": "recreate_slot",
                "message": f"Replication slot {slot_name} recreated successfully"
            }
            
        except Exception as e:
            logger.error(f"Error recreating replication slot: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def _record_recovery_attempt(
        self,
        pipeline_id: str,
        method: str,
        success: bool
    ) -> None:
        """Record a recovery attempt.
        
        Args:
            pipeline_id: Pipeline ID
            method: Recovery method used
            success: Whether recovery was successful
        """
        if pipeline_id not in self.recovery_attempts:
            self.recovery_attempts[pipeline_id] = {
                "attempts": 0,
                "last_method": None,
                "timestamp": None
            }
        
        self.recovery_attempts[pipeline_id]["attempts"] += 1
        self.recovery_attempts[pipeline_id]["last_method"] = method
        self.recovery_attempts[pipeline_id]["timestamp"] = datetime.utcnow()
        self.recovery_attempts[pipeline_id]["last_success"] = success
    
    def check_pipeline_health(
        self,
        pipeline_id: str
    ) -> Dict[str, Any]:
        """Comprehensive health check for a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Health check results
        """
        try:
            # Get pipeline from database
            pipeline_model = self.db_session.query(PipelineModel).filter_by(
                id=pipeline_id
            ).first()
            
            if not pipeline_model:
                return {
                    "pipeline_id": pipeline_id,
                    "status": "not_found",
                    "error": "Pipeline not found"
                }
            
            # Get source connection
            source_conn_model = self.db_session.query(ConnectionModel).filter_by(
                id=pipeline_model.source_connection_id
            ).first()
            
            if not source_conn_model:
                return {
                    "pipeline_id": pipeline_id,
                    "status": "error",
                    "error": "Source connection not found"
                }
            
            # Convert to Pipeline object
            pipeline = self._pipeline_model_to_pipeline(pipeline_model)
            
            # Check replication slot lag
            lag_info = self.check_replication_slot_lag(pipeline, source_conn_model)
            
            # Check connector health
            debezium_health = {}
            sink_health = {}
            
            if pipeline.debezium_connector_name:
                debezium_health = self.check_connector_health(pipeline.debezium_connector_name)
            
            if pipeline.sink_connector_name:
                sink_health = self.check_connector_health(pipeline.sink_connector_name)
            
            # Determine overall health
            overall_status = "healthy"
            issues = []
            
            if lag_info.get("status") in ["critical", "stuck"]:
                overall_status = "unhealthy"
                issues.append(f"Replication slot lag: {lag_info.get('lag_kb', 0):.2f} KB")
            
            if not debezium_health.get("is_healthy", False):
                overall_status = "unhealthy"
                issues.append("Debezium connector unhealthy")
            
            if not sink_health.get("is_healthy", False):
                overall_status = "unhealthy"
                issues.append("Sink connector unhealthy")
            
            # Auto-recover if needed
            recovery_result = None
            if lag_info.get("status") in ["critical", "stuck"]:
                recovery_result = self.recover_stuck_cdc(pipeline, source_conn_model, lag_info)
            
            return {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline.name,
                "status": overall_status,
                "lag_info": lag_info,
                "debezium_health": debezium_health,
                "sink_health": sink_health,
                "issues": issues,
                "recovery_attempted": recovery_result is not None,
                "recovery_result": recovery_result,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking pipeline health: {e}", exc_info=True)
            return {
                "pipeline_id": pipeline_id,
                "status": "error",
                "error": str(e)
            }
    
    def _pipeline_model_to_pipeline(self, pipeline_model: PipelineModel) -> Pipeline:
        """Convert PipelineModel to Pipeline object."""
        return Pipeline(
            id=pipeline_model.id,
            name=pipeline_model.name,
            source_connection_id=pipeline_model.source_connection_id,
            target_connection_id=pipeline_model.target_connection_id,
            source_database=pipeline_model.source_database,
            source_schema=pipeline_model.source_schema,
            source_tables=pipeline_model.source_tables,
            target_database=pipeline_model.target_database,
            target_schema=pipeline_model.target_schema,
            target_tables=pipeline_model.target_tables,
            mode=pipeline_model.mode.value if hasattr(pipeline_model.mode, 'value') else str(pipeline_model.mode),
            enable_full_load=pipeline_model.enable_full_load,
            auto_create_target=pipeline_model.auto_create_target,
            target_table_mapping=pipeline_model.target_table_mapping,
            table_filter=pipeline_model.table_filter,
            full_load_status=pipeline_model.full_load_status.value if hasattr(pipeline_model.full_load_status, 'value') else str(pipeline_model.full_load_status),
            full_load_lsn=pipeline_model.full_load_lsn,
            cdc_status=pipeline_model.cdc_status.value if hasattr(pipeline_model.cdc_status, 'value') else str(pipeline_model.cdc_status),
            debezium_connector_name=pipeline_model.debezium_connector_name,
            sink_connector_name=pipeline_model.sink_connector_name,
            kafka_topics=pipeline_model.kafka_topics,
            debezium_config=pipeline_model.debezium_config,
            sink_config=pipeline_model.sink_config,
            status=pipeline_model.status.value if hasattr(pipeline_model.status, 'value') else str(pipeline_model.status),
            created_at=pipeline_model.created_at,
            updated_at=pipeline_model.updated_at
        )
    
    def monitor_all_pipelines(self) -> List[Dict[str, Any]]:
        """Monitor all running pipelines.
        
        Returns:
            List of health check results
        """
        try:
            # Get all running pipelines
            pipelines = self.db_session.query(PipelineModel).filter(
                PipelineModel.deleted_at.is_(None),
                PipelineModel.status.in_(["RUNNING", "STARTING"])
            ).all()
            
            results = []
            for pipeline_model in pipelines:
                health = self.check_pipeline_health(pipeline_model.id)
                results.append(health)
            
            return results
            
        except Exception as e:
            logger.error(f"Error monitoring all pipelines: {e}", exc_info=True)
            return []


