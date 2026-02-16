"""Real-time metrics collection for CDC pipelines."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ingestion.database.models_db import PipelineMetricsModel, PipelineModel
from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.models import Pipeline

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collect real-time pipeline metrics."""
    
    def __init__(self, kafka_client: KafkaConnectClient, db_session: Session):
        """Initialize metrics collector.
        
        Args:
            kafka_client: Kafka Connect client
            db_session: Database session
        """
        self.kafka_client = kafka_client
        self.db_session = db_session
    
    def collect_pipeline_metrics(
        self,
        pipeline: Pipeline
    ) -> Dict[str, Any]:
        """Collect real-time metrics for a pipeline.
        
        Args:
            pipeline: Pipeline object
            
        Returns:
            Metrics dictionary
        """
        try:
            metrics = {
                "pipeline_id": pipeline.id,
                "pipeline_name": pipeline.name,
                "timestamp": datetime.utcnow().isoformat(),
                "throughput_events_per_sec": 0.0,
                "lag_seconds": 0.0,
                "error_count": 0,
                "bytes_processed": 0,
                "source_offset": None,
                "target_offset": None,
                "connector_states": {}
            }
            
            # Get Debezium connector metrics
            if pipeline.debezium_connector_name:
                try:
                    dbz_status = self.kafka_client.get_connector_status(
                        pipeline.debezium_connector_name
                    )
                    if dbz_status is None:
                        # Connector doesn't exist, skip this pipeline
                        logger.debug(f"Debezium connector {pipeline.debezium_connector_name} not found, skipping metrics")
                    else:
                        dbz_state = dbz_status.get("connector", {}).get("state", "UNKNOWN")
                        metrics["connector_states"]["debezium"] = dbz_state
                        
                        # Get task metrics
                        tasks = dbz_status.get("tasks", [])
                        running_tasks = [t for t in tasks if t.get("state") == "RUNNING"]
                        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                        
                        metrics["debezium_running_tasks"] = len(running_tasks)
                        metrics["debezium_failed_tasks"] = len(failed_tasks)
                        
                        if failed_tasks:
                            metrics["error_count"] += len(failed_tasks)
                            metrics["errors"] = [
                                task.get("trace", "Unknown error")
                                for task in failed_tasks
                            ]
                except Exception as e:
                    logger.warning(f"Failed to get Debezium metrics: {e}")
            
            # Get Sink connector metrics
            if pipeline.sink_connector_name:
                try:
                    sink_status = self.kafka_client.get_connector_status(
                        pipeline.sink_connector_name
                    )
                    if sink_status is None:
                        # Connector doesn't exist, skip this pipeline
                        logger.debug(f"Sink connector {pipeline.sink_connector_name} not found, skipping metrics")
                    else:
                        sink_state = sink_status.get("connector", {}).get("state", "UNKNOWN")
                        metrics["connector_states"]["sink"] = sink_state
                        
                        # Get task metrics
                        tasks = sink_status.get("tasks", [])
                        running_tasks = [t for t in tasks if t.get("state") == "RUNNING"]
                        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                        
                        metrics["sink_running_tasks"] = len(running_tasks)
                        metrics["sink_failed_tasks"] = len(failed_tasks)
                        
                        if failed_tasks:
                            metrics["error_count"] += len(failed_tasks)
                            if "errors" not in metrics:
                                metrics["errors"] = []
                            metrics["errors"].extend([
                                task.get("trace", "Unknown error")
                                for task in failed_tasks
                            ])
                except Exception as e:
                    logger.warning(f"Failed to get Sink metrics: {e}")
            
            # Store metrics in database
            self._store_metrics(pipeline.id, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}", exc_info=True)
            return {
                "pipeline_id": pipeline.id,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def _store_metrics(
        self,
        pipeline_id: str,
        metrics: Dict[str, Any]
    ) -> None:
        """Store metrics in database.
        
        Args:
            pipeline_id: Pipeline ID
            metrics: Metrics dictionary
        """
        try:
            # Get pipeline model
            pipeline_model = self.db_session.query(PipelineModel).filter_by(
                id=pipeline_id
            ).first()
            
            if not pipeline_model:
                logger.warning(f"Pipeline not found: {pipeline_id}")
                return
            
            # Create metrics record
            metrics_record = PipelineMetricsModel(
                pipeline_id=pipeline_id,
                timestamp=datetime.utcnow(),
                throughput_events_per_sec=metrics.get("throughput_events_per_sec", 0.0),
                lag_seconds=metrics.get("lag_seconds", 0.0),
                error_count=metrics.get("error_count", 0),
                bytes_processed=metrics.get("bytes_processed", 0),
                source_offset=metrics.get("source_offset"),
                target_offset=metrics.get("target_offset"),
                metadata=metrics
            )
            
            self.db_session.add(metrics_record)
            self.db_session.commit()
            
        except Exception as e:
            logger.error(f"Error storing metrics: {e}", exc_info=True)
            self.db_session.rollback()
    
    def get_metrics_history(
        self,
        pipeline_id: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get metrics history for a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            hours: Number of hours to look back
            
        Returns:
            List of metrics records
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            metrics = self.db_session.query(PipelineMetricsModel).filter(
                and_(
                    PipelineMetricsModel.pipeline_id == pipeline_id,
                    PipelineMetricsModel.timestamp >= cutoff_time
                )
            ).order_by(desc(PipelineMetricsModel.timestamp)).all()
            
            return [
                {
                    "timestamp": m.timestamp.isoformat(),
                    "throughput_events_per_sec": m.throughput_events_per_sec,
                    "lag_seconds": m.lag_seconds,
                    "error_count": m.error_count,
                    "bytes_processed": m.bytes_processed,
                    "source_offset": m.source_offset,
                    "target_offset": m.target_offset,
                    "metadata": m.metadata
                }
                for m in metrics
            ]
            
        except Exception as e:
            logger.error(f"Error getting metrics history: {e}", exc_info=True)
            return []


