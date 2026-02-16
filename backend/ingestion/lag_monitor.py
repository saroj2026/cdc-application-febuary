"""Replication lag monitoring for CDC pipelines."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from ingestion.connection_service import ConnectionService
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector

logger = logging.getLogger(__name__)


class LagMonitor:
    """Monitor replication lag across the pipeline."""
    
    def __init__(self, connection_service: ConnectionService):
        """Initialize lag monitor.
        
        Args:
            connection_service: Connection service instance
        """
        self.connection_service = connection_service
    
    def calculate_lag(
        self,
        source_connection_id: str,
        target_connection_id: str,
        source_database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate replication lag between source and target.
        
        Args:
            source_connection_id: Source connection ID
            target_connection_id: Target connection ID
            source_database: Source database name (optional)
            
        Returns:
            Lag information dictionary
        """
        try:
            # Get source LSN/timestamp
            source_lsn_info = self._get_source_lsn(
                source_connection_id,
                source_database
            )
            
            # Get target LSN/timestamp (if available)
            target_lsn_info = self._get_target_lsn(
                target_connection_id
            )
            
            # Calculate lag
            lag_seconds = 0.0
            
            # Check for PostgreSQL replication stats (more accurate)
            if source_lsn_info.get("metadata") and "replication_stats" in source_lsn_info["metadata"]:
                stats = source_lsn_info["metadata"]["replication_stats"]
                # Find relevant replication slot/application if possible, or take max lag
                current_max_lag = 0.0
                for stat in stats:
                    # Prefer replay_lag, then flush_lag, then write_lag
                    stat_lag = stat.get("replay_lag_seconds") or stat.get("flush_lag_seconds") or stat.get("write_lag_seconds") or 0.0
                    if stat_lag > current_max_lag:
                        current_max_lag = stat_lag
                
                if current_max_lag > 0:
                    lag_seconds = current_max_lag
            
            # Fallback to timestamp diff if no replication stats
            elif source_lsn_info.get("timestamp") and target_lsn_info.get("timestamp"):
                source_time = datetime.fromisoformat(source_lsn_info["timestamp"].replace("Z", "+00:00"))
                target_time = datetime.fromisoformat(target_lsn_info["timestamp"].replace("Z", "+00:00"))
                lag = (source_time - target_time).total_seconds()
                lag_seconds = max(0.0, lag)
            
            return {
                "source_lsn": source_lsn_info.get("lsn"),
                "source_timestamp": source_lsn_info.get("timestamp"),
                "target_lsn": target_lsn_info.get("lsn"),
                "target_timestamp": target_lsn_info.get("timestamp"),
                "lag_seconds": lag_seconds,
                "lag_status": "normal" if lag_seconds < 60 else ("warning" if lag_seconds < 300 else "critical"),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error calculating lag: {e}", exc_info=True)
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def _get_source_lsn(
        self,
        connection_id: str,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get source database LSN/offset.
        
        Args:
            connection_id: Connection ID
            database: Database name (optional)
            
        Returns:
            LSN information
        """
        try:
            from ingestion.database.models_db import ConnectionModel
            connection = self.connection_service.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {"error": f"Connection not found: {connection_id}"}
            
            connector = self.connection_service._get_connector(connection)
            
            if connection.database_type == "postgresql":
                lsn_info = connector.extract_lsn_offset(database=database or connection.database)
                return {
                    "lsn": lsn_info.get("lsn"),
                    "offset": lsn_info.get("offset"),
                    "timestamp": lsn_info.get("timestamp")
                }
            elif connection.database_type == "sqlserver":
                lsn_info = connector.extract_lsn_offset(database=database or connection.database)
                return {
                    "lsn": lsn_info.get("lsn"),
                    "offset": lsn_info.get("offset"),
                    "timestamp": lsn_info.get("timestamp")
                }
            else:
                return {"error": f"Unsupported database type: {connection.database_type}"}
                
        except Exception as e:
            logger.error(f"Error getting source LSN: {e}")
            return {"error": str(e)}
    
    def _get_target_lsn(
        self,
        connection_id: str
    ) -> Dict[str, Any]:
        """Get target database LSN/offset (if applicable).
        
        Args:
            connection_id: Connection ID
            
        Returns:
            LSN information
        """
        try:
            from ingestion.database.models_db import ConnectionModel
            connection = self.connection_service.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {"error": f"Connection not found: {connection_id}"}
            
            # For now, return current timestamp as target
            # In a full implementation, we'd track the last committed LSN in target
            return {
                "lsn": None,
                "offset": None,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting target LSN: {e}")
            return {"error": str(e)}
    
    def get_lag_trend(
        self,
        pipeline_id: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get lag trend over time.
        
        Args:
            pipeline_id: Pipeline ID
            hours: Number of hours to look back
            
        Returns:
            List of lag measurements
        """
        try:
            from ingestion.database.models_db import PipelineMetricsModel
            from sqlalchemy import and_
            
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            metrics = self.connection_service.session.query(PipelineMetricsModel).filter(
                and_(
                    PipelineMetricsModel.pipeline_id == pipeline_id,
                    PipelineMetricsModel.timestamp >= cutoff_time
                )
            ).order_by(PipelineMetricsModel.timestamp).all()
            
            return [
                {
                    "timestamp": m.timestamp.isoformat(),
                    "lag_seconds": m.lag_seconds
                }
                for m in metrics
            ]
            
        except Exception as e:
            logger.error(f"Error getting lag trend: {e}", exc_info=True)
            return []


