"""Pipeline service for orchestrating full load and CDC."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ingestion.cdc_manager import CDCManager
from ingestion.models import Pipeline, Connection, PipelineMode, PipelineStatus, FullLoadStatus, CDCStatus

logger = logging.getLogger(__name__)


class PipelineService:
    """Service for managing pipeline lifecycle."""
    
    def __init__(self, cdc_manager: CDCManager):
        """Initialize pipeline service.
        
        Args:
            cdc_manager: CDC Manager instance
        """
        self.cdc_manager = cdc_manager
    
    def create_pipeline(
        self,
        pipeline: Pipeline,
        source_connection: Connection,
        target_connection: Connection
    ) -> Pipeline:
        """Create a new pipeline.
        
        Args:
            pipeline: Pipeline object with the following required parameters:
                - id (str): Unique pipeline identifier (UUID)
                - name (str): Pipeline name
                - source_connection_id (str): Source connection ID
                - target_connection_id (str): Target connection ID
                - source_database (str): Source database name
                - source_schema (str): Source schema name
                - source_tables (List[str]): List of source table names
                
                Optional parameters with default values:
                - target_database (Optional[str]): Target database name (defaults to target_connection.database)
                - target_schema (Optional[str]): Target schema name (defaults based on database type:
                    - "dbo" for SQL Server/MSSQL
                    - "PUBLIC" for Snowflake
                    - "public" for PostgreSQL and others)
                - target_tables (Optional[List[str]]): List of target table names (defaults to source_tables)
                - mode (str): Pipeline mode (default: PipelineMode.FULL_LOAD_AND_CDC)
                    Options: "full_load_only", "cdc_only", "full_load_and_cdc"
                - enable_full_load (Optional[bool]): Deprecated, use mode instead (default: None)
                - auto_create_target (bool): Auto-create target tables (default: True)
                - target_table_mapping (Optional[Dict[str, str]]): Source to target table mapping (default: None)
                - table_filter (Optional[str]): Table filter expression (default: None)
                - full_load_status (str): Full load status (default: FullLoadStatus.NOT_STARTED)
                - full_load_lsn (Optional[str]): Full load LSN/offset (default: None)
                - cdc_status (str): CDC status (default: CDCStatus.NOT_STARTED)
                - debezium_connector_name (Optional[str]): Debezium connector name (default: None)
                - sink_connector_name (Optional[str]): Sink connector name (default: None)
                - kafka_topics (Optional[List[str]]): Kafka topic names (default: None)
                - debezium_config (Optional[Dict[str, Any]]): Debezium connector config (default: None)
                - sink_config (Optional[Dict[str, Any]]): Sink connector config (default: None)
                - status (str): Pipeline status (default: PipelineStatus.STOPPED)
                - created_at (Optional[datetime]): Creation timestamp (default: datetime.utcnow())
                - updated_at (Optional[datetime]): Update timestamp (default: datetime.utcnow())
                
            source_connection: Source connection object with required fields:
                - id (str): Connection ID
                - name (str): Connection name
                - connection_type (str): "source" or "target"
                - database_type (str): Database type (e.g., "postgresql", "sqlserver", "oracle", "snowflake")
                - host (str): Database host
                - port (int): Database port
                - database (str): Database name
                - username (str): Database username
                - password (str): Database password
                - schema (Optional[str]): Default schema (optional)
                - additional_config (Optional[Dict[str, Any]]): Additional connection config (optional)
                
            target_connection: Target connection object with same structure as source_connection
            
        Returns:
            Created pipeline with all default values applied
            
        Example:
            ```python
            pipeline = Pipeline(
                id=str(uuid.uuid4()),
                name="my-pipeline",
                source_connection_id="source-conn-id",
                target_connection_id="target-conn-id",
                source_database="source_db",
                source_schema="public",
                source_tables=["table1", "table2"],
                mode=PipelineMode.FULL_LOAD_AND_CDC,
                target_schema="dbo"  # For SQL Server target
            )
            
            created = pipeline_service.create_pipeline(
                pipeline=pipeline,
                source_connection=source_conn,
                target_connection=target_conn
            )
            ```
        """
        return self.cdc_manager.create_pipeline(
            pipeline=pipeline,
            source_connection=source_connection,
            target_connection=target_connection
        )
    
    def start_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Start a pipeline (full load → CDC).
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Startup result
        """
        return self.cdc_manager.start_pipeline(pipeline_id)
    
    def stop_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Stop a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Stop result
        """
        return self.cdc_manager.stop_pipeline(pipeline_id)
    
    def pause_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Pause a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pause result
        """
        return self.cdc_manager.pause_pipeline(pipeline_id)
    
    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline status
        """
        return self.cdc_manager.get_pipeline_status(pipeline_id)
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all pipelines.
        
        Returns:
            List of pipelines
        """
        return self.cdc_manager.list_pipelines()

