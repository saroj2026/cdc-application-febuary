"""CDC Manager service for managing CDC pipelines."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from ingestion.models import Connection, Pipeline, PipelineStatus, FullLoadStatus, CDCStatus, PipelineMode
from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.debezium_config import DebeziumConfigGenerator
from ingestion.sink_config import SinkConfigGenerator
from ingestion.transfer import DataTransfer
from ingestion.connectors import SQLServerConnector, PostgreSQLConnector
from ingestion.connectors.base_connector import BaseConnector
from ingestion.schema_service import SchemaService
from ingestion.exceptions import FullLoadError, ValidationError
from ingestion.validation import validate_source_data, validate_target_row_count

logger = logging.getLogger(__name__)

# Global reference to database session factory (set from api.py)
_db_session_factory = None

def set_db_session_factory(session_factory):
    """Set the database session factory for status persistence."""
    global _db_session_factory
    _db_session_factory = session_factory


class CDCManager:
    """Manages CDC pipeline lifecycle and operations."""
    
    def __init__(
        self,
        kafka_connect_url: str = "http://localhost:8083",
        connection_store: Optional[Dict[str, Connection]] = None
    ):
        """Initialize CDC Manager.
        
        Args:
            kafka_connect_url: Kafka Connect REST API URL
            connection_store: Dictionary to store connections (in-memory for now)
        """
        self.kafka_client = KafkaConnectClient(base_url=kafka_connect_url)
        self.connection_store = connection_store or {}
        self.pipeline_store: Dict[str, Pipeline] = {}
        self.schema_service = SchemaService()
    
    def add_connection(self, connection: Connection) -> None:
        """Add a connection to the store.
        
        Args:
            connection: Connection object to add
        """
        self.connection_store[connection.id] = connection
        logger.info(f"Added connection: {connection.name} ({connection.id})")
    
    def get_connection(self, connection_id: str) -> Optional[Connection]:
        """Get connection by ID.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Connection object or None if not found
        """
        return self.connection_store.get(connection_id)
    
    def create_pipeline(
        self,
        pipeline: Pipeline,
        source_connection: Connection,
        target_connection: Connection
    ) -> Pipeline:
        """Create a new CDC pipeline.
        
        Args:
            pipeline: Pipeline object
            source_connection: Source database connection
            target_connection: Target database connection
            
        Returns:
            Created pipeline
        """
        # Store connections
        self.add_connection(source_connection)
        self.add_connection(target_connection)
        
        # Store pipeline
        self.pipeline_store[pipeline.id] = pipeline
        
        logger.info(f"Created pipeline: {pipeline.name} ({pipeline.id})")
        return pipeline
    
    def _persist_pipeline_status(self, pipeline: Pipeline) -> None:
        """Persist pipeline status to database.
        
        Args:
            pipeline: Pipeline object with updated status
        """
        if not _db_session_factory:
            logger.debug("Database session factory not set, skipping status persistence")
            return
        
        try:
            from ingestion.database.models_db import PipelineStatus as DBPipelineStatus, FullLoadStatus as DBFullLoadStatus, CDCStatus as DBCDCStatus, PipelineModel
            from datetime import datetime
            
            db = next(_db_session_factory())
            try:
                pipeline_model = db.query(PipelineModel).filter(PipelineModel.id == pipeline.id).first()
                if not pipeline_model:
                    logger.warning(f"Pipeline {pipeline.id} not found in database for status update")
                    return
                
                # Update status fields - handle both enum objects and strings
                try:
                    # Convert status - handle both enum objects and string values
                    if isinstance(pipeline.status, str):
                        pipeline_model.status = DBPipelineStatus(pipeline.status)
                    elif hasattr(pipeline.status, 'value'):
                        pipeline_model.status = DBPipelineStatus(pipeline.status.value)
                    else:
                        pipeline_model.status = DBPipelineStatus.RUNNING
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to set status: {e}, using RUNNING")
                    pipeline_model.status = DBPipelineStatus.RUNNING
                
                try:
                    # Convert full_load_status - handle both enum objects and string values
                    if isinstance(pipeline.full_load_status, str):
                        pipeline_model.full_load_status = DBFullLoadStatus(pipeline.full_load_status)
                    elif hasattr(pipeline.full_load_status, 'value'):
                        pipeline_model.full_load_status = DBFullLoadStatus(pipeline.full_load_status.value)
                    else:
                        pipeline_model.full_load_status = DBFullLoadStatus.NOT_STARTED
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to set full_load_status: {e}, using NOT_STARTED")
                    pipeline_model.full_load_status = DBFullLoadStatus.NOT_STARTED
                
                try:
                    # Convert cdc_status - handle both enum objects and string values
                    if isinstance(pipeline.cdc_status, str):
                        pipeline_model.cdc_status = DBCDCStatus(pipeline.cdc_status)
                    elif hasattr(pipeline.cdc_status, 'value'):
                        pipeline_model.cdc_status = DBCDCStatus(pipeline.cdc_status.value)
                    else:
                        pipeline_model.cdc_status = DBCDCStatus.NOT_STARTED
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to set cdc_status: {e}, using NOT_STARTED")
                    pipeline_model.cdc_status = DBCDCStatus.NOT_STARTED
                
                # Update other fields
                pipeline_model.full_load_lsn = pipeline.full_load_lsn
                pipeline_model.debezium_connector_name = pipeline.debezium_connector_name
                pipeline_model.sink_connector_name = pipeline.sink_connector_name
                pipeline_model.kafka_topics = pipeline.kafka_topics or []
                pipeline_model.debezium_config = pipeline.debezium_config or {}
                pipeline_model.sink_config = pipeline.sink_config or {}
                pipeline_model.updated_at = datetime.utcnow()
                
                # Set full_load_completed_at if full load is completed
                # Check both enum object and string value
                full_load_completed = (
                    pipeline.full_load_status == FullLoadStatus.COMPLETED or
                    (isinstance(pipeline.full_load_status, str) and pipeline.full_load_status == "COMPLETED") or
                    (hasattr(pipeline.full_load_status, 'value') and pipeline.full_load_status.value == "COMPLETED")
                )
                if full_load_completed:
                    pipeline_model.full_load_completed_at = datetime.utcnow()
                
                db.commit()
                logger.info(
                    f"Persisted pipeline {pipeline.id} status to database: "
                    f"status={pipeline_model.status.value}, "
                    f"full_load_status={pipeline_model.full_load_status.value}, "
                    f"cdc_status={pipeline_model.cdc_status.value}"
                )
            except Exception as e:
                db.rollback()
                logger.error(f"Failed to persist pipeline status to database: {e}", exc_info=True)
                raise
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Error in _persist_pipeline_status: {e}", exc_info=True)
            # Don't raise - allow pipeline to continue even if status persistence fails
    
    def start_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Start a CDC pipeline.
        
        Default flow for any source and target (when mode is full_load_and_cdc):
        
        1. Auto-create schema: Create target schema and tables (source schema + CDC metadata
           columns: row_id, __op, __source_ts_ms, __deleted for SQL Server SCD2-style history).
        2. Full load: Copy existing data from source to target (with __op='r' for initial load).
        3. CDC: Create Debezium source connector and JDBC Sink connector; stream changes.
        
        Steps 1 and 2 are skipped if full load was already completed. Step 3 is skipped
        for full_load_only mode.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Dictionary with startup results
        """
        pipeline = self.pipeline_store.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        source_connection = self.get_connection(pipeline.source_connection_id)
        target_connection = self.get_connection(pipeline.target_connection_id)
        
        if not source_connection or not target_connection:
            raise ValueError("Source or target connection not found")
        
        result = {
            "pipeline_id": pipeline_id,
            "id": pipeline_id,  # Add id field for frontend compatibility
            "full_load": {},
            "debezium_connector": {},
            "sink_connector": {},
            "kafka_topics": [],
            "status": "STARTING",
            "source_connection_id": pipeline.source_connection_id,  # Add for frontend
            "target_connection_id": pipeline.target_connection_id,  # Add for frontend
            "source_uuid": pipeline.source_connection_id,  # Alias for frontend compatibility
            "target_uuid": pipeline.target_connection_id  # Alias for frontend compatibility
        }
        
        try:
            # Update pipeline status
            pipeline.status = PipelineStatus.STARTING
            # IMPORTANT: Don't reset full_load_status if it's already COMPLETED
            # Only reset if we're actually going to run a new full load
            # This preserves the completed status and LSN for CDC
            if pipeline.full_load_status != FullLoadStatus.COMPLETED:
                pipeline.full_load_status = FullLoadStatus.NOT_STARTED
            else:
                logger.info(f"Preserving full_load_status=COMPLETED (LSN: {pipeline.full_load_lsn})")
            pipeline.cdc_status = CDCStatus.STARTING
            # Persist initial status
            self._persist_pipeline_status(pipeline)
            
            # Determine pipeline mode
            mode = pipeline.mode
            if isinstance(mode, str):
                mode = PipelineMode(mode)
            
            # DB2 / AS400 source: use Debezium initial snapshot for "full load" (no backend ODBC needed)
            # Same path for as400 or db2 — use existing AS400 connection (e.g. f1bbe13a) with Db2 connector
            source_db_type = (source_connection.database_type.value if hasattr(source_connection.database_type, "value") else str(source_connection.database_type)).lower()
            source_uses_debezium_snapshot = source_db_type in ["db2", "as400", "ibm_i"]
            logger.info(f"start_pipeline: source_db_type={source_db_type!r}, source_uses_debezium_snapshot={source_uses_debezium_snapshot}, mode={getattr(mode, 'value', mode)}")
            if source_uses_debezium_snapshot and mode in [PipelineMode.FULL_LOAD_AND_CDC, PipelineMode.FULL_LOAD_ONLY]:
                logger.info("Source is DB2/AS400: using Debezium initial snapshot for initial load (no backend ODBC required)")
                if pipeline.full_load_status != FullLoadStatus.COMPLETED:
                    pipeline.full_load_status = FullLoadStatus.COMPLETED
                    pipeline.full_load_lsn = None
                    self._persist_pipeline_status(pipeline)
                result["full_load"] = {
                    "success": True,
                    "message": "Initial load will be done by Debezium snapshot (DB2/AS400 source, no backend full load)",
                    "tables_transferred": 0,
                    "total_rows": 0,
                    "lsn": None
                }
                if mode == PipelineMode.FULL_LOAD_ONLY:
                    logger.info("Step 2: FULL_LOAD_ONLY with DB2/AS400 source - use Debezium snapshot then stop, or run full_load_and_cdc")
                    pipeline.status = PipelineStatus.RUNNING
                    self._persist_pipeline_status(pipeline)
                    return result
            
            # Step 0: Auto-create target schema/tables if enabled
            # Skip if full load is already completed (schema/tables already exist)
            # Skip for DB2/AS400 source (no ODBC on backend; tables created by Sink auto.create when Debezium snapshot arrives)
            if source_uses_debezium_snapshot:
                logger.info("Step 0: DB2/AS400/IBM i source (Sink will auto-create tables), skipping schema creation (no ODBC)")
            elif pipeline.auto_create_target and pipeline.full_load_status != FullLoadStatus.COMPLETED:
                logger.info(f"Step 0: Auto-creating target schema/tables for pipeline: {pipeline.name}")
                try:
                    # For S3 targets, skip schema creation (S3 doesn't support schemas)
                    if target_connection.database_type == "s3":
                        logger.info("Step 0: Skipping schema creation for S3 target (S3 doesn't support schemas)")
                    else:
                        schema_created = self._auto_create_target_schema(
                            pipeline=pipeline,
                            source_connection=source_connection,
                            target_connection=target_connection
                        )
                        if schema_created:
                            logger.info("Step 0: Target schema/tables created successfully")
                        else:
                            logger.warning("Step 0: Target schema/tables creation returned False, but continuing")
                except Exception as e:
                    logger.error(f"Step 0: Auto-create target schema failed: {e}", exc_info=True)
                    # Don't continue if schema creation fails - it's critical for full load
                    raise FullLoadError(
                        f"Failed to create target schema/tables: {str(e)}. Full load cannot proceed without target schema.",
                        rows_transferred=0,
                        error=str(e)
                    )
            else:
                logger.info("Step 0: Full load already completed or schema creation disabled, skipping")
            
            # Step 1: Run full load if mode requires it AND it hasn't been completed yet
            if mode in [PipelineMode.FULL_LOAD_ONLY, PipelineMode.FULL_LOAD_AND_CDC]:
                # Skip full load for DB2/AS400 (Debezium snapshot does initial load; no backend ODBC)
                _full_done = (
                    getattr(pipeline.full_load_status, "value", str(pipeline.full_load_status or "")).lower() == "completed"
                    or source_uses_debezium_snapshot
                )
                if _full_done:
                    if source_uses_debezium_snapshot:
                        pipeline.full_load_status = FullLoadStatus.COMPLETED
                        pipeline.full_load_lsn = None
                        self._persist_pipeline_status(pipeline)
                        logger.info("Step 1: DB2/AS400 source - skipping backend full load (Debezium snapshot will do initial load)")
                    else:
                        logger.info(f"Step 1: Full load already completed, skipping (status: {pipeline.full_load_status})")
                    result["full_load"] = {
                        "success": True,
                        "message": "Full load already completed" if not source_uses_debezium_snapshot else "Initial load will be done by Debezium snapshot (DB2/AS400)",
                        "tables_transferred": [],
                        "total_rows": 0,
                        "lsn": pipeline.full_load_lsn
                    }
                    if pipeline.full_load_lsn:
                        logger.info(f"Step 1: Using existing LSN/offset: {pipeline.full_load_lsn}")
                else:
                    # Get default schema based on database type for logging
                    default_target_schema = (
                        "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                        else "PUBLIC" if target_connection.database_type == "snowflake"
                        else "public"
                    )
                    logger.info(f"Step 1: Starting full load for pipeline: {pipeline.name} (mode: {getattr(mode, 'value', mode)})")
                    logger.info(f"Step 1: Source: {pipeline.source_database}.{pipeline.source_schema}, Tables: {pipeline.source_tables}")
                    logger.info(f"Step 1: Target: {pipeline.target_database or target_connection.database}.{pipeline.target_schema or target_connection.schema or default_target_schema}")
                    
                    pipeline.full_load_status = FullLoadStatus.IN_PROGRESS
                    # Persist IN_PROGRESS status to database
                    self._persist_pipeline_status(pipeline)
                    logger.info("Step 1: Full load status set to IN_PROGRESS and persisted to database")
                    
                    full_load_result = self._run_full_load(
                        pipeline=pipeline,
                        source_connection=source_connection,
                        target_connection=target_connection
                    )
                    
                    result["full_load"] = full_load_result
                    
                    # Fix: Use 'is True' instead of truthy check to properly handle False values
                    if full_load_result.get("success") is True:
                        pipeline.full_load_status = FullLoadStatus.COMPLETED
                        # Extract LSN from result - handle both "lsn" key and nested "lsn_offset" dict
                        lsn_value = full_load_result.get("lsn")
                        if not lsn_value:
                            # Try to extract from lsn_offset dict (AS400 returns it this way)
                            lsn_offset = full_load_result.get("lsn_offset", {})
                            if isinstance(lsn_offset, dict):
                                lsn_value = lsn_offset.get("lsn")
                        
                        # CRITICAL: Store LSN for CDC to start from this offset
                        pipeline.full_load_lsn = lsn_value
                        tables_transferred = full_load_result.get("tables_transferred", 0)
                        total_rows = full_load_result.get("total_rows", 0)
                        
                        logger.info(f"Step 1: Full load completed successfully!")
                        logger.info(f"Step 1: Tables transferred: {tables_transferred}, Total rows: {total_rows}")
                        
                        if lsn_value:
                            logger.info(f"Step 1: ✅ LSN/offset captured: {lsn_value}")
                            logger.info(f"Step 1: CDC will start from this offset (no duplicate data)")
                        else:
                            logger.warning(f"Step 1: ⚠️  LSN/offset NOT captured - CDC will use initial snapshot (may have duplicates)")
                        # Persist COMPLETED status to database
                        self._persist_pipeline_status(pipeline)
                        logger.info("Step 1: Full load status set to COMPLETED and persisted to database")
                    else:
                        pipeline.full_load_status = FullLoadStatus.FAILED
                        error_msg = full_load_result.get('error', 'Unknown error')
                        logger.error(f"Step 1: Full load failed: {error_msg}")
                        # Persist FAILED status to database
                        self._persist_pipeline_status(pipeline)
                        raise FullLoadError(
                            f"Full load failed: {error_msg}",
                            rows_transferred=full_load_result.get('total_rows', 0)
                        )
            
            # If mode is FULL_LOAD_ONLY, skip CDC setup
            if mode == PipelineMode.FULL_LOAD_ONLY:
                logger.info(f"Step 2: Pipeline mode is FULL_LOAD_ONLY, skipping CDC setup")
                pipeline.cdc_status = CDCStatus.NOT_STARTED
                pipeline.status = PipelineStatus.RUNNING
                result["status"] = "RUNNING"
                result["message"] = "Full load completed. CDC is disabled for this pipeline."
                # Persist final status
                self._persist_pipeline_status(pipeline)
                logger.info("Step 2: Pipeline running in FULL_LOAD_ONLY mode (CDC disabled)")
                return result
            
            # Step 2: Generate and create Debezium connector (for CDC modes)
            logger.info(f"Step 2: Starting CDC setup for pipeline: {pipeline.name} (mode: {mode.value})")
            logger.info(f"Step 2: Full load completed, now setting up CDC connectors")
            
            # FIX: Ensure CDC status is set to STARTING before attempting setup
            pipeline.cdc_status = CDCStatus.STARTING
            self._persist_pipeline_status(pipeline)
            logger.info(f"CDC status set to STARTING and persisted")
            
            # Check if configs already exist in pipeline (loaded from database)
            # If they exist and connectors are running, reuse them
            debezium_exists = False
            if pipeline.debezium_config and pipeline.debezium_connector_name:
                logger.info(f"Pipeline has existing Debezium config and connector name: {pipeline.debezium_connector_name}")
                try:
                    connector_status = self.kafka_client.get_connector_status(pipeline.debezium_connector_name)
                    if connector_status is None:
                        # Connector doesn't exist, will create new one
                        logger.info(f"Debezium connector {pipeline.debezium_connector_name} not found, will create new one")
                        debezium_exists = False
                    else:
                        connector_state = connector_status.get('connector', {}).get('state', 'UNKNOWN')
                        if connector_state == 'RUNNING':
                            logger.info(f"Existing Debezium connector {pipeline.debezium_connector_name} is RUNNING, reusing it")
                            # Skip connector creation, but still need to discover topics and create sink
                            debezium_exists = True
                        else:
                            logger.info(f"Existing Debezium connector {pipeline.debezium_connector_name} state is {connector_state}, will recreate")
                            debezium_exists = False
                except Exception as e:
                    logger.warning(f"Could not check existing Debezium connector status: {e}, will create new one")
                    debezium_exists = False
            else:
                logger.info("No existing Debezium config found, will generate new one")
                debezium_exists = False
            
            # Determine snapshot mode based on mode and full load status
            # Handle both enum and string values for mode
            mode_value = mode.value if hasattr(mode, 'value') else str(mode)
            has_full_load_lsn = bool(pipeline.full_load_lsn)
            full_load_completed = pipeline.full_load_status == FullLoadStatus.COMPLETED
            
            # FIX: Log LSN status for debugging
            logger.info(f"CDC Setup - Full load status: {pipeline.full_load_status}, LSN: {pipeline.full_load_lsn}, Has LSN: {has_full_load_lsn}")
            
            # Decision logic:
            # 1. CDC_ONLY: Never snapshot, start streaming immediately
            # 2. FULL_LOAD_AND_CDC with completed full load: Never snapshot (data already loaded)
            # 3. FULL_LOAD_AND_CDC without full load: Initial snapshot (capture existing data)
            # 4. FULL_LOAD_ONLY: Should not reach here, but if it does, use initial_only
            # 5. Default: Initial snapshot
            
            # Check if source is Oracle (Oracle doesn't support "never" snapshot mode)
            is_oracle = source_connection.database_type.lower() == "oracle"
            
            if mode_value == PipelineMode.CDC_ONLY.value or mode_value == "cdc_only":
                # For Oracle, use "initial_only" instead of "never" (Oracle doesn't support "never")
                snapshot_mode = "initial_only" if is_oracle else "never"
                reason = f"CDC_ONLY mode - {'schema only (Oracle)' if is_oracle else 'streaming changes only'}"
            elif (mode_value == PipelineMode.FULL_LOAD_AND_CDC.value or mode_value == "full_load_and_cdc"):
                if full_load_completed and has_full_load_lsn:
                    # For Oracle, use "initial_only" instead of "never" (Oracle doesn't support "never")
                    snapshot_mode = "initial_only" if is_oracle else "never"
                    reason = f"Full load completed - {'schema only (Oracle)' if is_oracle else 'streaming changes only'}"
                else:
                    snapshot_mode = "initial"
                    reason = "Full load not completed - capturing initial snapshot"
            elif has_full_load_lsn:
                snapshot_mode = "initial_only"
                reason = "Full load LSN present - schema only"
            else:
                snapshot_mode = "initial"
                reason = "No full load - capturing initial snapshot"
            
            logger.info(
                f"Snapshot mode determined: {snapshot_mode} "
                f"(mode={mode_value}, full_load_status={pipeline.full_load_status}, "
                f"full_load_lsn={has_full_load_lsn}, reason={reason})"
            )
            
            # FIX: Validate source connection before generating config
            try:
                logger.info(f"Validating source connection for CDC setup...")
                # Test connection is available (basic validation)
                if not source_connection.host or not source_connection.port:
                    raise ValueError(f"Source connection missing required fields: host={source_connection.host}, port={source_connection.port}")
                logger.info(f"Source connection validated: {source_connection.database_type}://{source_connection.host}:{source_connection.port}")
            except Exception as e:
                error_msg = f"Source connection validation failed: {str(e)}"
                logger.error(error_msg)
                pipeline.cdc_status = CDCStatus.ERROR
                self._persist_pipeline_status(pipeline)
                raise Exception(error_msg) from e
            
            # FIX: Validate and normalize schema name before generating connector name
            # This prevents issues like "databa" vs "public" inconsistency
            source_schema = pipeline.source_schema or source_connection.schema or "public"
            # Normalize schema name - handle common truncations/typos
            if source_schema.lower() in ["databa", "datab", "dat"]:
                logger.warning(f"Detected potentially truncated schema name '{source_schema}', using 'public' as fallback")
                source_schema = "public"
            
            debezium_config = DebeziumConfigGenerator.generate_source_config(
                pipeline_name=pipeline.name,
                source_connection=source_connection,
                source_database=pipeline.source_database,
                source_schema=source_schema,  # Use normalized schema
                source_tables=pipeline.source_tables,
                full_load_lsn=pipeline.full_load_lsn,
                snapshot_mode=snapshot_mode
            )
            
            debezium_connector_name = DebeziumConfigGenerator.generate_connector_name(
                pipeline_name=pipeline.name,
                database_type=source_connection.database_type,
                schema=source_schema
            )
            
            # FIX: Log generated connector name and key config values
            logger.info(f"Generated Debezium connector name: {debezium_connector_name}")
            logger.info(f"Source schema used: {source_schema} (from pipeline: {pipeline.source_schema}, connection: {source_connection.schema})")
            logger.info(f"Debezium config snapshot.mode: {debezium_config.get('snapshot.mode', 'N/A')}")
            if pipeline.full_load_lsn:
                logger.info(f"Using full load LSN: {pipeline.full_load_lsn}")
            
            # FIX: Store connector name immediately (before creation attempt)
            # This ensures we have it even if creation fails
            pipeline.debezium_connector_name = debezium_connector_name
            self._persist_pipeline_status(pipeline)
            logger.info(f"Stored Debezium connector name in pipeline: {debezium_connector_name}")
            
            # Check if Debezium connector already exists and is running
            # Note: This is different from the earlier check - here we check using the newly generated connector name
            if not debezium_exists:
                try:
                    connector_info = self.kafka_client.get_connector_info(debezium_connector_name)
                    connector_status = self.kafka_client.get_connector_status(debezium_connector_name)
                    if connector_status is not None:
                        connector_state = connector_status.get('connector', {}).get('state', 'UNKNOWN')
                        connector_error = connector_status.get('connector', {}).get('error', '')
                        
                        # Check connector config to see if it has invalid slot name
                        connector_config = None
                        try:
                            connector_config = self.kafka_client.get_connector_config(debezium_connector_name)
                            old_slot_name = connector_config.get('slot.name', '') if connector_config else ''
                            # Check if old slot name contains hyphens (invalid)
                            if old_slot_name and '-' in old_slot_name:
                                logger.warning(f"Found connector with invalid slot name '{old_slot_name}' (contains hyphens), will delete and recreate")
                                self.kafka_client.delete_connector(debezium_connector_name)
                                logger.info(f"Deleted connector with invalid slot name: {debezium_connector_name}")
                                # Continue to create new connector below
                            elif connector_state == 'RUNNING':
                                # Check if slot name matches the new normalized one
                                new_slot_name = debezium_config.get('slot.name', '')
                                if old_slot_name != new_slot_name:
                                    logger.warning(f"Connector slot name mismatch: old='{old_slot_name}' vs new='{new_slot_name}', will recreate")
                                    self.kafka_client.delete_connector(debezium_connector_name)
                                else:
                                    logger.info(f"Debezium connector {debezium_connector_name} already exists and is RUNNING, reusing it")
                                    debezium_exists = True
                                    pipeline.debezium_connector_name = debezium_connector_name
                                    pipeline.debezium_config = connector_config
                        except Exception as config_error:
                            logger.warning(f"Could not get connector config: {config_error}")
                        
                        # If connector still exists and is RUNNING, use it
                        if connector_state == 'RUNNING' and not debezium_exists:
                            logger.info(f"Debezium connector {debezium_connector_name} already exists and is RUNNING, reusing it")
                            debezium_exists = True
                            pipeline.debezium_connector_name = debezium_connector_name
                            if connector_config:
                                pipeline.debezium_config = connector_config
                            else:
                                pipeline.debezium_config = self.kafka_client.get_connector_config(debezium_connector_name)
                        elif connector_state in ['FAILED', 'STOPPED']:
                            logger.warning(f"Debezium connector {debezium_connector_name} exists but is {connector_state}")
                            
                            # Check if the failure is due to invalid config (like invalid slot name)
                            # If so, delete and recreate with new config
                            if connector_error and ('slot.name' in connector_error.lower() or 'invalid' in connector_error.lower() or 'replication slot' in connector_error.lower()):
                                logger.warning(f"Connector has config validation error (likely invalid slot name), will delete and recreate: {connector_error[:200]}")
                                try:
                                    self.kafka_client.delete_connector(debezium_connector_name)
                                    logger.info(f"Deleted connector with invalid config: {debezium_connector_name}")
                                except Exception as delete_error:
                                    logger.warning(f"Could not delete connector: {delete_error}")
                            else:
                                # Try restart first for other errors
                                logger.info(f"Attempting to restart connector {debezium_connector_name}...")
                            try:
                                self.kafka_client.restart_connector(debezium_connector_name)
                                if self.kafka_client.wait_for_connector(debezium_connector_name, "RUNNING", max_wait_seconds=30):
                                    logger.info(f"Successfully restarted connector {debezium_connector_name}")
                                    debezium_exists = True
                                    pipeline.debezium_connector_name = debezium_connector_name
                                    pipeline.debezium_config = self.kafka_client.get_connector_config(debezium_connector_name)
                                else:
                                    logger.warning(f"Restart failed, will delete and recreate")
                                    self.kafka_client.delete_connector(debezium_connector_name)
                            except Exception as restart_error:
                                logger.warning(f"Could not restart connector: {restart_error}, will delete and recreate")
                                self.kafka_client.delete_connector(debezium_connector_name)
                        else:
                            logger.info(f"Debezium connector {debezium_connector_name} exists but state is {connector_state}, will recreate")
                            # Delete and recreate
                            self.kafka_client.delete_connector(debezium_connector_name)
                            logger.info(f"Deleted existing Debezium connector: {debezium_connector_name}")
                    else:
                        logger.debug(f"Debezium connector {debezium_connector_name} doesn't exist, will create")
                except requests.exceptions.HTTPError as e:
                    # If 404, connector doesn't exist (fine, will create)
                    if e.response and e.response.status_code == 404:
                        logger.debug(f"Debezium connector {debezium_connector_name} doesn't exist, will create")
                    else:
                        logger.warning(f"Could not check Debezium connector existence (continuing): {e}")
                except Exception as e:
                    logger.warning(f"Could not check Debezium connector existence (continuing): {e}")
            
            # Create Debezium connector only if it doesn't exist
            if not debezium_exists:
                logger.info(f"Creating Debezium connector: {debezium_connector_name}")
                # Log the slot name to verify it's correct
                slot_name = debezium_config.get('slot.name', 'N/A')
                logger.info(f"Using replication slot name: {slot_name}")
                logger.info(f"Debezium config keys: {list(debezium_config.keys())}")
                # Log sanitized config (without password) for debugging
                sanitized_config = {k: v for k, v in debezium_config.items() if 'password' not in k.lower()}
                logger.debug(f"Debezium config (sanitized): {sanitized_config}")
                try:
                    # FIX: Add retry logic for transient failures
                    max_retries = 2
                    retry_count = 0
                    last_error = None
                    
                    while retry_count <= max_retries:
                        try:
                            self.kafka_client.create_connector(
                                connector_name=debezium_connector_name,
                                config=debezium_config
                            )
                            # Success - break out of retry loop
                            logger.info(f"Debezium connector created successfully: {debezium_connector_name}")
                            break
                        except requests.exceptions.ConnectionError as e:
                            retry_count += 1
                            last_error = e
                            if retry_count <= max_retries:
                                wait_time = 2 * retry_count  # Exponential backoff: 2s, 4s
                                logger.warning(f"Kafka Connect connection error (attempt {retry_count}/{max_retries + 1}), retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                raise Exception(f"Failed to connect to Kafka Connect after {max_retries + 1} attempts: {str(e)}") from e
                        except requests.exceptions.Timeout as e:
                            retry_count += 1
                            last_error = e
                            if retry_count <= max_retries:
                                wait_time = 2 * retry_count
                                logger.warning(f"Kafka Connect timeout (attempt {retry_count}/{max_retries + 1}), retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                raise Exception(f"Kafka Connect timeout after {max_retries + 1} attempts: {str(e)}") from e
                        except requests.exceptions.HTTPError as e:
                            # HTTP errors are usually not transient - don't retry, break to handle below
                            raise
                        except Exception as e:
                            # Other exceptions - don't retry
                            raise
                    
                except requests.exceptions.HTTPError as e:
                    error_detail = ""
                    status_code = ""
                    
                    # Check if the exception has a detail_message or error_detail attribute (from DetailedHTTPError)
                    # The kafka_connect_client now sets error_detail on the exception
                    if hasattr(e, 'error_detail') and e.error_detail:
                        error_detail = e.error_detail
                        logger.info(f"Using error_detail from exception: {error_detail[:200]}")
                    elif hasattr(e, 'detail_message') and e.detail_message:
                        error_detail = e.detail_message
                        logger.info(f"Using detail_message from exception: {error_detail[:200]}")
                    elif hasattr(e, 'args') and len(e.args) > 0 and isinstance(e.args[0], str) and "Kafka Connect error:" in e.args[0]:
                        # Extract from exception message if it contains "Kafka Connect error:"
                        error_detail = e.args[0].replace("Kafka Connect error:", "").strip()
                        logger.info(f"Extracted error_detail from exception message: {error_detail[:200]}")
                    else:
                        # Fallback: try to extract from response
                        if e.response:
                            status_code = f" (HTTP {e.response.status_code})"
                            try:
                                error_json = e.response.json()
                                logger.error(f"Kafka Connect {e.response.status_code} error response: {json.dumps(error_json, indent=2)[:1000]}")
                                
                                # Try multiple possible error message fields
                                extracted_detail = (
                                    error_json.get('message') or 
                                    error_json.get('error') or 
                                    error_json.get('error_code') or
                                    error_json.get('error_message') or
                                    ""
                                )
                                
                                # If we have config validation errors, include them
                                if 'configs' in error_json:
                                    config_errors = []
                                    for config_item in error_json.get('configs', []):
                                        if isinstance(config_item, dict):
                                            if 'value' in config_item and isinstance(config_item['value'], dict) and 'errors' in config_item['value']:
                                                config_errors.extend(config_item['value']['errors'])
                                            if 'errors' in config_item:
                                                config_errors.extend(config_item['errors'])
                                    if config_errors:
                                        extracted_detail = '; '.join(config_errors[:5]) if not extracted_detail else f"{extracted_detail}; {'; '.join(config_errors[:5])}"
                                
                                # Use extracted detail if available
                                if extracted_detail and extracted_detail.strip():
                                    error_detail = extracted_detail
                                elif hasattr(e.response, 'text') and e.response.text:
                                    error_detail = e.response.text[:1000]
                                else:
                                    error_detail = str(error_json)
                                    
                                logger.info(f"Extracted error detail: {error_detail[:200]}")
                            except Exception as parse_error:
                                # Fallback to raw text
                                if hasattr(e.response, 'text') and e.response.text:
                                    error_detail = e.response.text[:1000]
                                    logger.warning(f"Using raw response text as error: {error_detail[:200]}")
                                else:
                                    error_detail = str(e)
                                logger.warning(f"Could not parse error JSON: {parse_error}, using raw text")
                        else:
                            # No response - try to extract from exception message
                            error_detail = str(e)
                            # Try to extract meaningful error from exception message
                            if hasattr(e, 'args') and e.args:
                                error_detail = str(e.args[0]) if e.args[0] else str(e)
                            logger.warning(f"HTTPError without response: {error_detail}")
                    
                    # Ensure we have some error detail
                    if not error_detail or error_detail.strip() == "":
                        error_detail = "Unknown error from Kafka Connect"
                    
                    # FIX: Store error in pipeline config for later retrieval
                    if not pipeline.debezium_config:
                        pipeline.debezium_config = {}
                    pipeline.debezium_config['_last_error'] = {
                        'message': error_detail,
                        'timestamp': datetime.utcnow().isoformat(),
                        'connector_name': debezium_connector_name,
                        'error_type': 'HTTPError',
                        'status_code': e.response.status_code if hasattr(e, 'response') and e.response else None
                    }
                    pipeline.cdc_status = CDCStatus.ERROR
                    self._persist_pipeline_status(pipeline)
                    logger.info(f"Stored detailed error in pipeline config: {error_detail[:200]}...")
                    
                except requests.exceptions.RequestException as e:
                    # Handle other request exceptions (ConnectionError, Timeout, etc.)
                    error_detail = str(e)
                    logger.error(f"Kafka Connect request exception: {error_detail}", exc_info=True)
                    if hasattr(e, 'response') and e.response:
                        try:
                            error_json = e.response.json()
                            if error_json.get('message'):
                                error_detail = error_json.get('message')
                        except:
                            pass
                    
                    # Ensure we have a meaningful error message
                    if not error_detail or error_detail.strip() == "":
                        error_detail = f"Kafka Connect connection error: {type(e).__name__}"
                    
                    # FIX: Store error in pipeline config
                    if not pipeline.debezium_config:
                        pipeline.debezium_config = {}
                    pipeline.debezium_config['_last_error'] = {
                        'message': error_detail,
                        'timestamp': datetime.utcnow().isoformat(),
                        'connector_name': debezium_connector_name,
                        'error_type': type(e).__name__
                    }
                    # FIX: Store connector name even on failure
                    pipeline.debezium_connector_name = debezium_connector_name
                    pipeline.cdc_status = CDCStatus.ERROR
                    self._persist_pipeline_status(pipeline)
                    logger.error(f"Persisted error and connector name to database: {debezium_connector_name}")
                    
                    full_error = f"Failed to create Debezium connector '{debezium_connector_name}': {error_detail}"
                    logger.error(f"Debezium connector creation failed: {full_error}")
                    logger.error(f"Exception type: {type(e).__name__}, Exception args: {e.args}")
                    logger.error(f"Connector name '{debezium_connector_name}' has been stored in database for debugging")
                    raise Exception(full_error) from e
                except Exception as e:
                    # Handle any other exception
                    error_detail = str(e)
                    logger.error(f"Unexpected exception when creating Debezium connector: {error_detail}", exc_info=True)
                    if hasattr(e, '__cause__') and e.__cause__:
                        error_detail = f"{error_detail} (Caused by: {str(e.__cause__)})"
                    
                    # Use the error detail directly (it should contain the actual Kafka Connect error message)
                    # Remove any HTTP wrapper text if present
                    if "400 Client Error" in error_detail or "Bad Request" in error_detail:
                        # Try to extract just the actual error message
                        if "for url:" in error_detail:
                            parts = error_detail.split("for url:")
                            if len(parts) > 0:
                                error_detail = parts[0].replace("400 Client Error:", "").replace("Bad Request", "").strip()
                        # If still contains HTTP wrapper, try to get from response
                        if hasattr(e, 'response') and e.response:
                            try:
                                error_json = e.response.json()
                                if error_json.get('message'):
                                    error_detail = error_json.get('message')
                                elif error_json.get('error'):
                                    error_detail = error_json.get('error')
                            except:
                                pass
                    
                    # Ensure we have a meaningful error message
                    if not error_detail or error_detail.strip() == "":
                        # Try to get status code from exception if available
                        status_code_str = "N/A"
                        if hasattr(e, 'response') and e.response:
                            status_code_str = str(e.response.status_code)
                        elif isinstance(e, requests.exceptions.HTTPError) and hasattr(e, 'response') and e.response:
                            status_code_str = str(e.response.status_code)
                        error_detail = f"Unknown error from Kafka Connect (HTTP {status_code_str})"
                    
                    # FIX: Store error in pipeline config
                    if not pipeline.debezium_config:
                        pipeline.debezium_config = {}
                    pipeline.debezium_config['_last_error'] = {
                        'message': error_detail,
                        'timestamp': datetime.utcnow().isoformat(),
                        'connector_name': debezium_connector_name
                    }
                    pipeline.cdc_status = CDCStatus.ERROR
                    self._persist_pipeline_status(pipeline)
                    
                    full_error = error_detail
                    logger.error(f"Debezium connector creation failed: {full_error}")
                    logger.error(f"Debezium connector name: {debezium_connector_name}")
                    logger.error(f"Debezium config keys: {list(debezium_config.keys())}")
                    if debezium_config:
                        # Log a few key config values (without sensitive data)
                        safe_config = {k: v for k, v in debezium_config.items() if 'password' not in k.lower()}
                        logger.error(f"Debezium config (sanitized): {safe_config}")
                    # Log the full exception for debugging
                    logger.error(f"Full exception: {e}", exc_info=True)
                    logger.error(f"Exception type: {type(e).__name__}, Exception args: {e.args}")
                    raise Exception(full_error) from e
                
                # Wait for connector to start
                logger.info(f"Waiting for Debezium connector to reach RUNNING state (max 60s)...")
                if not self.kafka_client.wait_for_connector(
                    connector_name=debezium_connector_name,
                    target_state="RUNNING",
                    max_wait_seconds=60
                ):
                    # FIX: Get connector status to provide better error message
                    try:
                        connector_status = self.kafka_client.get_connector_status(debezium_connector_name)
                        if connector_status:
                            connector_state = connector_status.get('connector', {}).get('state', 'UNKNOWN')
                            connector_error = connector_status.get('connector', {}).get('error', '')
                            tasks = connector_status.get('tasks', [])
                            failed_tasks = [t for t in tasks if t.get('state') == 'FAILED']
                            
                            error_parts = [f"Connector state: {connector_state}"]
                            if connector_error:
                                error_parts.append(f"Error: {connector_error}")
                            if failed_tasks:
                                for task in failed_tasks:
                                    task_error = task.get('trace', '')[:500] if task.get('trace') else 'Unknown error'
                                    error_parts.append(f"Task {task.get('id', 'unknown')}: {task_error}")
                            
                            detailed_error = "; ".join(error_parts)
                            pipeline.cdc_status = CDCStatus.ERROR
                            self._persist_pipeline_status(pipeline)
                            raise Exception(f"Debezium connector failed to start: {debezium_connector_name}. {detailed_error}")
                        else:
                            pipeline.cdc_status = CDCStatus.ERROR
                            self._persist_pipeline_status(pipeline)
                            raise Exception(f"Debezium connector {debezium_connector_name} not found after creation")
                    except Exception as status_error:
                        # If we can't get status, use generic error
                        pipeline.cdc_status = CDCStatus.ERROR
                        self._persist_pipeline_status(pipeline)
                        raise Exception(f"Debezium connector failed to start: {debezium_connector_name}. Check Kafka Connect logs for details.") from status_error
                
                pipeline.debezium_connector_name = debezium_connector_name
                pipeline.debezium_config = debezium_config
                
                # FIX: Persist immediately after successful creation
                self._persist_pipeline_status(pipeline)
                logger.info(f"Persisted Debezium connector info: {debezium_connector_name}")
            
            pipeline.debezium_connector_name = debezium_connector_name
            pipeline.debezium_config = debezium_config
            
            result["debezium_connector"] = {
                "name": debezium_connector_name,
                "status": "RUNNING"
            }
            
            # Step 3: Wait for topics to be created and discover them
            logger.info("Waiting for Kafka topics to be created by Debezium...")
            # FIX: Increase wait time and add retry logic for topic discovery
            max_topic_wait_attempts = 10  # 10 attempts * 5s = 50s total (increased from 30s)
            topics_discovered = False
            kafka_topics = []
            
            for attempt in range(max_topic_wait_attempts):
                if attempt > 0:
                    time.sleep(5)  # Wait for Debezium to create topics (skip sleep on first attempt)
            
                # Discover actual topics from the connector API
                # IMPORTANT: For Oracle, Debezium creates topics with UPPERCASE schema/table names
                # We must use the actual topic names from the connector, not generated ones
                try:
                    # Try to get actual topics from the connector using session
                    # Ensure base_url doesn't have /connectors suffix (normalize it)
                    base_url = self.kafka_client.base_url.rstrip('/connectors').rstrip('/')
                    connector_topics_response = self.kafka_client.session.get(
                        f"{base_url}/connectors/{debezium_connector_name}/topics",
                        timeout=10
                    )
                    if connector_topics_response.status_code == 200:
                        topics_data = connector_topics_response.json()
                        connector_topics = topics_data.get(debezium_connector_name, {}).get('topics', [])
                        # Filter out schema change topic (it's just the pipeline name)
                        # We only want table-specific topics (format: {pipeline}.{schema}.{table})
                        table_topics = [t for t in connector_topics if '.' in t and t != pipeline.name]
                        if table_topics:
                            kafka_topics = table_topics
                            logger.info(f"✅ Discovered actual topics from connector (attempt {attempt + 1}/{max_topic_wait_attempts}): {kafka_topics}")
                            topics_discovered = True
                            break
                        else:
                            logger.info(f"Attempt {attempt + 1}/{max_topic_wait_attempts}: Connector topics found but no table topics yet: {connector_topics}")
                    else:
                        logger.info(f"Attempt {attempt + 1}/{max_topic_wait_attempts}: Could not get topics (HTTP {connector_topics_response.status_code})")
                except Exception as e:
                    logger.info(f"Attempt {attempt + 1}/{max_topic_wait_attempts}: Could not discover topics from connector API: {e}")
            
            # Fallback: Generate topic names if discovery failed
            # FIX: Always generate topic names as fallback, even if discovery partially succeeded
            if not kafka_topics or len(kafka_topics) < len(pipeline.source_tables):
                logger.warning(f"Topic discovery incomplete (found {len(kafka_topics)} topics, expected {len(pipeline.source_tables)}), generating topic names as fallback...")
                generated_topics = []
                for table in pipeline.source_tables:
                    # For Oracle, use UPPERCASE schema/table names (Debezium Oracle creates uppercase topics)
                    if source_connection.database_type == "oracle":
                        schema_upper = (pipeline.source_schema or "public").upper()
                        table_upper = table.upper()
                        topic_name = f"{pipeline.name}.{schema_upper}.{table_upper}"
                    else:
                        # SQL Server Debezium uses {topic.prefix}.{database}.{schema}.{table}; pass database
                        source_db = pipeline.source_database or source_connection.database
                        topic_name = DebeziumConfigGenerator.get_topic_name(
                            pipeline_name=pipeline.name,
                            schema=pipeline.source_schema or "public",
                            table=table,
                            database=source_db if source_connection.database_type in ("sqlserver", "mssql") else None
                        )
                    generated_topics.append(topic_name)
                
                # Merge discovered topics with generated ones (avoid duplicates)
                all_topics = list(kafka_topics)
                for gen_topic in generated_topics:
                    if gen_topic not in all_topics:
                        all_topics.append(gen_topic)
                
                kafka_topics = all_topics
                # Count how many topics were discovered vs generated
                initial_discovered_count = len([t for t in kafka_topics if t not in generated_topics]) if kafka_topics else 0
                logger.info(f"✅ Using {len(kafka_topics)} topics (discovered: {initial_discovered_count}, generated: {len(generated_topics)}): {kafka_topics}")
            
            pipeline.kafka_topics = kafka_topics
            result["kafka_topics"] = kafka_topics
            
            # FIX: Don't fail if topics are empty - use generated topics instead
            if not kafka_topics:
                logger.warning("⚠️  No Kafka topics available - generating from table list as last resort...")
                for table in pipeline.source_tables:
                    if source_connection.database_type == "oracle":
                        schema_upper = (pipeline.source_schema or "public").upper()
                        table_upper = table.upper()
                        topic_name = f"{pipeline.name}.{schema_upper}.{table_upper}"
                    else:
                        source_db = pipeline.source_database or source_connection.database
                        topic_name = DebeziumConfigGenerator.get_topic_name(
                            pipeline_name=pipeline.name,
                            schema=pipeline.source_schema or "public",
                            table=table,
                            database=source_db if source_connection.database_type in ("sqlserver", "mssql") else None
                        )
                    kafka_topics.append(topic_name)
                pipeline.kafka_topics = kafka_topics
                result["kafka_topics"] = kafka_topics
                logger.warning(f"⚠️  Using generated topic names (topics may not exist yet in Kafka): {kafka_topics}")
            else:
                logger.info(f"✅ Using {len(kafka_topics)} Kafka topics: {kafka_topics}")
            
            # FIX: Persist topics immediately after discovery
            self._persist_pipeline_status(pipeline)
            logger.info(f"Persisted Kafka topics: {kafka_topics}")
            
            # Step 4: Generate and create Sink connector
            logger.info(f"Creating Sink connector for pipeline: {pipeline.name}")
            
            # Get default schema based on database type
            default_target_schema = (
                "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                else "PUBLIC" if target_connection.database_type == "snowflake"
                else "public"
            )
            
            # FIX: Validate and normalize target schema name
            target_schema = pipeline.target_schema or target_connection.schema or default_target_schema
            # Normalize schema name - handle common truncations/typos
            if target_schema.lower() in ["data", "dat"]:
                logger.warning(f"Detected potentially truncated target schema name '{target_schema}', using '{default_target_schema}' as fallback")
                target_schema = default_target_schema
            
            sink_connector_name = SinkConfigGenerator.generate_connector_name(
                pipeline_name=pipeline.name,
                database_type=target_connection.database_type,
                schema=target_schema
            )
            
            # FIX: Log target schema used
            logger.info(f"Generated Sink connector name: {sink_connector_name}")
            logger.info(f"Target schema used: {target_schema} (from pipeline: {pipeline.target_schema}, connection: {target_connection.schema}, default: {default_target_schema})")
            
            # FIX: Store sink connector name immediately (before creation attempt)
            pipeline.sink_connector_name = sink_connector_name
            self._persist_pipeline_status(pipeline)
            logger.info(f"Stored Sink connector name in pipeline: {sink_connector_name}")
            
            # Check if sink config already exists in pipeline (loaded from database)
            sink_exists = False
            if pipeline.sink_config and pipeline.sink_connector_name:
                logger.info(f"Pipeline has existing Sink config and connector name: {pipeline.sink_connector_name}")
                try:
                    connector_status = self.kafka_client.get_connector_status(pipeline.sink_connector_name)
                    if connector_status is None:
                        # Connector doesn't exist, will create new one
                        logger.info(f"Sink connector {pipeline.sink_connector_name} not found, will create new one")
                        sink_exists = False
                    else:
                        connector_state = connector_status.get('connector', {}).get('state', 'UNKNOWN')
                        if connector_state == 'RUNNING':
                            # Check if the existing config has the correct topics
                            existing_topics = pipeline.sink_config.get('topics', '').split(',')
                            existing_topics = [t.strip() for t in existing_topics if t.strip()]
                            # Compare with discovered topics (case-insensitive for comparison, but must match exactly)
                            topics_match = set(existing_topics) == set(kafka_topics)
                            if topics_match:
                                logger.info(f"Existing Sink connector {pipeline.sink_connector_name} is RUNNING with correct topics, reusing it")
                                sink_exists = True
                                sink_connector_name = pipeline.sink_connector_name
                                sink_config = pipeline.sink_config
                            else:
                                logger.warning(f"Existing Sink connector topics don't match discovered topics. Existing: {existing_topics}, Discovered: {kafka_topics}. Will recreate.")
                                # Delete the connector to force recreation
                                try:
                                    self.kafka_client.delete_connector(pipeline.sink_connector_name)
                                    logger.info(f"Deleted Sink connector {pipeline.sink_connector_name} due to topic mismatch")
                                except Exception as e:
                                    logger.warning(f"Could not delete connector: {e}")
                        else:
                            logger.info(f"Existing Sink connector {pipeline.sink_connector_name} state is {connector_state}, will recreate")
                except Exception as e:
                    logger.warning(f"Could not check existing Sink connector status: {e}, will create new one")
            
            if not sink_exists:
                # Get default schema based on database type
                default_target_schema = (
                    "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                    else "PUBLIC" if target_connection.database_type == "snowflake"
                    else "public"
                )
                sink_config = SinkConfigGenerator.generate_sink_config(
                    connector_name=sink_connector_name,
                    target_connection=target_connection,
                    target_database=pipeline.target_database or target_connection.database,
                    target_schema=pipeline.target_schema or target_connection.schema or default_target_schema,
                    kafka_topics=kafka_topics
                )
                
                # Try to delete existing connector if it exists
                # Skip the check and just try to delete - if it doesn't exist, that's fine
                try:
                    self.kafka_client.delete_connector(sink_connector_name)
                    logger.info(f"Deleted existing Sink connector: {sink_connector_name}")
                except Exception as e:
                    # Connector doesn't exist or Kafka Connect has issues - continue anyway
                    error_msg = str(e)
                    if "404" in error_msg or "not found" in error_msg.lower():
                        logger.debug(f"Connector {sink_connector_name} doesn't exist (will create fresh)")
                    else:
                        logger.warning(f"Could not delete connector (will try to create fresh): {e}")
                    # Continue - we'll try to create it
                
                # Create Sink connector
                logger.info(f"Creating Sink connector: {sink_connector_name}")
                logger.info(f"Sink config: {sink_config}")
                try:
                    self.kafka_client.create_connector(
                        connector_name=sink_connector_name,
                        config=sink_config
                    )
                except requests.exceptions.HTTPError as e:
                    error_detail = ""
                    if e.response:
                        try:
                            error_json = e.response.json()
                            # Try multiple possible error message fields
                            error_detail = (
                                error_json.get('message') or 
                                error_json.get('error') or 
                                error_json.get('error_code') or
                                str(error_json)
                            )
                            # If we have config validation errors, include them
                            if 'configs' in error_json:
                                config_errors = []
                                for config_item in error_json.get('configs', []):
                                    if 'value' in config_item and 'errors' in config_item['value']:
                                        config_errors.extend(config_item['value']['errors'])
                                    if 'errors' in config_item:
                                        config_errors.extend(config_item['errors'])
                                if config_errors:
                                    error_detail += f" Config errors: {'; '.join(config_errors[:5])}"
                        except Exception as parse_error:
                            error_detail = e.response.text[:1000] if hasattr(e.response, 'text') else str(e)
                            logger.warning(f"Could not parse error JSON: {parse_error}")
                    else:
                        error_detail = str(e)
                    
                    # Include status code if available
                    status_code = ""
                    if e.response:
                        status_code = f" (HTTP {e.response.status_code})"
                    
                    # Ensure we have a meaningful error message
                    if not error_detail or error_detail.strip() == "":
                        error_detail = f"Unknown error from Kafka Connect (HTTP {e.response.status_code if e.response else 'N/A'})"
                    
                    # FIX: Store error in pipeline config
                    if not pipeline.sink_config:
                        pipeline.sink_config = {}
                    pipeline.sink_config['_last_error'] = {
                        'message': error_detail,
                        'timestamp': datetime.utcnow().isoformat(),
                        'connector_name': sink_connector_name,
                        'http_status': e.response.status_code if e.response else None,
                        'error_type': 'HTTPError'
                    }
                    # FIX: Store connector name even on failure
                    pipeline.sink_connector_name = sink_connector_name
                    pipeline.cdc_status = CDCStatus.ERROR
                    self._persist_pipeline_status(pipeline)
                    logger.error(f"Persisted error and connector name to database: {sink_connector_name}")
                    
                    full_error = f"Failed to create Sink connector '{sink_connector_name}'{status_code}: {error_detail}" if error_detail else f"Failed to create Sink connector '{sink_connector_name}'{status_code}"
                    logger.error(full_error)
                    logger.error(f"Sink connector name: {sink_connector_name}")
                    logger.error(f"Sink config keys: {list(sink_config.keys())}")
                    logger.error(f"Exception type: {type(e).__name__}, Exception args: {e.args}")
                    logger.error(f"Connector name '{sink_connector_name}' has been stored in database for debugging")
                    raise Exception(full_error) from e
            else:
                logger.info(f"Reusing existing Sink connector: {sink_connector_name}")
            
            # Wait for connector to start
            logger.info(f"Waiting for Sink connector to reach RUNNING state (max 60s)...")
            if not self.kafka_client.wait_for_connector(
                connector_name=sink_connector_name,
                target_state="RUNNING",
                max_wait_seconds=60
            ):
                # FIX: Get connector status to provide better error message
                try:
                    connector_status = self.kafka_client.get_connector_status(sink_connector_name)
                    if connector_status:
                        connector_state = connector_status.get('connector', {}).get('state', 'UNKNOWN')
                        connector_error = connector_status.get('connector', {}).get('error', '')
                        tasks = connector_status.get('tasks', [])
                        failed_tasks = [t for t in tasks if t.get('state') == 'FAILED']
                        
                        error_parts = [f"Connector state: {connector_state}"]
                        if connector_error:
                            error_parts.append(f"Error: {connector_error}")
                        if failed_tasks:
                            for task in failed_tasks:
                                task_error = task.get('trace', '')[:500] if task.get('trace') else 'Unknown error'
                                error_parts.append(f"Task {task.get('id', 'unknown')}: {task_error}")
                        
                        detailed_error = "; ".join(error_parts)
                        pipeline.cdc_status = CDCStatus.ERROR
                        self._persist_pipeline_status(pipeline)
                        raise Exception(f"Sink connector failed to start: {sink_connector_name}. {detailed_error}")
                    else:
                        pipeline.cdc_status = CDCStatus.ERROR
                        self._persist_pipeline_status(pipeline)
                        raise Exception(f"Sink connector {sink_connector_name} not found after creation")
                except Exception as status_error:
                    # If we can't get status, use generic error
                    pipeline.cdc_status = CDCStatus.ERROR
                    self._persist_pipeline_status(pipeline)
                    raise Exception(f"Sink connector failed to start: {sink_connector_name}. Check Kafka Connect logs for details.") from status_error
            
            pipeline.sink_connector_name = sink_connector_name
            pipeline.sink_config = sink_config
            
            # FIX: Persist Sink connector info immediately after creation
            self._persist_pipeline_status(pipeline)
            logger.info(f"Persisted Sink connector info: {sink_connector_name}")
            
            pipeline.cdc_status = CDCStatus.RUNNING
            pipeline.status = PipelineStatus.RUNNING
            
            result["sink_connector"] = {
                "name": sink_connector_name,
                "status": "RUNNING"
            }
            result["status"] = "RUNNING"
            
            # Persist final status after CDC setup completes
            self._persist_pipeline_status(pipeline)
            
            # CRITICAL: Register topics with CDC event logger for monitoring
            # This creates the topic → pipeline_id mapping that Event Logger needs
            try:
                from ingestion.cdc_event_logger import get_event_logger
                event_logger = get_event_logger()
                if event_logger and kafka_topics:
                    registered_count = 0
                    for topic in kafka_topics:
                        if topic:  # Skip empty topics
                            event_logger.add_topic(topic, str(pipeline.id))  # Ensure string UUID
                            registered_count += 1
                            logger.debug(f"Registered topic {topic} → pipeline {pipeline.id}")
                    logger.info(f"✅ Registered {registered_count} topics with CDC event logger for pipeline {pipeline.name} (ID: {pipeline.id})")
                    logger.info(f"   Topics: {kafka_topics[:3]}..." if len(kafka_topics) > 3 else f"   Topics: {kafka_topics}")
                elif not event_logger:
                    logger.warning(f"⚠️  CDC Event Logger not available - topics will not be monitored for pipeline {pipeline.name}")
                elif not kafka_topics:
                    logger.warning(f"⚠️  No Kafka topics available to register for pipeline {pipeline.name}")
            except Exception as e:
                logger.error(f"❌ Failed to register topics with CDC event logger: {e}", exc_info=True)
                # Don't fail pipeline start if event logger registration fails
                logger.warning("Pipeline will continue, but CDC events may not be logged until Event Logger is fixed")
            
            logger.info(f"Pipeline started successfully: {pipeline.name}")
            
        except FullLoadError as e:
            logger.error(f"Full load failed for pipeline {pipeline_id}: {e}", exc_info=True)
            pipeline.status = PipelineStatus.ERROR
            pipeline.full_load_status = FullLoadStatus.FAILED
            pipeline.cdc_status = CDCStatus.ERROR
            result["status"] = "ERROR"
            result["error"] = str(e)
            result["full_load_error"] = True
            # Persist error status
            self._persist_pipeline_status(pipeline)
            raise
        except Exception as e:
            logger.error(f"Failed to start pipeline {pipeline_id}: {e}", exc_info=True)
            pipeline.status = PipelineStatus.ERROR
            pipeline.cdc_status = CDCStatus.ERROR
            result["status"] = "ERROR"
            result["error"] = str(e)
            # Persist error status
            self._persist_pipeline_status(pipeline)
            raise
        
        return result
    
    def _run_full_load(
        self,
        pipeline: Pipeline,
        source_connection: Connection,
        target_connection: Connection
    ) -> Dict[str, Any]:
        """Run full load for pipeline.
        
        Args:
            pipeline: Pipeline object
            source_connection: Source connection
            target_connection: Target connection
            
        Returns:
            Full load result dictionary
        """
        try:
            # Initialize connectors
            source_config = source_connection.get_connection_config()
            target_config = target_connection.get_connection_config()
            
            if source_connection.database_type == "postgresql":
                source_connector = PostgreSQLConnector(source_config)
            elif source_connection.database_type in ["sqlserver", "mssql"]:
                source_connector = SQLServerConnector(source_config)
            elif source_connection.database_type in ["as400", "ibm_i", "db2"]:
                # DB2 normalized to AS400 connector (same pyodbc/ODBC path); backend needs IBM i Access ODBC driver for full load
                from ingestion.connectors.as400 import AS400Connector
                source_connector = AS400Connector(source_config)
            elif source_connection.database_type == "oracle":
                from ingestion.connectors.oracle import OracleConnector
                source_connector = OracleConnector(source_config)
            else:
                raise ValueError(f"Unsupported source database type: {source_connection.database_type}")
            
            # Handle S3 target specially
            if target_connection.database_type == "s3":
                from ingestion.connectors.s3 import S3Connector
                target_connector = S3Connector(target_config)
                
                # For S3, we need to extract data from source and write to S3
                return self._run_full_load_to_s3(
                    pipeline=pipeline,
                    source_connector=source_connector,
                    source_connection=source_connection,
                    target_connector=target_connector,
                    target_connection=target_connection
                )
            
            # Handle Snowflake target - need to transfer data directly
            if target_connection.database_type == "snowflake":
                from ingestion.connectors.snowflake import SnowflakeConnector
                target_connector = SnowflakeConnector(target_config)
                # Test connection
                target_connector.test_connection()
                # Run full load to Snowflake
                return self._run_full_load_to_snowflake(
                    pipeline=pipeline,
                    source_connector=source_connector,
                    target_connector=target_connector,
                    target_connection=target_connection
                )
            
            if target_connection.database_type == "postgresql":
                target_connector = PostgreSQLConnector(target_config)
            elif target_connection.database_type in ["sqlserver", "mssql"]:
                target_connector = SQLServerConnector(target_config)
            elif target_connection.database_type == "oracle":
                from ingestion.connectors.oracle import OracleConnector
                target_connector = OracleConnector(target_config)
            else:
                raise ValueError(f"Unsupported target database type: {target_connection.database_type}")
            
            # Verify source has data before transfer
            for table_name in pipeline.source_tables:
                logger.info(f"Validating source data for table: {table_name}")
                try:
                    validation_result = validate_source_data(
                        connector=source_connector,
                        database=pipeline.source_database,
                        schema=pipeline.source_schema,
                        table_name=table_name
                    )
                    if not validation_result.get('has_data'):
                        logger.warning(f"Source table {table_name} has no data, but continuing with transfer")
                except ValidationError as e:
                    logger.warning(f"Source validation warning for {table_name}: {e}")
                    # Continue anyway - let transfer handle it
            
            # Initialize data transfer
            logger.info("Initializing data transfer...")
            transfer = DataTransfer(source_connector, target_connector)
            
            # Transfer all tables
            logger.info(f"Transferring {len(pipeline.source_tables)} table(s): {pipeline.source_tables}")
            logger.info(f"Transfer settings: schema=True, data=True, batch_size=10000")
            
            # Get default schema based on database type
            default_target_schema = (
                "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                else "PUBLIC" if target_connection.database_type == "snowflake"
                else "public"
            )
            transfer_result = transfer.transfer_tables(
                tables=pipeline.source_tables,
                source_database=pipeline.source_database,
                source_schema=pipeline.source_schema,
                target_database=pipeline.target_database or target_connection.database,
                target_schema=pipeline.target_schema or target_connection.schema or default_target_schema,
                transfer_schema=True,  # Transfer schema (create tables if needed)
                transfer_data=True,    # Transfer data
                batch_size=10000
            )
            
            logger.info(f"Transfer completed: {transfer_result.get('tables_successful', 0)} successful, {transfer_result.get('tables_failed', 0)} failed")
            
            # Build set of table names that failed transfer (skip validation for these)
            failed_table_names = set()
            for t in transfer_result.get("tables", []):
                if t.get("errors") or t.get("error"):
                    failed_table_names.add(t.get("table_name", ""))
            
            # Post-transfer validation: Verify target row counts (only for tables that transferred successfully)
            for table_name in pipeline.source_tables:
                if table_name in failed_table_names:
                    logger.warning(f"Skipping validation for {table_name} (transfer had errors)")
                    continue
                target_table_name = pipeline.target_table_mapping.get(table_name, table_name) if pipeline.target_table_mapping else table_name
                
                # Parse target_table_name to extract schema and table if it contains a dot
                # This prevents double schema prefix (e.g., "dbo.dbo.department")
                default_target_schema = (
                    "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                    else "PUBLIC" if target_connection.database_type == "snowflake"
                    else "public"
                )
                target_schema_final = pipeline.target_schema or target_connection.schema or default_target_schema
                target_table_final = target_table_name
                
                # If target_table_name contains schema prefix (e.g., "dbo.department"), extract just the table name
                if '.' in target_table_name:
                    parts = target_table_name.split('.')
                    if len(parts) == 2:
                        # If schema matches, use just the table name; otherwise keep as is
                        if parts[0].lower() == target_schema_final.lower():
                            target_table_final = parts[1]
                        # If schema doesn't match, use the provided schema and table
                        # But validate schema matches database type
                        else:
                            extracted_schema = parts[0]
                            # If SQL Server and extracted schema is "public", override to "dbo"
                            if target_connection.database_type in ("sqlserver", "mssql") and extracted_schema.lower() == "public":
                                logger.warning(f"Target table '{target_table_name}' has schema 'public' for SQL Server, overriding to 'dbo'")
                                target_schema_final = "dbo"
                                target_table_final = parts[1]
                            else:
                                target_schema_final = extracted_schema
                                target_table_final = parts[1]
                    elif len(parts) > 2:
                        # Handle database.schema.table format - extract schema and table
                        target_schema_final = parts[-2]
                        target_table_final = parts[-1]
                
                logger.info(f"Validating target row count for table: {target_schema_final}.{target_table_final}")
                try:
                    validation_result = validate_target_row_count(
                        source_connector=source_connector,
                        target_connector=target_connector,
                        source_database=pipeline.source_database,
                        source_schema=pipeline.source_schema,
                        source_table=table_name,
                        target_database=pipeline.target_database or target_connection.database,
                        target_schema=target_schema_final,
                        target_table=target_table_final
                    )
                    logger.info(f"Row count validation passed for {target_table_name}: {validation_result.get('source_rows', 0)} rows match")
                except ValidationError as e:
                    err_msg = str(e).lower()
                    # If target table does not exist, transfer likely failed - surface the transfer error
                    if "does not exist" in err_msg or "relation" in err_msg:
                        transfer_error = None
                        for t in transfer_result.get("tables", []):
                            if t.get("table_name") == table_name:
                                if t.get("errors"):
                                    transfer_error = t["errors"][0] if isinstance(t["errors"], list) else t["errors"]
                                else:
                                    transfer_error = t.get("error")
                                break
                        raise FullLoadError(
                            f"Target table {target_schema_final}.{target_table_final} was not created. "
                            f"{'Transfer error: ' + str(transfer_error) if transfer_error else str(e)}",
                            table_name=target_table_name,
                            rows_transferred=transfer_result.get('total_rows_transferred', 0)
                        )
                    # Check if this is a row count mismatch
                    if "row count mismatch" in err_msg or (hasattr(e, 'validation_type') and e.validation_type == "row_count"):
                        # Extract row counts from error details
                        source_rows = None
                        target_rows = None
                        if hasattr(e, 'details') and isinstance(e.details, dict):
                            source_rows = e.details.get('source_rows')
                            target_rows = e.details.get('target_rows')
                        # Fallback: try to parse from error message
                        if source_rows is None or target_rows is None:
                            import re
                            match = re.search(r'source has (\d+) rows.*target has (\d+) rows', str(e))
                            if match:
                                source_rows = int(match.group(1))
                                target_rows = int(match.group(2))
                        
                        # Log warning but don't fail - allow pipeline to continue
                        logger.warning(
                            f"⚠️  Row count mismatch for {target_table_name}: "
                            f"source has {source_rows or 'unknown'} rows, target has {target_rows or 'unknown'} rows. "
                            f"Pipeline will continue; please verify data integrity manually."
                        )
                    else:
                        logger.error(f"Validation failed for {target_table_name}: {e}")
                        raise FullLoadError(
                            f"Post-transfer validation failed for {target_table_name}: {str(e)}",
                            table_name=target_table_name,
                            rows_transferred=transfer_result.get('total_rows_transferred', 0)
                        )
            
            # Debug logging
            logger.info(f"Transfer result: tables_successful={transfer_result.get('tables_successful')}, "
                       f"tables_failed={transfer_result.get('tables_failed')}, "
                       f"total_rows_transferred={transfer_result.get('total_rows_transferred')}")
            logger.info(f"Transfer result details: {json.dumps(transfer_result, default=str)}")
            
            # Check if transfer actually succeeded
            if transfer_result["tables_successful"] == 0:
                error_msg = "No tables were successfully transferred"
                if transfer_result.get("tables"):
                    # Get error from first failed table
                    first_table = transfer_result["tables"][0]
                    if first_table.get("errors"):
                        error_msg = f"Transfer failed: {first_table['errors'][0]}"
                    elif first_table.get("error"):
                        error_msg = f"Transfer failed: {first_table['error']}"
                logger.error(f"Full load validation failed: {error_msg}")
                raise FullLoadError(
                    error_msg,
                    rows_transferred=0,
                    error=error_msg
                )
            
            # Check if rows were actually transferred (0 rows when tables were "successful" indicates failure)
            # But first check if source actually has data
            source_has_data = False
            for table_name in pipeline.source_tables:
                try:
                    validation_result = validate_source_data(
                        connector=source_connector,
                        database=pipeline.source_database,
                        schema=pipeline.source_schema,
                        table_name=table_name
                    )
                    if validation_result.get('has_data'):
                        source_has_data = True
                        break
                except Exception:
                    pass  # Ignore validation errors here
            
            if transfer_result["total_rows_transferred"] == 0 and source_has_data:
                error_msg = "Full load reported success but transferred 0 rows (source has data)"
                if transfer_result.get("tables"):
                    # Check each table result for details
                    for table_result in transfer_result["tables"]:
                        if table_result.get("data_transferred") and table_result.get("rows_transferred", 0) == 0:
                            # Table was marked as transferred but has 0 rows - this is suspicious
                            if table_result.get("errors"):
                                error_msg = f"Transfer failed: {table_result['errors'][0]}"
                            elif table_result.get("error"):
                                error_msg = f"Transfer failed: {table_result['error']}"
                            else:
                                error_msg = f"Table {table_result.get('table_name', 'unknown')} reported success but transferred 0 rows. Check for schema mismatches or data insertion failures."
                logger.error(f"Full load validation failed: {error_msg}")
                raise FullLoadError(
                    error_msg,
                    rows_transferred=0,
                    error=error_msg
                )
            elif transfer_result["total_rows_transferred"] == 0:
                logger.warning("Full load transferred 0 rows, but source tables appear to be empty (this may be OK)")
            
            # Capture LSN after full load
            logger.info("Capturing LSN after full load...")
            lsn_info = source_connector.extract_lsn_offset(database=pipeline.source_database)
            
            result = {
                "success": True,
                "tables_transferred": transfer_result["tables_successful"],
                "total_rows": transfer_result["total_rows_transferred"],
                "lsn": lsn_info.get("lsn"),
                "offset": lsn_info.get("offset"),
                "timestamp": lsn_info.get("timestamp")
            }
            
            logger.info(f"Full load result: success=True, tables={result['tables_transferred']}, rows={result['total_rows']}, LSN={result['lsn']}")
            return result
            
        except FullLoadError:
            # Re-raise FullLoadError as-is
            raise
        except ValidationError as e:
            # Convert ValidationError to FullLoadError
            logger.error(f"Full load validation failed: {e}", exc_info=True)
            raise FullLoadError(
                f"Full load validation failed: {str(e)}",
                rows_transferred=0
            )
        except Exception as e:
            logger.error(f"Full load failed: {e}", exc_info=True)
            raise FullLoadError(
                f"Full load failed: {str(e)}",
                rows_transferred=0,
                error=str(e)
            )
    
    def _run_full_load_to_s3(
        self,
        pipeline: Pipeline,
        source_connector: BaseConnector,
        source_connection: Connection,
        target_connector: BaseConnector,
        target_connection: Connection
    ) -> Dict[str, Any]:
        """Run full load from database to S3.
        
        Args:
            pipeline: Pipeline object
            source_connector: Source database connector
            target_connector: S3 connector
            target_connection: Target S3 connection
            
        Returns:
            Full load result dictionary
        """
        import json
        from datetime import datetime
        
        try:
            bucket = target_connection.database
            prefix = target_connection.schema or ""
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            tables_transferred = []
            total_rows = 0
            
            # Process each table
            for table_name in pipeline.source_tables:
                logger.info(f"Transferring table {table_name} to S3")
                
                # Extract schema
                schema_result = source_connector.extract_schema(
                    database=pipeline.source_database,
                    schema=pipeline.source_schema,
                    table=table_name
                )
                
                # PostgreSQL connector returns tables list, not success/error
                # Check if we got schema data
                tables = schema_result.get('tables', [])
                if not tables:
                    logger.warning(f"Could not extract schema for {table_name}: No tables found in schema result")
                    continue
                
                # Get columns from the first table (should be the requested table)
                columns = tables[0].get('columns', []) if tables else []
                if not columns:
                    logger.warning(f"Could not extract columns for {table_name}: No columns found")
                    continue
                
                # Extract data in batches
                batch_size = 10000
                offset = 0
                all_rows = []
                column_names = None
                
                while True:
                    try:
                        data_result = source_connector.extract_data(
                            database=pipeline.source_database,
                            schema=pipeline.source_schema,
                            table_name=table_name,
                            limit=batch_size,
                            offset=offset
                        )
                    except Exception as e:
                        logger.error(f"Error extracting data from {table_name}: {e}")
                        break
                    
                    # PostgreSQL connector returns rows as list of lists, not dicts
                    rows = data_result.get('rows', [])
                    if not rows:
                        break
                    
                    # Get column names on first batch
                    if column_names is None:
                        column_names = data_result.get('column_names', [])
                    
                    # Convert list of lists to list of dicts
                    row_dicts = []
                    for row in rows:
                        row_dict = dict(zip(column_names, row)) if column_names else {}
                        row_dicts.append(row_dict)
                    
                    all_rows.extend(row_dicts)
                    offset += len(rows)
                    
                    # Check if there are more rows
                    has_more = data_result.get('has_more', False)
                    if not has_more:
                        break
                
                if not all_rows:
                    logger.warning(f"No data found for table {table_name}")
                    continue
                
                # Format data as JSON
                json_data = json.dumps(all_rows, indent=2, default=str)
                
                # Upload to S3
                s3_key = f"{prefix}{table_name}/full_load_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                
                try:
                    s3_client = target_connector._get_s3_client()
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=s3_key,
                        Body=json_data.encode('utf-8'),
                        ContentType='application/json'
                    )
                    
                    logger.info(f"Uploaded {len(all_rows)} rows from {table_name} to s3://{bucket}/{s3_key}")
                    tables_transferred.append(table_name)
                    total_rows += len(all_rows)
                    
                except Exception as e:
                    logger.error(f"Error uploading {table_name} to S3: {e}")
                    continue
            
            # Capture LSN after full load - CRITICAL for CDC to start from correct offset
            logger.info(f"Capturing LSN/offset after full load for database {pipeline.source_database}...")
            try:
                lsn_info = source_connector.extract_lsn_offset(database=pipeline.source_database)
                lsn_value = lsn_info.get("lsn") if lsn_info else None
                
                if lsn_value:
                    logger.info(f"✅ Successfully captured LSN/offset: {lsn_value}")
                else:
                    logger.warning(f"⚠️  LSN extraction returned None. Full result: {lsn_info}")
                    # For AS400, create a fallback LSN using timestamp
                    if source_connection.database_type in ["as400", "ibm_i"]:
                        from datetime import datetime
                        journal_library = source_connection.additional_config.get("journal_library", "JRNRCV") if source_connection.additional_config else "JRNRCV"
                        lsn_value = f"JOURNAL:{journal_library}:{datetime.utcnow().isoformat()}"
                        logger.info(f"✅ Created fallback AS400 journal offset: {lsn_value}")
            except Exception as e:
                logger.error(f"❌ Error extracting LSN/offset after full load: {e}", exc_info=True)
                # For AS400, create a fallback LSN using timestamp
                if source_connection.database_type in ["as400", "ibm_i"]:
                    from datetime import datetime
                    journal_library = source_connection.additional_config.get("journal_library", "JRNRCV") if source_connection.additional_config else "JRNRCV"
                    lsn_value = f"JOURNAL:{journal_library}:{datetime.utcnow().isoformat()}"
                    logger.warning(f"⚠️  Using fallback AS400 journal offset due to extraction error: {lsn_value}")
                else:
                    lsn_value = None
            
            return {
                "success": True,
                "tables_transferred": tables_transferred,
                "total_rows": total_rows,
                "lsn": lsn_value,
                "offset": lsn_info.get("offset") if lsn_info else None,
                "timestamp": lsn_info.get("timestamp") if lsn_info else datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Full load to S3 failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def _run_full_load_to_snowflake(
        self,
        pipeline: Pipeline,
        source_connector: BaseConnector,
        target_connector: BaseConnector,
        target_connection: Connection
    ) -> Dict[str, Any]:
        """Run full load from database to Snowflake.
        
        Args:
            pipeline: Pipeline object
            source_connector: Source database connector
            target_connector: Snowflake connector
            target_connection: Target Snowflake connection
            
        Returns:
            Full load result dictionary
        """
        from datetime import datetime
        
        try:
            target_db = pipeline.target_database or target_connection.database
            target_schema = pipeline.target_schema or target_connection.schema or "PUBLIC"
            
            if not target_db:
                raise ValueError("Target database is required for Snowflake full load")
            
            tables_transferred = []
            total_rows = 0
            
            # Connect to Snowflake
            conn = target_connector.connect()
            cursor = conn.cursor()
            
            try:
                # Set database and schema context
                target_db_upper = target_db.upper().strip('"\'')
                target_schema_upper = target_schema.upper().strip('"\'')
                
                cursor.execute(f"USE DATABASE {target_db_upper}")
                cursor.execute(f"USE SCHEMA {target_schema_upper}")
                
                # Process each table
                for source_table in pipeline.source_tables:
                    # Get target table name from mapping
                    target_table = pipeline.target_table_mapping.get(source_table, source_table) if pipeline.target_table_mapping else source_table
                    target_table_upper = target_table.upper().strip('"\'')
                    
                    logger.info(f"Transferring table {source_table} to Snowflake table {target_table_upper}")
                    
                    # Extract data from source in batches
                    batch_size = 10000
                    offset = 0
                    column_names = None
                    rows_inserted = 0
                    
                    while True:
                        try:
                            data_result = source_connector.extract_data(
                                database=pipeline.source_database,
                                schema=pipeline.source_schema,
                                table_name=source_table,
                                limit=batch_size,
                                offset=offset
                            )
                        except Exception as e:
                            logger.error(f"Error extracting data from {source_table}: {e}")
                            break
                        
                        rows = data_result.get('rows', [])
                        if not rows:
                            break
                        
                        # Get column names on first batch
                        if column_names is None:
                            column_names = data_result.get('column_names', [])
                            if not column_names:
                                logger.warning(f"No column names found for {source_table}")
                                break
                        
                        # For Snowflake targets, insert data in RECORD_CONTENT/RECORD_METADATA format
                        # This matches the format that Snowflake Kafka connector uses for CDC
                        # RECORD_CONTENT: VARIANT column storing the record data as JSON
                        # RECORD_METADATA: OBJECT column storing metadata
                        
                        # Build INSERT statement for Snowflake Kafka connector format
                        # Use individual INSERT statements since Snowflake doesn't support PARSE_JSON in VALUES with executemany
                        # Insert rows one by one with PARSE_JSON in the SQL
                        rows_inserted_batch = 0
                        for row_idx, row in enumerate(rows):
                            try:
                                # Create RECORD_CONTENT as JSON object with column names as keys
                                record_content = {}
                                for i, col_name in enumerate(column_names):
                                    value = row[i] if i < len(row) else None
                                    # Convert value to JSON-serializable format
                                    if isinstance(value, (datetime,)):
                                        value = value.isoformat()
                                    elif hasattr(value, 'isoformat'):  # Handle other datetime-like objects
                                        value = value.isoformat()
                                    # Handle Oracle-specific types (LOB, etc.)
                                    elif hasattr(value, 'read'):  # LOB types
                                        try:
                                            value = value.read()
                                        except:
                                            value = str(value)
                                    record_content[col_name] = value
                                
                                # Create RECORD_METADATA as JSON object
                                # Format similar to what Snowflake Kafka connector uses
                                record_metadata = {
                                    "source": {
                                        "schema": pipeline.source_schema or "",
                                        "table": source_table,
                                        "database": pipeline.source_database or ""
                                    },
                                    "created_time": datetime.utcnow().isoformat(),
                                    "operation": "r",  # 'r' for full load (read/reload)
                                    "partition": 0,  # Default partition for full load
                                    "offset": offset + row_idx  # Sequential offset for full load
                                }
                                
                                # Convert to JSON strings for PARSE_JSON
                                # Use ensure_ascii=False to preserve Unicode and default=str for any non-serializable types
                                record_content_json = json.dumps(record_content, ensure_ascii=False, default=str)
                                record_metadata_json = json.dumps(record_metadata, ensure_ascii=False, default=str)
                                
                                # Escape single quotes in JSON strings for SQL
                                record_content_json_escaped = record_content_json.replace("'", "''")
                                record_metadata_json_escaped = record_metadata_json.replace("'", "''")
                                
                                # Insert with PARSE_JSON - use string formatting since we're inserting one at a time
                                insert_query = f'INSERT INTO "{target_table_upper}" ("RECORD_CONTENT", "RECORD_METADATA") SELECT PARSE_JSON(\'{record_content_json_escaped}\'), PARSE_JSON(\'{record_metadata_json_escaped}\')'
                                cursor.execute(insert_query)
                                rows_inserted_batch += 1
                                
                            except Exception as e:
                                logger.error(f"Error inserting row {row_idx} into {target_table_upper}: {e}")
                                # Log the problematic row for debugging
                                logger.error(f"Problematic record_content: {record_content_json[:200] if 'record_content_json' in locals() else 'N/A'}")
                                raise
                        
                        rows_inserted += rows_inserted_batch
                        logger.info(f"Inserted {rows_inserted_batch} rows into {target_table_upper} in RECORD_CONTENT/RECORD_METADATA format (total: {rows_inserted})")
                        
                        offset += len(rows)
                        
                        # Check if there are more rows
                        has_more = data_result.get('has_more', False)
                        if not has_more:
                            break
                    
                    if rows_inserted > 0:
                        logger.info(f"✓ Transferred {rows_inserted} rows from {source_table} to {target_table_upper}")
                        tables_transferred.append(target_table_upper)
                        total_rows += rows_inserted
                    else:
                        logger.warning(f"No data transferred for table {source_table}")
                
                # Commit all inserts
                conn.commit()
                
            finally:
                cursor.close()
                conn.close()
            
            # Capture LSN after full load
            logger.info(f"Capturing LSN/offset after full load for database {pipeline.source_database}...")
            try:
                lsn_info = source_connector.extract_lsn_offset(database=pipeline.source_database)
                lsn_value = lsn_info.get("lsn") if lsn_info else None
                
                if lsn_value:
                    logger.info(f"✅ Successfully captured LSN/offset: {lsn_value}")
                else:
                    logger.warning(f"⚠️  LSN extraction returned None. Full result: {lsn_info}")
            except Exception as e:
                logger.error(f"❌ Error extracting LSN/offset after full load: {e}", exc_info=True)
                lsn_value = None
                lsn_info = {}
            
            return {
                "success": True,
                "tables_transferred": tables_transferred,
                "total_rows": total_rows,
                "lsn": lsn_value,
                "offset": lsn_info.get("offset") if lsn_info else None,
                "timestamp": lsn_info.get("timestamp") if lsn_info else datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Full load to Snowflake failed: {e}", exc_info=True)
            raise FullLoadError(
                f"Full load to Snowflake failed: {str(e)}",
                rows_transferred=0,
                error=str(e)
            )
    
    def _discover_kafka_topics(
        self,
        pipeline_name: str,
        tables: List[str],
        schema: str = "public"
    ) -> List[str]:
        """Discover Kafka topics created by Debezium.
        
        Args:
            pipeline_name: Pipeline name (database.server.name)
            tables: List of source tables
            schema: Schema name (default: public)
            
        Returns:
            List of Kafka topic names
        """
        # Try to discover topics from Kafka first
        discovered_topics = []
        
        try:
            # Query Kafka REST API for topics (if available)
            # For now, we'll use the expected topic naming pattern
            # In production, you might want to use Kafka AdminClient to list topics
            logger.info(f"Discovering Kafka topics for pipeline {pipeline_name}")
            
            # Generate expected topic names based on Debezium naming convention
            # Format: {server_name}.{schema}.{table}
            for table in tables:
                # Use the same logic as DebeziumConfigGenerator
                topic = DebeziumConfigGenerator.get_topic_name(
                    pipeline_name=pipeline_name,
                    schema=schema,
                    table=table
                )
                discovered_topics.append(topic)
                logger.debug(f"Expected topic: {topic}")
            
            # TODO: In production, query actual Kafka topics using:
            # - Kafka AdminClient: admin_client.list_topics()
            # - Or Kafka REST API: GET /kafka/v3/clusters/{cluster_id}/topics
            # For now, we return expected topics
            
        except Exception as e:
            logger.warning(f"Failed to discover Kafka topics, using expected names: {e}")
            # Fallback to expected topic names
            for table in tables:
                topic = f"{pipeline_name}.{schema}.{table}"
                discovered_topics.append(topic)
        
        logger.info(f"Discovered {len(discovered_topics)} Kafka topics: {discovered_topics}")
        return discovered_topics
    
    def _auto_create_target_schema(
        self,
        pipeline: Pipeline,
        source_connection: Connection,
        target_connection: Connection
    ) -> bool:
        """Auto-create target schema and tables if enabled.
        
        Args:
            pipeline: Pipeline object
            source_connection: Source connection
            target_connection: Target connection
            
        Returns:
            True if schema/tables were created successfully, False otherwise
            
        Raises:
            FullLoadError: If critical schema creation failures occur
        """
        # Normalize database_type for comparison
        target_db_type = str(target_connection.database_type).lower()
        if target_db_type in ['aws_s3', 's3']:
            target_db_type = 's3'
        
        # Skip schema creation for S3 (S3 doesn't have schemas)
        if target_db_type == "s3":
            logger.info(f"Skipping schema creation for S3 target (S3 doesn't support schemas). Target DB type: {target_connection.database_type}")
            # S3 uses prefixes/folders instead of schemas, which are handled differently
            # Just proceed to table creation (which for S3 means creating the data files)
            target_schema = pipeline.target_schema or target_connection.schema or ""  # S3 uses empty or prefix
            target_database = pipeline.target_database or target_connection.database
        else:
            target_schema = pipeline.target_schema or target_connection.schema or ("public" if target_connection.database_type == "postgresql" else "dbo")
            target_database = pipeline.target_database or target_connection.database
            
            logger.info(f"Creating target schema: {target_schema} in database: {target_database}")
            
            # Create target schema if it doesn't exist
            try:
                schema_result = self.schema_service.create_target_schema(
                    connection_id=target_connection.id,
                    schema_name=target_schema,
                    database=target_database
                )
                
                if schema_result.get("success"):
                    if schema_result.get("created"):
                        logger.info(f"✓ Created target schema: {target_schema}")
                        # Wait a moment to ensure schema is visible across connections
                        import time
                        time.sleep(1)
                    else:
                        logger.info(f"✓ Target schema already exists: {target_schema}")
                else:
                    error_msg = schema_result.get('error', 'Unknown error')
                    logger.error(f"✗ Failed to create target schema: {error_msg}")
                    raise FullLoadError(
                        f"Failed to create target schema '{target_schema}': {error_msg}",
                        rows_transferred=0,
                        error=error_msg
                    )
            except FullLoadError:
                raise  # Re-raise FullLoadError
            except Exception as e:
                logger.error(f"✗ Error creating target schema: {e}", exc_info=True)
                raise FullLoadError(
                    f"Failed to create target schema '{target_schema}': {str(e)}",
                    rows_transferred=0,
                    error=str(e)
                )
        
        # Skip table creation for S3 (S3 doesn't have tables - data is stored as files)
        if target_db_type == "s3":
            logger.info(f"Skipping table creation for S3 target (S3 stores data as files, not database tables)")
            logger.info(f"✓ Schema/tables setup completed for S3 target")
            return True
        
        # Create target tables
        tables_created = 0
        tables_failed = 0
        
        for source_table in pipeline.source_tables:
            # Get target table name from mapping
            target_table = pipeline.target_table_mapping.get(source_table, source_table) if pipeline.target_table_mapping else source_table
            
            logger.info(f"Creating target table: {target_table} (from source: {source_table})")
            
            # Check if table exists
            try:
                table_schema = self.schema_service.connection_service.get_table_schema(
                    target_connection.id,
                    target_table,
                    database=target_database,
                    schema=target_schema
                )
                
                if table_schema.get("success"):
                    logger.info(f"✓ Target table {target_table} already exists, skipping creation")
                    tables_created += 1
                    continue
            except Exception:
                # Table doesn't exist, create it
                pass
            
            # Create table
            try:
                # For Oracle, use connection.database if pipeline.source_database is None
                source_db = pipeline.source_database or source_connection.database
                source_schema = pipeline.source_schema or source_connection.schema or source_connection.username
                
                table_result = self.schema_service.create_target_table(
                    source_connection_id=source_connection.id,
                    target_connection_id=target_connection.id,
                    table_name=source_table,
                    source_database=source_db,
                    source_schema=source_schema,
                    target_database=target_database,
                    target_schema=target_schema,
                    target_table_name=target_table
                )
                
                if table_result.get("success"):
                    logger.info(f"✓ Created target table: {target_table}")
                    tables_created += 1
                else:
                    error_msg = table_result.get('error', 'Unknown error')
                    logger.error(f"✗ Failed to create target table {target_table}: {error_msg}")
                    tables_failed += 1
                    # Raise error for critical table creation failures
                    raise FullLoadError(
                        f"Failed to create target table '{target_table}': {error_msg}",
                        rows_transferred=0,
                        error=error_msg
                    )
            except FullLoadError:
                raise  # Re-raise FullLoadError
            except Exception as e:
                logger.error(f"✗ Error creating target table {target_table}: {e}", exc_info=True)
                raise FullLoadError(
                    f"Failed to create target table '{target_table}': {str(e)}",
                    rows_transferred=0,
                    error=str(e)
                )
        
        if tables_created > 0:
            logger.info(f"✓ Schema creation completed: {tables_created} tables ready, {tables_failed} failed")
            return True
        else:
            logger.warning(f"⚠ No tables were created (all may already exist or all failed)")
            return False
    
    def _load_pipeline_from_db(self, pipeline_id: str) -> Optional[Pipeline]:
        """Load pipeline from database if not in memory store.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline object if found, None otherwise
        """
        try:
            from ingestion.database.session import get_db
            from ingestion.database.models_db import PipelineModel, ConnectionModel
            db = next(get_db())
            pipeline_model = db.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id,
                PipelineModel.deleted_at.is_(None)
            ).first()
            if pipeline_model:
                # Load connections
                source_conn_model = db.query(ConnectionModel).filter_by(id=pipeline_model.source_connection_id).first()
                target_conn_model = db.query(ConnectionModel).filter_by(id=pipeline_model.target_connection_id).first()
                
                if source_conn_model and target_conn_model:
                    # Convert to Connection objects
                    source_connection = Connection(
                        id=source_conn_model.id,
                        name=source_conn_model.name,
                        connection_type=source_conn_model.connection_type.value if hasattr(source_conn_model.connection_type, 'value') else str(source_conn_model.connection_type),
                        database_type=source_conn_model.database_type.value.lower() if hasattr(source_conn_model.database_type, 'value') else str(source_conn_model.database_type).lower(),
                        host=source_conn_model.host,
                        port=source_conn_model.port,
                        database=source_conn_model.database,
                        username=source_conn_model.username,
                        password=source_conn_model.password,
                        schema=source_conn_model.schema,
                        additional_config=source_conn_model.additional_config or {}
                    )
                    target_connection = Connection(
                        id=target_conn_model.id,
                        name=target_conn_model.name,
                        connection_type=target_conn_model.connection_type.value if hasattr(target_conn_model.connection_type, 'value') else str(target_conn_model.connection_type),
                        database_type=target_conn_model.database_type.value.lower() if hasattr(target_conn_model.database_type, 'value') else str(target_conn_model.database_type).lower(),
                        host=target_conn_model.host,
                        port=target_conn_model.port,
                        database=target_conn_model.database,
                        username=target_conn_model.username,
                        password=target_conn_model.password,
                        schema=target_conn_model.schema,
                        additional_config=target_conn_model.additional_config or {}
                    )
                    
                    # Convert PipelineModel to Pipeline
                    pipeline = Pipeline(
                        id=pipeline_model.id,
                        name=pipeline_model.name,
                        source_connection_id=pipeline_model.source_connection_id,
                        target_connection_id=pipeline_model.target_connection_id,
                        source_database=pipeline_model.source_database,
                        source_schema=pipeline_model.source_schema,
                        source_tables=pipeline_model.source_tables or [],
                        target_database=pipeline_model.target_database,
                        target_schema=pipeline_model.target_schema,
                        target_tables=pipeline_model.target_tables or [],
                        mode=pipeline_model.mode.value if hasattr(pipeline_model.mode, 'value') else str(pipeline_model.mode),
                        auto_create_target=pipeline_model.auto_create_target,
                        target_table_mapping=pipeline_model.target_table_mapping or {},
                        table_filter=pipeline_model.table_filter,
                        full_load_status=pipeline_model.full_load_status.value if hasattr(pipeline_model.full_load_status, 'value') else str(pipeline_model.full_load_status),
                        full_load_lsn=pipeline_model.full_load_lsn,
                        cdc_status=pipeline_model.cdc_status.value if hasattr(pipeline_model.cdc_status, 'value') else str(pipeline_model.cdc_status),
                        debezium_connector_name=pipeline_model.debezium_connector_name,
                        sink_connector_name=pipeline_model.sink_connector_name,
                        kafka_topics=pipeline_model.kafka_topics or [],
                        debezium_config=pipeline_model.debezium_config or {},
                        sink_config=pipeline_model.sink_config or {},
                        status=pipeline_model.status.value if hasattr(pipeline_model.status, 'value') else str(pipeline_model.status),
                        created_at=pipeline_model.created_at,
                        updated_at=pipeline_model.updated_at
                    )
                    
                    # Add connections and pipeline to store
                    self.add_connection(source_connection)
                    self.add_connection(target_connection)
                    self.pipeline_store[pipeline_id] = pipeline
                    logger.info(f"Loaded pipeline {pipeline_id} from database into memory store")
                    return pipeline
        except Exception as e:
            logger.warning(f"Could not load pipeline from database: {e}", exc_info=True)
        return None
    
    def stop_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Stop a CDC pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Stop result dictionary
        """
        pipeline = self.pipeline_store.get(pipeline_id)
        if not pipeline:
            # Try to load from database
            pipeline = self._load_pipeline_from_db(pipeline_id)
        if not pipeline:
            # If pipeline not in store and not in database, still return success
            # This handles cases where pipeline was already stopped or deleted
            logger.warning(f"Pipeline {pipeline_id} not found in store or database, assuming already stopped")
            return {
                "pipeline_id": pipeline_id,
                "debezium_stopped": False,
                "sink_stopped": False,
                "status": "STOPPED",
                "message": "Pipeline not found, assuming already stopped"
            }
        
        result = {
            "pipeline_id": pipeline_id,
            "debezium_stopped": False,
            "sink_stopped": False,
            "status": "STOPPING"
        }
        
        try:
            pipeline.status = PipelineStatus.STOPPING
            
            # Stop Debezium connector - try both pause and delete
            if pipeline.debezium_connector_name:
                try:
                    # First try to pause
                    self.kafka_client.pause_connector(pipeline.debezium_connector_name)
                    result["debezium_stopped"] = True
                    logger.info(f"Paused Debezium connector: {pipeline.debezium_connector_name}")
                except Exception as pause_error:
                    logger.warning(f"Failed to pause Debezium connector {pipeline.debezium_connector_name}: {pause_error}")
                    # If pause fails, try to delete (connector might not exist)
                    try:
                        self.kafka_client.delete_connector(pipeline.debezium_connector_name)
                        result["debezium_stopped"] = True
                        logger.info(f"Deleted Debezium connector: {pipeline.debezium_connector_name}")
                    except Exception as delete_error:
                        logger.warning(f"Failed to delete Debezium connector {pipeline.debezium_connector_name}: {delete_error}")
                        # Continue anyway - connector might already be stopped/deleted
            
            # Stop Sink connector - try both pause and delete
            if pipeline.sink_connector_name:
                try:
                    # First try to pause
                    self.kafka_client.pause_connector(pipeline.sink_connector_name)
                    result["sink_stopped"] = True
                    logger.info(f"Paused Sink connector: {pipeline.sink_connector_name}")
                except Exception as pause_error:
                    logger.warning(f"Failed to pause Sink connector {pipeline.sink_connector_name}: {pause_error}")
                    # If pause fails, try to delete (connector might not exist)
                    try:
                        self.kafka_client.delete_connector(pipeline.sink_connector_name)
                        result["sink_stopped"] = True
                        logger.info(f"Deleted Sink connector: {pipeline.sink_connector_name}")
                    except Exception as delete_error:
                        logger.warning(f"Failed to delete Sink connector {pipeline.sink_connector_name}: {delete_error}")
                        # Continue anyway - connector might already be stopped/deleted
            
            pipeline.status = PipelineStatus.STOPPED
            pipeline.cdc_status = CDCStatus.STOPPED
            result["status"] = "STOPPED"
            
            # Persist status to database
            self._persist_pipeline_status(pipeline)
            
            # Unregister topics from CDC event logger
            try:
                from ingestion.cdc_event_logger import get_event_logger
                event_logger = get_event_logger()
                if event_logger and pipeline.kafka_topics:
                    for topic in pipeline.kafka_topics:
                        event_logger.remove_topic(topic)
                    logger.info(f"Unregistered {len(pipeline.kafka_topics)} topics from CDC event logger for pipeline {pipeline.name}")
            except Exception as e:
                logger.warning(f"Failed to unregister topics from CDC event logger: {e}")
            
            logger.info(f"Pipeline stopped: {pipeline.name}")
            
        except Exception as e:
            logger.error(f"Failed to stop pipeline {pipeline_id}: {e}", exc_info=True)
            pipeline.status = PipelineStatus.ERROR
            result["status"] = "ERROR"
            result["error"] = str(e)
            raise
        
        return result
    
    def pause_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Pause a CDC pipeline (temporarily stop without deleting connectors).
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pause result dictionary
        """
        pipeline = self.pipeline_store.get(pipeline_id)
        if not pipeline:
            # Try to load from database
            pipeline = self._load_pipeline_from_db(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        result = {
            "pipeline_id": pipeline_id,
            "debezium_paused": False,
            "sink_paused": False,
            "status": "PAUSING"
        }
        
        try:
            # Pause Debezium connector
            if pipeline.debezium_connector_name:
                try:
                    self.kafka_client.pause_connector(pipeline.debezium_connector_name)
                    result["debezium_paused"] = True
                except Exception as e:
                    logger.warning(f"Failed to pause Debezium connector: {e}")
            
            # Pause Sink connector
            if pipeline.sink_connector_name:
                try:
                    self.kafka_client.pause_connector(pipeline.sink_connector_name)
                    result["sink_paused"] = True
                except Exception as e:
                    logger.warning(f"Failed to pause Sink connector: {e}")
            
            pipeline.status = PipelineStatus.PAUSED
            pipeline.cdc_status = CDCStatus.PAUSED
            result["status"] = "PAUSED"
            
            # Persist status to database
            self._persist_pipeline_status(pipeline)
            
            logger.info(f"Pipeline paused: {pipeline.name}")
            
        except Exception as e:
            logger.error(f"Failed to pause pipeline {pipeline_id}: {e}", exc_info=True)
            pipeline.status = PipelineStatus.ERROR
            result["status"] = "ERROR"
            result["error"] = str(e)
            raise
        
        return result
    
    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline status dictionary
        """
        pipeline = self.pipeline_store.get(pipeline_id)
        if not pipeline:
            # Try to load from database if not in memory store
            pipeline = self._load_pipeline_from_db(pipeline_id)
        
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        # Reload from database to get latest sink_connector_name if in-memory is stale
        # Also retrieve error messages from database
        error_message = None
        last_error_time = None
        pipeline_model = None
        error_message = None  # Initialize early to ensure filtering works
        last_error_time = None
        try:
            from ingestion.database.session import get_db
            from ingestion.database.models_db import PipelineModel, PipelineRunModel
            from sqlalchemy import desc
            db = next(get_db())
            pipeline_model = db.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id,
                PipelineModel.deleted_at.is_(None)
            ).first()
            
            # FIX: Early check and clear of stale errors from debezium_config (both DB and in-memory)
            if pipeline_model and pipeline_model.debezium_config and isinstance(pipeline_model.debezium_config, dict):
                if '_last_error' in pipeline_model.debezium_config:
                    last_error = pipeline_model.debezium_config.get('_last_error')
                    if isinstance(last_error, dict):
                        error_msg = last_error.get('message', '')
                        if error_msg and ":8080" in str(error_msg) and ":8083" not in str(error_msg):
                            logger.info(f"Early clearing of stale error from debezium_config for pipeline {pipeline_id}")
                            del pipeline_model.debezium_config['_last_error']
                            db.commit()
                            logger.info(f"Cleared stale error from debezium_config early in get_pipeline_status")
            
            # FIX: Also clear from in-memory pipeline object
            if pipeline.debezium_config and isinstance(pipeline.debezium_config, dict):
                if '_last_error' in pipeline.debezium_config:
                    last_error = pipeline.debezium_config.get('_last_error')
                    if isinstance(last_error, dict):
                        error_msg = last_error.get('message', '')
                        if error_msg and ":8080" in str(error_msg) and ":8083" not in str(error_msg):
                            logger.info(f"Early clearing of stale error from in-memory pipeline.debezium_config for pipeline {pipeline_id}")
                            del pipeline.debezium_config['_last_error']
                            logger.info(f"Cleared stale error from in-memory pipeline.debezium_config")
            if pipeline_model:
                # Reload connector names from database if they exist there but not in memory
                if pipeline_model.debezium_connector_name and not pipeline.debezium_connector_name:
                    pipeline.debezium_connector_name = pipeline_model.debezium_connector_name
                    logger.info(f"Reloaded debezium_connector_name from database: {pipeline.debezium_connector_name}")
                if pipeline_model.sink_connector_name and not pipeline.sink_connector_name:
                    pipeline.sink_connector_name = pipeline_model.sink_connector_name
                    logger.info(f"Reloaded sink_connector_name from database: {pipeline.sink_connector_name}")
                # Also reload kafka_topics if they exist in database
                if pipeline_model.kafka_topics and (not pipeline.kafka_topics or len(pipeline.kafka_topics) == 0):
                    pipeline.kafka_topics = pipeline_model.kafka_topics
                    logger.info(f"Reloaded kafka_topics from database: {pipeline.kafka_topics}")
                
                # FIX: If connector names are still missing, try to discover them from Kafka Connect
                # This handles cases where connectors exist but weren't persisted to database
                if not pipeline.debezium_connector_name or not pipeline.sink_connector_name or not pipeline.kafka_topics:
                    logger.info(f"Pipeline {pipeline_id} missing connector info, attempting to discover from Kafka Connect...")
                    try:
                        # Generate expected connector names based on pipeline configuration
                        source_connection = self.get_connection(pipeline.source_connection_id)
                        target_connection = self.get_connection(pipeline.target_connection_id)
                        
                        if source_connection:
                            expected_debezium_name = DebeziumConfigGenerator.generate_connector_name(
                                pipeline_name=pipeline.name,
                                database_type=source_connection.database_type,
                                schema=pipeline.source_schema
                            )
                            
                            # Check if this connector exists in Kafka Connect
                            if not pipeline.debezium_connector_name:
                                try:
                                    connector_status = self.kafka_client.get_connector_status(expected_debezium_name)
                                    if connector_status is not None:
                                        logger.info(f"Discovered Debezium connector in Kafka Connect: {expected_debezium_name}")
                                        pipeline.debezium_connector_name = expected_debezium_name
                                        # Get config from Kafka Connect
                                        try:
                                            pipeline.debezium_config = self.kafka_client.get_connector_config(expected_debezium_name)
                                        except Exception as e:
                                            logger.warning(f"Could not get Debezium config: {e}")
                                except Exception as e:
                                    logger.debug(f"Debezium connector {expected_debezium_name} not found in Kafka Connect: {e}")
                            
                            # Discover Kafka topics from the Debezium connector
                            if pipeline.debezium_connector_name and (not pipeline.kafka_topics or len(pipeline.kafka_topics) == 0):
                                try:
                                    # Ensure base_url doesn't have /connectors suffix (normalize it)
                                    base_url = self.kafka_client.base_url.rstrip('/connectors').rstrip('/')
                                    connector_topics_response = self.kafka_client.session.get(
                                        f"{base_url}/connectors/{pipeline.debezium_connector_name}/topics"
                                    )
                                    if connector_topics_response.status_code == 200:
                                        topics_data = connector_topics_response.json()
                                        connector_topics = topics_data.get(pipeline.debezium_connector_name, {}).get('topics', [])
                                        # Filter out schema change topic
                                        table_topics = [t for t in connector_topics if '.' in t and t != pipeline.name]
                                        if table_topics:
                                            pipeline.kafka_topics = table_topics
                                            logger.info(f"Discovered Kafka topics from connector: {pipeline.kafka_topics}")
                                except Exception as e:
                                    logger.warning(f"Could not discover topics from connector: {e}")
                        
                        if target_connection:
                            # Get default schema based on database type
                            default_target_schema = (
                                "dbo" if target_connection.database_type in ("sqlserver", "mssql")
                                else "PUBLIC" if target_connection.database_type == "snowflake"
                                else "public"
                            )
                            expected_sink_name = SinkConfigGenerator.generate_connector_name(
                                pipeline_name=pipeline.name,
                                database_type=target_connection.database_type,
                                schema=pipeline.target_schema or target_connection.schema or default_target_schema
                            )
                            
                            # Check if this connector exists in Kafka Connect
                            if not pipeline.sink_connector_name:
                                try:
                                    connector_status = self.kafka_client.get_connector_status(expected_sink_name)
                                    if connector_status is not None:
                                        logger.info(f"Discovered Sink connector in Kafka Connect: {expected_sink_name}")
                                        pipeline.sink_connector_name = expected_sink_name
                                        # Get config from Kafka Connect
                                        try:
                                            pipeline.sink_config = self.kafka_client.get_connector_config(expected_sink_name)
                                        except Exception as e:
                                            logger.warning(f"Could not get Sink config: {e}")
                                except Exception as e:
                                    logger.debug(f"Sink connector {expected_sink_name} not found in Kafka Connect: {e}")
                        
                        # If we discovered connector names or topics, persist them to database
                        if (pipeline.debezium_connector_name or pipeline.sink_connector_name or 
                            (pipeline.kafka_topics and len(pipeline.kafka_topics) > 0)):
                            logger.info(f"Persisting discovered connector info to database for pipeline {pipeline_id}")
                            self._persist_pipeline_status(pipeline)
                    except Exception as e:
                        logger.warning(f"Failed to discover connectors from Kafka Connect: {e}")
                
                # Get the most recent failed run to extract error message
                failed_run = db.query(PipelineRunModel).filter(
                    PipelineRunModel.pipeline_id == pipeline_id,
                    PipelineRunModel.error_message.isnot(None),
                    PipelineRunModel.error_message != ''
                ).order_by(desc(PipelineRunModel.started_at)).first()
                
                if failed_run and failed_run.error_message:
                    error_message = str(failed_run.error_message)
                    # FIX: Filter out stale error messages that contain old Kafka Connect URL (port 8080)
                    # These are from previous attempts before the URL was fixed
                    if ":8080" in error_message and ":8083" not in error_message:
                        logger.info(f"Ignoring stale error message with old URL (8080): {error_message[:200]}...")
                        # FIX: Also clear the error from the database to prevent it from showing again
                        try:
                            if 'db' in locals() and db:
                                failed_run.error_message = None
                                db.commit()
                                logger.info(f"Cleared stale error message from PipelineRunModel for pipeline {pipeline_id}")
                        except Exception as clear_error:
                            logger.warning(f"Could not clear stale error from database: {clear_error}")
                        error_message = None
                        last_error_time = None
                    else:
                        last_error_time = failed_run.started_at.isoformat() if failed_run.started_at else None
                        logger.info(f"Found error message from failed run: {error_message[:200]}...")
                
                # Fallback: check pipeline's debezium_config for last error
                if not error_message and pipeline_model.debezium_config and isinstance(pipeline_model.debezium_config, dict):
                    last_error_from_config = pipeline_model.debezium_config.get('_last_error')
                    if last_error_from_config and isinstance(last_error_from_config, dict) and last_error_from_config.get('message'):
                        error_message = last_error_from_config['message']
                        # FIX: Filter out stale error messages that contain old Kafka Connect URL (port 8080)
                        if ":8080" in error_message and ":8083" not in error_message:
                            logger.info(f"Ignoring stale error message with old URL (8080) from config: {error_message[:200]}...")
                            # FIX: Also clear the error from the database config to prevent it from showing again
                            try:
                                if '_last_error' in pipeline_model.debezium_config:
                                    del pipeline_model.debezium_config['_last_error']
                                    db.commit()
                                    logger.info(f"Cleared stale error from debezium_config for pipeline {pipeline_id}")
                            except Exception as clear_error:
                                logger.warning(f"Could not clear stale error from config: {clear_error}")
                            error_message = None
                            last_error_time = None
                        else:
                            last_error_time = last_error_from_config.get('timestamp')
                            logger.info(f"Found error message from pipeline config: {error_message[:200]}...")
        except Exception as e:
            logger.debug(f"Could not reload pipeline from database: {e}")
        
        # Get mode value - handle both enum and string
        mode_value = pipeline.mode.value if hasattr(pipeline.mode, 'value') else str(pipeline.mode) if pipeline.mode else None
        
        status = {
            "pipeline_id": pipeline_id,
            "id": pipeline_id,
            "name": pipeline.name,
            "status": pipeline.status.value if hasattr(pipeline.status, 'value') else str(pipeline.status),
            "full_load_status": pipeline.full_load_status.value if hasattr(pipeline.full_load_status, 'value') else str(pipeline.full_load_status),
            "cdc_status": pipeline.cdc_status.value if hasattr(pipeline.cdc_status, 'value') else str(pipeline.cdc_status),
            "mode": mode_value,
            "debezium_connector_name": pipeline.debezium_connector_name,
            "sink_connector_name": pipeline.sink_connector_name,
            "kafka_topics": pipeline.kafka_topics or [],
            "debezium_connector": None,
            "sink_connector": None,
            "source_connection_id": pipeline.source_connection_id,  # Add for frontend
            "target_connection_id": pipeline.target_connection_id,  # Add for frontend
            "source_uuid": pipeline.source_connection_id,  # Alias for frontend compatibility
            "target_uuid": pipeline.target_connection_id  # Alias for frontend compatibility
        }
        
        # Get Debezium connector status (with timeout to prevent blocking)
        if pipeline.debezium_connector_name:
            try:
                import threading
                
                # Use a timeout for connector status check (3 seconds max)
                debezium_status = None
                status_result = [None]  # Use list to allow modification in nested function
                exception_result = [None]
                
                def get_status():
                    try:
                        status_result[0] = self.kafka_client.get_connector_status(
                    pipeline.debezium_connector_name
                )
                    except Exception as e:
                        exception_result[0] = e
                
                # Run in a thread with timeout
                thread = threading.Thread(target=get_status)
                thread.daemon = True
                thread.start()
                thread.join(timeout=3.0)  # 3 second timeout
                
                if thread.is_alive():
                    logger.warning(f"Debezium connector status check timed out for {pipeline.debezium_connector_name}")
                    debezium_status = None
                else:
                    if exception_result[0]:
                        raise exception_result[0]
                    debezium_status = status_result[0]
                
                # None means connector doesn't exist or timed out - that's okay
                status["debezium_connector"] = debezium_status
                
                # Check for connector task errors and add to error message
                if debezium_status:
                    connector_state = debezium_status.get("connector", {}).get("state", "").upper()
                    tasks = debezium_status.get("tasks", [])
                    failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                    
                    if failed_tasks:
                        task_errors = []
                        for task in failed_tasks:
                            trace = task.get("trace", "")
                            if trace:
                                # Extract first line of error trace
                                first_line = trace.split('\n')[0] if trace else "Unknown error"
                                task_errors.append(f"Debezium task {task.get('id', 'unknown')}: {first_line}")
                        
                        if task_errors:
                            connector_error = "; ".join(task_errors[:3])  # Limit to first 3 errors
                            if error_message:
                                error_message = f"{error_message}; {connector_error}"
                            else:
                                error_message = connector_error
                            logger.info(f"Found Debezium connector task errors: {connector_error[:200]}...")
                    
                    # If connector is FAILED, also check for connector-level error
                    if connector_state == "FAILED" and not error_message:
                        connector_error = debezium_status.get("connector", {}).get("error", "")
                        if connector_error:
                            error_message = f"Debezium connector failed: {connector_error}"
                            logger.info(f"Found Debezium connector error: {error_message[:200]}...")
            except Exception as e:
                logger.warning(f"Failed to get Debezium connector status: {e}")
        
        # Get Sink connector status (with timeout to prevent blocking)
        if pipeline.sink_connector_name:
            try:
                import threading
                
                # Use a timeout for connector status check (3 seconds max)
                sink_status = None
                status_result = [None]
                exception_result = [None]
                
                def get_status():
                    try:
                        status_result[0] = self.kafka_client.get_connector_status(
                    pipeline.sink_connector_name
                )
                    except Exception as e:
                        exception_result[0] = e
                
                # Run in a thread with timeout
                thread = threading.Thread(target=get_status)
                thread.daemon = True
                thread.start()
                thread.join(timeout=3.0)  # 3 second timeout
                
                if thread.is_alive():
                    logger.warning(f"Sink connector status check timed out for {pipeline.sink_connector_name}")
                    sink_status = None
                else:
                    if exception_result[0]:
                        raise exception_result[0]
                    sink_status = status_result[0]
                
                # None means connector doesn't exist or timed out - that's okay
                status["sink_connector"] = sink_status
                
                # Check for connector task errors and add to error message
                if sink_status:
                    connector_state = sink_status.get("connector", {}).get("state", "").upper()
                    tasks = sink_status.get("tasks", [])
                    failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                    
                    if failed_tasks:
                        task_errors = []
                        for task in failed_tasks:
                            trace = task.get("trace", "")
                            if trace:
                                # Extract first line of error trace
                                first_line = trace.split('\n')[0] if trace else "Unknown error"
                                task_errors.append(f"Sink task {task.get('id', 'unknown')}: {first_line}")
                        
                        if task_errors:
                            connector_error = "; ".join(task_errors[:3])  # Limit to first 3 errors
                            if error_message:
                                error_message = f"{error_message}; {connector_error}"
                            else:
                                error_message = connector_error
                            logger.info(f"Found Sink connector task errors: {connector_error[:200]}...")
                    
                    # If connector is FAILED, also check for connector-level error
                    if connector_state == "FAILED" and not error_message:
                        connector_error = sink_status.get("connector", {}).get("error", "")
                        if connector_error:
                            error_message = f"Sink connector failed: {connector_error}"
                            logger.info(f"Found Sink connector error: {error_message[:200]}...")
            except Exception as e:
                logger.warning(f"Failed to get Sink connector status: {e}")
        
        # Update error message in status if we found one (even if status is not ERROR, CDC might be ERROR)
        # FIX: Only include error message if it's not a stale error with old URL
        if error_message:
            # Double-check: don't include stale errors with old URL
            if ":8080" in error_message and ":8083" not in error_message:
                logger.info("Filtering out stale error message with old URL from status response")
                error_message = None
                last_error_time = None
            
        if error_message:
            status["error_message"] = error_message
            if last_error_time:
                status["last_error_time"] = last_error_time
            # Also add to status if CDC status is ERROR
            if pipeline.cdc_status == CDCStatus.ERROR:
                status["cdc_error"] = error_message
        
        # FIX: Clear stale error messages that contain old URL before checking stuck states
        # This ensures we don't show outdated errors
        if error_message and ":8080" in error_message and ":8083" not in error_message:
            logger.info(f"Clearing stale error message with old URL (8080) for pipeline {pipeline_id}")
            error_message = None
            last_error_time = None
            # Also clear from status if it was already set
            if "error_message" in status:
                del status["error_message"]
            if "cdc_error" in status:
                del status["cdc_error"]
            if "last_error_time" in status:
                del status["last_error_time"]
        
        # Detect stuck STARTING state: CDC is STARTING but connectors don't exist
        # This indicates CDC setup failed or is stuck
        if (pipeline.cdc_status == CDCStatus.STARTING or pipeline.status == PipelineStatus.STARTING):
            # Check if connectors should exist but don't
            if not pipeline.debezium_connector_name and not pipeline.sink_connector_name:
                # Check if full load is completed - if so, CDC should have started
                if pipeline.full_load_status == FullLoadStatus.COMPLETED:
                    # Full load completed but CDC connectors don't exist - this is an error state
                    logger.warning(f"Pipeline {pipeline_id}: CDC status is STARTING but connectors don't exist after full load completion. This indicates CDC setup failed.")
                    
                    # Check database for error messages if not already found
                    if not error_message:
                        # Try to reload pipeline_model if not already loaded
                        if not pipeline_model:
                            try:
                                from ingestion.database.session import get_db
                                from ingestion.database.models_db import PipelineModel
                                db = next(get_db())
                                pipeline_model = db.query(PipelineModel).filter(
                                    PipelineModel.id == pipeline_id,
                                    PipelineModel.deleted_at.is_(None)
                                ).first()
                            except Exception as e:
                                logger.debug(f"Could not reload pipeline_model: {e}")
                        
                        # Check debezium_config for last error
                        if pipeline_model and pipeline_model.debezium_config and isinstance(pipeline_model.debezium_config, dict):
                            last_error = pipeline_model.debezium_config.get('_last_error')
                            if last_error and isinstance(last_error, dict):
                                error_message = last_error.get('message', 'CDC setup failed: Connectors were not created')
                                # FIX: Filter out stale errors with old URL
                                if error_message and ":8080" in error_message and ":8083" not in error_message:
                                    logger.info(f"Filtering out stale error with old URL (8080) from debezium_config")
                                    error_message = None
                        
                        if not error_message:
                            error_message = "CDC setup failed: Connectors were not created after full load completion. Please restart the pipeline to retry with the correct configuration."
                    
                    # FIX: Filter out stale error messages before setting status
                    if error_message and ":8080" in error_message and ":8083" not in error_message:
                        logger.info(f"Filtering out stale error message with old URL (8080) before setting status")
                        error_message = "CDC setup failed: Connectors were not created after full load completion. Please restart the pipeline to retry with the correct configuration."
                    
                    # FIX: Filter out stale error messages before setting status
                    if error_message and ":8080" in error_message and ":8083" not in error_message:
                        logger.info(f"Filtering out stale error message with old URL (8080) before setting stuck state error")
                        error_message = "CDC setup failed: Connectors were not created after full load completion. Please restart the pipeline to retry with the correct configuration."
                    
                    # Update status to ERROR
                    pipeline.status = PipelineStatus.ERROR
                    pipeline.cdc_status = CDCStatus.ERROR
                    status["status"] = "ERROR"
                    status["cdc_status"] = "ERROR"
                    if error_message:
                        status["error_message"] = error_message
                        logger.info(f"Updated pipeline {pipeline_id} status to ERROR: {error_message}")
                    
                    # Persist the error status
                    self._persist_pipeline_status(pipeline)
        
        # Update status based on actual connector states
        if status.get("debezium_connector") and status.get("sink_connector"):
            dbz_state = status["debezium_connector"].get("connector", {}).get("state", "").upper()
            sink_state = status["sink_connector"].get("connector", {}).get("state", "").upper()
            
            if dbz_state == "RUNNING" and sink_state == "RUNNING":
                # Both connectors are running, update status to RUNNING
                if pipeline.status != PipelineStatus.RUNNING:
                    logger.info(f"Updating pipeline status to RUNNING (connectors are running)")
                    pipeline.status = PipelineStatus.RUNNING
                    pipeline.cdc_status = CDCStatus.RUNNING
                    status["status"] = "RUNNING"
                    status["cdc_status"] = "RUNNING"
                    # Persist the updated status
                    self._persist_pipeline_status(pipeline)
            elif dbz_state == "FAILED" or sink_state == "FAILED":
                # At least one connector failed
                if pipeline.status != PipelineStatus.ERROR:
                    logger.warning(f"Updating pipeline status to ERROR (connector failed: Debezium={dbz_state}, Sink={sink_state})")
                    pipeline.status = PipelineStatus.ERROR
                    pipeline.cdc_status = CDCStatus.ERROR
                    status["status"] = "ERROR"
                    status["cdc_status"] = "ERROR"
                    # Persist the error status
                    self._persist_pipeline_status(pipeline)
        elif (pipeline.debezium_connector_name or pipeline.sink_connector_name):
            # Connectors should exist but status check failed
            # This might be a transient issue, but if we have connector names, try to get status
            if not status.get("debezium_connector") and pipeline.debezium_connector_name:
                logger.warning(f"Debezium connector {pipeline.debezium_connector_name} should exist but status check returned None")
            if not status.get("sink_connector") and pipeline.sink_connector_name:
                logger.warning(f"Sink connector {pipeline.sink_connector_name} should exist but status check returned None")
        
        return status
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all pipelines.
        
        Returns:
            List of pipeline dictionaries
        """
        return [pipeline.to_dict() for pipeline in self.pipeline_store.values()]

