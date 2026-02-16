"""Database models for CDC pipeline management."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
import json


class PipelineStatus(str, Enum):
    """Pipeline status enumeration."""
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class FullLoadStatus(str, Enum):
    """Full load status enumeration."""
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CDCStatus(str, Enum):
    """CDC status enumeration."""
    NOT_STARTED = "NOT_STARTED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class PipelineMode(str, Enum):
    """Pipeline mode enumeration."""
    FULL_LOAD_ONLY = "full_load_only"
    CDC_ONLY = "cdc_only"
    FULL_LOAD_AND_CDC = "full_load_and_cdc"


class UserRole(str, Enum):
    """User role enumeration."""
    USER = "user"
    OPERATOR = "operator"
    VIEWER = "viewer"
    ADMIN = "admin"


class Connection:
    """Database connection model.
    
    This is a simple data class to represent database connections.
    In a full implementation, this would use SQLAlchemy ORM.
    """
    
    def __init__(
        self,
        id: str,
        name: str,
        connection_type: str,  # 'source' or 'target'
        database_type: str,  # 'postgresql', 'sqlserver', etc.
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        schema: Optional[str] = None,
        additional_config: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.id = id
        self.name = name
        self.connection_type = connection_type
        self.database_type = database_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.schema = schema
        self.additional_config = additional_config or {}
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert connection to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "connection_type": self.connection_type,
            "database_type": self.database_type,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": "***",  # Don't expose password
            "schema": self.schema,
            "additional_config": self.additional_config,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    def get_connection_config(self) -> Dict[str, Any]:
        """Get connection configuration for connector initialization."""
        # Handle S3 connections specially
        if self.database_type == "s3":
            config = {
                "bucket": self.database,  # Bucket name is stored in database field
                "aws_access_key_id": self.username,  # Access key is stored in username field
                "aws_secret_access_key": self.password,  # Secret key is stored in password field
            }
            if self.schema:
                config["prefix"] = self.schema  # S3 prefix is stored in schema field
            # Add additional config (region, endpoint, etc.)
            config.update(self.additional_config)
            return config
        
        # Handle database connections
        # PostgreSQL and Oracle use 'host', SQL Server and AS400 use 'server'
        host_key = "host" if self.database_type in ["postgresql", "oracle"] else "server"
        config = {
            host_key: self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
        }
        
        if self.schema:
            config["schema"] = self.schema
        
        # For Oracle, ensure 'username' is also set (connector normalizes 'user' to 'username')
        if self.database_type == "oracle":
            config["username"] = self.username
        
        # For SQL Server, set default SSL settings to avoid certificate verification issues
        if self.database_type in ["sqlserver", "mssql"]:
            # Default to trusting server certificate to avoid SSL verification issues
            # This can be overridden in additional_config if needed
            if "trust_server_certificate" not in self.additional_config:
                config["trust_server_certificate"] = True
            # Default to encrypting connection (required for ODBC Driver 18)
            if "encrypt" not in self.additional_config:
                config["encrypt"] = True
        
        # For AS400, set default port if not specified
        if self.database_type in ["as400", "ibm_i"]:
            if "port" not in config or not config["port"]:
                config["port"] = 446  # Default AS400 port
        
        # Add additional config (this will override defaults if explicitly set)
        config.update(self.additional_config)
        
        return config


class Pipeline:
    """CDC pipeline model.
    
    This is a simple data class to represent CDC pipelines.
    In a full implementation, this would use SQLAlchemy ORM.
    """
    
    def __init__(
        self,
        id: str,
        name: str,
        source_connection_id: str,
        target_connection_id: str,
        source_database: str,
        source_schema: str,
        source_tables: List[str],
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_tables: Optional[List[str]] = None,
        mode: str = PipelineMode.FULL_LOAD_AND_CDC,
        enable_full_load: Optional[bool] = None,  # Deprecated, use mode instead
        auto_create_target: bool = True,
        target_table_mapping: Optional[Dict[str, str]] = None,
        table_filter: Optional[str] = None,
        full_load_status: str = FullLoadStatus.NOT_STARTED,
        full_load_lsn: Optional[str] = None,
        cdc_status: str = CDCStatus.NOT_STARTED,
        debezium_connector_name: Optional[str] = None,
        sink_connector_name: Optional[str] = None,
        kafka_topics: Optional[List[str]] = None,
        debezium_config: Optional[Dict[str, Any]] = None,
        sink_config: Optional[Dict[str, Any]] = None,
        status: str = PipelineStatus.STOPPED,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.id = id
        self.name = name
        self.source_connection_id = source_connection_id
        self.target_connection_id = target_connection_id
        self.source_database = source_database
        self.source_schema = source_schema
        self.source_tables = source_tables
        self.target_database = target_database
        self.target_schema = target_schema
        self.target_tables = target_tables or source_tables.copy()
        
        # Handle backward compatibility: if enable_full_load is provided, convert to mode
        if enable_full_load is not None:
            if enable_full_load:
                self.mode = PipelineMode.FULL_LOAD_AND_CDC if mode == PipelineMode.FULL_LOAD_AND_CDC else PipelineMode.FULL_LOAD_ONLY
            else:
                self.mode = PipelineMode.CDC_ONLY
        else:
            self.mode = mode if isinstance(mode, str) else mode.value if hasattr(mode, 'value') else PipelineMode.FULL_LOAD_AND_CDC
        
        self.auto_create_target = auto_create_target
        self.target_table_mapping = target_table_mapping or {}
        self.table_filter = table_filter
        
        # Backward compatibility property
        self.enable_full_load = self.mode in [PipelineMode.FULL_LOAD_ONLY, PipelineMode.FULL_LOAD_AND_CDC]
        
        self.full_load_status = full_load_status
        self.full_load_lsn = full_load_lsn
        self.cdc_status = cdc_status
        self.debezium_connector_name = debezium_connector_name
        self.sink_connector_name = sink_connector_name
        self.kafka_topics = kafka_topics or []
        self.debezium_config = debezium_config or {}
        self.sink_config = sink_config or {}
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert pipeline to dictionary."""
        mode_value = self.mode.value if hasattr(self.mode, 'value') else str(self.mode)
        return {
            "id": self.id,
            "name": self.name,
            "source_connection_id": self.source_connection_id,
            "target_connection_id": self.target_connection_id,
            "source_database": self.source_database,
            "source_schema": self.source_schema,
            "source_tables": self.source_tables,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "target_tables": self.target_tables,
            "mode": mode_value,
            "enable_full_load": self.enable_full_load,  # Backward compatibility
            "auto_create_target": self.auto_create_target,
            "target_table_mapping": self.target_table_mapping,
            "table_filter": self.table_filter,
            "full_load_status": self.full_load_status,
            "full_load_lsn": self.full_load_lsn,
            "cdc_status": self.cdc_status,
            "debezium_connector_name": self.debezium_connector_name,
            "sink_connector_name": self.sink_connector_name,
            "kafka_topics": self.kafka_topics,
            "debezium_config": self.debezium_config,
            "sink_config": self.sink_config,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class ConnectorStatus:
    """Kafka Connect connector status model."""
    
    def __init__(
        self,
        name: str,
        connector_state: str,
        worker_id: Optional[str] = None,
        tasks: Optional[List[Dict[str, Any]]] = None
    ):
        self.name = name
        self.connector_state = connector_state  # RUNNING, FAILED, PAUSED, etc.
        self.worker_id = worker_id
        self.tasks = tasks or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert connector status to dictionary."""
        return {
            "name": self.name,
            "connector_state": self.connector_state,
            "worker_id": self.worker_id,
            "tasks": self.tasks
        }
    
    @property
    def is_running(self) -> bool:
        """Check if connector is running."""
        return self.connector_state == "RUNNING"
    
    @property
    def is_failed(self) -> bool:
        """Check if connector has failed."""
        return self.connector_state == "FAILED"
    
    @property
    def has_failed_tasks(self) -> bool:
        """Check if any tasks have failed."""
        return any(task.get("state") == "FAILED" for task in self.tasks)



