"""SQLAlchemy ORM models for CDC pipeline management."""

from datetime import datetime
from typing import Optional
import uuid
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, JSON, Enum as SQLEnum, ForeignKey, Index
from sqlalchemy.orm import relationship
from ingestion.database.base import Base
import enum


class ConnectionType(str, enum.Enum):
    SOURCE = "source"
    TARGET = "target"


class DatabaseType(str, enum.Enum):
    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    MYSQL = "mysql"
    S3 = "s3"
    AWS_S3 = "aws_s3"  # Alternative name for S3
    AS400 = "as400"
    IBM_I = "ibm_i"  # Alternative name for AS400
    DB2 = "db2"  # IBM DB2 (maps to AS400 connector)
    SNOWFLAKE = "snowflake"
    ORACLE = "oracle"


class PipelineStatus(str, enum.Enum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class FullLoadStatus(str, enum.Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CDCStatus(str, enum.Enum):
    NOT_STARTED = "NOT_STARTED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class PipelineMode(str, enum.Enum):
    FULL_LOAD_ONLY = "full_load_only"
    CDC_ONLY = "cdc_only"
    FULL_LOAD_AND_CDC = "full_load_and_cdc"


class ConnectionModel(Base):
    __tablename__ = "connections"
    
    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    connection_type = Column(SQLEnum(ConnectionType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    database_type = Column(SQLEnum(DatabaseType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    database = Column(String(255), nullable=False)
    username = Column(String(255), nullable=False)
    password = Column(Text, nullable=False)
    schema = Column(String(255), nullable=True)
    additional_config = Column(JSON, default={})
    
    is_active = Column(Boolean, default=True)
    last_tested_at = Column(DateTime, nullable=True)
    last_test_status = Column(String(50), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    
    pipelines_as_source = relationship("PipelineModel", foreign_keys="PipelineModel.source_connection_id", back_populates="source_connection")
    pipelines_as_target = relationship("PipelineModel", foreign_keys="PipelineModel.target_connection_id", back_populates="target_connection")
    test_history = relationship("ConnectionTestModel", back_populates="connection", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_connection_type_active', 'connection_type', 'is_active'),
    )


class PipelineModel(Base):
    __tablename__ = "pipelines"
    
    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    
    source_connection_id = Column(String(36), ForeignKey('connections.id'), nullable=False)
    target_connection_id = Column(String(36), ForeignKey('connections.id'), nullable=False)
    
    source_database = Column(String(255), nullable=False)
    source_schema = Column(String(255), nullable=False)
    source_tables = Column(JSON, nullable=False)
    
    target_database = Column(String(255), nullable=True)
    target_schema = Column(String(255), nullable=True)
    target_tables = Column(JSON, nullable=True)
    
    mode = Column(SQLEnum(PipelineMode, values_callable=lambda x: [e.value for e in x]), default=PipelineMode.FULL_LOAD_AND_CDC, nullable=False)
    
    enable_full_load = Column(Boolean, default=True)
    full_load_status = Column(SQLEnum(FullLoadStatus, values_callable=lambda x: [e.value for e in x]), default=FullLoadStatus.NOT_STARTED)
    full_load_lsn = Column(Text, nullable=True)
    full_load_completed_at = Column(DateTime, nullable=True)
    
    cdc_status = Column(SQLEnum(CDCStatus, values_callable=lambda x: [e.value for e in x]), default=CDCStatus.NOT_STARTED)
    status = Column(SQLEnum(PipelineStatus, values_callable=lambda x: [e.value for e in x]), default=PipelineStatus.STOPPED)
    
    debezium_connector_name = Column(String(255), nullable=True)
    sink_connector_name = Column(String(255), nullable=True)
    kafka_topics = Column(JSON, default=[])
    
    debezium_config = Column(JSON, default={})
    sink_config = Column(JSON, default={})
    
    auto_create_target = Column(Boolean, default=True)
    target_table_mapping = Column(JSON, nullable=True)  # Dict mapping source_table -> target_table
    table_filter = Column(String(500), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    
    source_connection = relationship("ConnectionModel", foreign_keys=[source_connection_id], back_populates="pipelines_as_source")
    target_connection = relationship("ConnectionModel", foreign_keys=[target_connection_id], back_populates="pipelines_as_target")
    runs = relationship("PipelineRunModel", back_populates="pipeline", cascade="all, delete-orphan")
    metrics = relationship("PipelineMetricsModel", back_populates="pipeline", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_pipeline_status', 'status'),
        Index('idx_pipeline_cdc_status', 'cdc_status'),
    )


class PipelineRunModel(Base):
    __tablename__ = "pipeline_runs"
    
    id = Column(String(36), primary_key=True)
    pipeline_id = Column(String(36), ForeignKey('pipelines.id'), nullable=False)
    
    run_type = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False)
    
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    
    rows_processed = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    
    run_metadata = Column(JSON, default={})
    
    pipeline = relationship("PipelineModel", back_populates="runs")
    
    __table_args__ = (
        Index('idx_run_pipeline_started', 'pipeline_id', 'started_at'),
    )


class ConnectionTestModel(Base):
    __tablename__ = "connection_tests"
    
    id = Column(String(36), primary_key=True)
    connection_id = Column(String(36), ForeignKey('connections.id'), nullable=False)
    
    test_status = Column(String(50), nullable=False)
    response_time_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    
    tested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    connection = relationship("ConnectionModel", back_populates="test_history")
    
    __table_args__ = (
        Index('idx_test_connection_tested', 'connection_id', 'tested_at'),
    )


class PipelineMetricsModel(Base):
    __tablename__ = "pipeline_metrics"
    
    id = Column(String(36), primary_key=True)
    pipeline_id = Column(String(36), ForeignKey('pipelines.id'), nullable=False)
    
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    throughput_events_per_sec = Column(Integer, default=0)
    lag_seconds = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    
    source_offset = Column(Text, nullable=True)
    target_offset = Column(Text, nullable=True)
    
    connector_status = Column(JSON, default={})
    
    pipeline = relationship("PipelineModel", back_populates="metrics")
    
    __table_args__ = (
        Index('idx_metrics_pipeline_timestamp', 'pipeline_id', 'timestamp'),
    )


class AlertRuleModel(Base):
    __tablename__ = "alert_rules"
    
    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    
    metric = Column(String(100), nullable=False)
    condition = Column(String(50), nullable=False)
    threshold = Column(Integer, nullable=False)
    duration_seconds = Column(Integer, default=60)
    
    severity = Column(String(50), nullable=False)
    channels = Column(JSON, default=[])
    
    enabled = Column(Boolean, default=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    alerts = relationship("AlertHistoryModel", back_populates="rule", cascade="all, delete-orphan")


class AlertHistoryModel(Base):
    __tablename__ = "alert_history"
    
    id = Column(String(36), primary_key=True)
    rule_id = Column(String(36), ForeignKey('alert_rules.id'), nullable=False)
    pipeline_id = Column(String(36), nullable=True)
    
    status = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    
    triggered_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    resolved_at = Column(DateTime, nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)
    
    alert_metadata = Column(JSON, default={})
    
    rule = relationship("AlertRuleModel", back_populates="alerts")
    
    __table_args__ = (
        Index('idx_alert_triggered', 'triggered_at'),
        Index('idx_alert_status', 'status'),
    )


class AuditLogModel(Base):
    """Audit log model for tracking all user actions.
    
    Supports both old structure (entity_type/entity_id) and new structure (resource_type/resource_id).
    """
    __tablename__ = "audit_logs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    
    # New structure (GitHub repo)
    tenant_id = Column(String(36), nullable=True, index=True)  # Tenant isolation
    user_id = Column(String(36), nullable=True, index=True)
    action = Column(String(100), nullable=False, index=True)
    resource_type = Column(String(50), nullable=True, index=True)  # Maps to entity_type
    resource_id = Column(String(36), nullable=True, index=True)  # Maps to entity_id
    old_value = Column(JSON, nullable=True)  # Maps to old_values
    new_value = Column(JSON, nullable=True)  # Maps to new_values
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Legacy structure (for backward compatibility)
    entity_type = Column(String(50), nullable=True)  # Maps to resource_type
    entity_id = Column(String(36), nullable=True)  # Maps to resource_id
    old_values = Column(JSON, nullable=True)  # Maps to old_value
    new_values = Column(JSON, nullable=True)  # Maps to new_value
    timestamp = Column(DateTime, nullable=True)  # Maps to created_at
    
    __table_args__ = (
        Index('idx_audit_entity', 'entity_type', 'entity_id'),
        Index('idx_audit_timestamp', 'timestamp'),
    )


class UserModel(Base):
    """User model for authentication and authorization.
    
    Note: New columns (tenant_id, status, last_login) are nullable for backward compatibility.
    Run the migration script to add these columns to the database.
    """
    __tablename__ = "users"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    hashed_password = Column(Text, nullable=False)
    role_name = Column(String(50), nullable=False, default="user")  # 'user', 'operator', 'viewer', 'admin'
    is_active = Column(Boolean, default=True, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # New columns - added via migration (nullable for backward compatibility)
    # These will be None until migration is run
    tenant_id = Column(String(36), nullable=True)  # Tenant isolation (UUID as string)
    status = Column(String(20), nullable=True)  # User status (INVITED, ACTIVE, SUSPENDED, DEACTIVATED)
    last_login = Column(DateTime, nullable=True)  # Last login timestamp
    
    __table_args__ = (
        Index('idx_users_email', 'email'),
    )


class ApplicationLogModel(Base):
    """Application log entry model."""
    __tablename__ = "application_logs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    level = Column(String(20), nullable=False, index=True)  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    logger = Column(String(255), nullable=True, index=True)  # Logger name
    message = Column(Text, nullable=False)  # Log message
    module = Column(String(255), nullable=True)  # Module name
    function = Column(String(255), nullable=True)  # Function name
    line = Column(Integer, nullable=True)  # Line number
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    extra = Column(JSON, nullable=True)  # Additional context data
    
    __table_args__ = (
        Index('idx_log_level_timestamp', 'level', 'timestamp'),
        Index('idx_log_timestamp', 'timestamp'),
    )


class UserSessionModel(Base):
    """User session model for refresh tokens."""
    __tablename__ = "user_sessions"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), nullable=False, index=True)
    refresh_token_hash = Column(Text, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        """Safe string representation that doesn't expose sensitive data."""
        return f"<UserSessionModel(id='{self.id[:8]}...', user_id='{self.user_id[:8] if self.user_id else None}...', expires_at={self.expires_at})>"
