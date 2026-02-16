"""Initial migration - create all tables.

Revision ID: 001
Revises: 
Create Date: 2025-12-24

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'connections',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('connection_type', sa.Enum('source', 'target', name='connectiontype'), nullable=False),
        sa.Column('database_type', sa.Enum('postgresql', 'sqlserver', 'mysql', name='databasetype'), nullable=False),
        sa.Column('host', sa.String(255), nullable=False),
        sa.Column('port', sa.Integer(), nullable=False),
        sa.Column('database', sa.String(255), nullable=False),
        sa.Column('username', sa.String(255), nullable=False),
        sa.Column('password', sa.Text(), nullable=False),
        sa.Column('schema', sa.String(255), nullable=True),
        sa.Column('additional_config', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('last_tested_at', sa.DateTime(), nullable=True),
        sa.Column('last_test_status', sa.String(50), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('deleted_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_connection_type_active', 'connections', ['connection_type', 'is_active'])
    op.create_index(op.f('ix_connections_name'), 'connections', ['name'])
    
    op.create_table(
        'pipelines',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('source_connection_id', sa.String(36), nullable=False),
        sa.Column('target_connection_id', sa.String(36), nullable=False),
        sa.Column('source_database', sa.String(255), nullable=False),
        sa.Column('source_schema', sa.String(255), nullable=False),
        sa.Column('source_tables', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('target_database', sa.String(255), nullable=True),
        sa.Column('target_schema', sa.String(255), nullable=True),
        sa.Column('target_tables', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('mode', sa.Enum('full_load_only', 'cdc_only', 'full_load_and_cdc', name='pipelinemode'), nullable=False),
        sa.Column('enable_full_load', sa.Boolean(), default=True),
        sa.Column('full_load_status', sa.Enum('NOT_STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', name='fullloadstatus'), nullable=True),
        sa.Column('full_load_lsn', sa.Text(), nullable=True),
        sa.Column('full_load_completed_at', sa.DateTime(), nullable=True),
        sa.Column('cdc_status', sa.Enum('NOT_STARTED', 'STARTING', 'RUNNING', 'STOPPED', 'ERROR', name='cdcstatus'), nullable=True),
        sa.Column('status', sa.Enum('STOPPED', 'STARTING', 'RUNNING', 'STOPPING', 'ERROR', name='pipelinestatus'), nullable=False),
        sa.Column('debezium_connector_name', sa.String(255), nullable=True),
        sa.Column('sink_connector_name', sa.String(255), nullable=True),
        sa.Column('kafka_topics', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('debezium_config', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('sink_config', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('auto_create_target', sa.Boolean(), default=True),
        sa.Column('table_filter', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('deleted_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['source_connection_id'], ['connections.id']),
        sa.ForeignKeyConstraint(['target_connection_id'], ['connections.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name')
    )
    op.create_index('idx_pipeline_status', 'pipelines', ['status'])
    op.create_index('idx_pipeline_cdc_status', 'pipelines', ['cdc_status'])
    op.create_index(op.f('ix_pipelines_name'), 'pipelines', ['name'], unique=True)
    
    op.create_table(
        'pipeline_runs',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('pipeline_id', sa.String(36), nullable=False),
        sa.Column('run_type', sa.String(50), nullable=False),
        sa.Column('status', sa.String(50), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('rows_processed', sa.Integer(), default=0),
        sa.Column('errors_count', sa.Integer(), default=0),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('run_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(['pipeline_id'], ['pipelines.id']),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_run_pipeline_started', 'pipeline_runs', ['pipeline_id', 'started_at'])
    
    op.create_table(
        'connection_tests',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('connection_id', sa.String(36), nullable=False),
        sa.Column('test_status', sa.String(50), nullable=False),
        sa.Column('response_time_ms', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('tested_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['connection_id'], ['connections.id']),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_test_connection_tested', 'connection_tests', ['connection_id', 'tested_at'])
    
    op.create_table(
        'pipeline_metrics',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('pipeline_id', sa.String(36), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('throughput_events_per_sec', sa.Integer(), default=0),
        sa.Column('lag_seconds', sa.Integer(), default=0),
        sa.Column('error_count', sa.Integer(), default=0),
        sa.Column('bytes_processed', sa.Integer(), default=0),
        sa.Column('source_offset', sa.Text(), nullable=True),
        sa.Column('target_offset', sa.Text(), nullable=True),
        sa.Column('connector_status', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(['pipeline_id'], ['pipelines.id']),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_metrics_pipeline_timestamp', 'pipeline_metrics', ['pipeline_id', 'timestamp'])
    op.create_index(op.f('ix_pipeline_metrics_timestamp'), 'pipeline_metrics', ['timestamp'])
    
    op.create_table(
        'alert_rules',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('metric', sa.String(100), nullable=False),
        sa.Column('condition', sa.String(50), nullable=False),
        sa.Column('threshold', sa.Integer(), nullable=False),
        sa.Column('duration_seconds', sa.Integer(), default=60),
        sa.Column('severity', sa.String(50), nullable=False),
        sa.Column('channels', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('enabled', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_table(
        'alert_history',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('rule_id', sa.String(36), nullable=False),
        sa.Column('pipeline_id', sa.String(36), nullable=True),
        sa.Column('status', sa.String(50), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('triggered_at', sa.DateTime(), nullable=False),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.Column('acknowledged_at', sa.DateTime(), nullable=True),
        sa.Column('alert_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(['rule_id'], ['alert_rules.id']),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_alert_triggered', 'alert_history', ['triggered_at'])
    op.create_index('idx_alert_status', 'alert_history', ['status'])
    
    op.create_table(
        'audit_logs',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('entity_type', sa.String(50), nullable=False),
        sa.Column('entity_id', sa.String(36), nullable=False),
        sa.Column('action', sa.String(50), nullable=False),
        sa.Column('user_id', sa.String(36), nullable=True),
        sa.Column('old_values', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('new_values', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_audit_entity', 'audit_logs', ['entity_type', 'entity_id'])
    op.create_index('idx_audit_timestamp', 'audit_logs', ['timestamp'])


def downgrade() -> None:
    op.drop_table('audit_logs')
    op.drop_table('alert_history')
    op.drop_table('alert_rules')
    op.drop_table('pipeline_metrics')
    op.drop_table('connection_tests')
    op.drop_table('pipeline_runs')
    op.drop_table('pipelines')
    op.drop_table('connections')
