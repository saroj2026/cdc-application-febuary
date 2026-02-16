"""CLI tool for CDC pipeline management."""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Optional

try:
    import click
    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False
    # Create dummy click for when not available
    class click:
        @staticmethod
        def group(*args, **kwargs):
            return lambda f: f
        @staticmethod
        def command(*args, **kwargs):
            return lambda f: f
        @staticmethod
        def option(*args, **kwargs):
            return lambda f: f
        @staticmethod
        def argument(*args, **kwargs):
            return lambda f: f
        @staticmethod
        def echo(*args, **kwargs):
            print(*args)

from ingestion.cdc_manager import CDCManager
from ingestion.pipeline_service import PipelineService
from ingestion.models import Connection, Pipeline, PipelineStatus, FullLoadStatus, CDCStatus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize services
cdc_manager = CDCManager()
pipeline_service = PipelineService(cdc_manager)


@click.group()
def cdc():
    """CDC Pipeline Management CLI."""
    pass


@cdc.command()
@click.option('--name', required=True, help='Connection name')
@click.option('--type', 'connection_type', required=True, type=click.Choice(['source', 'target']), help='Connection type')
@click.option('--db-type', 'database_type', required=True, type=click.Choice(['postgresql', 'sqlserver']), help='Database type')
@click.option('--host', required=True, help='Database host')
@click.option('--port', required=True, type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', required=True, help='Database username')
@click.option('--password', required=True, help='Database password')
@click.option('--schema', help='Schema name (optional)')
def create_connection(name, connection_type, database_type, host, port, database, username, password, schema):
    """Create a new database connection."""
    try:
        connection = Connection(
            id=str(uuid.uuid4()),
            name=name,
            connection_type=connection_type,
            database_type=database_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema
        )
        
        cdc_manager.add_connection(connection)
        click.echo(f"✓ Connection created: {connection.id}")
        click.echo(f"  Name: {connection.name}")
        click.echo(f"  Type: {connection.connection_type}")
        click.echo(f"  Database: {connection.database_type}")
        
    except Exception as e:
        click.echo(f"✗ Failed to create connection: {e}", err=True)
        sys.exit(1)


@cdc.command()
def list_connections():
    """List all connections."""
    connections = cdc_manager.connection_store.values()
    
    if not connections:
        click.echo("No connections found.")
        return
    
    click.echo(f"\nFound {len(connections)} connections:\n")
    for conn in connections:
        click.echo(f"  ID: {conn.id}")
        click.echo(f"  Name: {conn.name}")
        click.echo(f"  Type: {conn.connection_type}")
        click.echo(f"  Database: {conn.database_type} @ {conn.host}:{conn.port}/{conn.database}")
        click.echo()


@cdc.command()
@click.option('--name', required=True, help='Pipeline name')
@click.option('--source-conn', 'source_connection_id', required=True, help='Source connection ID')
@click.option('--target-conn', 'target_connection_id', required=True, help='Target connection ID')
@click.option('--source-db', 'source_database', required=True, help='Source database name')
@click.option('--source-schema', required=True, help='Source schema name')
@click.option('--source-tables', required=True, help='Comma-separated list of source tables')
@click.option('--target-db', 'target_database', help='Target database name (optional)')
@click.option('--target-schema', help='Target schema name (optional)')
@click.option('--enable-full-load/--no-full-load', default=True, help='Enable full load')
def create_pipeline(name, source_connection_id, target_connection_id, source_database,
                    source_schema, source_tables, target_database, target_schema, enable_full_load):
    """Create a new CDC pipeline."""
    try:
        source_connection = cdc_manager.get_connection(source_connection_id)
        target_connection = cdc_manager.get_connection(target_connection_id)
        
        if not source_connection:
            click.echo(f"✗ Source connection not found: {source_connection_id}", err=True)
            sys.exit(1)
        
        if not target_connection:
            click.echo(f"✗ Target connection not found: {target_connection_id}", err=True)
            sys.exit(1)
        
        tables = [t.strip() for t in source_tables.split(',')]
        
        pipeline = Pipeline(
            id=str(uuid.uuid4()),
            name=name,
            source_connection_id=source_connection_id,
            target_connection_id=target_connection_id,
            source_database=source_database,
            source_schema=source_schema,
            source_tables=tables,
            target_database=target_database,
            target_schema=target_schema,
            enable_full_load=enable_full_load
        )
        
        created_pipeline = pipeline_service.create_pipeline(
            pipeline=pipeline,
            source_connection=source_connection,
            target_connection=target_connection
        )
        
        click.echo(f"✓ Pipeline created: {created_pipeline.id}")
        click.echo(f"  Name: {created_pipeline.name}")
        click.echo(f"  Source: {source_database}.{source_schema} ({len(tables)} tables)")
        click.echo(f"  Target: {target_database or target_connection.database}.{target_schema or target_connection.schema or 'public'}")
        click.echo(f"  Full Load: {'Enabled' if enable_full_load else 'Disabled'}")
        
    except Exception as e:
        click.echo(f"✗ Failed to create pipeline: {e}", err=True)
        sys.exit(1)


@cdc.command()
def list_pipelines():
    """List all pipelines."""
    pipelines = pipeline_service.list_pipelines()
    
    if not pipelines:
        click.echo("No pipelines found.")
        return
    
    click.echo(f"\nFound {len(pipelines)} pipelines:\n")
    for pipeline in pipelines:
        click.echo(f"  ID: {pipeline['id']}")
        click.echo(f"  Name: {pipeline['name']}")
        click.echo(f"  Status: {pipeline['status']}")
        click.echo(f"  Full Load: {pipeline['full_load_status']}")
        click.echo(f"  CDC: {pipeline['cdc_status']}")
        click.echo()


@cdc.command()
@click.argument('pipeline_id')
def start_pipeline(pipeline_id):
    """Start a pipeline."""
    try:
        click.echo(f"Starting pipeline: {pipeline_id}...")
        result = pipeline_service.start_pipeline(pipeline_id)
        
        click.echo(f"\n✓ Pipeline started successfully!")
        click.echo(f"  Status: {result.get('status')}")
        
        if result.get('full_load'):
            fl = result['full_load']
            if fl.get('success'):
                click.echo(f"  Full Load: Completed ({fl.get('total_rows', 0)} rows)")
            else:
                click.echo(f"  Full Load: Failed - {fl.get('error')}")
        
        if result.get('debezium_connector'):
            click.echo(f"  Debezium Connector: {result['debezium_connector'].get('name')} - {result['debezium_connector'].get('status')}")
        
        if result.get('sink_connector'):
            click.echo(f"  Sink Connector: {result['sink_connector'].get('name')} - {result['sink_connector'].get('status')}")
        
        if result.get('kafka_topics'):
            click.echo(f"  Kafka Topics: {len(result['kafka_topics'])} topics")
        
    except Exception as e:
        click.echo(f"✗ Failed to start pipeline: {e}", err=True)
        sys.exit(1)


@cdc.command()
@click.argument('pipeline_id')
def stop_pipeline(pipeline_id):
    """Stop a pipeline."""
    try:
        click.echo(f"Stopping pipeline: {pipeline_id}...")
        result = pipeline_service.stop_pipeline(pipeline_id)
        
        click.echo(f"\n✓ Pipeline stopped successfully!")
        click.echo(f"  Status: {result.get('status')}")
        
    except Exception as e:
        click.echo(f"✗ Failed to stop pipeline: {e}", err=True)
        sys.exit(1)


@cdc.command()
@click.argument('pipeline_id')
def pipeline_status(pipeline_id):
    """Get pipeline status."""
    try:
        status_info = pipeline_service.get_pipeline_status(pipeline_id)
        
        click.echo(f"\nPipeline Status: {status_info.get('name')}")
        click.echo(f"  ID: {pipeline_id}")
        click.echo(f"  Status: {status_info.get('status')}")
        click.echo(f"  Full Load: {status_info.get('full_load_status')}")
        click.echo(f"  CDC: {status_info.get('cdc_status')}")
        
        if status_info.get('debezium_connector'):
            dbz = status_info['debezium_connector']
            click.echo(f"  Debezium: {dbz.get('connector', {}).get('state', 'UNKNOWN')}")
        
        if status_info.get('sink_connector'):
            sink = status_info['sink_connector']
            click.echo(f"  Sink: {sink.get('connector', {}).get('state', 'UNKNOWN')}")
        
    except Exception as e:
        click.echo(f"✗ Failed to get pipeline status: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    if not CLICK_AVAILABLE:
        print("Error: click is not installed. Install it with: pip install click")
        sys.exit(1)
    cdc()




