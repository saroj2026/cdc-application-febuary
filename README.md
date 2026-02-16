<<<<<<< HEAD
# Real-Time CDC Pipeline System

## Overview

This is a **production-ready Change Data Capture (CDC) system** that automatically creates and manages real-time data replication pipelines between PostgreSQL and SQL Server using Kafka Connect, Debezium, and JDBC Sink Connectors.

## What This System Does

### 🎯 Core Functionality

**Automatically creates end-to-end CDC pipelines from database credentials:**

1. **Takes user credentials** (source PostgreSQL + target SQL Server)
2. **Automatically generates** all Kafka Connect configurations:
   - Debezium source connector configuration
   - JDBC Sink connector configuration
3. **Creates and deploys** connectors to Kafka Connect
4. **Starts real-time replication** - changes in PostgreSQL are automatically replicated to SQL Server

### ✨ Key Features

- **Zero Manual Configuration**: Just provide credentials, the system handles everything
- **Automatic Config Generation**: All Kafka Connect configs are auto-generated
- **Real-Time Replication**: Changes are captured and replicated within seconds
- **Full Load Support**: Optional initial data synchronization
- **Schema Auto-Creation**: Target tables are automatically created with correct schemas
- **Multi-Table Support**: Replicate multiple tables in a single pipeline
- **Production Ready**: Error handling, logging, and monitoring built-in

## Architecture

```
┌─────────────────┐
│  PostgreSQL     │  (Source Database)
│  (Source)       │
└────────┬────────┘
         │ Changes
         ▼
┌─────────────────┐
│   Debezium      │  (Captures changes via logical replication)
│   Connector     │
└────────┬────────┘
         │ Kafka Messages
         ▼
┌─────────────────┐
│   Kafka         │  (Message broker)
│   Topics        │
└────────┬────────┘
         │ Consume
         ▼
┌─────────────────┐
│   JDBC Sink     │  (Writes to target)
│   Connector     │
└────────┬────────┘
         │ Inserts/Updates
         ▼
┌─────────────────┐
│  SQL Server     │  (Target Database)
│  (Target)       │
└─────────────────┘
```

## How It Works

### 1. Pipeline Creation Flow

When you provide credentials, the system:

1. **Validates connections** to both databases
2. **Creates Connection objects** (source and target)
3. **Creates Pipeline object** with table mappings
4. **Generates Debezium config**:
   - Replication slot configuration
   - Publication settings
   - Table include lists
   - Snapshot mode
   - Schema settings
5. **Generates Sink config**:
   - JDBC connection URL
   - Table name mapping
   - Transform configurations (ExtractField for Debezium envelope)
   - Schema settings
6. **Deploys to Kafka Connect** via REST API
7. **Starts replication** automatically

### 2. Real-Time CDC Process

1. **Change Detection**: Debezium monitors PostgreSQL WAL (Write-Ahead Log)
2. **Change Capture**: Inserts, updates, deletes are captured
3. **Message Publishing**: Changes are published to Kafka topics
4. **Message Consumption**: JDBC Sink connector consumes messages
5. **Data Transformation**: ExtractField transform extracts actual data from Debezium envelope
6. **Target Write**: Data is inserted/updated in SQL Server

## Quick Start

### Prerequisites

1. **Docker Desktop** running
2. **Kafka Connect** running (via `docker-compose up -d`)
3. **PostgreSQL** configured for CDC:
   - `wal_level = logical`
   - Replication user with `REPLICATION` privilege
   - Publication created (or auto-created)
4. **SQL Server** accessible

### Create a Pipeline

#### Option 1: Using Python Script

```python
from create_cdc_pipeline import create_pipeline_from_credentials

result = create_pipeline_from_credentials(
    # Source PostgreSQL
    source_host="your-postgres-host",
    source_port=5432,
    source_database="your_database",
    source_schema="public",
    source_user="cdc_user",
    source_password="password",
    source_tables=["table1", "table2"],
    
    # Target SQL Server
    target_host="your-sqlserver-host",
    target_port=1433,
    target_database="target_db",
    target_schema="dbo",
    target_user="sa",
    target_password="password",
    
    # Optional
    enable_full_load=True  # For initial data sync
)
```

#### Option 2: Using the Example Script

Modify `start_cdc_docker_postgres_to_sqlserver.py` with your credentials and run:

```bash
python start_cdc_docker_postgres_to_sqlserver.py
```

### Verify Replication

```bash
python check_realtime_cdc.py
```

## System Components

### Core Modules

- **`ingestion/cdc_manager.py`**: Main pipeline orchestration
- **`ingestion/debezium_config.py`**: Auto-generates Debezium configurations
- **`ingestion/sink_config.py`**: Auto-generates JDBC Sink configurations
- **`ingestion/kafka_connect_client.py`**: Kafka Connect REST API client
- **`ingestion/models.py`**: Data models (Connection, Pipeline)
- **`ingestion/transfer.py`**: Full load data transfer

### Key Scripts

- **`create_cdc_pipeline.py`**: Main user-facing interface
- **`start_cdc_docker_postgres_to_sqlserver.py`**: Example pipeline creation
- **`check_realtime_cdc.py`**: Verify replication status
- **`check_replication.py`**: Compare source and target data
- **`check_debezium_slot.py`**: Check Debezium replication slot status

## Automatic Configuration Generation

### ✅ Yes, It Automatically Creates Kafka Configs!

When you provide credentials, the system **automatically generates**:

1. **Debezium Source Connector Config**:
   - Database connection details
   - Replication slot name
   - Publication configuration
   - Table include/exclude lists
   - Snapshot mode
   - Schema converter settings
   - Error handling

2. **JDBC Sink Connector Config**:
   - Target database connection URL
   - Table name mapping
   - Transform configurations (ExtractField)
   - Schema converter settings
   - Batch size and error handling
   - Auto-create and auto-evolve settings

**No manual Kafka configuration needed!** Just provide database credentials.

## Configuration Details

### PostgreSQL Requirements

- **WAL Level**: `logical` (required for CDC)
- **Replication Slots**: Automatically created by Debezium
- **Publication**: Can be auto-created or use existing
- **User Permissions**: 
  - `REPLICATION` privilege
  - Access to target database
  - Access to target tables

### SQL Server Requirements

- **Database**: Must exist
- **Schema**: Defaults to `dbo` if not specified
- **Tables**: Auto-created by sink connector with correct schema
- **User Permissions**: 
  - `CREATE TABLE` permission
  - `INSERT`, `UPDATE`, `DELETE` permissions

## Pipeline Lifecycle

1. **CREATE**: Pipeline object created with connections
2. **START**: 
   - Full load (optional)
   - Debezium connector created
   - Sink connector created
   - Replication starts
3. **RUNNING**: Real-time CDC active
4. **STOP**: Connectors paused/stopped
5. **DELETE**: Connectors removed

## Monitoring

### Check Pipeline Status

```python
from ingestion.cdc_manager import CDCManager

cdc_manager = CDCManager()
status = cdc_manager.get_pipeline_status(pipeline_id)
```

### Check Replication

```bash
# Check data replication
python check_realtime_cdc.py

# Check Debezium slot
python check_debezium_slot.py

# Check Kafka consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group <group-name> --describe
```

## Troubleshooting

### Common Issues

1. **Slot INACTIVE**: 
   - Check Debezium connector status
   - Verify `snapshot.mode` is set to `never` for streaming
   - Restart Debezium connector

2. **No data replicating**:
   - Check sink connector status
   - Verify table schema matches
   - Check Kafka consumer lag

3. **Connection errors**:
   - Verify network connectivity
   - Check firewall rules
   - Verify credentials

## API Usage

The system can be used programmatically:

```python
from ingestion.cdc_manager import CDCManager
from ingestion.models import Connection, Pipeline

# Initialize
cdc_manager = CDCManager(kafka_connect_url="http://localhost:8083")

# Create connections
source_conn = Connection(...)
target_conn = Connection(...)

# Create pipeline
pipeline = Pipeline(...)
created = cdc_manager.create_pipeline(pipeline, source_conn, target_conn)

# Start pipeline (auto-generates all configs)
result = cdc_manager.start_pipeline(created.id)
```

## Product Vision

This system is designed as a **product** where:

1. **User provides credentials** (PostgreSQL + SQL Server)
2. **System automatically**:
   - Validates connections
   - Creates pipeline
   - Generates all Kafka configurations
   - Deploys connectors
   - Starts real-time CDC
3. **User gets** real-time data replication with zero manual configuration

**No Kafka knowledge required!** Just provide database credentials and the system handles everything.

## File Structure

```
.
├── ingestion/              # Core CDC system
│   ├── cdc_manager.py     # Pipeline orchestration
│   ├── debezium_config.py # Debezium config generator
│   ├── sink_config.py      # Sink config generator
│   ├── kafka_connect_client.py # Kafka Connect API
│   ├── models.py          # Data models
│   └── connectors/        # Database connectors
├── create_cdc_pipeline.py # Main user interface
├── start_cdc_docker_postgres_to_sqlserver.py # Example
├── check_realtime_cdc.py  # Verification script
├── docker-compose.yml      # Kafka infrastructure
└── README.md              # This file
```

## Next Steps

To make this a complete product:

1. **Add REST API** (FastAPI) for web interface
2. **Add database persistence** for pipelines and connections
3. **Add user authentication** and multi-tenancy
4. **Add web UI** for pipeline management
5. **Add monitoring dashboard** for pipeline health
6. **Add alerting** for failures

The core functionality is **already production-ready** - it automatically creates all Kafka configurations from user credentials!
=======
# seg-cdc-application
seg-cdc
>>>>>>> 6724e7273eadfd3b3e851553a519923f25c95479
